//! The recording a bag is being written into, and the topics it holds.

use std::collections::HashMap;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use anyhow::Context as _;
use re_lenses::Runtime;
use rerun::archetypes::TextDocument;
use rerun::external::re_log_types::RecordingId;
use rerun::external::re_uri::ProxyUri;
use rerun::log::Chunk;
use rerun::sink::{GrpcSink, GrpcSinkConnectionState};
use rerun::{AsComponents, RecordingStream, RecordingStreamResult};

use crate::config::RecordingConfig;
use crate::pipeline::{self, LensSet};
use crate::record::Topic;
use crate::statics::METADATA_PROPERTY;

/// The streams a bag is recorded into: the file, and optionally a running Rerun server.
///
/// Two streams rather than one stream with two sinks: sinks on a stream share its batcher,
/// so a server that stops answering would report as the bag's own error and could hold up
/// its writes. Kept apart, the file's result is the bag's result and the server's is a
/// warning. Every chunk goes to both, so what was watched live is what is on disk.
pub struct Recording {
    file: RecordingStream,
    live: Option<RecordingStream>,
}

impl Recording {
    /// How long a server named in the config gets to accept the connection.
    const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

    fn open(path: &str, config: &RecordingConfig) -> anyhow::Result<Self> {
        // We decide chunk boundaries ourselves, so the SDK's batcher only needs to pass
        // them along. Left at its default it would hold a finished chunk for another
        // 200ms, which would defeat the low-latency presets.
        let batcher = rerun::log::ChunkBatcherConfig {
            flush_tick: config
                .flush
                .max_latency
                .unwrap_or(Duration::from_millis(200)),
            ..rerun::log::ChunkBatcherConfig::DEFAULT
        };
        // Chosen here rather than left to each builder, so both streams share it.
        let recording_id = config
            .recording_id
            .as_deref()
            .map_or_else(RecordingId::random, RecordingId::from);
        let builder = || {
            rerun::RecordingStreamBuilder::new("rosbag2_storage_rrd")
                .batcher_config(batcher)
                .recording_id(recording_id.clone())
        };

        let file = builder()
            .save(path)
            .with_context(|| format!("failed to open '{path}' for writing"))?;
        let live = match &config.server_uri {
            Some(uri) => Some(
                builder()
                    .set_sinks((Self::connect(uri)?,))
                    .with_context(|| format!("failed to stream to the Rerun server at '{uri}'"))?,
            ),
            None => None,
        };

        Ok(Self { file, live })
    }

    /// Connects to the server, or says why not.
    ///
    /// The SDK's client queues at most a hundred messages until it has connected and never
    /// gives up trying, so a sink that never connects would eventually hold the bag's writes
    /// too. Connecting before anything is recorded turns that into an error at open.
    fn connect(uri: &ProxyUri) -> anyhow::Result<GrpcSink> {
        let sink = GrpcSink::new(uri.clone());
        let started = Instant::now();
        loop {
            match sink.status() {
                GrpcSinkConnectionState::Connected => return Ok(sink),
                GrpcSinkConnectionState::Disconnected(result) => {
                    let reason = result.map_or_else(
                        |err| err.to_string(),
                        |()| "the connection was closed".to_owned(),
                    );
                    anyhow::bail!("could not connect to the Rerun server at '{uri}': {reason}");
                }
                GrpcSinkConnectionState::Connecting { .. } => {
                    anyhow::ensure!(
                        started.elapsed() < Self::CONNECT_TIMEOUT,
                        "no Rerun server answered at '{uri}' within {:?}; start `rerun` (or \
                         `rerun server`) first, or remove server_uri from the storage config",
                        Self::CONNECT_TIMEOUT
                    );
                    thread::sleep(Duration::from_millis(50));
                }
            }
        }
    }

    pub fn send_chunk(&self, chunk: Chunk) {
        if let Some(live) = &self.live {
            live.send_chunk(chunk.clone());
        }
        self.file.send_chunk(chunk);
    }

    /// Records a recording property; the file's result is returned, the server's is logged.
    pub fn send_property(
        &self,
        name: &str,
        values: &impl AsComponents,
    ) -> RecordingStreamResult<()> {
        if let Some(live) = &self.live
            && let Err(err) = live.send_property(name, values)
        {
            log::warn!("Failed to stream the '{name}' property to the Rerun server: {err}");
        }
        self.file.send_property(name, values)
    }

    /// Flushes both streams and waits for the file to be complete.
    ///
    /// The server gets a bounded wait and only a warning: a viewer closed mid-recording must
    /// not fail the bag, and once disconnected the SDK reports rather than waits.
    fn close(self) -> anyhow::Result<()> {
        if let Some(live) = &self.live
            && let Err(err) = live.flush_with_timeout(Self::CONNECT_TIMEOUT)
        {
            log::warn!("Failed to flush the live stream to the Rerun server: {err}");
        }
        self.file
            .flush_blocking()
            .context("failed to flush the recording")
    }
}

/// One open bag being written.
pub struct Writer {
    rec: Recording,
    config: RecordingConfig,

    /// The ROS 2 lenses, built once and shared by every topic; `None` unless asked for.
    lenses: Option<Arc<LensSet>>,
    runtime: Arc<Runtime>,

    /// Topics in creation order. The C++ side addresses them by index, so recording a
    /// message costs no string hashing and no UTF-8 validation.
    topics: Vec<Topic>,

    /// Only consulted by `create_topic` and `remove_topic`, never per message.
    topic_ids: HashMap<String, usize>,

    /// Write order across the whole bag, not per topic.
    next_sequence: i64,

    /// When quiet topics were last checked for stranded rows; see
    /// [`Self::sweep_stale_topics`].
    last_sweep: Instant,

    /// Set by `update_metadata` and written at close, when the message counts and time
    /// range rosbag2 puts in it are final.
    metadata_yaml: Option<String>,
}

impl Writer {
    /// Opens `path` for writing.
    pub fn open(path: &str, config: RecordingConfig) -> anyhow::Result<Self> {
        let lenses = pipeline::lenses_for(&config.representations, config.unmatched)?.map(Arc::new);
        let rec = Recording::open(path, &config)?;

        Ok(Self {
            rec,
            config,
            lenses,
            runtime: re_lenses::default_runtime(),
            topics: Vec::new(),
            topic_ids: HashMap::new(),
            next_sequence: 0,
            last_sweep: Instant::now(),
            metadata_yaml: None,
        })
    }

    /// Declares a topic.
    pub fn create_topic(
        &mut self,
        topic: &str,
        type_name: &str,
        schema_encoding: &str,
        schema_text: &[u8],
        qos_profiles: &str,
        type_hash: &str,
    ) -> usize {
        if let Some(&id) = self.topic_ids.get(topic) {
            return id;
        }

        let id = self.topics.len();
        self.topic_ids.insert(topic.to_owned(), id);
        self.topics.push(Topic::new(
            &self.rec,
            &self.config,
            self.lenses.clone(),
            Arc::clone(&self.runtime),
            topic,
            type_name,
            schema_encoding,
            schema_text,
            qos_profiles,
            type_hash,
        ));

        id
    }

    /// Records one serialized message.
    pub fn write(
        &mut self,
        topic: usize,
        cdr: &[u8],
        recv_timestamp: i64,
        send_timestamp: i64,
    ) -> anyhow::Result<()> {
        let sequence = self.next_sequence;
        self.next_sequence += 1;

        self.topics
            .get_mut(topic)
            .with_context(|| format!("no topic with index {topic}"))?
            .write(&self.rec, cdr, recv_timestamp, send_timestamp, sequence)?;

        self.sweep_stale_topics()
    }

    /// Flushes rows stranded on topics that have gone quiet.
    ///
    /// A topic checks its latency limit only when one of its own messages arrives, so a
    /// topic that stops publishing would otherwise hold its last rows until close. Any
    /// topic's traffic sweeps the others instead, at most once per latency window; the
    /// worst case for an idle topic is about twice `max_latency`. A fully quiet
    /// recording still holds its tail until close.
    fn sweep_stale_topics(&mut self) -> anyhow::Result<()> {
        let Some(max_latency) = self.config.flush.max_latency else {
            return Ok(());
        };
        if self.last_sweep.elapsed() < max_latency {
            return Ok(());
        }
        self.last_sweep = Instant::now();

        for topic in &mut self.topics {
            topic.flush_if_stale(&self.rec)?;
        }
        Ok(())
    }

    /// Flushes a topic and stops recording it.
    ///
    /// The slot is kept so that indices already handed out stay valid.
    pub fn remove_topic(&mut self, topic: &str) -> anyhow::Result<()> {
        if let Some(id) = self.topic_ids.remove(topic)
            && let Some(topic) = self.topics.get_mut(id)
        {
            topic.flush(&self.rec)?;
            topic.retire();
        }
        Ok(())
    }

    /// Stores the bag metadata to be written at close.
    pub fn set_metadata(&mut self, yaml: &str) {
        self.metadata_yaml = Some(yaml.to_owned());
    }

    /// Flushes every topic, writes the metadata, and closes the recording.
    pub fn close(mut self) -> anyhow::Result<()> {
        for topic in &mut self.topics {
            if let Err(err) = topic.flush(&self.rec) {
                log::error!("Failed to flush topic '{}': {err:#}", topic.entity());
            }
        }

        if let Some(yaml) = &self.metadata_yaml
            && let Err(err) = self
                .rec
                .send_property(METADATA_PROPERTY, &TextDocument::new(yaml.clone()))
        {
            log::error!("Failed to record bag metadata: {err}");
        }

        self.rec.close()
    }
}
