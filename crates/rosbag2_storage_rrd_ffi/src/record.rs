//! The write path: ROS 2 messages in, Rerun chunks out.
//!
//! Each topic feeds its messages to its [`Pipeline`] and, when its flush policy says so,
//! takes them back out as a chunk per representation the recording asked for. Everything
//! about *what* is written lives in the pipeline; this module only decides *when*.

use std::sync::Arc;
use std::time::Instant;

use re_lenses::Runtime;
use rerun::EntityPath;

use crate::config::{FlushPolicy, RecordingConfig, format_representations};
use crate::pipeline::{LensSet, Pipeline};
use crate::statics;
use crate::writer::Recording;

/// Everything buffered for one topic between flushes, and how it becomes chunks.
pub struct Topic {
    entity: EntityPath,
    pipeline: Pipeline,

    policy: FlushPolicy,
    last_flush: Instant,

    /// Set by [`Self::retire`]; a retired topic quietly drops anything still arriving.
    retired: bool,
}

impl Topic {
    /// Prepares a topic and records what it is as static data.
    // Mirrors the fields rosbag2's `create_topic` hands across the FFI, one by one.
    #[expect(clippy::too_many_arguments)]
    pub fn new(
        rec: &Recording,
        config: &RecordingConfig,
        lenses: Option<Arc<LensSet>>,
        runtime: Arc<Runtime>,
        topic: &str,
        type_name: &str,
        schema_encoding: &str,
        schema_text: &[u8],
        qos_profiles: &str,
        type_hash: &str,
    ) -> Self {
        let entity = EntityPath::from(topic);

        let pipeline = Pipeline::new(
            entity.clone(),
            &config.representations,
            type_name,
            schema_encoding,
            schema_text,
            lenses,
            runtime,
        );

        let record = statics::TopicRecord {
            topic,
            type_name,
            schema_encoding,
            schema_text,
            qos_profiles,
            type_hash,
        };
        match statics::topic_info(&entity, &record) {
            Ok(chunk) => rec.send_chunk(chunk),
            Err(err) => log::error!("Failed to record what topic '{topic}' is: {err:#}"),
        }

        // What is stored can differ from what was asked for, so say which it is.
        log::info!(
            "Topic '{topic}' ({type_name}) is stored as [{}]",
            format_representations(pipeline.representations())
        );

        // Both are what was asked for, but quiet enough to be mistaken for a bug, so say so.
        if pipeline.records_nothing() {
            log::warn!(
                "Topic '{topic}' records nothing: no lens matches it and this recording asks \
                 for 'unmatched: drop', so its messages are dropped"
            );
        } else if !pipeline.is_replayable() {
            log::warn!(
                "Topic '{topic}' is recorded as lenses only; the bag will open in Rerun but \
                 this topic cannot be replayed"
            );
        }

        Self {
            entity,
            pipeline,
            policy: config.flush,
            last_flush: Instant::now(),
            retired: false,
        }
    }

    /// Stops accepting messages, after `remove_topic`.
    ///
    /// The slot stays so that indices already handed out remain valid.
    pub fn retire(&mut self) {
        self.retired = true;
    }

    /// Buffers one message, flushing first if that would exceed the policy.
    pub fn write(
        &mut self,
        rec: &Recording,
        cdr: &[u8],
        recv_timestamp: i64,
        send_timestamp: i64,
        sequence: i64,
    ) -> anyhow::Result<()> {
        if self.retired {
            return Ok(());
        }

        self.pipeline
            .push(cdr, recv_timestamp, send_timestamp, sequence);
        if self.should_flush() {
            self.flush(rec)?;
        }
        Ok(())
    }

    fn should_flush(&self) -> bool {
        let rows = self.pipeline.len();
        if rows == 0 {
            return false;
        }

        // Ordered cheapest-first and short-circuited: reading the clock is the only part
        // that costs anything, and at a million messages a second it is not worth paying
        // when a counter has already decided the answer.
        (self.policy.max_rows > 0 && rows >= self.policy.max_rows)
            || (self.policy.max_bytes > 0 && self.pipeline.bytes() >= self.policy.max_bytes)
            || self
                .policy
                .max_latency
                .is_some_and(|max| self.last_flush.elapsed() >= max)
    }

    /// Flushes only if buffered rows have waited past the latency limit.
    ///
    /// Called from the writer's sweep, so a topic that has gone quiet still honours the
    /// limit its own writes would have enforced.
    pub fn flush_if_stale(&mut self, rec: &Recording) -> anyhow::Result<()> {
        let stale = self
            .policy
            .max_latency
            .is_some_and(|max| self.last_flush.elapsed() >= max);
        if stale && !self.pipeline.is_empty() {
            self.flush(rec)?;
        }
        Ok(())
    }

    /// Hands every buffered row to the recording stream.
    pub fn flush(&mut self, rec: &Recording) -> anyhow::Result<()> {
        // Reset the clock even when there is nothing to send, so an idle topic does not
        // re-check the latency limit on every message.
        self.last_flush = Instant::now();

        for chunk in self.pipeline.flush()? {
            rec.send_chunk(chunk);
        }
        Ok(())
    }

    pub fn entity(&self) -> &EntityPath {
        &self.entity
    }
}
