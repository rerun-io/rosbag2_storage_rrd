# Release Checklist

* [ ] Draft the changelog with `./scripts/generate_changelog.py --version 0.NEW.VERSION`, then edit it into `CHANGELOG.md`.
* [ ] Bump the version in all four places: `crates/rosbag2_storage_rrd_ffi/Cargo.toml`, `package.xml`, `pixi.toml`, `pyproject.toml`. Run `cargo check` to update `Cargo.lock`.
* [ ] `git commit -m 'Release 0.x.0 - summary'`
* [ ] `git tag -a 0.x.0 -m 'Release 0.x.0 - summary'`
* [ ] `git pull --tags ; git tag -d latest && git tag -a latest -m 'Latest release' && git push --tags origin latest --force ; git push --tags`
* [ ] Do a GitHub release: https://github.com/rerun-io/rosbag2_storage_rrd/releases/new

The Rust crate is a staticlib linked into the plugin; nothing is published to crates.io.
