//! Startup probe: does reading a file move its atime on this mount?
//!
//! ```text
//!   create probe ─► set atime 2 days back ─► statx ─► read it ─► statx ─► unlink
//!                                            before               after
//! ```

use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use crate::fsops;

const PROBE_LEN: usize = 4096;
const BACKDATE: Duration = Duration::from_secs(2 * 24 * 3600);
const BACKDATE_TOLERANCE: Duration = Duration::from_secs(3600);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AtimeBehavior {
    /// Reads move atime: hot protection and LRU order follow last access.
    Tracked,
    /// atime stays put (or cannot be set): hot means recently written and
    /// eviction is oldest-written-first.
    Untracked,
}

impl AtimeBehavior {
    fn classify(backdated: SystemTime, before: SystemTime, after: SystemTime) -> Self {
        let took_backdate = before <= backdated + BACKDATE_TOLERANCE;
        if took_backdate && after > before {
            Self::Tracked
        } else {
            Self::Untracked
        }
    }
}

/// Removes the probe file however the probe ends.
struct ProbeFile(PathBuf);

impl Drop for ProbeFile {
    fn drop(&mut self) {
        if let Err(e) = fs::remove_file(&self.0) {
            tracing::warn!(path = %self.0.display(), error = %e, "could not remove atime probe file");
        }
    }
}

fn read_buffered(path: &Path) -> io::Result<()> {
    fs::read(path).map(drop)
}

#[cfg(any(target_os = "linux", target_os = "android"))]
fn read_once(path: &Path) -> io::Result<()> {
    use rustix::fs::{Mode, OFlags};
    use rustix::io::Errno;

    let flags = OFlags::RDONLY | OFlags::DIRECT | OFlags::CLOEXEC;
    let fd = match rustix::fs::open(path, flags, Mode::empty()) {
        Ok(fd) => fd,
        Err(Errno::INVAL) => return read_buffered(path),
        Err(e) => return Err(e.into()),
    };
    let mut buf = vec![0u8; 2 * PROBE_LEN];
    let offset = buf.as_ptr().align_offset(PROBE_LEN);
    let aligned = buf
        .get_mut(offset..offset + PROBE_LEN)
        .ok_or_else(|| io::Error::other("cannot align O_DIRECT buffer"))?;
    match rustix::io::read(&fd, aligned) {
        Ok(_) => Ok(()),
        Err(Errno::INVAL) => read_buffered(path),
        Err(e) => Err(e.into()),
    }
}

#[cfg(not(any(target_os = "linux", target_os = "android")))]
fn read_once(path: &Path) -> io::Result<()> {
    read_buffered(path)
}

/// Creates a probe file in `dir`, reads it with a back-dated atime and reports
/// whether the read moved atime. The probe file is removed before returning.
pub fn probe(dir: &Path) -> io::Result<AtimeBehavior> {
    let path = dir.join(format!(".kvreap-atime-probe-{}", std::process::id()));
    let mut file = fs::File::create_new(&path)?;
    let guard = ProbeFile(path);
    let backdated = SystemTime::now() - BACKDATE;
    file.write_all(&[0u8; PROBE_LEN])?;
    file.set_times(
        fs::FileTimes::new()
            .set_accessed(backdated)
            .set_modified(backdated),
    )?;
    file.sync_all()?;
    drop(file);
    let before = fsops::stat_path_synced(&guard.0)?.atime;
    read_once(&guard.0)?;
    let after = fsops::stat_path_synced(&guard.0)?.atime;
    Ok(AtimeBehavior::classify(backdated, before, after))
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::time::{Duration, SystemTime};

    use crate::atime::{AtimeBehavior, BACKDATE, probe};

    #[test]
    fn classify_needs_the_backdate_to_stick_and_the_read_to_move_atime() {
        let now = SystemTime::now();
        let backdated = now - BACKDATE;
        assert_eq!(
            AtimeBehavior::classify(backdated, backdated, now),
            AtimeBehavior::Tracked
        );
        assert_eq!(
            AtimeBehavior::classify(backdated, backdated, backdated),
            AtimeBehavior::Untracked,
            "read did not move atime"
        );
        assert_eq!(
            AtimeBehavior::classify(backdated, now, now + Duration::from_secs(1)),
            AtimeBehavior::Untracked,
            "atime could not be back-dated"
        );
        assert_eq!(
            AtimeBehavior::classify(
                backdated,
                backdated + Duration::from_secs(1),
                backdated + Duration::from_secs(2)
            ),
            AtimeBehavior::Tracked,
            "timestamp granularity rounding is fine"
        );
    }

    #[test]
    fn probe_reports_and_cleans_up() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let got = probe(tmp.path()).expect("probe");
        // Linux defaults to relatime, which updates an atime older than 24 hours.
        if cfg!(target_os = "linux") {
            assert_eq!(got, AtimeBehavior::Tracked);
        }
        assert_eq!(
            fs::read_dir(tmp.path()).expect("read_dir").count(),
            0,
            "probe file left behind"
        );
    }

    #[test]
    fn probe_in_missing_dir_fails() {
        let tmp = tempfile::tempdir().expect("tempdir");
        assert!(probe(&tmp.path().join("missing")).is_err());
    }
}
