use std::ffi::{CStr, CString};
use std::io;
use std::os::fd::{AsFd, OwnedFd};
use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rustix::fs::{AtFlags, Dir, FileType, Mode, OFlags};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntryKind {
    Dir,
    File,
    Other,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    pub name: CString,
    pub kind: EntryKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Meta {
    pub atime: SystemTime,
    pub mtime: SystemTime,
    pub size: u64,
    pub kind: EntryKind,
}

pub fn open_dir(path: &Path) -> io::Result<OwnedFd> {
    Ok(rustix::fs::open(
        path,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC | OFlags::NOFOLLOW,
        Mode::empty(),
    )?)
}

pub fn open_dir_at(parent: impl AsFd, name: &CStr) -> io::Result<OwnedFd> {
    Ok(rustix::fs::openat(
        parent,
        name,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC | OFlags::NOFOLLOW,
        Mode::empty(),
    )?)
}

/// Lists `dir`, skipping `.` and `..`. Entry types come from `d_type`; when the
/// filesystem reports `DT_UNKNOWN` the entry is stat'ed to resolve it.
pub fn list(dir: impl AsFd) -> io::Result<Vec<Entry>> {
    let dir_fd = dir.as_fd();
    let mut out = Vec::new();
    for entry in Dir::read_from(dir_fd)? {
        let entry = entry?;
        let name = entry.file_name();
        if name == c"." || name == c".." {
            continue;
        }
        let kind = match entry.file_type() {
            FileType::Directory => EntryKind::Dir,
            FileType::RegularFile => EntryKind::File,
            FileType::Unknown => match stat_at(dir_fd, name) {
                Ok(meta) => meta.kind,
                Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
                Err(e) => return Err(e),
            },
            _ => EntryKind::Other,
        };
        out.push(Entry {
            name: name.to_owned(),
            kind,
        });
    }
    Ok(out)
}

fn kind_from_mode(mode: u32) -> EntryKind {
    match FileType::from_raw_mode(mode as _) {
        FileType::Directory => EntryKind::Dir,
        FileType::RegularFile => EntryKind::File,
        _ => EntryKind::Other,
    }
}

fn to_system_time(secs: i64, nanos: u32) -> SystemTime {
    if secs >= 0 {
        UNIX_EPOCH + Duration::new(secs.unsigned_abs(), nanos)
    } else {
        UNIX_EPOCH - Duration::from_secs(secs.unsigned_abs())
            + Duration::from_nanos(u64::from(nanos))
    }
}

/// Whether a stat may be answered from cached attributes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Freshness {
    Cached,
    Synced,
}

#[cfg(any(target_os = "linux", target_os = "android"))]
fn stat_at_with(dir: impl AsFd, name: &CStr, freshness: Freshness) -> io::Result<Meta> {
    use rustix::fs::StatxFlags;
    let sync = match freshness {
        Freshness::Cached => AtFlags::STATX_DONT_SYNC,
        Freshness::Synced => AtFlags::STATX_FORCE_SYNC,
    };
    let st = rustix::fs::statx(
        dir,
        name,
        AtFlags::SYMLINK_NOFOLLOW | sync,
        StatxFlags::TYPE | StatxFlags::SIZE | StatxFlags::ATIME | StatxFlags::MTIME,
    )?;
    Ok(Meta {
        atime: to_system_time(st.stx_atime.tv_sec, st.stx_atime.tv_nsec),
        mtime: to_system_time(st.stx_mtime.tv_sec, st.stx_mtime.tv_nsec),
        size: st.stx_size,
        kind: kind_from_mode(u32::from(st.stx_mode)),
    })
}

#[cfg(not(any(target_os = "linux", target_os = "android")))]
fn stat_at_with(dir: impl AsFd, name: &CStr, _freshness: Freshness) -> io::Result<Meta> {
    let st = rustix::fs::statat(dir, name, AtFlags::SYMLINK_NOFOLLOW)?;
    let nanos = |n: i64| u32::try_from(n).unwrap_or(0);
    Ok(Meta {
        atime: to_system_time(st.st_atime, nanos(st.st_atime_nsec)),
        mtime: to_system_time(st.st_mtime, nanos(st.st_mtime_nsec)),
        size: u64::try_from(st.st_size).unwrap_or(0),
        kind: kind_from_mode(u32::from(st.st_mode)),
    })
}

/// `statx(AT_STATX_DONT_SYNC)` relative to `dir`, without following symlinks.
pub fn stat_at(dir: impl AsFd, name: &CStr) -> io::Result<Meta> {
    stat_at_with(dir, name, Freshness::Cached)
}

pub fn unlink_at(dir: impl AsFd, name: &CStr) -> io::Result<()> {
    Ok(rustix::fs::unlinkat(dir, name, AtFlags::empty())?)
}

/// Removes an empty directory; fails with `DirectoryNotEmpty` otherwise.
pub fn rmdir_at(dir: impl AsFd, name: &CStr) -> io::Result<()> {
    Ok(rustix::fs::unlinkat(dir, name, AtFlags::REMOVEDIR)?)
}

fn c_path(path: &Path) -> io::Result<CString> {
    CString::new(path.as_os_str().as_encoded_bytes())
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))
}

pub fn stat_path(path: &Path) -> io::Result<Meta> {
    stat_at(rustix::fs::CWD, &c_path(path)?)
}

/// `statx(AT_STATX_FORCE_SYNC)`: attributes as the server has them, not as cached.
pub fn stat_path_synced(path: &Path) -> io::Result<Meta> {
    stat_at_with(rustix::fs::CWD, &c_path(path)?, Freshness::Synced)
}

pub fn unlink_path(path: &Path) -> io::Result<()> {
    unlink_at(rustix::fs::CWD, &c_path(path)?)
}

pub fn rmdir_path(path: &Path) -> io::Result<()> {
    rmdir_at(rustix::fs::CWD, &c_path(path)?)
}

pub fn is_not_found(e: &io::Error) -> bool {
    e.kind() == io::ErrorKind::NotFound
}

pub fn is_not_empty(e: &io::Error) -> bool {
    e.kind() == io::ErrorKind::DirectoryNotEmpty
        || e.raw_os_error() == Some(rustix::io::Errno::EXIST.raw_os_error())
}

#[cfg(test)]
mod tests {
    use std::ffi::CString;
    use std::fs;
    use std::time::{Duration, SystemTime};

    use crate::fsops::{
        EntryKind, is_not_empty, is_not_found, list, open_dir, rmdir_at, stat_at, stat_path_synced,
        unlink_at,
    };

    fn c(s: &str) -> CString {
        CString::new(s).expect("no nul")
    }

    #[test]
    fn list_reports_files_and_dirs() {
        let tmp = tempfile::tempdir().expect("tempdir");
        fs::create_dir(tmp.path().join("sub")).expect("mkdir");
        fs::write(tmp.path().join("a.bin"), b"x").expect("write");
        std::os::unix::fs::symlink("a.bin", tmp.path().join("link")).expect("symlink");

        let fd = open_dir(tmp.path()).expect("open");
        let mut entries = list(&fd).expect("list");
        entries.sort_by(|a, b| a.name.cmp(&b.name));
        let got: Vec<_> = entries
            .iter()
            .map(|e| (e.name.to_str().expect("utf8"), e.kind))
            .collect();
        assert_eq!(
            got,
            vec![
                ("a.bin", EntryKind::File),
                ("link", EntryKind::Other),
                ("sub", EntryKind::Dir)
            ]
        );
    }

    #[test]
    fn stat_at_reads_size_and_times() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let path = tmp.path().join("f.bin");
        fs::write(&path, vec![0u8; 1234]).expect("write");
        let old = SystemTime::now() - Duration::from_secs(7200);
        fs::File::options()
            .write(true)
            .open(&path)
            .and_then(|f| f.set_times(fs::FileTimes::new().set_accessed(old).set_modified(old)))
            .expect("set times");

        let fd = open_dir(tmp.path()).expect("open");
        let meta = stat_at(&fd, &c("f.bin")).expect("stat");
        assert_eq!(meta.size, 1234);
        assert_eq!(meta.kind, EntryKind::File);
        let skew = |t: SystemTime| t.duration_since(old).unwrap_or_else(|e| e.duration());
        assert!(skew(meta.atime) < Duration::from_secs(1));
        assert!(skew(meta.mtime) < Duration::from_secs(1));
        assert_eq!(stat_path_synced(&path).expect("synced stat"), meta);
    }

    #[test]
    fn stat_at_does_not_follow_symlinks() {
        let tmp = tempfile::tempdir().expect("tempdir");
        fs::create_dir(tmp.path().join("d")).expect("mkdir");
        std::os::unix::fs::symlink("d", tmp.path().join("link")).expect("symlink");
        let fd = open_dir(tmp.path()).expect("open");
        assert_eq!(
            stat_at(&fd, &c("link")).expect("stat").kind,
            EntryKind::Other
        );
    }

    #[test]
    fn missing_entries_are_not_found() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let fd = open_dir(tmp.path()).expect("open");
        assert!(stat_at(&fd, &c("nope")).is_err_and(|e| is_not_found(&e)));
        assert!(unlink_at(&fd, &c("nope")).is_err_and(|e| is_not_found(&e)));
    }

    #[test]
    fn unlink_and_rmdir() {
        let tmp = tempfile::tempdir().expect("tempdir");
        fs::create_dir(tmp.path().join("leaf")).expect("mkdir");
        fs::write(tmp.path().join("leaf/f.bin"), b"x").expect("write");
        let fd = open_dir(tmp.path()).expect("open");

        let err = rmdir_at(&fd, &c("leaf")).expect_err("non-empty");
        assert!(is_not_empty(&err), "{err:?}");

        let leaf = open_dir(&tmp.path().join("leaf")).expect("open leaf");
        unlink_at(&leaf, &c("f.bin")).expect("unlink");
        rmdir_at(&fd, &c("leaf")).expect("rmdir");
        assert!(!tmp.path().join("leaf").exists());
    }
}
