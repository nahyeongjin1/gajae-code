//! Portable descriptor-relative authority for the chat-daemon command channel.
//!
//! The channel exchanges request/response/settlement documents inside a
//! directory that lives under the agent directory. Those files are explicitly
//! untrusted: any process able to write the parent may rename the directory
//! aside and leave a symlink, a replacement directory, or a special file at the
//! pathname. A `lstat` precheck followed by an ordinary pathname syscall cannot
//! defend against that, because the pathname is re-resolved by the kernel on
//! the second call.
//!
//! [`open_retained_command_dir`] therefore walks the absolute path one
//! component at a time with `openat(O_DIRECTORY|O_NOFOLLOW|O_CLOEXEC)`, proves
//! each opened component against the `fstatat(AT_SYMLINK_NOFOLLOW)` it was
//! selected by, and *retains* the final directory descriptor. Every later
//! operation is a `*at` syscall relative to that descriptor, so replacing the
//! pathname afterwards is irrelevant: the retained identity keeps receiving the
//! work, and nothing can be redirected outside the managed root.
//!
//! `recovery_fs` provides the same shape for Linux recovery artifacts, but it
//! is gated to Linux. This module is `cfg(unix)`, which is what the chat daemon
//! needs on macOS, and fails closed with `unsupported_platform` elsewhere
//! rather than silently downgrading to pathname operations.
//!
//! # Supported storage
//!
//! Every guarantee here — `flock` mutual exclusion over the directory inode,
//! `fsync` on the directory as a publication barrier, and the atomic no-replace
//! rename used to restore a captured object — is claimed for **local
//! filesystems on a single host only**. A network filesystem such as NFS or SMB
//! may accept all three calls and still provide weaker semantics than their
//! local counterparts, and that weakening is not detectable from here. Nothing
//! in this module verifies, or claims, correctness on such a mount.

#[cfg(unix)]
use std::{
	ffi::{CStr, CString, OsStr},
	fs::File,
	io::Write,
	os::{
		fd::{AsRawFd, FromRawFd},
		unix::ffi::OsStrExt,
	},
	path::{Component, Path},
	sync::atomic::{AtomicU64, Ordering},
	time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use napi::bindgen_prelude::Uint8Array;
use napi_derive::napi;
#[cfg(unix)]
use parking_lot::Mutex;

/// Command documents carry identifiers only; anything larger is not ours.
#[cfg(unix)]
const MAX_ENTRY_BYTES: u64 = 8 * 1024;
#[cfg(unix)]
const MAX_ENTRY_NAME_LENGTH: usize = 128;
#[cfg(unix)]
const MAX_DIRECTORY_ENTRIES: usize = 4096;
/// Bounded wait for the directory's cross-process mutation lock.
#[cfg(unix)]
const MUTATION_LOCK_TIMEOUT_MS: u64 = 5_000;
/// Internal prefix of an in-flight identity-bound retirement.
#[cfg(unix)]
const RETIRE_PREFIX: &str = ".retire.";
#[cfg(unix)]
static RETIRE_SEQUENCE: AtomicU64 = AtomicU64::new(0);
/// Internal prefix of a captured object that could not be restored because a
/// third object took its name. Quarantine is never retirement residue: no sweep
/// reclaims it and `list` never exposes it.
#[cfg(unix)]
const QUARANTINE_PREFIX: &str = ".quarantine.";
#[cfg(unix)]
static QUARANTINE_SEQUENCE: AtomicU64 = AtomicU64::new(0);
/// `renameatx_np` flag that refuses to replace an existing destination.
#[cfg(target_os = "macos")]
const RENAME_EXCL: libc::c_uint = 0x0000_0004;

#[cfg(target_os = "macos")]
unsafe extern "C" {
	fn renameatx_np(
		fromfd: libc::c_int,
		from: *const libc::c_char,
		tofd: libc::c_int,
		to: *const libc::c_char,
		flags: libc::c_uint,
	) -> libc::c_int;
}

/// Non-dereferenced identity of one retained directory or one of its entries.
#[napi(object)]
pub struct RetainedDirIdentity {
	pub dev:      String,
	pub ino:      String,
	pub size:     String,
	pub nlink:    String,
	pub mtime_ms: f64,
	pub mode:     u32,
	pub uid:      u32,
	/// `file`, `directory`, `symlink`, or `other`.
	pub kind:     String,
}

/// Fail-closed outcome of one descriptor-relative operation.
#[napi(object)]
pub struct RetainedDirResult {
	pub ok:       bool,
	/// Machine-readable failure category; never a path or a message body.
	pub code:     Option<String>,
	pub identity: Option<RetainedDirIdentity>,
	pub data:     Option<Uint8Array>,
	pub names:    Option<Vec<String>>,
}

impl RetainedDirResult {
	fn failure(code: &str) -> Self {
		Self {
			ok:       false,
			code:     Some(code.to_owned()),
			identity: None,
			data:     None,
			names:    None,
		}
	}

	const fn success() -> Self {
		Self { ok: true, code: None, identity: None, data: None, names: None }
	}

	#[cfg(unix)]
	const fn with_identity(identity: RetainedDirIdentity) -> Self {
		Self {
			ok:       true,
			code:     None,
			identity: Some(identity),
			data:     None,
			names:    None,
		}
	}
}

#[cfg(unix)]
fn last_errno() -> i32 {
	std::io::Error::last_os_error().raw_os_error().unwrap_or(0)
}

#[cfg(unix)]
const fn errno_code(errno: i32) -> &'static str {
	match errno {
		libc::ENOENT => "not_found",
		libc::EEXIST => "exists",
		libc::ELOOP => "symlink_refused",
		libc::ENOTDIR => "not_a_directory",
		libc::EISDIR => "is_a_directory",
		libc::EACCES | libc::EPERM => "permission",
		libc::EXDEV => "cross_device",
		libc::ENOTEMPTY => "not_empty",
		_ => "io_error",
	}
}

/// Cross-process serialization for every namespace mutation in one retained
/// command directory.
///
/// POSIX offers no conditional `unlink`, so an identity-bound removal cannot be
/// made safe by proving the name and then removing it: the two syscalls are
/// separate and any other process may replace the name in between. This lock
/// closes that window for every process that participates in the protocol.
///
/// It is `flock` on a descriptor opened relative to the retained directory, so
/// it is the *directory inode* that is locked, not a pathname a replacement
/// could redirect. The kernel releases it when the descriptor closes or the
/// holder dies, so there is no lock file to reclaim and no stale-lock recovery
/// that could itself reintroduce a stale deletion.
///
/// A host that cannot provide it fails closed with `lock_unsupported` rather
/// than downgrading to unserialized mutations.
#[cfg(unix)]
struct MutationLock {
	fd: i32,
}

#[cfg(unix)]
impl MutationLock {
	fn acquire(directory: &File) -> Result<Self, &'static str> {
		Self::hold(directory, Instant::now() + Duration::from_millis(MUTATION_LOCK_TIMEOUT_MS))
	}

	/// Prove the host can serialize namespace mutations without waiting for the
	/// current holder. A lock that is merely *taken* is a working lock.
	fn probe(directory: &File) -> Result<(), &'static str> {
		match Self::hold(directory, Instant::now()) {
			Ok(lock) => {
				drop(lock);
				Ok(())
			},
			Err("lock_timeout") => Ok(()),
			Err(code) => Err(code),
		}
	}

	fn hold(directory: &File, deadline: Instant) -> Result<Self, &'static str> {
		// `.` relative to the retained descriptor is the retained directory
		// itself; it resolves to the same inode and can never be redirected. A
		// fresh descriptor per acquisition is required because `flock` is held per
		// open file description, so reusing the retained one would make the lock
		// re-entrant and therefore no lock at all.
		// SAFETY: the retained descriptor is open for the duration of the call.
		let fd = unsafe {
			libc::openat(
				directory.as_raw_fd(),
				c".".as_ptr(),
				libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC,
			)
		};
		if fd < 0 {
			return Err(errno_code(last_errno()));
		}
		loop {
			// SAFETY: `fd` is an open descriptor owned by this call.
			if unsafe { libc::flock(fd, libc::LOCK_EX | libc::LOCK_NB) } == 0 {
				return Ok(Self { fd });
			}
			let errno = last_errno();
			if errno == libc::EINTR {
				continue;
			}
			if errno == libc::EWOULDBLOCK {
				if Instant::now() >= deadline {
					close_descriptor(fd);
					return Err("lock_timeout");
				}
				std::thread::sleep(Duration::from_micros(200));
				continue;
			}
			close_descriptor(fd);
			return Err(
				if errno == libc::ENOTSUP || errno == libc::EOPNOTSUPP || errno == libc::ENOLCK {
					"lock_unsupported"
				} else {
					errno_code(errno)
				},
			);
		}
	}
}

#[cfg(unix)]
impl Drop for MutationLock {
	fn drop(&mut self) {
		// SAFETY: the descriptor is open and owned by this guard.
		unsafe {
			libc::flock(self.fd, libc::LOCK_UN);
		}
		close_descriptor(self.fd);
	}
}

#[cfg(unix)]
fn close_descriptor(fd: i32) {
	// SAFETY: `fd` is an open descriptor that is not used after this call.
	unsafe {
		libc::close(fd);
	}
}

/// A name only this retirement can address, so the object it captures can be
/// removed without ever re-resolving the caller-supplied name.
#[cfg(unix)]
fn reserved_retire_name() -> Result<CString, &'static str> {
	let nanos = SystemTime::now()
		.duration_since(UNIX_EPOCH)
		.map_or(0, |value| value.as_nanos());
	let sequence = RETIRE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
	let name = format!("{RETIRE_PREFIX}{}.{nanos:x}.{sequence:x}", std::process::id());
	if name.len() > MAX_ENTRY_NAME_LENGTH {
		return Err("invalid_name");
	}
	CString::new(name).map_err(|_| "invalid_name")
}

/// A quarantine name for an object this protocol captured but may not restore.
#[cfg(unix)]
fn quarantine_name() -> Result<CString, &'static str> {
	let nanos = SystemTime::now()
		.duration_since(UNIX_EPOCH)
		.map_or(0, |value| value.as_nanos());
	let sequence = QUARANTINE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
	let name = format!("{QUARANTINE_PREFIX}{}.{nanos:x}.{sequence:x}", std::process::id());
	if name.len() > MAX_ENTRY_NAME_LENGTH {
		return Err("invalid_name");
	}
	CString::new(name).map_err(|_| "invalid_name")
}

/// Rename `from` to `to` *without* replacing an existing `to`.
///
/// POSIX `renameat` is unconditionally replacing, which makes it unusable for
/// putting a captured object back: a third object may have taken the name in
/// the meantime, and replacing it would destroy material this protocol never
/// decided about. macOS supplies `renameatx_np(RENAME_EXCL)` and Linux supplies
/// `renameat2(RENAME_NOREPLACE)`; every other host, and every filesystem that
/// refuses the flag, is reported as unsupported so the caller fails closed
/// instead of downgrading to a replacing rename.
///
/// `Err` carries the raw errno; `EEXIST` means the destination is occupied and
/// was deliberately left alone.
#[cfg(unix)]
fn renameat_no_replace(dir: &File, from: &CStr, to: &CStr) -> Result<(), i32> {
	#[cfg(target_os = "macos")]
	// SAFETY: the descriptor is open and both names stay live for the call.
	let result = unsafe {
		renameatx_np(dir.as_raw_fd(), from.as_ptr(), dir.as_raw_fd(), to.as_ptr(), RENAME_EXCL)
	};
	#[cfg(target_os = "linux")]
	// SAFETY: the descriptor is open and both names stay live for the call.
	let result = unsafe {
		libc::syscall(
			libc::SYS_renameat2,
			dir.as_raw_fd(),
			from.as_ptr(),
			dir.as_raw_fd(),
			to.as_ptr(),
			libc::RENAME_NOREPLACE,
		) as libc::c_int
	};
	#[cfg(not(any(target_os = "macos", target_os = "linux")))]
	let result = {
		let _ = (dir, from, to);
		-1
	};
	#[cfg(not(any(target_os = "macos", target_os = "linux")))]
	// SAFETY: no syscall was made; report the primitive as absent.
	return Err(libc::ENOSYS);
	#[cfg(any(target_os = "macos", target_os = "linux"))]
	if result == 0 {
		Ok(())
	} else {
		Err(last_errno())
	}
}

/// Whether an errno means the host or filesystem does not provide a no-replace
/// rename at all, as opposed to refusing this particular rename.
#[cfg(unix)]
const fn no_replace_unsupported(errno: i32) -> bool {
	matches!(errno, libc::ENOSYS | libc::EINVAL | libc::ENOTSUP | libc::EOPNOTSUPP)
}

/// Prove the host and filesystem provide a working no-replace rename before the
/// directory is retained.
///
/// The primitive is load-bearing for successor-safe retirement, so a host that
/// lacks it fails closed here rather than being discovered halfway through a
/// restore that would otherwise have to clobber a third object.
///
/// The probe deliberately does *not* take the directory's mutation lock: both
/// names it uses are private to this process and this call, so it decides about
/// nothing another participant could be deciding about, and waiting for a
/// holder would turn capturing the directory into a blocking operation.
#[cfg(unix)]
fn probe_no_replace_rename(directory: &File) -> Result<(), &'static str> {
	let source = reserved_retire_name()?;
	let destination = reserved_retire_name()?;
	if create_probe_entry(directory, &source).is_err() {
		// A directory this process cannot stage into is refused elsewhere; the
		// probe never weakens that decision into a pass.
		return Err("rename_noreplace_unsupported");
	}
	let outcome = renameat_no_replace(directory, &source, &destination);
	let cleanup = |name: &CStr| {
		// SAFETY: the descriptor is open and `name` stays live for the call.
		unsafe { libc::unlinkat(directory.as_raw_fd(), name.as_ptr(), 0) };
	};
	match outcome {
		Ok(()) => {
			cleanup(&destination);
			Ok(())
		},
		Err(errno) if no_replace_unsupported(errno) => {
			cleanup(&source);
			Err("rename_noreplace_unsupported")
		},
		Err(_) => {
			cleanup(&source);
			Err("rename_noreplace_unsupported")
		},
	}
}

#[cfg(unix)]
fn create_probe_entry(directory: &File, name: &CStr) -> Result<(), &'static str> {
	// SAFETY: the descriptor is open and `name` stays live for the call.
	let fd = unsafe {
		libc::openat(
			directory.as_raw_fd(),
			name.as_ptr(),
			libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL | libc::O_NOFOLLOW | libc::O_CLOEXEC,
			libc::c_uint::from(0o600 as libc::mode_t),
		)
	};
	if fd < 0 {
		return Err(errno_code(last_errno()));
	}
	close_descriptor(fd);
	Ok(())
}

/// Residue of a retirement whose process died mid-flight, and only that.
#[cfg(unix)]
fn retire_residue_pid(name: &str) -> Option<libc::pid_t> {
	let pid = name
		.strip_prefix(RETIRE_PREFIX)?
		.split('.')
		.next()?
		.parse::<libc::pid_t>()
		.ok()?;
	// A non-positive value would address a process group instead of a process.
	(pid > 0).then_some(pid)
}

#[cfg(unix)]
fn pid_is_live(pid: libc::pid_t) -> bool {
	// SAFETY: signal 0 performs the permission and existence check only.
	unsafe { libc::kill(pid, 0) == 0 || last_errno() == libc::EPERM }
}

/// Retire retirement residue left behind by processes that are provably gone.
///
/// A live holder's residue is never touched: the owning process is still inside
/// its own mutation-locked retirement, and removing its captured object would
/// be exactly the stale deletion this protocol exists to prevent.
#[cfg(unix)]
fn sweep_retire_residue(directory: &File) {
	let Ok(names) = read_directory(directory) else {
		return;
	};
	if !names.iter().any(|name| name.starts_with(RETIRE_PREFIX)) {
		return;
	}
	let Ok(_lock) = MutationLock::hold(directory, Instant::now()) else {
		return;
	};
	for name in names {
		let Some(pid) = retire_residue_pid(&name) else {
			continue;
		};
		if pid_is_live(pid) {
			continue;
		}
		let Ok(entry) = entry_name(&name) else {
			continue;
		};
		// SAFETY: the descriptor is open and `entry` stays live for the call.
		unsafe {
			libc::unlinkat(directory.as_raw_fd(), entry.as_ptr(), 0);
		}
	}
}

/// Reject anything that could traverse, escape, or address the directory
/// itself.
#[cfg(unix)]
fn entry_name(name: &str) -> Result<CString, &'static str> {
	if name.is_empty()
		|| name.len() > MAX_ENTRY_NAME_LENGTH
		|| name == "."
		|| name == ".."
		|| name.contains('/')
		|| name.contains('\\')
		|| name.contains('\0')
	{
		return Err("invalid_name");
	}
	CString::new(name).map_err(|_| "invalid_name")
}

#[cfg(unix)]
const fn stat_kind(mode: libc::mode_t) -> &'static str {
	match mode & libc::S_IFMT {
		libc::S_IFREG => "file",
		libc::S_IFDIR => "directory",
		libc::S_IFLNK => "symlink",
		_ => "other",
	}
}

#[cfg(unix)]
fn identity_of(stat: &libc::stat) -> RetainedDirIdentity {
	RetainedDirIdentity {
		dev:      stat.st_dev.to_string(),
		ino:      stat.st_ino.to_string(),
		size:     (stat.st_size.max(0) as u64).to_string(),
		nlink:    (stat.st_nlink as u64).to_string(),
		mtime_ms: (stat.st_mtime as f64).mul_add(1_000.0, stat.st_mtime_nsec as f64 / 1_000_000.0),
		mode:     u32::from(stat.st_mode) & 0o7777,
		uid:      stat.st_uid,
		kind:     stat_kind(stat.st_mode).to_owned(),
	}
}

#[cfg(unix)]
fn fstat(fd: i32) -> Result<libc::stat, &'static str> {
	// SAFETY: `libc::stat` is a plain C structure that `fstat` fully initializes
	// on success.
	let mut stat: libc::stat = unsafe { std::mem::zeroed() };
	// SAFETY: `fd` is an open descriptor and `stat` is valid writable storage.
	if unsafe { libc::fstat(fd, &mut stat) } != 0 {
		return Err("io_error");
	}
	Ok(stat)
}

#[cfg(unix)]
fn fstatat_nofollow(dir: i32, name: &CStr) -> Result<libc::stat, i32> {
	// SAFETY: `libc::stat` is a plain C structure that `fstatat` fully
	// initializes on success.
	let mut stat: libc::stat = unsafe { std::mem::zeroed() };
	// SAFETY: `dir` is an open directory descriptor, `name` stays NUL-terminated
	// and live for the call, and `stat` is valid writable storage.
	if unsafe { libc::fstatat(dir, name.as_ptr(), &mut stat, libc::AT_SYMLINK_NOFOLLOW) } != 0 {
		return Err(last_errno());
	}
	Ok(stat)
}

/// An entry may only be treated as ours when it is owner-only and unshared.
#[cfg(unix)]
fn is_owner_only_regular(stat: &libc::stat) -> bool {
	stat.st_mode & libc::S_IFMT == libc::S_IFREG
		&& stat.st_nlink == 1
		&& stat.st_uid
			== unsafe {
				// SAFETY: `geteuid` takes no arguments and cannot fail.
				libc::geteuid()
			} && u32::from(stat.st_mode) & 0o077 == 0
}

/// Open one path component beneath `dir`, proving the opened object is exactly
/// the non-symlink directory the name resolved to.
#[cfg(unix)]
fn open_child_directory(dir: i32, name: &CStr) -> Result<File, &'static str> {
	let named = fstatat_nofollow(dir, name).map_err(errno_code)?;
	if named.st_mode & libc::S_IFMT != libc::S_IFDIR {
		return Err("untrusted_root");
	}
	// SAFETY: `dir` is open and `name` is a live NUL-terminated component.
	let next = unsafe {
		libc::openat(
			dir,
			name.as_ptr(),
			libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC | libc::O_NOFOLLOW,
		)
	};
	if next < 0 {
		return Err(errno_code(last_errno()));
	}
	// SAFETY: `next` is an owned descriptor whose ownership transfers exactly once.
	let opened = unsafe { File::from_raw_fd(next) };
	let proven = fstat(opened.as_raw_fd())?;
	if proven.st_mode & libc::S_IFMT != libc::S_IFDIR
		|| proven.st_dev != named.st_dev
		|| proven.st_ino != named.st_ino
	{
		return Err("untrusted_root");
	}
	Ok(opened)
}

#[cfg(unix)]
fn mkdirat_owner_only(dir: i32, name: &CStr, mode: u32) -> Result<(), &'static str> {
	// SAFETY: `dir` is open and `name` is a live NUL-terminated component.
	if unsafe { libc::mkdirat(dir, name.as_ptr(), mode as libc::mode_t) } == 0 {
		return Ok(());
	}
	let errno = last_errno();
	if errno == libc::EEXIST {
		Ok(())
	} else {
		Err(errno_code(errno))
	}
}

/// Repair and then re-prove owner-only permissions on the retained descriptor
/// itself, so a weakened directory is never accepted as captured.
#[cfg(unix)]
fn enforce_owner_only(directory: &File, mode: u32) -> Result<(), &'static str> {
	let stat = fstat(directory.as_raw_fd())?;
	// SAFETY: `geteuid` takes no arguments and cannot fail.
	if stat.st_uid != unsafe { libc::geteuid() } {
		return Err("untrusted_owner");
	}
	if u32::from(stat.st_mode) & 0o077 == 0 {
		return Ok(());
	}
	// SAFETY: the descriptor is open and owned by this process.
	if unsafe { libc::fchmod(directory.as_raw_fd(), mode as libc::mode_t) } != 0 {
		return Err(errno_code(last_errno()));
	}
	let repaired = fstat(directory.as_raw_fd())?;
	if u32::from(repaired.st_mode) & 0o077 == 0 {
		Ok(())
	} else {
		Err("untrusted_mode")
	}
}

/// Open the canonical trust root by absolute pathname.
///
/// The root is the caller's own agent directory. It is canonical, so it holds
/// no symlink components of its own, and it is the only pathname resolution
/// this module ever performs.
#[cfg(unix)]
fn open_trust_root(path: &Path) -> Result<File, &'static str> {
	if !path.is_absolute() {
		return Err("invalid_path");
	}
	let canonical = path.canonicalize().map_err(|_| "untrusted_root")?;
	let name = CString::new(canonical.as_os_str().as_bytes()).map_err(|_| "invalid_path")?;
	// SAFETY: `name` stays NUL-terminated and live for the duration of the call.
	let fd = unsafe {
		libc::open(
			name.as_ptr(),
			libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC | libc::O_NOFOLLOW,
		)
	};
	if fd < 0 {
		return Err(errno_code(last_errno()));
	}
	// SAFETY: `fd` is an owned descriptor whose ownership transfers exactly once.
	let opened = unsafe { File::from_raw_fd(fd) };
	let stat = fstat(opened.as_raw_fd())?;
	if stat.st_mode & libc::S_IFMT != libc::S_IFDIR {
		return Err("untrusted_root");
	}
	Ok(opened)
}

/// Walk the managed suffix beneath the trust root without ever following a
/// link.
///
/// Every component below the trust root is attacker-reachable, so each one is
/// proven to be a real directory selected by the exact name it was opened
/// through. A symlink, a special file, or an identity that changes mid-walk
/// fails closed instead of being retained.
#[cfg(unix)]
fn walk_managed_suffix(
	root: &File,
	relative: &str,
	create: bool,
	mode: u32,
) -> Result<File, &'static str> {
	let path = Path::new(relative);
	if path.is_absolute() || relative.contains('\0') {
		return Err("invalid_path");
	}
	let mut names = Vec::new();
	for component in path.components() {
		match component {
			Component::Normal(raw) => names.push(component_name(raw)?),
			Component::CurDir | Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
				return Err("invalid_path");
			},
		}
	}
	if names.is_empty() {
		return Err("invalid_path");
	}
	let mut current = root.try_clone().map_err(|_| "io_error")?;
	for name in &names {
		if create {
			mkdirat_owner_only(current.as_raw_fd(), name, mode)?;
		}
		current = open_child_directory(current.as_raw_fd(), name)?;
	}
	Ok(current)
}

#[cfg(unix)]
fn component_name(raw: &OsStr) -> Result<CString, &'static str> {
	if raw.as_bytes().is_empty() || raw.as_bytes().contains(&b'/') {
		return Err("invalid_path");
	}
	CString::new(raw.as_bytes()).map_err(|_| "invalid_path")
}

/// A retained chat-daemon command directory.
///
/// The descriptor is the authority. Once captured, replacing, moving, or
/// symlinking the pathname changes nothing about where this object's operations
/// land, which is what makes the channel's arbitration objects trustworthy.
#[napi]
pub struct RetainedCommandDir {
	#[cfg(unix)]
	directory: Mutex<Option<File>>,
}

#[napi]
impl RetainedCommandDir {
	/// Identity of the retained directory descriptor itself.
	#[napi]
	pub fn identity(&self) -> RetainedDirResult {
		#[cfg(unix)]
		{
			self.with_dir(|dir| {
				fstat(dir.as_raw_fd()).map_or_else(RetainedDirResult::failure, |stat| {
					RetainedDirResult::with_identity(identity_of(&stat))
				})
			})
		}
		#[cfg(not(unix))]
		RetainedDirResult::failure("unsupported_platform")
	}

	/// List the retained directory's own entries, skipping `.` and `..` and the
	/// protocol's own in-flight retirement objects.
	#[napi]
	pub fn list(&self) -> RetainedDirResult {
		#[cfg(unix)]
		{
			self.with_dir(|dir| match read_directory(dir) {
				Ok(names) => RetainedDirResult {
					ok:       true,
					code:     None,
					identity: None,
					data:     None,
					names:    Some(
						names
							.into_iter()
							.filter(|name| {
								!name.starts_with(RETIRE_PREFIX) && !name.starts_with(QUARANTINE_PREFIX)
							})
							.collect(),
					),
				},
				Err(code) => RetainedDirResult::failure(code),
			})
		}
		#[cfg(not(unix))]
		RetainedDirResult::failure("unsupported_platform")
	}

	/// Non-dereferenced identity of one entry, or `not_found`.
	#[napi]
	pub fn stat_entry(&self, name: String) -> RetainedDirResult {
		#[cfg(unix)]
		{
			self.with_dir(|dir| {
				let Ok(entry) = entry_name(&name) else {
					return RetainedDirResult::failure("invalid_name");
				};
				fstatat_nofollow(dir.as_raw_fd(), &entry).map_or_else(
					|errno| RetainedDirResult::failure(errno_code(errno)),
					|stat| RetainedDirResult::with_identity(identity_of(&stat)),
				)
			})
		}
		#[cfg(not(unix))]
		{
			let _ = name;
			RetainedDirResult::failure("unsupported_platform")
		}
	}

	/// Create one entry that must not already exist.
	///
	/// `O_CREAT|O_EXCL` is the channel's arbitration primitive, and `O_NOFOLLOW`
	/// keeps a planted link from redirecting the creation. `exists` is a
	/// definitive loss, never an error.
	#[napi]
	pub fn create_exclusive(
		&self,
		name: String,
		data: Option<Uint8Array>,
		mode: u32,
	) -> RetainedDirResult {
		#[cfg(unix)]
		{
			self.with_dir(|dir| {
				let Ok(entry) = entry_name(&name) else {
					return RetainedDirResult::failure("invalid_name");
				};
				match create_entry(dir, &entry, data.as_deref(), mode) {
					Ok(identity) => RetainedDirResult::with_identity(identity),
					Err(code) => RetainedDirResult::failure(code),
				}
			})
		}
		#[cfg(not(unix))]
		{
			let _ = (name, data, mode);
			RetainedDirResult::failure("unsupported_platform")
		}
	}

	/// Read one owner-only, single-linked regular file without following a link.
	#[napi]
	pub fn read_entry(&self, name: String) -> RetainedDirResult {
		#[cfg(unix)]
		{
			self.with_dir(|dir| {
				let Ok(entry) = entry_name(&name) else {
					return RetainedDirResult::failure("invalid_name");
				};
				match read_entry(dir, &entry) {
					Ok((identity, data)) => RetainedDirResult {
						ok:       true,
						code:     None,
						identity: Some(identity),
						data:     Some(Uint8Array::new(data)),
						names:    None,
					},
					Err(code) => RetainedDirResult::failure(code),
				}
			})
		}
		#[cfg(not(unix))]
		{
			let _ = name;
			RetainedDirResult::failure("unsupported_platform")
		}
	}

	/// Replace `to` with `from` atomically, both relative to the retained
	/// directory.
	///
	/// The replacement participates in the directory's mutation lock, so it can
	/// never install a successor while an identity-bound decision about the same
	/// name is in flight.
	#[napi]
	pub fn rename_entry(&self, from: String, to: String) -> RetainedDirResult {
		#[cfg(unix)]
		{
			self.with_dir(|dir| {
				let (Ok(source), Ok(destination)) = (entry_name(&from), entry_name(&to)) else {
					return RetainedDirResult::failure("invalid_name");
				};
				let lock = match MutationLock::acquire(dir) {
					Ok(lock) => lock,
					Err(code) => return RetainedDirResult::failure(code),
				};
				// SAFETY: the descriptor is open and both names stay live for the call.
				if unsafe {
					libc::renameat(
						dir.as_raw_fd(),
						source.as_ptr(),
						dir.as_raw_fd(),
						destination.as_ptr(),
					)
				} != 0
				{
					return RetainedDirResult::failure(errno_code(last_errno()));
				}
				drop(lock);
				RetainedDirResult::success()
			})
		}
		#[cfg(not(unix))]
		{
			let _ = (from, to);
			RetainedDirResult::failure("unsupported_platform")
		}
	}

	/// Publish `from` under the absent name `to`; `exists` is a definitive loss.
	///
	/// The publication participates in the directory's mutation lock for the
	/// same reason the replacement does.
	#[napi]
	pub fn link_entry(&self, from: String, to: String) -> RetainedDirResult {
		#[cfg(unix)]
		{
			self.with_dir(|dir| {
				let (Ok(source), Ok(destination)) = (entry_name(&from), entry_name(&to)) else {
					return RetainedDirResult::failure("invalid_name");
				};
				let lock = match MutationLock::acquire(dir) {
					Ok(lock) => lock,
					Err(code) => return RetainedDirResult::failure(code),
				};
				// SAFETY: the descriptor is open and both names stay live for the call.
				if unsafe {
					libc::linkat(
						dir.as_raw_fd(),
						source.as_ptr(),
						dir.as_raw_fd(),
						destination.as_ptr(),
						0,
					)
				} != 0
				{
					return RetainedDirResult::failure(errno_code(last_errno()));
				}
				drop(lock);
				RetainedDirResult::success()
			})
		}
		#[cfg(not(unix))]
		{
			let _ = (from, to);
			RetainedDirResult::failure("unsupported_platform")
		}
	}

	/// Remove one entry.
	///
	/// Without `expected_dev`/`expected_ino` the caller owns the name outright
	/// and the removal is unconditional.
	///
	/// With them the removal is bound to that exact object. POSIX has no
	/// conditional `unlink`, so this is not a proof followed by a hopeful
	/// `unlinkat`: the entry is proven, then *moved* to a name only this call
	/// can address, then re-proven at that private name, and only then removed.
	/// A replacement that beat the move is put back exactly where it was and
	/// reported as `identity_mismatch`; nothing is ever deleted on its behalf.
	#[napi]
	pub fn unlink_entry(
		&self,
		name: String,
		expected_dev: Option<String>,
		expected_ino: Option<String>,
	) -> RetainedDirResult {
		#[cfg(unix)]
		{
			self.with_dir(|dir| {
				let Ok(entry) = entry_name(&name) else {
					return RetainedDirResult::failure("invalid_name");
				};
				match unlink_entry(
					dir,
					&entry,
					expected_dev.as_deref(),
					expected_ino.as_deref(),
					&RemovalSeams::none(),
				) {
					Ok(()) => RetainedDirResult::success(),
					Err(code) => RetainedDirResult::failure(code),
				}
			})
		}
		#[cfg(not(unix))]
		{
			let _ = (name, expected_dev, expected_ino);
			RetainedDirResult::failure("unsupported_platform")
		}
	}

	/// Flush the retained directory itself so a publication survives a crash.
	///
	/// A host that refuses the barrier is reported as explicit uncertainty
	/// rather than silently downgraded to success: the namespace change is
	/// applied, but nothing here proves it survives a crash, and only the
	/// caller can decide what an unprovable publication means for its protocol.
	#[napi]
	pub fn sync_dir(&self) -> RetainedDirResult {
		#[cfg(unix)]
		{
			self.with_dir(|dir| {
				// SAFETY: the descriptor is open for the duration of the call.
				if unsafe { libc::fsync(dir.as_raw_fd()) } != 0 {
					let errno = last_errno();
					if errno == libc::EINVAL || errno == libc::ENOTSUP || errno == libc::EOPNOTSUPP {
						return RetainedDirResult::failure("durability_unsupported");
					}
					return RetainedDirResult::failure(errno_code(errno));
				}
				RetainedDirResult::success()
			})
		}
		#[cfg(not(unix))]
		RetainedDirResult::failure("unsupported_platform")
	}

	/// Release the retained descriptor. Later operations report `closed`.
	#[napi]
	pub fn close(&self) {
		#[cfg(unix)]
		{
			self.directory.lock().take();
		}
	}

	#[cfg(unix)]
	fn with_dir(&self, operation: impl FnOnce(&File) -> RetainedDirResult) -> RetainedDirResult {
		let guard = self.directory.lock();
		guard
			.as_ref()
			.map_or_else(|| RetainedDirResult::failure("closed"), operation)
	}
}

#[cfg(unix)]
fn read_directory(dir: &File) -> Result<Vec<String>, &'static str> {
	// SAFETY: the descriptor is open and owned; `dup` yields an independent fd
	// that `fdopendir` takes ownership of.
	let duplicated = unsafe { libc::dup(dir.as_raw_fd()) };
	if duplicated < 0 {
		return Err("io_error");
	}
	// SAFETY: `duplicated` is an open directory descriptor whose ownership moves
	// into the returned stream.
	let stream = unsafe { libc::fdopendir(duplicated) };
	if stream.is_null() {
		// SAFETY: `fdopendir` did not take ownership, so the fd is still ours.
		unsafe { libc::close(duplicated) };
		return Err("io_error");
	}
	// SAFETY: `stream` is a live directory stream positioned at its start.
	unsafe { libc::rewinddir(stream) };
	let mut names = Vec::new();
	loop {
		// SAFETY: `stream` stays live for the duration of the iteration.
		let entry = unsafe { libc::readdir(stream) };
		if entry.is_null() {
			break;
		}
		// SAFETY: `readdir` returned a live entry owned by the stream.
		let raw = unsafe { CStr::from_ptr((*entry).d_name.as_ptr()) };
		let Ok(name) = raw.to_str() else {
			continue;
		};
		if name == "." || name == ".." {
			continue;
		}
		if names.len() >= MAX_DIRECTORY_ENTRIES {
			break;
		}
		names.push(name.to_owned());
	}
	// SAFETY: `stream` is live and is not used after this call.
	unsafe { libc::closedir(stream) };
	Ok(names)
}

#[cfg(unix)]
fn create_entry(
	dir: &File,
	name: &CStr,
	data: Option<&[u8]>,
	mode: u32,
) -> Result<RetainedDirIdentity, &'static str> {
	// The exclusive creation participates in the directory's mutation lock, so a
	// name can never be installed while an identity-bound decision about that
	// same name is in flight.
	let _lock = MutationLock::acquire(dir)?;
	// SAFETY: the descriptor is open and `name` stays live for the call.
	let fd = unsafe {
		libc::openat(
			dir.as_raw_fd(),
			name.as_ptr(),
			libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL | libc::O_NOFOLLOW | libc::O_CLOEXEC,
			libc::c_uint::from(mode as libc::mode_t),
		)
	};
	if fd < 0 {
		return Err(errno_code(last_errno()));
	}
	// SAFETY: `fd` is an owned descriptor whose ownership transfers exactly once.
	let mut file = unsafe { File::from_raw_fd(fd) };
	if let Some(bytes) = data {
		if bytes.len() as u64 > MAX_ENTRY_BYTES {
			return Err("content_too_large");
		}
		file.write_all(bytes).map_err(|_| "io_error")?;
		file.sync_all().map_err(|_| "io_error")?;
	}
	let stat = fstat(file.as_raw_fd())?;
	Ok(identity_of(&stat))
}

#[cfg(unix)]
fn read_entry(dir: &File, name: &CStr) -> Result<(RetainedDirIdentity, Vec<u8>), &'static str> {
	use std::io::Read as _;

	// SAFETY: the descriptor is open and `name` stays live for the call.
	let fd = unsafe {
		libc::openat(
			dir.as_raw_fd(),
			name.as_ptr(),
			libc::O_RDONLY | libc::O_NOFOLLOW | libc::O_NONBLOCK | libc::O_CLOEXEC,
		)
	};
	if fd < 0 {
		return Err(errno_code(last_errno()));
	}
	// SAFETY: `fd` is an owned descriptor whose ownership transfers exactly once.
	let mut file = unsafe { File::from_raw_fd(fd) };
	let before = fstat(file.as_raw_fd())?;
	if !is_owner_only_regular(&before) {
		return Err("not_regular_file");
	}
	if before.st_size.max(0) as u64 > MAX_ENTRY_BYTES {
		return Err("content_too_large");
	}
	let mut data = Vec::with_capacity(before.st_size.max(0) as usize);
	let mut buffer = [0u8; 4096];
	loop {
		let count = file.read(&mut buffer).map_err(|_| "io_error")?;
		if count == 0 {
			break;
		}
		if data.len().saturating_add(count) as u64 > MAX_ENTRY_BYTES {
			return Err("content_too_large");
		}
		data.extend_from_slice(&buffer[..count]);
	}
	let after = fstat(file.as_raw_fd())?;
	if after.st_dev != before.st_dev
		|| after.st_ino != before.st_ino
		|| after.st_size != before.st_size
		|| after.st_nlink != before.st_nlink
	{
		return Err("identity_mismatch");
	}
	Ok((identity_of(&after), data))
}

/// The deterministic interleaving seams of one identity-bound removal.
///
/// A non-participating writer can act at exactly two instants a
/// `prove then remove` protocol cannot cover on its own: after the identity is
/// proven, and after the proven object has been captured under a private name
/// and the caller-supplied name is momentarily free. Production passes no-ops;
/// tests install a successor at each seam.
#[cfg(unix)]
struct RemovalSeams<'a> {
	after_proof:   &'a dyn Fn(),
	after_capture: &'a dyn Fn(),
}

#[cfg(unix)]
impl RemovalSeams<'_> {
	const fn none() -> Self {
		Self { after_proof: &|| (), after_capture: &|| () }
	}
}

/// Remove one entry, and — when an expected identity is supplied — only ever
/// the exact object that identity names.
///
/// Three independent mechanisms cover the interleaving seams:
///
/// - every create, link, rename, and removal in this module holds the
///   directory's mutation lock, so no participating process can install or
///   retire the name while the decision is in flight;
/// - the removal is identity-bound rather than name-bound. The entry is moved
///   to a private, single-use name, re-proven *there*, and removed through that
///   name, so even a non-participating mutation cannot get a successor deleted;
/// - the restore of a captured non-match is a *no-replace* rename. A third
///   object installed at the caller-supplied name after the capture is never
///   overwritten: the captured object is quarantined under a name that no
///   retirement sweep ever reclaims, and the call reports `identity_unrestored`
///   instead of sacrificing the successor.
#[cfg(unix)]
fn unlink_entry(
	dir: &File,
	name: &CStr,
	expected_dev: Option<&str>,
	expected_ino: Option<&str>,
	seams: &RemovalSeams<'_>,
) -> Result<(), &'static str> {
	let _lock = MutationLock::acquire(dir)?;
	let (Some(dev), Some(ino)) = (expected_dev, expected_ino) else {
		(seams.after_proof)();
		// SAFETY: the descriptor is open and `name` stays live for the call.
		if unsafe { libc::unlinkat(dir.as_raw_fd(), name.as_ptr(), 0) } != 0 {
			return Err(errno_code(last_errno()));
		}
		return Ok(());
	};
	let proven = open_proven_entry(dir, name, dev, ino)?;
	(seams.after_proof)();
	let reserved = reserved_retire_name()?;
	// SAFETY: the descriptor is open and both names stay live for the call.
	if unsafe { libc::renameat(dir.as_raw_fd(), name.as_ptr(), dir.as_raw_fd(), reserved.as_ptr()) }
		!= 0
	{
		return Err(errno_code(last_errno()));
	}
	(seams.after_capture)();
	let captured = fstatat_nofollow(dir.as_raw_fd(), &reserved).map_err(errno_code)?;
	if captured.st_dev.to_string() != dev || captured.st_ino.to_string() != ino {
		// The name was replaced by something outside this protocol between the
		// proof and the move. Put the captured object back exactly where it was —
		// but only if the name is still free. A replacing rename here would delete
		// a third object that took the name after the capture, and a successor is
		// never this call's to destroy.
		return Err(restore_or_quarantine(dir, &reserved, name));
	}
	// SAFETY: the descriptor is open and `reserved` stays live for the call.
	if unsafe { libc::unlinkat(dir.as_raw_fd(), reserved.as_ptr(), 0) } != 0 {
		return Err(errno_code(last_errno()));
	}
	let removed = fstat(proven.as_raw_fd())?;
	debug_assert_eq!(removed.st_nlink, 0, "the proven object is what was retired");
	Ok(())
}

/// Put a captured non-match back, or park it where nothing will ever reclaim
/// it.
///
/// The restore is a no-replace rename, so an object installed at `name` after
/// the capture is left exactly as it is. When the name is occupied the captured
/// object is moved to a quarantine name instead: quarantine is deliberately
/// *not* retirement residue, so no sweep — this process's or a successor's —
/// ever deletes it, and `list` never exposes it as dispatchable material.
#[cfg(unix)]
fn restore_or_quarantine(dir: &File, reserved: &CStr, name: &CStr) -> &'static str {
	match renameat_no_replace(dir, reserved, name) {
		Ok(()) => "identity_mismatch",
		Err(errno) if errno == libc::EEXIST => {
			let Ok(quarantine) = quarantine_name() else {
				return "identity_unrestored";
			};
			// The quarantine name is unique to this call, so it can only fail if the
			// host lost the primitive entirely; the captured object then stays under
			// its retirement name, which this process's own liveness protects.
			let _ = renameat_no_replace(dir, reserved, &quarantine);
			"identity_unrestored"
		},
		Err(_) => "identity_unrestored",
	}
}

/// Open the entry and prove, without dereferencing a link, that it is exactly
/// the object the caller decided about.
#[cfg(unix)]
fn open_proven_entry(dir: &File, name: &CStr, dev: &str, ino: &str) -> Result<File, &'static str> {
	// SAFETY: the descriptor is open and `name` stays live for the call.
	let fd = unsafe {
		libc::openat(
			dir.as_raw_fd(),
			name.as_ptr(),
			libc::O_RDONLY | libc::O_NOFOLLOW | libc::O_NONBLOCK | libc::O_CLOEXEC,
		)
	};
	if fd < 0 {
		return Err(errno_code(last_errno()));
	}
	// SAFETY: `fd` is an owned descriptor whose ownership transfers exactly once.
	let file = unsafe { File::from_raw_fd(fd) };
	let stat = fstat(file.as_raw_fd())?;
	if stat.st_dev.to_string() != dev || stat.st_ino.to_string() != ino {
		return Err("identity_mismatch");
	}
	Ok(file)
}

/// Capture a chat-daemon command directory as a retained descriptor.
///
/// `root` is the caller's own trust root (its agent directory) and is the only
/// pathname this module resolves; `relative` is the managed suffix beneath it
/// and is walked component by component without following any link.
///
/// `create` materialises the managed suffix owner-only. Failure is fail-closed:
/// an untrusted component, a weakened directory that cannot be repaired, a host
/// that cannot serialize namespace mutations across processes, or a host
/// without descriptor-relative filesystem authority all raise instead of
/// downgrading to unserialized or pathname operations.
#[napi]
pub fn open_retained_command_dir(
	root: String,
	relative: String,
	create: bool,
	mode: u32,
) -> napi::Result<RetainedCommandDir> {
	#[cfg(unix)]
	{
		let trust_root = open_trust_root(Path::new(&root)).map_err(napi::Error::from_reason)?;
		let directory = walk_managed_suffix(&trust_root, &relative, create, mode)
			.map_err(napi::Error::from_reason)?;
		enforce_owner_only(&directory, mode).map_err(napi::Error::from_reason)?;
		// The mutation lock is load-bearing for every identity-bound decision this
		// channel makes, so a host that cannot provide it is refused here rather
		// than discovered halfway through a retirement.
		MutationLock::probe(&directory).map_err(napi::Error::from_reason)?;
		// Successor-safe retirement needs an atomic no-replace rename. Prove the
		// host and the filesystem actually provide one before the directory is
		// retained; a host without it would otherwise have to choose between
		// clobbering a third object and losing a captured one.
		probe_no_replace_rename(&directory).map_err(napi::Error::from_reason)?;
		sweep_retire_residue(&directory);
		Ok(RetainedCommandDir { directory: Mutex::new(Some(directory)) })
	}
	#[cfg(not(unix))]
	{
		let _ = (root, relative, create, mode);
		Err(napi::Error::from_reason("unsupported_platform"))
	}
}

#[cfg(all(test, unix))]
mod tests {
	use super::*;

	/// Separates one test root from every other root minted by this process.
	/// PID and timestamp remain in the name only to separate this process from
	/// other processes and from stale runs; neither can separate two threads of
	/// this process, which routinely read the same coarse clock value.
	static TEMP_ROOT_SEQUENCE: AtomicU64 = AtomicU64::new(0);

	fn temp_root_name(nanos: u128) -> String {
		let sequence = TEMP_ROOT_SEQUENCE.fetch_add(1, Ordering::Relaxed);
		format!("pi-command-dir-{}-{nanos:x}-{sequence:x}", std::process::id())
	}

	fn temp_root() -> std::path::PathBuf {
		let nanos = std::time::SystemTime::now()
			.duration_since(std::time::UNIX_EPOCH)
			.map_or(0, |value| value.as_nanos());
		let base = std::env::temp_dir().join(temp_root_name(nanos));
		// Exclusive: a name that ever repeats must fail here rather than quietly
		// hand two parallel tests the same directory to create and remove.
		std::fs::create_dir(&base).expect("temp root");
		base.canonicalize().expect("canonical temp root")
	}

	fn open(root: &std::path::Path, create: bool) -> napi::Result<RetainedCommandDir> {
		open_retained_command_dir(
			root.to_string_lossy().into_owned(),
			"commands".to_owned(),
			create,
			0o700,
		)
	}

	/// Every test here owns a root and removes it recursively, so a repeated
	/// pathname lets one test delete a directory another test is still using.
	/// The clock cannot carry that uniqueness: `SystemTime::now()` advances in
	/// 1000 ns steps on this host, which is coarser than the rate at which
	/// parallel test threads allocate. Pin the timestamp so this proof depends
	/// on the name itself and never on timing.
	#[test]
	fn test_root_names_minted_at_one_timestamp_are_distinct() {
		const FROZEN_NANOS: u128 = 0x0dec_ade0_0000;
		const MINTS: usize = 1024;

		let mut seen = std::collections::HashSet::with_capacity(MINTS);
		for _ in 0..MINTS {
			let name = temp_root_name(FROZEN_NANOS);
			assert!(
				seen.insert(name.clone()),
				"a test root name repeated at a fixed timestamp: {name}"
			);
		}
		assert_eq!(seen.len(), MINTS, "every mint at one timestamp is its own root");
	}

	/// The same guarantee through the real allocator, from many threads at once:
	/// disjoint pathnames, and every directory still present after all of its
	/// neighbours were allocated.
	#[test]
	fn concurrently_allocated_test_roots_are_disjoint_and_survive() {
		const THREADS: usize = 8;
		const PER_THREAD: usize = 16;

		let barrier = std::sync::Arc::new(std::sync::Barrier::new(THREADS));
		let handles: Vec<_> = (0..THREADS)
			.map(|_| {
				let barrier = std::sync::Arc::clone(&barrier);
				std::thread::spawn(move || {
					barrier.wait();
					(0..PER_THREAD)
						.map(|_| temp_root())
						.collect::<Vec<std::path::PathBuf>>()
				})
			})
			.collect();
		let roots: Vec<std::path::PathBuf> = handles
			.into_iter()
			.flat_map(|handle| handle.join().expect("allocator thread"))
			.collect();

		let distinct =
			std::collections::HashSet::<&std::path::PathBuf>::from_iter(roots.iter()).len();
		let survivors = roots.iter().filter(|root| root.is_dir()).count();
		// Clean up before asserting: a regression here allocates one root per
		// thread per iteration, and none of them should outlive a red run.
		for root in &roots {
			std::fs::remove_dir_all(root).ok();
		}

		assert_eq!(distinct, roots.len(), "concurrent allocation produced a repeated test root");
		assert_eq!(survivors, roots.len(), "an allocated test root was removed by a neighbour");
	}

	#[test]
	fn retains_the_directory_after_the_pathname_is_replaced() {
		let root = temp_root();
		let commands = root.join("commands");
		let retained = root.join("commands-retained");
		let outside = root.join("outside");
		std::fs::create_dir_all(&outside).expect("outside");
		let authority = open(&root, true).expect("retained directory");

		std::fs::rename(&commands, &retained).expect("rename aside");
		std::os::unix::fs::symlink(&outside, &commands).expect("symlink");

		let created = authority.create_exclusive("entry.json".to_owned(), None, 0o600);
		assert!(created.ok, "creation must land on the retained identity");
		assert!(retained.join("entry.json").exists(), "the retained directory received the entry");
		assert!(!outside.join("entry.json").exists(), "nothing escaped the managed root");

		let again = authority.create_exclusive("entry.json".to_owned(), None, 0o600);
		assert_eq!(again.code.as_deref(), Some("exists"));

		authority.close();
		assert_eq!(authority.identity().code.as_deref(), Some("closed"));
		std::fs::remove_dir_all(&root).ok();
	}

	#[test]
	fn refuses_symlinked_hard_linked_and_group_readable_entries() {
		let root = temp_root();
		let commands = root.join("commands");
		let secret = root.join("secret.json");
		std::fs::write(&secret, b"{}\n").expect("secret");
		let authority = open(&root, true).expect("retained directory");

		std::os::unix::fs::symlink(&secret, commands.join("linked.json")).expect("symlink");
		assert_eq!(
			authority
				.read_entry("linked.json".to_owned())
				.code
				.as_deref(),
			Some("symlink_refused")
		);

		std::fs::hard_link(&secret, commands.join("hard.json")).expect("hard link");
		assert_eq!(
			authority.read_entry("hard.json".to_owned()).code.as_deref(),
			Some("not_regular_file")
		);

		std::fs::write(commands.join("loose.json"), b"{}\n").expect("loose");
		std::fs::set_permissions(
			commands.join("loose.json"),
			<std::fs::Permissions as std::os::unix::fs::PermissionsExt>::from_mode(0o644),
		)
		.expect("chmod");
		assert_eq!(
			authority
				.read_entry("loose.json".to_owned())
				.code
				.as_deref(),
			Some("not_regular_file")
		);

		std::fs::create_dir(commands.join("dir.json")).expect("directory");
		assert_eq!(
			authority.read_entry("dir.json".to_owned()).code.as_deref(),
			Some("not_regular_file")
		);

		for name in ["..", ".", "a/b", ""] {
			assert_eq!(authority.read_entry(name.to_owned()).code.as_deref(), Some("invalid_name"));
		}
		std::fs::remove_dir_all(&root).ok();
	}

	#[test]
	fn repairs_a_weakened_directory_and_round_trips_documents() {
		let root = temp_root();
		let commands = root.join("commands");
		let first = open(&root, true).expect("retained directory");
		first.close();
		std::fs::set_permissions(
			&commands,
			<std::fs::Permissions as std::os::unix::fs::PermissionsExt>::from_mode(0o777),
		)
		.expect("weaken");
		let authority = open(&root, false).expect("repaired directory");
		let mode = std::fs::metadata(&commands).expect("metadata");
		assert_eq!(
			<std::fs::Metadata as std::os::unix::fs::MetadataExt>::mode(&mode) & 0o077,
			0,
			"the directory is repaired owner-only before it is retained"
		);

		let payload = b"{\"marker\":\"retained\"}\n".to_vec();
		assert!(
			authority
				.create_exclusive("tmp.json".to_owned(), Some(Uint8Array::new(payload.clone())), 0o600)
				.ok
		);
		assert!(
			authority
				.rename_entry("tmp.json".to_owned(), "entry.json".to_owned())
				.ok
		);
		let read = authority.read_entry("entry.json".to_owned());
		assert!(read.ok);
		assert_eq!(read.data.expect("data").to_vec(), payload);

		assert!(
			authority
				.create_exclusive("second.tmp".to_owned(), Some(Uint8Array::new(payload)), 0o600)
				.ok
		);
		assert!(
			authority
				.link_entry("second.tmp".to_owned(), "published.json".to_owned())
				.ok
		);
		assert_eq!(
			authority
				.link_entry("second.tmp".to_owned(), "published.json".to_owned())
				.code
				.as_deref(),
			Some("exists")
		);
		assert!(
			authority
				.unlink_entry("second.tmp".to_owned(), None, None)
				.ok
		);

		let identity = authority.stat_entry("entry.json".to_owned());
		let entry = identity.identity.expect("identity");
		assert_eq!(entry.kind, "file");
		assert!(
			authority
				.unlink_entry("entry.json".to_owned(), Some(entry.dev.clone()), Some(entry.ino.clone()))
				.ok
		);
		assert_eq!(
			authority
				.unlink_entry("published.json".to_owned(), Some(entry.dev), Some(entry.ino))
				.code
				.as_deref(),
			Some("identity_mismatch"),
			"an identity-checked removal never retires a successor"
		);

		let listed = authority.list();
		assert!(listed.ok);
		assert_eq!(listed.names.expect("names"), vec!["published.json".to_owned()]);
		assert!(authority.sync_dir().ok);
		std::fs::remove_dir_all(&root).ok();
	}
	/// Retire `name` while a successor is installed at exactly the instant a
	/// naive `prove then unlinkat` protocol cannot defend: after the identity
	/// proof and before the removal.
	fn retire_with_successor(
		authority: &RetainedCommandDir,
		name: &str,
		dev: &str,
		ino: &str,
		install_successor: &dyn Fn(),
	) -> Result<(), &'static str> {
		retire_with_seams(authority, name, dev, ino, &RemovalSeams {
			after_proof:   install_successor,
			after_capture: &|| (),
		})
	}

	fn retire_with_seams(
		authority: &RetainedCommandDir,
		name: &str,
		dev: &str,
		ino: &str,
		seams: &RemovalSeams<'_>,
	) -> Result<(), &'static str> {
		let guard = authority.directory.lock();
		let dir = guard.as_ref().expect("retained directory");
		let entry = entry_name(name).expect("entry name");
		unlink_entry(dir, &entry, Some(dev), Some(ino), seams)
	}

	fn entries_with_prefix(commands: &std::path::Path, prefix: &str) -> Vec<String> {
		let mut found: Vec<String> = std::fs::read_dir(commands)
			.expect("read commands")
			.filter_map(|entry| {
				let name = entry.ok()?.file_name().to_string_lossy().into_owned();
				name.starts_with(prefix).then_some(name)
			})
			.collect();
		found.sort();
		found
	}

	fn identity_of_entry(authority: &RetainedCommandDir, name: &str) -> (String, String) {
		let stat = authority.stat_entry(name.to_owned());
		let identity = stat.identity.expect("identity");
		(identity.dev, identity.ino)
	}

	#[test]
	fn a_successor_installed_after_the_identity_proof_is_never_retired() {
		let root = temp_root();
		let commands = root.join("commands");
		let authority = open(&root, true).expect("retained directory");
		assert!(
			authority
				.create_exclusive(
					"settled.json".to_owned(),
					Some(Uint8Array::new(b"{\"outcome\":\"ok\"}\n".to_vec())),
					0o600
				)
				.ok
		);
		let (dev, ino) = identity_of_entry(&authority, "settled.json");

		// A successor takes the same name outside this protocol, in the window a
		// post-unlink recheck can only observe and never undo.
		std::fs::write(commands.join("successor.tmp"), b"{\"outcome\":\"rejected\"}\n")
			.expect("successor");
		let successor_ino = <std::fs::Metadata as std::os::unix::fs::MetadataExt>::ino(
			&std::fs::metadata(commands.join("successor.tmp")).expect("metadata"),
		);
		let outcome = retire_with_successor(&authority, "settled.json", &dev, &ino, &|| {
			std::fs::rename(commands.join("successor.tmp"), commands.join("settled.json"))
				.expect("install the successor");
		});

		assert_eq!(outcome, Err("identity_mismatch"), "the retirement defers instead of removing");
		let surviving =
			std::fs::metadata(commands.join("settled.json")).expect("the successor still exists");
		assert_eq!(
			<std::fs::Metadata as std::os::unix::fs::MetadataExt>::ino(&surviving),
			successor_ino,
			"the retained terminal settlement is exactly the successor, unretired"
		);
		assert_eq!(
			std::fs::read(commands.join("settled.json")).expect("successor contents"),
			b"{\"outcome\":\"rejected\"}\n".to_vec(),
			"the successor's replay authority is intact"
		);
		std::fs::remove_dir_all(&root).ok();
	}

	/// Repeated non-participating renames: the proven object A is displaced by
	/// B, B is captured for retirement, and a third object C takes the canonical
	/// name while the capture is in flight. The restore may never sacrifice C.
	#[test]
	fn a_third_object_installed_after_the_capture_is_never_clobbered_by_the_restore() {
		let root = temp_root();
		let commands = root.join("commands");
		let authority = open(&root, true).expect("retained directory");
		assert!(
			authority
				.create_exclusive(
					"settled.json".to_owned(),
					Some(Uint8Array::new(b"{\"outcome\":\"a\"}\n".to_vec())),
					0o600
				)
				.ok
		);
		let (dev, ino) = identity_of_entry(&authority, "settled.json");

		std::fs::write(commands.join("b.tmp"), b"{\"outcome\":\"b\"}\n").expect("b");
		std::fs::write(commands.join("c.tmp"), b"{\"outcome\":\"c\"}\n").expect("c");
		let b_ino = <std::fs::Metadata as std::os::unix::fs::MetadataExt>::ino(
			&std::fs::metadata(commands.join("b.tmp")).expect("b metadata"),
		);
		let c_metadata = std::fs::metadata(commands.join("c.tmp")).expect("c metadata");
		let c_dev = <std::fs::Metadata as std::os::unix::fs::MetadataExt>::dev(&c_metadata);
		let c_ino = <std::fs::Metadata as std::os::unix::fs::MetadataExt>::ino(&c_metadata);

		let outcome = {
			let install_b = {
				let commands = commands.clone();
				move || {
					std::fs::rename(commands.join("b.tmp"), commands.join("settled.json"))
						.expect("install B at the canonical name");
				}
			};
			let install_c = {
				let commands = commands.clone();
				move || {
					std::fs::rename(commands.join("c.tmp"), commands.join("settled.json"))
						.expect("install C at the freed canonical name");
				}
			};
			retire_with_seams(&authority, "settled.json", &dev, &ino, &RemovalSeams {
				after_proof:   &install_b,
				after_capture: &install_c,
			})
		};

		assert_eq!(
			outcome,
			Err("identity_unrestored"),
			"a restore that cannot run without clobbering a third object is reported, never forced"
		);
		let surviving = std::fs::metadata(commands.join("settled.json")).expect("C still exists");
		assert_eq!(
			<std::fs::Metadata as std::os::unix::fs::MetadataExt>::dev(&surviving),
			c_dev,
			"C keeps its exact device"
		);
		assert_eq!(
			<std::fs::Metadata as std::os::unix::fs::MetadataExt>::ino(&surviving),
			c_ino,
			"C keeps its exact inode: nothing replaced it"
		);
		assert_eq!(
			std::fs::read(commands.join("settled.json")).expect("C contents"),
			b"{\"outcome\":\"c\"}\n".to_vec(),
			"C's replay authority is intact"
		);

		// B is not deleted: it is quarantined, and quarantine is never listed as
		// dispatchable material.
		let quarantined = entries_with_prefix(&commands, QUARANTINE_PREFIX);
		assert_eq!(quarantined.len(), 1, "the captured object is parked, never destroyed");
		assert_eq!(
			<std::fs::Metadata as std::os::unix::fs::MetadataExt>::ino(
				&std::fs::metadata(commands.join(&quarantined[0])).expect("quarantine metadata")
			),
			b_ino,
			"the quarantined object is exactly the captured B"
		);
		assert_eq!(
			authority.list().names.expect("names"),
			vec!["settled.json".to_owned()],
			"quarantine is never dispatchable"
		);
		assert!(
			entries_with_prefix(&commands, RETIRE_PREFIX).is_empty(),
			"no retirement residue is left behind"
		);

		// A later retention sweep, including one that runs after the capturing
		// process is gone, never reclaims a quarantined object.
		let dead = format!("{QUARANTINE_PREFIX}2147480000.dead.0");
		std::fs::rename(commands.join(&quarantined[0]), commands.join(&dead)).expect("age B");
		authority.close();
		let reopened = open(&root, false).expect("retained directory");
		assert!(
			commands.join(&dead).exists(),
			"a mismatch-captured object is never swept as retirement residue"
		);
		assert_eq!(
			reopened.list().names.expect("names"),
			vec!["settled.json".to_owned()],
			"the successor is still the only dispatchable entry"
		);
		assert!(commands.join("settled.json").exists(), "C survives the sweep");
		std::fs::remove_dir_all(&root).ok();
	}

	#[test]
	fn no_participating_mutation_lands_while_an_identity_bound_decision_is_in_flight() {
		use std::sync::{
			Arc, Mutex as StdMutex,
			atomic::{AtomicBool, Ordering as AtomicOrdering},
		};

		let root = temp_root();
		let commands = root.join("commands");
		let authority = open(&root, true).expect("retained directory");
		assert!(
			authority
				.create_exclusive("settled.json".to_owned(), None, 0o600)
				.ok
		);
		let (dev, ino) = identity_of_entry(&authority, "settled.json");

		let attempted = Arc::new(AtomicBool::new(false));
		let landed = Arc::new(AtomicBool::new(false));
		let worker: Arc<StdMutex<Option<std::thread::JoinHandle<()>>>> =
			Arc::new(StdMutex::new(None));
		let outcome = {
			let attempted = Arc::clone(&attempted);
			let landed = Arc::clone(&landed);
			let worker = Arc::clone(&worker);
			let commands = commands.clone();
			let root = root.clone();
			retire_with_successor(&authority, "settled.json", &dev, &ino, &move || {
				// An independently captured authority: it shares no in-process lock
				// with the decision in flight, so only cross-process serialization
				// can hold it back.
				let rival = open(&root, false).expect("independent retained directory");
				let spawned = {
					let attempted = Arc::clone(&attempted);
					let landed = Arc::clone(&landed);
					std::thread::spawn(move || {
						attempted.store(true, AtomicOrdering::SeqCst);
						let created = rival.create_exclusive("rival.json".to_owned(), None, 0o600);
						landed.store(created.ok, AtomicOrdering::SeqCst);
					})
				};
				*worker.lock().expect("worker slot") = Some(spawned);
				while !attempted.load(AtomicOrdering::SeqCst) {
					std::thread::sleep(Duration::from_millis(1));
				}
				std::thread::sleep(Duration::from_millis(150));
				assert!(
					!landed.load(AtomicOrdering::SeqCst),
					"a participating mutation must not land while the decision is in flight"
				);
				assert!(
					!commands.join("rival.json").exists(),
					"nothing was installed in the directory during the decision"
				);
			})
		};

		assert_eq!(outcome, Ok(()), "the proven object is what was retired");
		let spawned = worker
			.lock()
			.expect("worker slot")
			.take()
			.expect("worker thread");
		spawned.join().expect("worker thread");
		assert!(
			landed.load(AtomicOrdering::SeqCst),
			"the serialized mutation lands once the lock is released"
		);
		assert!(commands.join("rival.json").exists(), "the mutation is only delayed, never lost");
		std::fs::remove_dir_all(&root).ok();
	}

	#[test]
	fn retirement_residue_of_a_dead_process_is_reclaimed_and_never_listed() {
		let root = temp_root();
		let commands = root.join("commands");
		let authority = open(&root, true).expect("retained directory");
		// Residue whose owner is provably gone, and residue whose owner is this
		// still-running process.
		std::fs::write(commands.join(format!("{RETIRE_PREFIX}2147480000.abc.0")), b"{}\n")
			.expect("dead residue");
		std::fs::write(
			commands.join(format!("{RETIRE_PREFIX}{}.abc.1", std::process::id())),
			b"{}\n",
		)
		.expect("live residue");
		assert_eq!(authority.list().names.expect("names"), Vec::<String>::new());
		authority.close();

		let reopened = open(&root, false).expect("retained directory");
		assert_eq!(reopened.list().names.expect("names"), Vec::<String>::new());
		assert!(
			!commands
				.join(format!("{RETIRE_PREFIX}2147480000.abc.0"))
				.exists(),
			"residue of a dead holder is reclaimed"
		);
		assert!(
			commands
				.join(format!("{RETIRE_PREFIX}{}.abc.1", std::process::id()))
				.exists(),
			"residue of a live holder is never touched"
		);
		std::fs::remove_dir_all(&root).ok();
	}
}
