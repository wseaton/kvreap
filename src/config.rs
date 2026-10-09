use std::num::NonZeroU64;
use std::path::PathBuf;
use std::time::Duration;

use thiserror::Error;

#[derive(Debug, Error, PartialEq)]
pub enum ConfigError {
    #[error("{var}={value:?} is not a number")]
    NotANumber { var: &'static str, value: String },
    #[error("{var}={value} must be within {min}..={max}")]
    OutOfRange {
        var: &'static str,
        value: f64,
        min: f64,
        max: f64,
    },
    #[error("TARGET_THRESHOLD ({target}) must be below CLEANUP_THRESHOLD ({cleanup})")]
    TargetNotBelowCleanup { target: f64, cleanup: f64 },
    #[error("EMERGENCY_THRESHOLD ({emergency}) must not be below CLEANUP_THRESHOLD ({cleanup})")]
    EmergencyBelowCleanup { emergency: f64, cleanup: f64 },
    #[error("NUM_CRAWLER_PROCESSES must be a power of 2 from 1 to 16, got {0}")]
    InvalidWorkerCount(i64),
}

#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
pub struct Percent(f64);

impl Percent {
    pub fn new(var: &'static str, value: f64) -> Result<Self, ConfigError> {
        if (0.0..=100.0).contains(&value) {
            Ok(Self(value))
        } else {
            Err(ConfigError::OutOfRange {
                var,
                value,
                min: 0.0,
                max: 100.0,
            })
        }
    }

    pub fn get(self) -> f64 {
        self.0
    }
}

/// `f64` parse that rejects NaN and infinities, like the Python evictor's `float()` plus checks.
fn parse_number(var: &'static str, raw: String) -> Result<f64, ConfigError> {
    raw.trim()
        .parse::<f64>()
        .ok()
        .filter(|v| v.is_finite())
        .ok_or(ConfigError::NotANumber { var, value: raw })
}

/// Where liveness and readiness sentinel files live. Parsed on its own so
/// `kvreap healthcheck` does not depend on the rest of the configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HealthConfig {
    /// Local, writable, never on the PVC.
    pub dir: PathBuf,
    /// `healthcheck --live` fails when the heartbeat is older than this.
    pub max_age: Duration,
}

impl HealthConfig {
    pub fn from_lookup(lookup: impl Fn(&str) -> Option<String>) -> Result<Self, ConfigError> {
        let present = |var: &str| lookup(var).filter(|s| !s.is_empty());
        let max_age = match present("HEALTH_MAX_AGE_SECONDS") {
            Some(raw) => parse_max_age("HEALTH_MAX_AGE_SECONDS", raw)?,
            None => Duration::from_secs(30),
        };
        Ok(Self {
            dir: PathBuf::from(present("HEALTH_DIR").unwrap_or_else(|| "/tmp/kvreap".into())),
            max_age,
        })
    }
}

/// Whole seconds, at least 1 (`int(float(raw))`).
pub fn parse_max_age(var: &'static str, raw: String) -> Result<Duration, ConfigError> {
    let secs = parse_number(var, raw)?.trunc();
    if secs < 1.0 {
        return Err(ConfigError::OutOfRange {
            var,
            value: secs,
            min: 1.0,
            max: f64::MAX,
        });
    }
    Ok(Duration::from_secs_f64(secs))
}

/// Size of the volume in bytes, for filesystems whose `statvfs` does not report it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CapacityBytes(NonZeroU64);

impl CapacityBytes {
    fn parse(var: &'static str, raw: &str) -> Result<Self, ConfigError> {
        let n = raw
            .trim()
            .parse::<u64>()
            .map_err(|_| ConfigError::NotANumber {
                var,
                value: raw.to_string(),
            })?;
        NonZeroU64::new(n).map(Self).ok_or(ConfigError::OutOfRange {
            var,
            value: 0.0,
            min: 1.0,
            max: u64::MAX as f64,
        })
    }

    pub fn get(self) -> u64 {
        self.0.get()
    }
}

/// Number of worker threads, one per hex-modulo shard (mirrors NUM_CRAWLER_PROCESSES).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WorkerCount(u8);

impl WorkerCount {
    pub fn new(n: i64) -> Result<Self, ConfigError> {
        match n {
            1 | 2 | 4 | 8 | 16 => u8::try_from(n)
                .map(Self)
                .map_err(|_| ConfigError::InvalidWorkerCount(n)),
            _ => Err(ConfigError::InvalidWorkerCount(n)),
        }
    }

    pub fn get(self) -> u8 {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogLevel {
    Debug,
    Info,
    Warning,
    Error,
}

impl LogLevel {
    /// Same leniency as Python's `getattr(logging, level.upper(), logging.INFO)`.
    fn parse(raw: &str) -> Self {
        match raw.to_ascii_uppercase().as_str() {
            "DEBUG" => Self::Debug,
            "WARNING" | "WARN" => Self::Warning,
            "ERROR" | "CRITICAL" => Self::Error,
            _ => Self::Info,
        }
    }

    pub fn as_filter(self) -> &'static str {
        match self {
            Self::Debug => "debug",
            Self::Info => "info",
            Self::Warning => "warn",
            Self::Error => "error",
        }
    }
}

#[derive(Debug, Clone)]
pub struct Config {
    pub pvc_mount_path: PathBuf,
    pub cache_directory: PathBuf,
    pub cleanup_threshold: Percent,
    pub target_threshold: Percent,
    /// Usage at which pacing is dropped.
    pub emergency_threshold: Percent,
    pub dry_run: bool,
    pub log_level: LogLevel,
    pub workers: WorkerCount,
    pub usage_poll_interval: Duration,
    pub event_batch_size: usize,
    /// 0 means unlimited.
    pub max_files_per_second: f64,
    pub hot_threshold: Duration,
    pub hex_bucket_len: usize,
    pub enable_dir_cleanup: bool,
    pub dir_cleanup_ttl: Duration,
    pub log_file_path: Option<PathBuf>,
    pub storage_events_endpoint: Option<String>,
    /// When set, usage is estimated from bucket samples against this size instead of `statvfs`.
    pub capacity_bytes: Option<CapacityBytes>,
    pub health: HealthConfig,
}

/// Default emergency band floor; the default band is `max(97, CLEANUP_THRESHOLD)`.
const DEFAULT_EMERGENCY_FLOOR: f64 = 97.0;

const IGNORED_VARS: [&str; 2] = ["FILE_QUEUE_MAXSIZE", "FILE_QUEUE_MIN_SIZE"];

impl Config {
    pub fn cache_path(&self) -> PathBuf {
        self.pvc_mount_path.join(&self.cache_directory)
    }

    pub fn from_env() -> Result<Self, ConfigError> {
        Self::from_lookup(|k| std::env::var(k).ok())
    }

    /// Variables accepted for compatibility with the Python evictor but unused here.
    pub fn ignored_vars_set(lookup: impl Fn(&str) -> Option<String>) -> Vec<&'static str> {
        IGNORED_VARS
            .into_iter()
            .filter(|v| lookup(v).is_some_and(|s| !s.is_empty()))
            .collect()
    }

    pub fn from_lookup(lookup: impl Fn(&str) -> Option<String>) -> Result<Self, ConfigError> {
        let get = |var: &'static str, default: &str| -> String {
            lookup(var).unwrap_or_else(|| default.to_string())
        };
        let num = |var: &'static str, default: &str| parse_number(var, get(var, default));
        let int = |var: &'static str, default: &str| -> Result<i64, ConfigError> {
            num(var, default).map(|v| v.trunc() as i64)
        };
        let non_negative = |var: &'static str, v: f64| -> Result<f64, ConfigError> {
            if v >= 0.0 {
                Ok(v)
            } else {
                Err(ConfigError::OutOfRange {
                    var,
                    value: v,
                    min: 0.0,
                    max: f64::MAX,
                })
            }
        };
        let flag =
            |var: &'static str, default: &str| get(var, default).eq_ignore_ascii_case("true");
        let optional = |var: &'static str| lookup(var).filter(|s| !s.is_empty());

        let cleanup = Percent::new("CLEANUP_THRESHOLD", num("CLEANUP_THRESHOLD", "85.0")?)?;
        let target = Percent::new("TARGET_THRESHOLD", num("TARGET_THRESHOLD", "70.0")?)?;
        if target >= cleanup {
            return Err(ConfigError::TargetNotBelowCleanup {
                target: target.get(),
                cleanup: cleanup.get(),
            });
        }

        let emergency = Percent::new(
            "EMERGENCY_THRESHOLD",
            match optional("EMERGENCY_THRESHOLD") {
                Some(raw) => parse_number("EMERGENCY_THRESHOLD", raw)?,
                None => cleanup.get().max(DEFAULT_EMERGENCY_FLOOR),
            },
        )?;
        if emergency < cleanup {
            return Err(ConfigError::EmergencyBelowCleanup {
                emergency: emergency.get(),
                cleanup: cleanup.get(),
            });
        }

        for var in IGNORED_VARS {
            int(var, "0")?;
        }

        let poll = num("LOGGER_INTERVAL_SECONDS", "0.5")?;
        if poll <= 0.0 {
            return Err(ConfigError::OutOfRange {
                var: "LOGGER_INTERVAL_SECONDS",
                value: poll,
                min: f64::MIN_POSITIVE,
                max: f64::MAX,
            });
        }

        let batch = int("DELETION_BATCH_SIZE", "100")?;
        let hex_len = int("HEX_BUCKET_LEN", "3")?;
        if !(1..=4).contains(&hex_len) {
            return Err(ConfigError::OutOfRange {
                var: "HEX_BUCKET_LEN",
                value: hex_len as f64,
                min: 1.0,
                max: 4.0,
            });
        }

        Ok(Self {
            pvc_mount_path: PathBuf::from(get("PVC_MOUNT_PATH", "/kv-cache")),
            cache_directory: PathBuf::from(get("CACHE_DIRECTORY", "kv/model-cache/models")),
            cleanup_threshold: cleanup,
            target_threshold: target,
            emergency_threshold: emergency,
            dry_run: flag("DRY_RUN", "false"),
            log_level: LogLevel::parse(&get("LOG_LEVEL", "INFO")),
            workers: WorkerCount::new(int("NUM_CRAWLER_PROCESSES", "8")?)?,
            usage_poll_interval: Duration::from_secs_f64(poll),
            event_batch_size: usize::try_from(batch.max(1)).unwrap_or(1),
            max_files_per_second: non_negative(
                "DELETION_MAX_FILES_PER_SECOND",
                num("DELETION_MAX_FILES_PER_SECOND", "0")?,
            )?,
            hot_threshold: Duration::from_secs_f64(
                non_negative(
                    "FILE_ACCESS_TIME_THRESHOLD_MINUTES",
                    num("FILE_ACCESS_TIME_THRESHOLD_MINUTES", "60.0")?,
                )? * 60.0,
            ),
            hex_bucket_len: usize::try_from(hex_len).unwrap_or(3),
            enable_dir_cleanup: flag("ENABLE_DIR_CLEANUP", "true"),
            dir_cleanup_ttl: Duration::from_secs_f64(non_negative(
                "DIR_CLEANUP_TTL_SECONDS",
                num("DIR_CLEANUP_TTL_SECONDS", "120.0")?,
            )?),
            log_file_path: optional("LOG_FILE_PATH").map(PathBuf::from),
            storage_events_endpoint: optional("STORAGE_EVENTS_ENDPOINT"),
            capacity_bytes: optional("CAPACITY_BYTES")
                .map(|raw| CapacityBytes::parse("CAPACITY_BYTES", &raw))
                .transpose()?,
            health: HealthConfig::from_lookup(&lookup)?,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use std::time::Duration;

    use crate::config::{Config, ConfigError, HealthConfig, LogLevel};

    fn cfg(pairs: &[(&str, &str)]) -> Result<Config, ConfigError> {
        let env: HashMap<String, String> = pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        Config::from_lookup(|k| env.get(k).cloned())
    }

    #[test]
    fn defaults_match_python_evictor() {
        let c = cfg(&[]).expect("defaults parse");
        assert_eq!(c.pvc_mount_path.to_str(), Some("/kv-cache"));
        assert_eq!(
            c.cache_path().to_str(),
            Some("/kv-cache/kv/model-cache/models")
        );
        assert_eq!(c.cleanup_threshold.get(), 85.0);
        assert_eq!(c.target_threshold.get(), 70.0);
        assert_eq!(c.emergency_threshold.get(), 97.0);
        assert!(!c.dry_run);
        assert_eq!(c.log_level, LogLevel::Info);
        assert_eq!(c.workers.get(), 8);
        assert_eq!(c.usage_poll_interval.as_secs_f64(), 0.5);
        assert_eq!(c.event_batch_size, 100);
        assert_eq!(c.max_files_per_second, 0.0);
        assert_eq!(c.hot_threshold.as_secs(), 3600);
        assert_eq!(c.hex_bucket_len, 3);
        assert!(c.enable_dir_cleanup);
        assert_eq!(c.dir_cleanup_ttl.as_secs(), 120);
        assert_eq!(c.log_file_path, None);
        assert_eq!(c.storage_events_endpoint, None);
        assert_eq!(c.capacity_bytes, None);
        assert_eq!(c.health.dir.to_str(), Some("/tmp/kvreap"));
        assert_eq!(c.health.max_age, Duration::from_secs(30));
    }

    #[test]
    fn helm_chart_values_parse() {
        // Values exactly as the chart renders them (every value is quoted).
        let c = cfg(&[
            ("PVC_MOUNT_PATH", "/kv-cache"),
            ("CLEANUP_THRESHOLD", "85"),
            ("TARGET_THRESHOLD", "70"),
            ("CACHE_DIRECTORY", "kv/model-cache/models"),
            ("NUM_CRAWLER_PROCESSES", "8"),
            ("LOGGER_INTERVAL_SECONDS", "0.5"),
            ("FILE_QUEUE_MAXSIZE", "10000"),
            ("FILE_QUEUE_MIN_SIZE", "1000"),
            ("DELETION_BATCH_SIZE", "100"),
            ("DELETION_MAX_FILES_PER_SECOND", "250"),
            ("FILE_ACCESS_TIME_THRESHOLD_MINUTES", "60"),
            ("ENABLE_DIR_CLEANUP", "true"),
            ("DIR_CLEANUP_TTL_SECONDS", "120"),
            ("DRY_RUN", "false"),
            ("LOG_LEVEL", "INFO"),
            ("LOG_FILE_PATH", "/tmp/evictor_all_logs.txt"),
        ])
        .expect("chart values parse");
        assert_eq!(c.max_files_per_second, 250.0);
        assert_eq!(
            c.log_file_path.as_deref().and_then(|p| p.to_str()),
            Some("/tmp/evictor_all_logs.txt")
        );
    }

    #[test]
    fn integers_truncate_like_python_int_float() {
        let c = cfg(&[
            ("NUM_CRAWLER_PROCESSES", "4.9"),
            ("DELETION_BATCH_SIZE", "250.7"),
        ])
        .expect("parse");
        assert_eq!(c.workers.get(), 4);
        assert_eq!(c.event_batch_size, 250);
    }

    #[test]
    fn booleans_only_true_is_true() {
        let c = cfg(&[("DRY_RUN", "TRUE"), ("ENABLE_DIR_CLEANUP", "yes")]).expect("parse");
        assert!(c.dry_run);
        assert!(!c.enable_dir_cleanup);
    }

    #[test]
    fn empty_optional_strings_are_none() {
        let c = cfg(&[("LOG_FILE_PATH", ""), ("STORAGE_EVENTS_ENDPOINT", "")]).expect("parse");
        assert_eq!(c.log_file_path, None);
        assert_eq!(c.storage_events_endpoint, None);
    }

    #[test]
    fn unknown_log_level_falls_back_to_info() {
        assert_eq!(
            cfg(&[("LOG_LEVEL", "chatty")]).expect("parse").log_level,
            LogLevel::Info
        );
        assert_eq!(
            cfg(&[("LOG_LEVEL", "debug")]).expect("parse").log_level,
            LogLevel::Debug
        );
    }

    #[test]
    fn rejects_invalid_worker_count() {
        assert_eq!(
            cfg(&[("NUM_CRAWLER_PROCESSES", "3")]).err(),
            Some(ConfigError::InvalidWorkerCount(3))
        );
        assert_eq!(
            cfg(&[("NUM_CRAWLER_PROCESSES", "32")]).err(),
            Some(ConfigError::InvalidWorkerCount(32))
        );
    }

    #[test]
    fn rejects_target_at_or_above_cleanup() {
        assert!(matches!(
            cfg(&[("CLEANUP_THRESHOLD", "70"), ("TARGET_THRESHOLD", "70")]),
            Err(ConfigError::TargetNotBelowCleanup { .. })
        ));
    }

    #[test]
    fn rejects_garbage_numbers() {
        assert!(matches!(
            cfg(&[("CLEANUP_THRESHOLD", "lots")]),
            Err(ConfigError::NotANumber {
                var: "CLEANUP_THRESHOLD",
                ..
            })
        ));
        assert!(matches!(
            cfg(&[("FILE_QUEUE_MAXSIZE", "nan")]),
            Err(ConfigError::NotANumber {
                var: "FILE_QUEUE_MAXSIZE",
                ..
            })
        ));
    }

    #[test]
    fn rejects_negative_rates_and_out_of_range_percent() {
        assert!(matches!(
            cfg(&[("DELETION_MAX_FILES_PER_SECOND", "-1")]),
            Err(ConfigError::OutOfRange { .. })
        ));
        assert!(matches!(
            cfg(&[("CLEANUP_THRESHOLD", "101")]),
            Err(ConfigError::OutOfRange { .. })
        ));
    }

    #[test]
    fn emergency_threshold_defaults_to_max_of_97_and_cleanup() {
        let c = cfg(&[("CLEANUP_THRESHOLD", "98.5"), ("TARGET_THRESHOLD", "90")]).expect("parse");
        assert_eq!(c.emergency_threshold.get(), 98.5);
        let c = cfg(&[("EMERGENCY_THRESHOLD", "")]).expect("parse");
        assert_eq!(c.emergency_threshold.get(), 97.0);
    }

    #[test]
    fn emergency_threshold_is_configurable_down_to_cleanup() {
        let c = cfg(&[("EMERGENCY_THRESHOLD", "90")]).expect("parse");
        assert_eq!(c.emergency_threshold.get(), 90.0);
        let c = cfg(&[("EMERGENCY_THRESHOLD", "85")]).expect("parse");
        assert_eq!(c.emergency_threshold.get(), 85.0);
        let c = cfg(&[("EMERGENCY_THRESHOLD", "100")]).expect("parse");
        assert_eq!(c.emergency_threshold.get(), 100.0);
    }

    #[test]
    fn emergency_threshold_rejects_bad_values() {
        assert_eq!(
            cfg(&[("EMERGENCY_THRESHOLD", "80")]).err(),
            Some(ConfigError::EmergencyBelowCleanup {
                emergency: 80.0,
                cleanup: 85.0
            })
        );
        assert!(matches!(
            cfg(&[("EMERGENCY_THRESHOLD", "101")]),
            Err(ConfigError::OutOfRange {
                var: "EMERGENCY_THRESHOLD",
                ..
            })
        ));
        assert!(matches!(
            cfg(&[("EMERGENCY_THRESHOLD", "soon")]),
            Err(ConfigError::NotANumber {
                var: "EMERGENCY_THRESHOLD",
                ..
            })
        ));
    }

    #[test]
    fn health_config_parses_on_its_own() {
        let env: HashMap<&str, &str> = [
            ("HEALTH_DIR", "/run/kvreap"),
            ("HEALTH_MAX_AGE_SECONDS", "12.9"),
            ("CLEANUP_THRESHOLD", "lots"),
        ]
        .into_iter()
        .collect();
        let h = HealthConfig::from_lookup(|k| env.get(k).map(|v| v.to_string()))
            .expect("health config ignores unrelated vars");
        assert_eq!(h.dir.to_str(), Some("/run/kvreap"));
        assert_eq!(h.max_age, Duration::from_secs(12));
        let c = cfg(&[("HEALTH_MAX_AGE_SECONDS", "45"), ("HEALTH_DIR", "/x")]).expect("parse");
        assert_eq!(c.health.max_age, Duration::from_secs(45));
        assert_eq!(c.health.dir.to_str(), Some("/x"));
    }

    #[test]
    fn health_max_age_must_be_at_least_a_second() {
        for raw in ["0", "0.9", "-5"] {
            assert!(
                matches!(
                    cfg(&[("HEALTH_MAX_AGE_SECONDS", raw)]),
                    Err(ConfigError::OutOfRange {
                        var: "HEALTH_MAX_AGE_SECONDS",
                        ..
                    })
                ),
                "{raw}"
            );
        }
        assert!(matches!(
            cfg(&[("HEALTH_MAX_AGE_SECONDS", "soon")]),
            Err(ConfigError::NotANumber { .. })
        ));
    }

    #[test]
    fn capacity_bytes_parses_plain_byte_counts() {
        let c = cfg(&[("CAPACITY_BYTES", " 21474836480 ")]).expect("parse");
        assert_eq!(c.capacity_bytes.map(|b| b.get()), Some(21_474_836_480));
        let c = cfg(&[("CAPACITY_BYTES", "")]).expect("parse");
        assert_eq!(c.capacity_bytes, None);
        assert_eq!(c.health.dir.to_str(), Some("/tmp/kvreap"));
        assert_eq!(c.health.max_age, Duration::from_secs(30));
    }

    #[test]
    fn capacity_bytes_rejects_zero_fractions_and_suffixes() {
        assert!(matches!(
            cfg(&[("CAPACITY_BYTES", "0")]),
            Err(ConfigError::OutOfRange {
                var: "CAPACITY_BYTES",
                ..
            })
        ));
        for raw in ["20Gi", "1.5", "-1", "lots"] {
            assert!(
                matches!(
                    cfg(&[("CAPACITY_BYTES", raw)]),
                    Err(ConfigError::NotANumber {
                        var: "CAPACITY_BYTES",
                        ..
                    })
                ),
                "{raw}"
            );
        }
    }

    #[test]
    fn reports_ignored_compat_vars() {
        let env: HashMap<&str, &str> = [("FILE_QUEUE_MAXSIZE", "10000")].into_iter().collect();
        assert_eq!(
            Config::ignored_vars_set(|k| env.get(k).map(|v| v.to_string())),
            vec!["FILE_QUEUE_MAXSIZE"]
        );
    }
}
