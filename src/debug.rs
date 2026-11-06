use std::{
    fmt,
    io::{self, Write},
    sync::{OnceLock, atomic::{AtomicBool, Ordering}},
    time::Instant,
};

static ENABLED: AtomicBool = AtomicBool::new(false);
static START: OnceLock<Instant> = OnceLock::new();

pub fn init(enabled: bool) {
    START.get_or_init(Instant::now);
    ENABLED.store(enabled, Ordering::Relaxed);
}

pub fn log(message: fmt::Arguments<'_>) {
    if ENABLED.load(Ordering::Relaxed) {
        let elapsed = START.get_or_init(Instant::now).elapsed().as_millis();
        write_stderr(format_args!("[debug +{elapsed}ms] {message}"));
    }
}

pub fn write_stderr(message: fmt::Arguments<'_>) {
    let _ = writeln!(io::stderr().lock(), "{message}");
}

#[macro_export]
macro_rules! debug_log {
    ($($arg:tt)*) => {
        $crate::debug::log(format_args!($($arg)*))
    };
}
