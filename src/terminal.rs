#[cfg(unix)]
mod platform {
    use std::{
        io,
        os::fd::{AsRawFd, FromRawFd, OwnedFd},
    };
    use tokio::io::{Interest, unix::AsyncFd};

    pub struct Input {
        fd: AsyncFd<OwnedFd>,
        original_flags: i32,
    }

    impl Input {
        pub fn new() -> io::Result<Self> {
            // dup shares status flags with stdin. Restore them when the shell ends.
            let raw = unsafe { libc::dup(libc::STDIN_FILENO) };
            if raw < 0 {
                return Err(io::Error::last_os_error());
            }
            let fd = unsafe { OwnedFd::from_raw_fd(raw) };
            let original_flags = unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_GETFL) };
            if original_flags < 0 {
                return Err(io::Error::last_os_error());
            }
            if unsafe {
                libc::fcntl(
                    fd.as_raw_fd(),
                    libc::F_SETFL,
                    original_flags | libc::O_NONBLOCK,
                )
            } < 0
            {
                return Err(io::Error::last_os_error());
            }
            // OwnedFd keeps this exact descriptor open for the registration's lifetime.
            match unsafe { AsyncFd::register_with_interest(fd, Interest::READABLE) } {
                Ok(fd) => Ok(Self { fd, original_flags }),
                Err(error) => {
                    // Registration may fail after changing the shared flags.
                    unsafe {
                        libc::fcntl(libc::STDIN_FILENO, libc::F_SETFL, original_flags);
                    }
                    Err(error.into())
                }
            }
        }

        pub async fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
            loop {
                let mut readiness = self.fd.readable().await?;
                match readiness.try_io(|fd| {
                    let result = unsafe {
                        libc::read(fd.as_raw_fd(), buffer.as_mut_ptr().cast(), buffer.len())
                    };
                    if result < 0 {
                        Err(io::Error::last_os_error())
                    } else {
                        Ok(result as usize)
                    }
                }) {
                    Ok(result) => return result,
                    Err(_) => continue,
                }
            }
        }
    }

    impl Drop for Input {
        fn drop(&mut self) {
            unsafe {
                libc::fcntl(
                    self.fd.get_ref().as_raw_fd(),
                    libc::F_SETFL,
                    self.original_flags,
                );
            }
        }
    }
}

#[cfg(not(unix))]
mod platform {
    use std::io;
    use tokio::io::AsyncReadExt;

    pub struct Input(tokio::io::Stdin);
    impl Input {
        pub fn new() -> io::Result<Self> {
            Ok(Self(tokio::io::stdin()))
        }
        pub async fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
            self.0.read(buffer).await
        }
    }
}

pub use platform::Input;
