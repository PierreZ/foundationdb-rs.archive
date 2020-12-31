// Copyright 2018 foundationdb-rs developers, https://github.com/Clikengo/foundationdb-rs/graphs/contributors
// Copyright 2013-2018 Apple, Inc and the FoundationDB project authors.
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.

//! Errors that can be thrown by Directory.

use crate::error;
use crate::tuple::hca::HcaError;
use std::io;

/// The enumeration holding all possible errors from a Directory.
#[derive(Debug)]
pub enum DirectoryError {
    /// missing path.
    NoPathProvided,
    /// tried to create an already existing path.
    DirAlreadyExists,
    /// missing path.
    DirNotExists,
    /// the layer is incompatible.
    IncompatibleLayer,
    Message(String),
    /// Bad directory version.
    Version(String),
    IoError(io::Error),
    FdbError(error::FdbError),
    HcaError(HcaError),
}

impl From<error::FdbError> for DirectoryError {
    fn from(err: error::FdbError) -> Self {
        DirectoryError::FdbError(err)
    }
}

impl From<HcaError> for DirectoryError {
    fn from(err: HcaError) -> Self {
        DirectoryError::HcaError(err)
    }
}
