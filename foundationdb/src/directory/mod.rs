// Copyright 2018 foundationdb-rs developers, https://github.com/Clikengo/foundationdb-rs/graphs/contributors
// Copyright 2013-2018 Apple, Inc and the FoundationDB project authors.
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.

pub mod directory;
mod node;

use crate::error;
use crate::tuple::hca::HcaError;
pub use directory::*;
use std::io;

#[derive(Debug)]
pub enum DirectoryError {
    Message(String),
    Version(String),
    NoPathProvided,
    DirAlreadyExists,
    DirNotExists,
    IncompatibleLayer,
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
