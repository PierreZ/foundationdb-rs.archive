// Copyright 2018 foundationdb-rs developers, https://github.com/Clikengo/foundationdb-rs/graphs/contributors
// Copyright 2013-2018 Apple, Inc and the FoundationDB project authors.
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.

/// Implementation of the official Directory layer.
///
/// The FoundationDB API provides directories as a tool for managing related Subspaces.
/// For general guidance on directory usage, see the discussion in the [Developer Guide](https://apple.github.io/foundationdb/developer-guide.html#directories).
///
pub mod directory_layer;
mod directory_partition;
pub mod directory_subspace;
pub mod error;
pub(crate) mod node;

use crate::directory::directory_subspace::DirectorySubspace;
use crate::directory::error::DirectoryError;
use async_trait::async_trait;

use crate::Transaction;

use crate::directory::directory_partition::DirectoryPartition;
use crate::tuple::{PackResult, Subspace, TuplePack, TupleUnpack};
use core::cmp;
use std::cmp::Ordering;
use std::fmt::Formatter;

/// Represents a directory in the DirectoryLayer. A Directory stores the path at which it is located
/// and the layer that was used to create it. The Directory interface contains methods to operate
/// on itself and its subdirectories.
#[async_trait]
pub trait Directory {
    /// Creates or opens the subdirectory of this Directory located at subpath (creating parent directories, if necessary).
    async fn create_or_open(
        &self,
        txn: &Transaction,
        path: Vec<String>,
        prefix: Option<Vec<u8>>,
        layer: Option<Vec<u8>>,
    ) -> Result<DirectoryOutput, DirectoryError>;

    /// Creates a subdirectory of this Directory located at subpath (creating parent directories if necessary).
    async fn create(
        &self,
        txn: &Transaction,
        path: Vec<String>,
        prefix: Option<Vec<u8>>,
        layer: Option<Vec<u8>>,
    ) -> Result<DirectoryOutput, DirectoryError>;

    /// Opens the subdirectory of this Directory located at subpath.
    async fn open(
        &self,
        txn: &Transaction,
        path: Vec<String>,
        layer: Option<Vec<u8>>,
    ) -> Result<DirectoryOutput, DirectoryError>;

    /// Checks if the subdirectory of this Directory located at subpath exists.
    async fn exists(&self, trx: &Transaction, path: Vec<String>) -> Result<bool, DirectoryError>;

    /// Moves this Directory to the specified newAbsolutePath.
    async fn move_directory(
        &self,
        trx: &Transaction,
        new_path: Vec<String>,
    ) -> Result<DirectoryOutput, DirectoryError>;

    /// Moves the subdirectory of this Directory located at oldSubpath to newSubpath.
    async fn move_to(
        &self,
        trx: &Transaction,
        old_path: Vec<String>,
        new_path: Vec<String>,
    ) -> Result<DirectoryOutput, DirectoryError>;

    /// Removes the subdirectory of this Directory located at subpath and all of its subdirectories, as well as all of their contents.
    async fn remove(&self, trx: &Transaction, path: Vec<String>) -> Result<bool, DirectoryError>;

    /// List the subdirectories of this directory at a given subpath.
    async fn list(
        &self,
        trx: &Transaction,
        path: Vec<String>,
    ) -> Result<Vec<String>, DirectoryError>;
}

pub fn compare_slice<T: Ord>(a: &[T], b: &[T]) -> cmp::Ordering {
    for (ai, bi) in a.iter().zip(b.iter()) {
        match ai.cmp(&bi) {
            Ordering::Equal => continue,
            ord => return ord,
        }
    }

    // if every single element was equal, compare length
    a.len().cmp(&b.len())
}

// TODO: Find a better name
/// DirectoryOutput represents the differents output of a Directory.
#[derive(Clone, Debug)]
pub enum DirectoryOutput {
    DirectorySubspace(DirectorySubspace),
    DirectoryPartition(DirectoryPartition),
}

// TODO: should we have a Subspace trait?
impl DirectoryOutput {
    pub fn subspace<T: TuplePack>(&self, t: &T) -> Subspace {
        match self {
            DirectoryOutput::DirectorySubspace(d) => d.subspace(t),
            DirectoryOutput::DirectoryPartition(_) => {
                panic!("cannot open subspace in the root of a directory partition")
            }
        }
    }

    pub fn bytes(&self) -> &[u8] {
        match self {
            DirectoryOutput::DirectorySubspace(d) => d.bytes(),
            DirectoryOutput::DirectoryPartition(_) => {
                panic!("cannot get key for the root of a directory partition")
            }
        }
    }

    pub fn pack<T: TuplePack>(&self, t: &T) -> Vec<u8> {
        match self {
            DirectoryOutput::DirectorySubspace(d) => d.pack(t),
            DirectoryOutput::DirectoryPartition(_) => {
                panic!("cannot pack for the root of a directory partition")
            }
        }
    }

    pub fn unpack<'de, T: TupleUnpack<'de>>(&self, key: &'de [u8]) -> PackResult<T> {
        match self {
            DirectoryOutput::DirectorySubspace(d) => d.unpack(key),
            DirectoryOutput::DirectoryPartition(_) => {
                panic!("cannot unpack keys using the root of a directory partition")
            }
        }
    }

    pub fn range(&self) -> (Vec<u8>, Vec<u8>) {
        match self {
            DirectoryOutput::DirectorySubspace(d) => d.range(),
            DirectoryOutput::DirectoryPartition(_) => {
                panic!("cannot get range for the root of a directory partition")
            }
        }
    }

    pub fn get_path(&self) -> Vec<String> {
        match self {
            DirectoryOutput::DirectorySubspace(d) => d.get_path(),
            DirectoryOutput::DirectoryPartition(d) => d.get_path(),
        }
    }

    pub fn get_layer(&self) -> Vec<u8> {
        match self {
            DirectoryOutput::DirectorySubspace(d) => d.get_layer(),
            DirectoryOutput::DirectoryPartition(d) => d.get_layer(),
        }
    }
}

#[async_trait]
impl Directory for DirectoryOutput {
    async fn create_or_open(
        &self,
        txn: &Transaction,
        path: Vec<String>,
        prefix: Option<Vec<u8>>,
        layer: Option<Vec<u8>>,
    ) -> Result<DirectoryOutput, DirectoryError> {
        match self {
            DirectoryOutput::DirectorySubspace(d) => {
                d.create_or_open(txn, path, prefix, layer).await
            }
            DirectoryOutput::DirectoryPartition(d) => {
                d.create_or_open(txn, path, prefix, layer).await
            }
        }
    }

    async fn create(
        &self,
        txn: &Transaction,
        path: Vec<String>,
        prefix: Option<Vec<u8>>,
        layer: Option<Vec<u8>>,
    ) -> Result<DirectoryOutput, DirectoryError> {
        match self {
            DirectoryOutput::DirectorySubspace(d) => d.create(txn, path, prefix, layer).await,
            DirectoryOutput::DirectoryPartition(d) => d.create(txn, path, prefix, layer).await,
        }
    }

    async fn open(
        &self,
        txn: &Transaction,
        path: Vec<String>,
        layer: Option<Vec<u8>>,
    ) -> Result<DirectoryOutput, DirectoryError> {
        match self {
            DirectoryOutput::DirectorySubspace(d) => d.open(txn, path, layer).await,
            DirectoryOutput::DirectoryPartition(d) => d.open(txn, path, layer).await,
        }
    }

    async fn exists(&self, trx: &Transaction, path: Vec<String>) -> Result<bool, DirectoryError> {
        match self {
            DirectoryOutput::DirectorySubspace(d) => d.exists(trx, path).await,
            DirectoryOutput::DirectoryPartition(d) => d.exists(trx, path).await,
        }
    }

    async fn move_directory(
        &self,
        trx: &Transaction,
        new_path: Vec<String>,
    ) -> Result<DirectoryOutput, DirectoryError> {
        match self {
            DirectoryOutput::DirectorySubspace(d) => d.move_directory(trx, new_path).await,
            DirectoryOutput::DirectoryPartition(d) => d.move_directory(trx, new_path).await,
        }
    }

    async fn move_to(
        &self,
        trx: &Transaction,
        old_path: Vec<String>,
        new_path: Vec<String>,
    ) -> Result<DirectoryOutput, DirectoryError> {
        match self {
            DirectoryOutput::DirectorySubspace(d) => d.move_to(trx, old_path, new_path).await,
            DirectoryOutput::DirectoryPartition(d) => d.move_to(trx, old_path, new_path).await,
        }
    }

    async fn remove(&self, trx: &Transaction, path: Vec<String>) -> Result<bool, DirectoryError> {
        match self {
            DirectoryOutput::DirectorySubspace(d) => d.remove(trx, path).await,
            DirectoryOutput::DirectoryPartition(d) => d.remove(trx, path).await,
        }
    }

    async fn list(
        &self,
        trx: &Transaction,
        path: Vec<String>,
    ) -> Result<Vec<String>, DirectoryError> {
        match self {
            DirectoryOutput::DirectorySubspace(d) => d.list(trx, path).await,
            DirectoryOutput::DirectoryPartition(d) => d.list(trx, path).await,
        }
    }
}

pub(crate) fn strinc(key: Vec<u8>) -> Vec<u8> {
    let mut key = key.clone();
    for i in (0..key.len()).rev() {
        if key[i] != 0xff {
            key[i] += 1;
            return Vec::from(key);
        }
    }
    panic!("failed to strinc");
}
