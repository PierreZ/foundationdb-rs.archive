// Copyright 2018 foundationdb-rs developers, https://github.com/Clikengo/foundationdb-rs/graphs/contributors
// Copyright 2013-2018 Apple, Inc and the FoundationDB project authors.
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.

//! Implementation of the official Directory layer.
//!
//! The FoundationDB API provides directories as a tool for managing related Subspaces.
//! For general guidance on directory usage, see the discussion in the [Developer Guide](https://apple.github.io/foundationdb/developer-guide.html#directories).
//!
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

#[async_trait]
pub trait Directory {
    async fn create_or_open(
        &self,
        txn: &Transaction,
        path: Vec<String>,
        prefix: Option<Vec<u8>>,
        layer: Option<Vec<u8>>,
    ) -> Result<DirectoryOutput, DirectoryError>;

    async fn create(
        &self,
        txn: &Transaction,
        path: Vec<String>,
        prefix: Option<Vec<u8>>,
        layer: Option<Vec<u8>>,
    ) -> Result<DirectoryOutput, DirectoryError>;
    async fn open(
        &self,
        txn: &Transaction,
        path: Vec<String>,
        layer: Option<Vec<u8>>,
    ) -> Result<DirectoryOutput, DirectoryError>;
    async fn exists(&self, trx: &Transaction, path: Vec<String>) -> Result<bool, DirectoryError>;
    async fn move_directory(
        &self,
        trx: &Transaction,
        new_path: Vec<String>,
    ) -> Result<DirectoryOutput, DirectoryError>;
    async fn move_to(
        &self,
        trx: &Transaction,
        old_path: Vec<String>,
        new_path: Vec<String>,
    ) -> Result<DirectoryOutput, DirectoryError>;
    async fn remove(&self, trx: &Transaction, path: Vec<String>) -> Result<bool, DirectoryError>;
    async fn list(
        &self,
        trx: &Transaction,
        path: Vec<String>,
    ) -> Result<Vec<String>, DirectoryError>;

    //  fn get_path(&self) -> Vec<String>;
    //  fn get_layer(&self) -> Vec<u8>;
}

pub fn compare_slice<T: Ord>(a: &[T], b: &[T]) -> cmp::Ordering {
    for (ai, bi) in a.iter().zip(b.iter()) {
        match ai.cmp(&bi) {
            Ordering::Equal => continue,
            ord => return ord,
        }
    }

    /* if every single element was equal, compare length */
    a.len().cmp(&b.len())
}

// TODO: Find a better name
pub enum DirectoryOutput {
    DirectorySubspace(DirectorySubspace),
    DirectoryPartition(DirectoryPartition),
}

// TODO: should we have a Subspace trait?
impl DirectoryOutput {
    fn subspace<T: TuplePack>(&self, t: &T) -> Subspace {
        unimplemented!()
    }

    fn bytes(&self) -> &[u8] {
        unimplemented!()
    }

    fn pack<T: TuplePack>(&self, t: &T) -> Vec<u8> {
        unimplemented!()
    }

    fn unpack<'de, T: TupleUnpack<'de>>(&self, key: &'de [u8]) -> PackResult<T> {
        unimplemented!()
    }

    fn range(&self) -> (Vec<u8>, Vec<u8>) {
        unimplemented!()
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
