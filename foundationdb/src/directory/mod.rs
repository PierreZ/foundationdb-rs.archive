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
pub mod directory_subspace;
pub mod error;
mod node;

use crate::directory::error::DirectoryError;

use crate::Transaction;

use crate::directory::directory_subspace::DirectorySubspace;
use async_trait::async_trait;
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
    ) -> Result<DirectorySubspace, DirectoryError>;
    async fn create(
        &self,
        txn: &Transaction,
        path: Vec<String>,
        prefix: Option<Vec<u8>>,
        layer: Option<Vec<u8>>,
    ) -> Result<DirectorySubspace, DirectoryError>;
    async fn open(
        &self,
        txn: &Transaction,
        path: Vec<String>,
        layer: Option<Vec<u8>>,
    ) -> Result<DirectorySubspace, DirectoryError>;
    async fn exists(&self, trx: &Transaction, path: Vec<String>) -> Result<bool, DirectoryError>;
    async fn move_directory(
        &self,
        trx: &Transaction,
        new_path: Vec<String>,
    ) -> Result<DirectorySubspace, DirectoryError>;
    async fn move_to(
        &self,
        trx: &Transaction,
        old_path: Vec<String>,
        new_path: Vec<String>,
    ) -> Result<DirectorySubspace, DirectoryError>;
    async fn remove(&self, trx: &Transaction, path: Vec<String>) -> Result<bool, DirectoryError>;
    async fn list(
        &self,
        trx: &Transaction,
        path: Vec<String>,
    ) -> Result<Vec<String>, DirectoryError>;

    fn get_path(&self) -> Vec<String>;
    fn get_layer(&self) -> Vec<u8>;
}

pub fn compare_slice_string(a: &[String], b: &[String]) -> cmp::Ordering {
    for (ai, bi) in a.iter().zip(b.iter()) {
        match ai.cmp(&bi) {
            Ordering::Equal => continue,
            ord => return ord,
        }
    }

    /* if every single element was equal, compare length */
    a.len().cmp(&b.len())
}
