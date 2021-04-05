use crate::directory::directory_layer::DirectoryLayer;
use crate::directory::error::DirectoryError;
use crate::directory::{compare_slice_string, Directory};
use crate::tuple::{PackResult, Subspace, TuplePack, TupleUnpack};
use crate::Transaction;
use async_trait::async_trait;

use std::cmp::Ordering;

/// `DirectorySubspace` is a directory that can act as a subspace
#[derive(Clone, Debug)]
pub struct DirectorySubspace {
    subspace: Subspace,
    directory: DirectoryLayer,
    path: Vec<String>,
    layer: Vec<u8>,
}

impl DirectorySubspace {
    pub fn new(
        subspace: Subspace,
        directory: DirectoryLayer,
        path: Vec<String>,
        layer: Vec<u8>,
    ) -> Self {
        DirectorySubspace {
            subspace,
            directory,
            path,
            layer,
        }
    }
    pub fn subspace<T: TuplePack>(&self, t: &T) -> Subspace {
        self.subspace.subspace(t)
    }
}

#[async_trait]
impl Directory for DirectorySubspace {
    async fn create_or_open(
        &self,
        txn: &Transaction,
        path: Vec<String>,
        prefix: Option<Vec<u8>>,
        layer: Option<Vec<u8>>,
    ) -> Result<DirectorySubspace, DirectoryError> {
        self.directory
            .create_or_open(
                txn,
                self.directory
                    .partition_subpath(self.path.clone(), path.clone()),
                prefix,
                layer,
            )
            .await
    }

    async fn create(
        &self,
        txn: &Transaction,
        path: Vec<String>,
        prefix: Option<Vec<u8>>,
        layer: Option<Vec<u8>>,
    ) -> Result<DirectorySubspace, DirectoryError> {
        self.directory
            .create(
                txn,
                self.directory
                    .partition_subpath(self.path.clone(), path.clone()),
                prefix,
                layer,
            )
            .await
    }

    async fn open(
        &self,
        txn: &Transaction,
        path: Vec<String>,
        layer: Option<Vec<u8>>,
    ) -> Result<DirectorySubspace, DirectoryError> {
        self.directory
            .open(
                txn,
                self.directory
                    .partition_subpath(self.path.clone(), path.clone()),
                layer,
            )
            .await
    }

    async fn exists(&self, trx: &Transaction, path: Vec<String>) -> Result<bool, DirectoryError> {
        self.directory
            .exists(
                trx,
                self.directory
                    .partition_subpath(self.path.clone(), path.clone()),
            )
            .await
    }

    async fn move_directory(
        &self,
        trx: &Transaction,
        new_path: Vec<String>,
    ) -> Result<DirectorySubspace, DirectoryError> {
        // equivalent of moveTo in Go
        // return moveTo(t, d.dl, d.path, newAbsolutePath)
        let partition_length = self.path.len();

        if new_path.is_empty() {
            return Err(DirectoryError::CannotMoveBetweenPartition);
        }

        if compare_slice_string(&new_path[..partition_length], &self.path) != Ordering::Equal {
            Err(DirectoryError::CannotMoveBetweenPartition)
        } else {
            self.move_to(
                trx,
                Vec::from(&self.path[partition_length..]),
                Vec::from(&new_path[partition_length..]),
            )
            .await
        }
    }

    async fn move_to(
        &self,
        trx: &Transaction,
        old_path: Vec<String>,
        new_path: Vec<String>,
    ) -> Result<DirectorySubspace, DirectoryError> {
        self.directory
            .move_to(
                trx,
                self.directory
                    .partition_subpath(self.path.clone(), old_path.clone()),
                self.directory
                    .partition_subpath(self.path.clone(), new_path),
            )
            .await
    }

    async fn remove(&self, trx: &Transaction, path: Vec<String>) -> Result<bool, DirectoryError> {
        self.directory
            .remove(
                trx,
                self.directory.partition_subpath(self.path.clone(), path),
            )
            .await
    }

    async fn list(
        &self,
        trx: &Transaction,
        path: Vec<String>,
    ) -> Result<Vec<String>, DirectoryError> {
        self.directory
            .list(
                trx,
                self.directory.partition_subpath(self.path.clone(), path),
            )
            .await
    }

    fn get_path(&self) -> Vec<String> {
        self.path.clone()
    }

    fn get_layer(&self) -> Vec<u8> {
        self.layer.clone()
    }
}

// Subspace related func
impl DirectorySubspace {
    /// Returns the key encoding the specified Tuple with the prefix of this Subspace
    /// prepended.
    pub fn pack<T: TuplePack>(&self, v: &T) -> Vec<u8> {
        self.subspace.pack(v)
    }

    /// `unpack` returns the Tuple encoded by the given key with the prefix of this Subspace
    /// removed.  `unpack` will return an error if the key is not in this Subspace or does not
    /// encode a well-formed Tuple.
    pub fn unpack<'de, T: TupleUnpack<'de>>(&self, key: &'de [u8]) -> PackResult<T> {
        self.subspace.unpack(key)
    }

    /// `bytes` returns the literal bytes of the prefix of this Subspace.
    pub fn bytes(&self) -> &[u8] {
        self.subspace.bytes()
    }
}
