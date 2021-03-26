use crate::directory::directory_layer::DirectoryLayer;
use crate::directory::error::DirectoryError;
use crate::directory::Directory;
use crate::tuple::{PackResult, Subspace, TuplePack, TupleUnpack};
use crate::Transaction;
use async_trait::async_trait;

/// `DirectorySubspace` is a directory that can act as a subspace
#[derive(Clone, Debug)]
pub struct DirectorySubspace {
    subspace: Subspace,
    directory: DirectoryLayer,
    path: Vec<String>,
    layer: Vec<u8>,
}

// directory related func
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

    async fn move_to(
        &self,
        trx: &Transaction,
        old_path: Vec<String>,
        new_path: Vec<String>,
    ) -> Result<Subspace, DirectoryError> {
        self.move_to(
            trx,
            self.directory
                .partition_subpath(self.path.clone(), old_path.clone()),
            self.directory
                .partition_subpath(self.path.clone(), new_path),
        )
        .await
    }

    async fn remove(&self, trx: &Transaction, path: Vec<String>) -> Result<bool, DirectoryError> {
        self.remove(
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
        self.list(
            trx,
            self.directory.partition_subpath(self.path.clone(), path),
        )
        .await
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
