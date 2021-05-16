use crate::directory::directory_layer::DirectoryLayer;
use crate::directory::error::DirectoryError;
use crate::directory::{Directory, DirectoryOutput};
use crate::tuple::{PackResult, Subspace, TuplePack, TupleUnpack};
use crate::Transaction;
use async_trait::async_trait;

#[derive(Debug, Clone)]
pub struct DirectorySubspace {
    directory_layer: DirectoryLayer,
    subspace: Subspace,
    path: Vec<String>,
    layer: Vec<u8>,
}

impl DirectorySubspace {
    pub fn new(
        path: Vec<String>,
        prefix: Vec<u8>,
        directory_layer: &DirectoryLayer,
        layer: Vec<u8>,
    ) -> Self {
        DirectorySubspace {
            directory_layer: directory_layer.clone(),
            subspace: Subspace::from_bytes(&prefix),
            path,
            layer,
        }
    }

    // https://github.com/apple/foundationdb/blob/master/bindings/flow/DirectorySubspace.cpp#L105
    fn get_partition_subpath(&self, path: Vec<String>) -> Vec<String> {
        let mut new_path = vec![];

        new_path.extend_from_slice(&self.path);
        new_path.extend_from_slice(&path);

        new_path
    }
}

impl DirectorySubspace {
    pub fn subspace<T: TuplePack>(&self, t: &T) -> Subspace {
        self.subspace.subspace(t)
    }

    pub fn bytes(&self) -> &[u8] {
        self.subspace.bytes()
    }

    pub fn pack<T: TuplePack>(&self, t: &T) -> Vec<u8> {
        self.subspace.pack(t)
    }

    pub fn unpack<'de, T: TupleUnpack<'de>>(&self, key: &'de [u8]) -> PackResult<T> {
        self.subspace.unpack(key)
    }

    pub fn range(&self) -> (Vec<u8>, Vec<u8>) {
        self.subspace.range()
    }

    pub fn get_path(&self) -> Vec<String> {
        self.path.clone()
    }

    pub fn get_layer(&self) -> Vec<u8> {
        self.layer.clone()
    }

    pub fn is_start_of(&self, key: &[u8]) -> bool {
        self.subspace.is_start_of(&key)
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
    ) -> Result<DirectoryOutput, DirectoryError> {
        self.directory_layer
            .create_or_open(
                txn,
                self.get_partition_subpath(path.to_owned()),
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
    ) -> Result<DirectoryOutput, DirectoryError> {
        self.directory_layer
            .create(
                txn,
                self.get_partition_subpath(path.to_owned()),
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
    ) -> Result<DirectoryOutput, DirectoryError> {
        self.directory_layer
            .open(txn, self.get_partition_subpath(path.to_owned()), layer)
            .await
    }

    async fn exists(&self, trx: &Transaction, path: Vec<String>) -> Result<bool, DirectoryError> {
        self.directory_layer
            .exists(trx, self.get_partition_subpath(path.to_owned()))
            .await
    }

    async fn move_directory(
        &self,
        trx: &Transaction,
        new_path: Vec<String>,
    ) -> Result<DirectoryOutput, DirectoryError> {
        let directory_layer_path = self.directory_layer.path.to_owned();

        if directory_layer_path.len() > new_path.len() {
            return Err(DirectoryError::CannotMoveBetweenPartition);
        }

        let mut new_relative_path = vec![];
        new_relative_path.extend_from_slice(&new_path[directory_layer_path.len()..]);

        self.directory_layer
            .move_to(
                trx,
                self.get_partition_subpath(vec![]),
                new_relative_path.to_owned(),
            )
            .await
    }

    async fn move_to(
        &self,
        trx: &Transaction,
        old_path: Vec<String>,
        new_path: Vec<String>,
    ) -> Result<DirectoryOutput, DirectoryError> {
        self.directory_layer
            .move_to(
                trx,
                self.get_partition_subpath(old_path),
                self.get_partition_subpath(new_path),
            )
            .await
    }

    async fn remove(&self, trx: &Transaction, path: Vec<String>) -> Result<bool, DirectoryError> {
        self.directory_layer
            .remove(trx, self.get_partition_subpath(path.to_owned()))
            .await
    }

    async fn list(
        &self,
        trx: &Transaction,
        path: Vec<String>,
    ) -> Result<Vec<String>, DirectoryError> {
        self.directory_layer
            .list(trx, self.get_partition_subpath(path.to_owned()))
            .await
    }
}
