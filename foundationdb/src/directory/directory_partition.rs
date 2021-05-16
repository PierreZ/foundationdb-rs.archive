use crate::directory::directory_layer::{DirectoryLayer, DEFAULT_NODE_PREFIX, PARTITION_LAYER};
use crate::directory::directory_subspace::DirectorySubspace;
use crate::directory::error::DirectoryError;
use crate::directory::{Directory, DirectoryOutput};
use crate::tuple::{Subspace, TuplePack};
use crate::Transaction;
use async_trait::async_trait;

/// A `DirectoryPartition` is a DirectorySubspace whose prefix is preprended to all of its descendant
/// directories's prefixes. It cannot be used as a Subspace. Instead, you must create at
/// least one subdirectory to store content.
#[derive(Debug, Clone)]
pub struct DirectoryPartition {
    pub(crate) directory_subspace: DirectorySubspace,
    pub(crate) parent_directory_layer: DirectoryLayer,
}

impl DirectoryPartition {
    // https://github.com/apple/foundationdb/blob/master/bindings/flow/DirectoryPartition.h#L34-L43
    pub fn new(path: Vec<String>, prefix: Vec<u8>, parent_directory_layer: DirectoryLayer) -> Self {
        let mut node_subspace_bytes = vec![];
        node_subspace_bytes.extend_from_slice(&prefix);
        node_subspace_bytes.extend_from_slice(DEFAULT_NODE_PREFIX);

        let new_directory_layer = DirectoryLayer::new_with_path(
            Subspace::from_bytes(&node_subspace_bytes),
            Subspace::from_bytes(prefix.clone().as_slice()),
            false,
            path.to_vec(),
        );

        DirectoryPartition {
            directory_subspace: DirectorySubspace::new(
                path.clone(),
                prefix.clone(),
                &new_directory_layer,
                Vec::from(PARTITION_LAYER),
            ),
            parent_directory_layer,
        }
    }
}

impl DirectoryPartition {
    pub fn get_path(&self) -> Vec<String> {
        self.directory_subspace.get_path()
    }

    fn get_partition_subpath(&self, path: Vec<String>) -> Vec<String> {
        let mut new_path = vec![];

        new_path.extend_from_slice(&self.get_path());
        new_path.extend_from_slice(&path);

        new_path
    }

    pub fn get_layer(&self) -> Vec<u8> {
        String::from("partition").into_bytes()
    }
}

#[async_trait]
impl Directory for DirectoryPartition {
    async fn create_or_open(
        &self,
        txn: &Transaction,
        path: Vec<String>,
        prefix: Option<Vec<u8>>,
        layer: Option<Vec<u8>>,
    ) -> Result<DirectoryOutput, DirectoryError> {
        self.directory_subspace
            .create_or_open(txn, path, prefix, layer)
            .await
    }

    async fn create(
        &self,
        txn: &Transaction,
        path: Vec<String>,
        prefix: Option<Vec<u8>>,
        layer: Option<Vec<u8>>,
    ) -> Result<DirectoryOutput, DirectoryError> {
        self.directory_subspace
            .create(txn, path, prefix, layer)
            .await
    }

    async fn open(
        &self,
        txn: &Transaction,
        path: Vec<String>,
        layer: Option<Vec<u8>>,
    ) -> Result<DirectoryOutput, DirectoryError> {
        self.directory_subspace.open(txn, path, layer).await
    }

    async fn exists(&self, trx: &Transaction, path: Vec<String>) -> Result<bool, DirectoryError> {
        if path.is_empty() {
            self.parent_directory_layer
                .exists(trx, self.get_partition_subpath(path))
                .await
        } else {
            self.directory_subspace.exists(trx, path).await
        }
    }

    async fn move_directory(
        &self,
        trx: &Transaction,
        new_path: Vec<String>,
    ) -> Result<DirectoryOutput, DirectoryError> {
        self.directory_subspace.move_directory(trx, new_path).await
    }

    async fn move_to(
        &self,
        trx: &Transaction,
        old_path: Vec<String>,
        new_path: Vec<String>,
    ) -> Result<DirectoryOutput, DirectoryError> {
        self.directory_subspace
            .move_to(trx, old_path, new_path)
            .await
    }

    async fn remove(&self, trx: &Transaction, path: Vec<String>) -> Result<bool, DirectoryError> {
        if path.is_empty() {
            self.parent_directory_layer
                .remove(trx, self.get_partition_subpath(path))
                .await
        } else {
            self.directory_subspace.remove(trx, path).await
        }
    }

    async fn list(
        &self,
        trx: &Transaction,
        path: Vec<String>,
    ) -> Result<Vec<String>, DirectoryError> {
        if path.is_empty() {
            self.parent_directory_layer
                .list(trx, self.get_partition_subpath(path))
                .await
        } else {
            self.directory_subspace.list(trx, path).await
        }
    }
}
