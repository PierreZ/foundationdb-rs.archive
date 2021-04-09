use crate::directory::directory_layer::{DirectoryLayer, PARTITION_LAYER};
use crate::directory::directory_subspace::DirectorySubspace;
use crate::directory::error::DirectoryError;
use crate::directory::{Directory, DirectoryOutput};
use crate::tuple::Subspace;
use crate::Transaction;
use async_trait::async_trait;

pub struct DirectoryPartition {
    directory_subspace: DirectorySubspace,
    parent_directory_layer: DirectoryLayer,
}

impl DirectoryPartition {
    pub fn new(path: Vec<String>, prefix: Vec<u8>, parent_directory_layer: DirectoryLayer) -> Self {
        let mut node_subspace_bytes = prefix.to_vec();
        node_subspace_bytes.extend_from_slice(&prefix);

        let mut d = DirectoryPartition {
            directory_subspace: DirectorySubspace::new(
                path,
                prefix,
                &DirectoryLayer::new(
                    Subspace::from_bytes(&node_subspace_bytes),
                    Subspace::from_bytes(prefix.as_slice()),
                    false,
                ),
                Vec::from(PARTITION_LAYER),
            ),
            parent_directory_layer,
        };

        d.parent_directory_layer.path = path.to_vec();

        d
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
        self.directory_subspace.exists(trx, path).await
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
        self.directory_subspace.remove(trx, path).await
    }

    async fn list(
        &self,
        trx: &Transaction,
        path: Vec<String>,
    ) -> Result<Vec<String>, DirectoryError> {
        self.directory_subspace.list(trx, path).await
    }
}
