// Copyright 2018 foundationdb-rs developers, https://github.com/Clikengo/foundationdb-rs/graphs/contributors
// Copyright 2013-2018 Apple, Inc and the FoundationDB project authors.
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.

use crate::directory::DirectoryError;
use crate::tuple::Subspace;
use crate::{FdbError, RangeOption, Transaction};

/// Node are used to represent the paths generated by a Directory.
/// They are stored in the `Directory.`
#[derive(Debug)]
pub(crate) struct Node {
    pub(crate) layer: Option<Vec<u8>>,

    pub(crate) paths: Vec<String>,

    pub(crate) node_subspace: Subspace,
    pub(crate) content_subspace: Option<Subspace>,
}

impl Node {
    pub(crate) fn check_layer(&self, layer: Vec<u8>) -> Result<(), DirectoryError> {
        match &self.layer {
            None => Err(DirectoryError::IncompatibleLayer),
            Some(layer_bytes) => {
                if layer_bytes.len() != layer.len() {
                    Err(DirectoryError::IncompatibleLayer)
                } else {
                    Ok(())
                }
            }
        }
    }

    /// This will use the generated id and:
    ///
    /// * persist the node in the directory subspace
    /// * create the content_subspace and returns it
    pub(crate) async fn create_and_write_content_subspace(
        &mut self,
        trx: &Transaction,
        generated_id: i64,
        parent_subspace: &Subspace,
    ) -> Result<Subspace, DirectoryError> {
        let subspace = parent_subspace.subspace(&generated_id);
        self.persist_content_subspace(&trx, subspace).await
    }

    /// `persist_content_subspace` will save the provided subspace as the `content_subspace`
    pub(crate) async fn persist_content_subspace(
        &mut self,
        trx: &Transaction,
        subspace: Subspace,
    ) -> Result<Subspace, DirectoryError> {
        let key = self.node_subspace.to_owned();
        trx.set(key.bytes(), subspace.bytes());
        self.content_subspace = Some(subspace.to_owned());
        Ok(subspace)
    }

    /// delete subspace from the node_subspace
    pub(crate) async fn delete_content_subspace(
        &mut self,
        trx: &Transaction,
    ) -> Result<(), DirectoryError> {
        let key = self.node_subspace.to_owned();
        trx.clear(key.bytes());
        Ok(())
    }

    /// retrieve the layer used for this node
    pub(crate) async fn retrieve_layer(&mut self, trx: &Transaction) -> Result<(), FdbError> {
        if self.layer == None {
            let key = self.node_subspace.subspace(&b"layer".to_vec());
            self.layer = match trx.get(key.bytes(), false).await {
                Ok(None) => Some(vec![]),
                Err(err) => return Err(err),
                Ok(Some(fdb_slice)) => Some(fdb_slice.to_vec()),
            }
        }
        Ok(())
    }

    /// list sub-folders for a node
    pub(crate) async fn list(&self, trx: &Transaction) -> Result<Vec<String>, DirectoryError> {
        let mut results = vec![];

        let range_option = RangeOption::from(&self.node_subspace.to_owned());

        let fdb_values = trx.get_range(&range_option, 1_024, false).await?;

        for fdb_value in fdb_values {
            let subspace = Subspace::from_bytes(fdb_value.key());
            // stripping from subspace
            let sub_directory: (Vec<u8>, String) = self.node_subspace.unpack(subspace.bytes())?;
            results.push(sub_directory.1);
        }
        Ok(results)
    }
}
