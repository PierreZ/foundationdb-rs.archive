// Copyright 2018 foundationdb-rs developers, https://github.com/Clikengo/foundationdb-rs/graphs/contributors
// Copyright 2013-2018 Apple, Inc and the FoundationDB project authors.
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.

use crate::directory::DirectoryError;
use crate::tuple::{Subspace, TuplePack};
use crate::{FdbError, RangeOption, Transaction};

/// Node are used to represent the paths generated by a Directory.
/// They are stored in the `Directory.node_subspace``.
#[derive(Debug)]
pub(crate) struct Node {
    /// The layer is checked during opening, if it does not match the layer
    /// stored, an Error is thrown. It can be used as some ownership's feature
    /// on a path.
    pub(crate) layer: Option<Vec<u8>>,

    /// the current path of a node.
    pub(crate) path: Vec<String>,

    /// the node_space of this node,
    pub(crate) node_subspace: Subspace,
    /// the content_subspace of this node.
    pub(crate) content_subspace: Option<Subspace>,

    /// The subspace used to find this node, attached to a parent.
    /// Used to properly handle deletes/move
    pub(crate) parent_node_reference: Subspace,
}

impl Node {
    /// `check_layer` is checking the layer, throwing `IncompatibleLayer` when
    /// the provided layer does not match the one provided.
    pub(crate) fn check_layer(&self, layer: Vec<u8>) -> Result<(), DirectoryError> {
        match &self.layer {
            None => Err(DirectoryError::IncompatibleLayer),
            Some(layer_bytes) => {
                if String::from("partition").pack_to_vec().eq(&layer) {
                    unimplemented!("partition is not yet supported")
                }

                if layer_bytes.len() != layer.len() {
                    Err(DirectoryError::IncompatibleLayer)
                } else {
                    Ok(())
                }
            }
        }
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
            let sub_directory: (i64, String) = self.node_subspace.unpack(subspace.bytes())?;
            results.push(sub_directory.1);
        }
        Ok(results)
    }

    pub(crate) async fn remove_all(&self, trx: &Transaction) -> Result<(), DirectoryError> {
        self.remove_children_nodes(trx).await?;
        self.remove_parent_reference(trx).await?;
        self.remove_content_subspace(trx).await?;

        Ok(())
    }

    pub(crate) async fn remove_children_nodes(
        &self,
        trx: &Transaction,
    ) -> Result<(), DirectoryError> {
        trx.clear_subspace_range(&self.node_subspace);
        Ok(())
    }

    pub(crate) async fn remove_parent_reference(
        &self,
        trx: &Transaction,
    ) -> Result<(), DirectoryError> {
        trx.clear(&self.parent_node_reference.bytes());
        Ok(())
    }

    pub(crate) async fn remove_content_subspace(
        &self,
        trx: &Transaction,
    ) -> Result<(), DirectoryError> {
        match &self.content_subspace {
            None => {}
            Some(content_subspace) => trx.clear_subspace_range(&content_subspace),
        };
        Ok(())
    }
}
