// Copyright 2018 foundationdb-rs developers, https://github.com/Clikengo/foundationdb-rs/graphs/contributors
// Copyright 2013-2018 Apple, Inc and the FoundationDB project authors.
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.

use crate::tuple::Subspace;
use crate::{DirectoryError, FdbError, Transaction};

pub(crate) struct Node {
    pub(crate) layer: Option<Vec<u8>>,

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

    pub(crate) async fn create_subspace(
        &mut self,
        trx: &Transaction,
        allocator: i64,
        parent_subspace: &Subspace,
    ) -> Result<Subspace, DirectoryError> {
        let new_subspace = parent_subspace.subspace(&allocator);

        let key = self.node_subspace.to_owned();
        trx.set(key.bytes(), new_subspace.bytes());

        self.content_subspace = Some(new_subspace.to_owned());

        Ok(new_subspace.clone())
    }

    // retrieve the layer used for this node
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
}
