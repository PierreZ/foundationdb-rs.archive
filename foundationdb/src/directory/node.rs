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
    pub(crate) subspace: Subspace,
    pub(crate) path: Vec<String>,
    pub(crate) target_path: Vec<String>,
    pub(crate) layer: Option<Vec<u8>>,

    pub(crate) exists: bool,

    pub(crate) already_fetched_metadata: bool,
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

    // TODO: https://docs.rs/futures/0.3.4/futures/future/trait.FutureExt.html#method.shared
    pub(crate) async fn prefetch_metadata(&mut self, trx: &Transaction) -> Result<(), FdbError> {
        if !self.already_fetched_metadata {
            self.layer(trx).await?;
            self.already_fetched_metadata = true;
        }
        Ok(())
    }

    // retrieve the layer used for this node
    pub(crate) async fn layer(&mut self, trx: &Transaction) -> Result<(), FdbError> {
        if self.layer == None {
            let key = self.subspace.subspace(&b"layer".to_vec());
            self.layer = match trx.get(key.bytes(), false).await {
                Ok(None) => Some(vec![]),
                Err(err) => return Err(err),
                Ok(Some(fv)) => {
                    self.exists = true;
                    Some(fv.to_vec())
                }
            }
        }
        Ok(())
    }

    pub(crate) fn get_content_subspace(&self) -> Result<Subspace, DirectoryError> {
        unimplemented!("get content subspace");
    }
}
