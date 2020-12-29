// Copyright 2018 foundationdb-rs developers, https://github.com/Clikengo/foundationdb-rs/graphs/contributors
// Copyright 2013-2018 Apple, Inc and the FoundationDB project authors.
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.

use crate::directory::node;
use crate::future::FdbSlice;
use crate::tuple::hca::HighContentionAllocator;
use crate::tuple::{pack_into, Subspace};
use crate::DirectoryError::Version;
use crate::{DirectoryError, FdbError, FdbResult, Transaction};
use byteorder::{LittleEndian, WriteBytesExt};

const LAYER_VERSION: (u8, u8, u8) = (1, 0, 0);
const MAJOR_VERSION: u32 = 1;
const MINOR_VERSION: u32 = 0;
const PATCH_VERSION: u32 = 0;
const DEFAULT_NODE_PREFIX: &[u8] = b"\xFE";
const DEFAULT_HCA_PREFIX: &[u8] = b"hca";
const DEFAULT_SUB_DIRS: u8 = 0;

/// Provides a class for managing directories in FoundationDB.
/// The FoundationDB API provides directories as a tool for managing related Subspaces.
/// Directories are a recommended approach for administering applications. Each application should create or open at least one directory to manage its subspaces.
/// For general guidance on directory usage, see the discussion in the Developer Guide.
///
/// Directories are identified by hierarchical paths analogous to the paths in a Unix-like file system.
/// A path is represented as a List of strings. Each directory has an associated subspace used to store its content.
/// The layer maps each path to a short prefix used for the corresponding subspace.
/// In effect, directories provide a level of indirection for access to subspaces.
pub struct DirectoryLayer {
    node_subspace: Subspace,
    content_subspace: Subspace,

    allocator: HighContentionAllocator,

    allow_manual_prefixes: bool,

    path: Vec<String>,
    layer: Vec<u8>,
}

impl Default for DirectoryLayer {
    fn default() -> Self {
        DirectoryLayer {
            node_subspace: Subspace::from_bytes(DEFAULT_NODE_PREFIX),
            content_subspace: Subspace::all(),
            allocator: HighContentionAllocator::new(
                Subspace::from_bytes(DEFAULT_NODE_PREFIX).subspace(&DEFAULT_HCA_PREFIX),
            ),
            allow_manual_prefixes: false,
            path: vec![],
            layer: vec![],
        }
    }
}

impl DirectoryLayer {
    fn get_path(&self) -> Vec<String> {
        self.path.to_owned()
    }

    fn get_layer(&self) -> Vec<u8> {
        self.layer.to_owned()
    }

    pub async fn create_or_open(
        &self,
        txn: &Transaction,
        paths: Vec<String>,
    ) -> Result<Subspace, DirectoryError> {
        self.create_or_open_internal(txn, paths, vec![], vec![], true, true)
            .await
    }

    async fn create_or_open_internal(
        &self,
        trx: &Transaction,
        paths: Vec<String>,
        prefix: Vec<u8>,
        layer: Vec<u8>,
        allow_create: bool,
        allow_open: bool,
    ) -> Result<Subspace, DirectoryError> {
        self.check_version(trx, allow_create).await?;

        if prefix.len() > 0 && !self.allow_manual_prefixes {
            if self.path.len() == 0 {
                return Err(DirectoryError::Message(
                    "cannot specify a prefix unless manual prefixes are enabled".to_string(),
                ));
            }

            return Err(DirectoryError::Message(
                "cannot specify a prefix in a partition".to_string(),
            ));
        }

        if paths.len() == 0 {
            return Err(DirectoryError::NoPathProvided);
        }

        let mut node = self.find(trx, paths.to_owned()).await?;
        node.prefetch_metadata(trx).await?;

        // subspace already exists
        if node.content_subspace.is_some() {
            if !allow_open {
                return Err(DirectoryError::DirAlreadyExists);
            }

            if layer.len() > 0 {
                node.check_layer(layer)?;
            }

            return Ok(node.content_subspace.unwrap().to_owned());
        }

        // subspace does not exists
        if !allow_create {
            return Err(DirectoryError::DirNotExists);
        }

        if prefix.len() > 0 {
            unimplemented!("no prefix allowed yet")
        }

        if paths.len() != 1 {
            unimplemented!("paths too long for now")
        }

        let allocator = self.allocator.allocate(trx).await?;
        let new_subspace = self.content_subspace.subspace(&allocator);

        // store node in the node_subspace
        let mut new_node_key = vec![DEFAULT_SUB_DIRS];
        pack_into(paths.get(0).unwrap(), &mut new_node_key);
        trx.set(
            self.node_subspace.subspace(&new_node_key).bytes(),
            new_subspace.bytes(),
        );
        println!("writing to {:?}", &new_node_key);

        Ok(new_subspace)
    }

    async fn check_version(
        &self,
        trx: &Transaction,
        allow_creation: bool,
    ) -> Result<(), DirectoryError> {
        let version = self.get_version_value(trx).await?;
        match version {
            None => {
                if allow_creation {
                    self.initialize_directory(trx).await
                } else {
                    Err(DirectoryError::DirNotExists)
                }
            }
            Some(versions) => {
                if versions.len() < 12 {
                    return Err(Version("incorrect version length".to_string()));
                }
                let mut arr = [0u8; 4];
                arr.copy_from_slice(&versions[0..4]);
                let major: u32 = u32::from_le_bytes(arr);

                arr.copy_from_slice(&versions[4..8]);
                let minor: u32 = u32::from_le_bytes(arr);

                arr.copy_from_slice(&versions[8..12]);
                let patch: u32 = u32::from_le_bytes(arr);

                if major > MAJOR_VERSION {
                    let msg = format!("cannot load directory with version {}.{}.{} using directory layer {}.{}.{}", major, minor, patch, MAJOR_VERSION, MINOR_VERSION, PATCH_VERSION);
                    return Err(Version(msg));
                }

                if minor > MINOR_VERSION {
                    let msg = format!("directory with version {}.{}.{} is read-only when opened using directory layer {}.{}.{}", major, minor, patch, MAJOR_VERSION, MINOR_VERSION, PATCH_VERSION);
                    return Err(Version(msg));
                }

                Ok(())
            }
        }
    }

    async fn initialize_directory(&self, trx: &Transaction) -> Result<(), DirectoryError> {
        let mut value = vec![];
        value.write_u32::<LittleEndian>(MAJOR_VERSION).unwrap();
        value.write_u32::<LittleEndian>(MINOR_VERSION).unwrap();
        value.write_u32::<LittleEndian>(PATCH_VERSION).unwrap();
        let version_subspace: &[u8] = b"version";
        let version_key = self.node_subspace.subspace(&version_subspace);

        trx.set(version_key.bytes(), &value);

        Ok(())
    }

    async fn find(&self, trx: &Transaction, path: Vec<String>) -> Result<node::Node, FdbError> {
        let mut node = node::Node {
            subspace: self.node_subspace.to_owned(),
            path: path.to_owned(),
            target_path: vec![],
            layer: None,
            content_subspace: None,
            already_fetched_metadata: false,
        };
        for path_name in path.to_owned() {
            let mut next_node_key = vec![DEFAULT_SUB_DIRS];
            pack_into(&path_name, &mut next_node_key);
            let next_node_subspace = node.subspace.subspace(&next_node_key);

            match trx.get(next_node_subspace.bytes(), false).await? {
                None => {
                    println!("found none on `{:?}`", next_node_subspace.bytes());
                    if !path.ends_with(&[path_name]) {
                        unimplemented!("node not found")
                    }
                }
                Some(fdb_slice) => {
                    println!("found a node {:?}", fdb_slice.get(0));
                    node.subspace = next_node_subspace;
                    node.content_subspace = Some(Subspace::from_bytes(&*fdb_slice));
                    node.path.push(path_name.to_owned());
                }
            }
        }

        Ok(node)
    }

    async fn get_version_value(&self, trx: &Transaction) -> FdbResult<Option<FdbSlice>> {
        let version_subspace: &[u8] = b"version";
        let version_key = self.node_subspace.subspace(&version_subspace);
        trx.get(version_key.bytes(), false).await
    }
}
