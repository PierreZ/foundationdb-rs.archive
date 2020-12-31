// Copyright 2018 foundationdb-rs developers, https://github.com/Clikengo/foundationdb-rs/graphs/contributors
// Copyright 2013-2018 Apple, Inc and the FoundationDB project authors.
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.

//! Implementation of the official Directory layer.
//!
//! The FoundationDB API provides directories as a tool for managing related Subspaces.
//! For general guidance on directory usage, see the discussion in the [Developer Guide](https://apple.github.io/foundationdb/developer-guide.html#directories).
//!
pub mod error;
mod node;

use crate::directory::error::DirectoryError;
use crate::directory::node::Node;
use crate::future::FdbSlice;
use crate::tuple::hca::HighContentionAllocator;
use crate::tuple::{pack_into, Subspace};
use crate::{FdbError, FdbResult, Transaction};
use byteorder::{LittleEndian, WriteBytesExt};

const _LAYER_VERSION: (u8, u8, u8) = (1, 0, 0);
const MAJOR_VERSION: u32 = 1;
const MINOR_VERSION: u32 = 0;
const PATCH_VERSION: u32 = 0;
const DEFAULT_NODE_PREFIX: &[u8] = b"\xFE";
const DEFAULT_HCA_PREFIX: &[u8] = b"hca";
const DEFAULT_SUB_DIRS: u8 = 0;

/// An implementation of FoundationDB's directory that is compatible with other bindings.
///
///
/// Directories are a recommended approach for administering applications. Each application should create or open at least one directory to manage its subspaces.
///
/// Directories are identified by hierarchical paths analogous to the paths in a Unix-like file system.
/// A path is represented as a List of strings. Each directory has an associated subspace used to store its content.
/// The layer maps each path to a short prefix used for the corresponding subspace.
/// In effect, directories provide a level of indirection for access to subspaces.
/// ## How-to use the Directory
///
/// ```rust
/// use futures::prelude::*;
///
/// async fn async_main() -> foundationdb::FdbResult<()> {
///     let db = foundationdb::Database::default()?;
///
///     // creates a transaction
///     let trx = db.create_trx()?;
///
///     // creates a directory
///     let directory = foundationdb::directory::DirectoryLayer::default();
///
///     // use the directory to create a subspace to use
///     let content_subspace = directory.create_or_open(
///         // the transaction used to read/write the directory.
///         &trx,
///         // the path used, which can view as a UNIX path like `/app/my-app`.
///         vec![String::from("app"), String::from("my-app")]
///     ).await;
///     assert_eq!(true, content_subspace.is_ok());
///     
///     // Don't forget to commit your transaction to persist the subspace
///     trx.commit().await?;
///
///     Ok(())
/// }
///
/// // Safe because drop is called before the program exits
/// let network = unsafe { foundationdb::boot() };
/// futures::executor::block_on(async_main()).expect("failed to run");
/// drop(network);
/// ```
pub struct DirectoryLayer {
    /// the subspace used to store nodes.
    pub node_subspace: Subspace,
    /// the subspace used to actually store the data.
    pub content_subspace: Subspace,

    /// The allocator used to generates i64 paths to shorten keys
    pub allocator: HighContentionAllocator,

    pub allow_manual_prefixes: bool,

    pub path: Vec<String>,

    /// This is set at node creation time and never mutated by the directory layer.
    /// If a layer is provided when opening nodes it checks to see that the layer matches nodes that are read.
    /// When there's a mismatch an error is thrown.
    ///
    /// It can be used as a primitive "ownership" over part of the directory tree.
    pub layer: Vec<u8>,
}

impl Default for DirectoryLayer {
    /// creates a default Directory to use
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
    /// Creates or opens the directory located at path(creating parent directories, if necessary).
    pub async fn create_or_open(
        &self,
        txn: &Transaction,
        paths: Vec<String>,
    ) -> Result<Subspace, DirectoryError> {
        self.create_or_open_internal(txn, paths, vec![], vec![], true, true)
            .await
    }

    /// Creates a directory located at path (creating parent directories if necessary).
    pub async fn create(&self, txn: &Transaction, paths: Vec<String>) -> Option<DirectoryError> {
        self.create_or_open_internal(txn, paths, vec![], vec![], true, false)
            .await
            .err()
    }

    /// Opens the directory located at path.
    pub async fn open(
        &self,
        txn: &Transaction,
        paths: Vec<String>,
    ) -> Result<Subspace, DirectoryError> {
        self.create_or_open_internal(txn, paths, vec![], vec![], false, true)
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

        let nodes = self.find_nodes(trx, paths.to_owned()).await?;

        let last_node = nodes.last().expect("could not contain 0 nodes");

        // if the node_subspace of the last element exists, then we do not need to create anything
        // and we can return it directly
        if last_node.content_subspace.is_some() {
            let node = nodes.last().expect("could not contain 0 node");

            if !allow_open {
                return Err(DirectoryError::DirAlreadyExists);
            }

            if layer.len() > 0 {
                node.check_layer(layer)?;
            }

            return Ok(node.content_subspace.clone().unwrap());
        }

        // at least one node does not exists, we need to create them
        if !allow_create {
            return Err(DirectoryError::DirNotExists);
        }

        let mut subspace = self.content_subspace.clone();

        for mut node in nodes {
            let allocator = self.allocator.allocate(trx).await?;
            subspace = node.create_subspace(&trx, allocator, &subspace).await?;
        }
        Ok(subspace)
    }

    /// checks the version of the directory within FDB
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
                    return Err(DirectoryError::Version(
                        "incorrect version length".to_string(),
                    ));
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
                    return Err(DirectoryError::Version(msg));
                }

                if minor > MINOR_VERSION {
                    let msg = format!("directory with version {}.{}.{} is read-only when opened using directory layer {}.{}.{}", major, minor, patch, MAJOR_VERSION, MINOR_VERSION, PATCH_VERSION);
                    return Err(DirectoryError::Version(msg));
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
        let directory_version_key = self.node_subspace.subspace(&version_subspace);
        trx.set(directory_version_key.bytes(), &value);

        Ok(())
    }

    /// walk is crawling the node_subspace and searching for the nodes.
    /// It returns a Vec of `Node`, each node represents an element of the paths provided.
    ///
    /// If all paths are already existing, then the last node will have the content_subspace set.
    async fn find_nodes(
        &self,
        trx: &Transaction,
        paths: Vec<String>,
    ) -> Result<Vec<Node>, FdbError> {
        let mut nodes = vec![];

        let mut subspace = self.node_subspace.to_owned();

        for path_name in paths {
            let mut next_node_key = vec![DEFAULT_SUB_DIRS];
            pack_into(&path_name, &mut next_node_key);
            subspace = subspace.subspace(&next_node_key);

            let mut node = Node {
                layer: None,
                node_subspace: subspace.to_owned(),
                content_subspace: None,
            };

            node.retrieve_layer(&trx).await?;

            match trx.get(node.node_subspace.bytes(), false).await? {
                Some(fdb_slice) => {
                    node.content_subspace = Some(Subspace::from_bytes(&*fdb_slice));
                }
                _ => {} // noop in case of a none existing node
            }
            nodes.push(node);
        }

        Ok(nodes)
    }

    async fn get_version_value(&self, trx: &Transaction) -> FdbResult<Option<FdbSlice>> {
        let version_subspace: &[u8] = b"version";
        let version_key = self.node_subspace.subspace(&version_subspace);
        trx.get(version_key.bytes(), false).await
    }
}
