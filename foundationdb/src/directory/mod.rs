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
use crate::tuple::Subspace;
use crate::{FdbResult, Transaction};
use byteorder::{LittleEndian, WriteBytesExt};

// TODO: useful?
const _LAYER_VERSION: (u8, u8, u8) = (1, 0, 0);
const MAJOR_VERSION: u32 = 1;
const MINOR_VERSION: u32 = 0;
const PATCH_VERSION: u32 = 0;
const DEFAULT_NODE_PREFIX: &[u8] = b"\xFE";
const DEFAULT_HCA_PREFIX: &[u8] = b"hca";
const DEFAULT_SUB_DIRS: i64 = 0;

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
///         vec![String::from("app"), String::from("my-app")],
///         None, None,
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
/// ## How it works
///
/// Here's what will be generated when using the Directory to create a path `/app/my-app`:
///
/// ```text
///                +
///                |
///                | version = (1,0,0)              # Directory's version
///                |
///                |      +
///                | "hca"|                          # used to allocate numbers like 12 and 42
///                |      +
///      \xFE      |
///     node's     | (0,"app")=12                    # id allocated by the hca for "path"
///    subspace    | (0,"app","layer")=""            # layer allow an ownership's mecanism
///                |
///                |
///                | (0,"app",0,"my-app","layer")="" # layer allow an ownership's mecanism
///                | (0,"app",0,"my-app")=42         # id allocated by the hca for "layer"
///                +
///
///
///                +
///                |
///                |
///    (12,42)     |
///    content     | # data's subspace for path "app","my-app"
///   subspace     |
///                |
///                +
/// ```
/// In this schema:
///
/// * vertical lines represents `Subspaces`,
/// * `()` `Tuples`,
/// * `#` comments.
///
#[derive(Debug)]
pub struct DirectoryLayer {
    /// the subspace used to store the hierarchy of paths. Each path is composed of Nodes.
    /// Default is `Subspace::all()`.
    pub node_subspace: Subspace,
    /// the subspace used to actually store the data.
    /// Default is `Subspace::from_bytes(b"\xFE")`
    pub content_subspace: Subspace,

    /// The allocator used to generates i64 paths that will reduce keys's length.
    /// Default HAC is using  `Subspace::from_bytes(b"hca")` as the subspace.
    pub allocator: HighContentionAllocator,

    pub allow_manual_prefix: bool,

    pub path: Vec<String>,
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
            allow_manual_prefix: false,
            path: vec![],
        }
    }
}

impl DirectoryLayer {
    /// `create_or_open` opens the directory specified by path (relative to this
    /// Directory), and returns the directory and its contents as a
    /// Subspace. If the directory does not exist, it is created
    /// (creating parent directories if necessary).
    pub async fn create_or_open(
        &self,
        txn: &Transaction,
        path: Vec<String>,
        prefix: Option<Vec<u8>>,
        layer: Option<Vec<u8>>,
    ) -> Result<Subspace, DirectoryError> {
        self.create_or_open_internal(txn, path, prefix, layer, true, true)
            .await
    }

    /// `create` creates a directory specified by path (relative to this
    /// Directory), and returns the directory and its contents as a
    /// Subspace (or ErrDirAlreadyExists if the directory already exists).
    pub async fn create(
        &self,
        txn: &Transaction,
        path: Vec<String>,
        prefix: Option<Vec<u8>>,
        layer: Option<Vec<u8>>,
    ) -> Result<Subspace, DirectoryError> {
        self.create_or_open_internal(txn, path, prefix, layer, true, true)
            .await
    }

    /// `open` opens the directory specified by path (relative to this Directory),
    /// and returns the directory and its contents as a Subspace (or Err(DirNotExists)
    /// error if the directory does not exist, or ErrParentDirDoesNotExist if one of the parent
    /// directories in the path does not exist).
    pub async fn open(
        &self,
        txn: &Transaction,
        path: Vec<String>,
        layer: Option<Vec<u8>>,
    ) -> Result<Subspace, DirectoryError> {
        self.create_or_open_internal(txn, path, None, layer, false, true)
            .await
    }

    /// `exists` returns true if the directory at path (relative to the default root directory) exists, and false otherwise.
    pub async fn exists(
        &self,
        trx: &Transaction,
        path: Vec<String>,
    ) -> Result<bool, DirectoryError> {
        match dbg!(self.find_node(trx, path.to_owned(), false, None).await) {
            Ok(_node) => Ok(true),
            Err(err) => match err {
                DirectoryError::PathDoesNotExists => Ok(false),
                _ => Err(err),
            },
        }
    }

    /// `move_to` the directory from old_path to new_path(both relative to this
    /// Directory), and returns the directory (at its new location) and its
    /// contents as a Subspace. Move will return an error if a directory
    /// does not exist at oldPath, a directory already exists at newPath, or the
    /// parent directory of newPath does not exist.
    pub async fn move_to(
        &self,
        _trx: &Transaction,
        _old_path: Vec<String>,
        _new_path: Vec<String>,
    ) -> Result<Subspace, DirectoryError> {
        unimplemented!()
    }

    /// `remove` the subdirectory of this Directory located at `path` and all of its subdirectories,
    /// as well as all of their contents.
    pub async fn remove(
        &self,
        trx: &Transaction,
        path: Vec<String>,
    ) -> Result<bool, DirectoryError> {
        let node = self.find_node(trx, path.to_owned(), false, None).await?;
        node.remove_all(trx).await?;
        Ok(true)
    }

    /// `list` returns the names of the immediate subdirectories of the default root directory as a slice of strings.
    /// Each string is the name of the last component of a subdirectory's path.  
    pub async fn list(
        &self,
        trx: &Transaction,
        path: Vec<String>,
    ) -> Result<Vec<String>, DirectoryError> {
        let node = self.find_node(trx, path.to_owned(), false, None).await?;
        node.list(&trx).await
    }

    /// `create_or_open_internal` is the function used to open and/or create a directory.
    async fn create_or_open_internal(
        &self,
        trx: &Transaction,
        path: Vec<String>,
        prefix: Option<Vec<u8>>,
        layer: Option<Vec<u8>>,
        allow_create: bool,
        allow_open: bool,
    ) -> Result<Subspace, DirectoryError> {
        dbg!(&path, &prefix, &layer, allow_create, allow_open);
        self.check_version(trx, allow_create).await?;

        if prefix.is_some() && !self.allow_manual_prefix {
            if self.path.is_empty() {
                return Err(DirectoryError::PrefixNotAllowed);
            }

            return Err(DirectoryError::CannotPrefixInPartition);
        }

        if path.is_empty() {
            return Err(DirectoryError::NoPathProvided);
        }

        match self
            .find_node(&trx, path.to_owned(), allow_create, prefix)
            .await
        {
            Ok(node) => {
                // node exists, checking layer
                if !allow_open {
                    return Err(DirectoryError::DirAlreadyExists);
                }

                match layer {
                    None => {}
                    Some(l) => {
                        node.check_layer(l)?;
                    }
                }

                Ok(node.content_subspace.unwrap())
            }
            Err(err) => Err(err),
        }
    }

    /// `check_version` is checking the Directory's version in FDB.
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
                    Err(DirectoryError::MissingDirectory)
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

    /// `initialize_directory` is initializing the directory
    async fn initialize_directory(&self, trx: &Transaction) -> Result<(), DirectoryError> {
        let mut value = vec![];
        value.write_u32::<LittleEndian>(MAJOR_VERSION).unwrap();
        value.write_u32::<LittleEndian>(MINOR_VERSION).unwrap();
        value.write_u32::<LittleEndian>(PATCH_VERSION).unwrap();
        let version_subspace: &[u8] = b"version";
        let directory_version_key = self.get_root_node_subspace().subspace(&version_subspace);
        trx.set(directory_version_key.bytes(), &value);

        Ok(())
    }

    async fn find_node(
        &self,
        trx: &Transaction,
        path: Vec<String>,
        allow_creation: bool,
        prefix: Option<Vec<u8>>,
    ) -> Result<Node, DirectoryError> {
        let mut node = Node {
            layer: None,
            path: vec![],
            node_subspace: self.get_root_node_subspace(),
            content_subspace: None,
            parent_node_reference: self.get_root_node_subspace(),
        };
        let mut node_path = vec![];

        let last_path_index = match path.len() {
            0 => 0,
            size => size - 1,
        };

        for (i, path_name) in path.iter().enumerate() {
            node_path.push(path_name.to_owned());
            let key = node
                .node_subspace
                .subspace(&(DEFAULT_SUB_DIRS, path_name.to_owned()));

            let (prefix, new_node) = match trx.get(key.bytes(), false).await {
                Ok(value) => match value {
                    None => {
                        if !allow_creation {
                            return Err(DirectoryError::PathDoesNotExists);
                        }

                        // if we are on the last node and a prefix was provided,
                        // using the provided prefix as the content_subspace instead of
                        // generating one.
                        if i == last_path_index && prefix.is_some() {
                            (
                                Subspace::from_bytes(&*prefix.clone().unwrap())
                                    .bytes()
                                    .to_vec(),
                                true,
                            )
                        } else {
                            // creating the subspace for this not-existing node using the allocator
                            let allocator = self.allocator.allocate(trx).await?;
                            let subspace = self.content_subspace.subspace(&allocator);
                            (subspace.bytes().to_vec(), true)
                        }
                    }
                    Some(fdb_slice) => ((&*fdb_slice).to_vec(), false),
                },
                Err(err) => return Err(DirectoryError::FdbError(err)),
            };

            node = Node {
                path: node_path.clone(),
                layer: None,
                node_subspace: self.node_subspace.subspace(&prefix.as_slice()),
                content_subspace: Some(Subspace::from_bytes(&prefix.as_slice())),
                parent_node_reference: key.to_owned(),
            };

            node.retrieve_layer(&trx).await?;

            if new_node {
                trx.set(key.bytes(), prefix.as_slice());
            }
        }

        Ok(node)
    }

    fn get_root_node_subspace(&self) -> Subspace {
        return self
            .node_subspace
            .subspace::<&[u8]>(&self.node_subspace.bytes());
    }

    async fn get_version_value(&self, trx: &Transaction) -> FdbResult<Option<FdbSlice>> {
        let version_subspace: &[u8] = b"version";
        let version_key = self.get_root_node_subspace().subspace(&version_subspace);
        trx.get(version_key.bytes(), false).await
    }
}
