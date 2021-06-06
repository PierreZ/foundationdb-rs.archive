use crate::directory::directory_partition::DirectoryPartition;
use crate::directory::directory_subspace::DirectorySubspace;
use crate::directory::error::DirectoryError;
use crate::directory::node::Node;
use crate::directory::{compare_slice, strinc, Directory, DirectoryOutput};
use crate::future::{FdbKeyValue, FdbSlice, FdbValues};
use crate::tuple::hca::HighContentionAllocator;
use crate::tuple::{unpack, Element, Subspace, TuplePack};
use crate::{FdbError, RangeOption};
use crate::{FdbResult, Transaction};
use async_recursion::async_recursion;
use async_trait::async_trait;
use byteorder::{LittleEndian, WriteBytesExt};
use futures::try_join;
use std::cmp::Ordering;
use std::option::Option::Some;

pub(crate) const DEFAULT_SUB_DIRS: i64 = 0;
const MAJOR_VERSION: u32 = 1;
const MINOR_VERSION: u32 = 0;
const PATCH_VERSION: u32 = 0;
pub const DEFAULT_NODE_PREFIX: &[u8] = b"\xFE";
const DEFAULT_HCA_PREFIX: &[u8] = b"hca";
pub(crate) const PARTITION_LAYER: &[u8] = b"partition";

/// Directories are identified by hierarchical paths analogous to the paths
/// in a Unix-like file system. A path is represented as a List of strings.
/// Each directory has an associated subspace used to store its content. The
/// layer maps each path to a short prefix used for the corresponding
/// subspace. In effect, directories provide a level of indirection for
/// access to subspaces.
#[derive(Debug, Clone)]
pub struct DirectoryLayer {
    pub root_node: Subspace,
    pub node_subspace: Subspace,
    pub content_subspace: Subspace,
    pub allocator: HighContentionAllocator,
    pub allow_manual_prefixes: bool,

    pub(crate) path: Vec<String>,
}

impl Default for DirectoryLayer {
    /// creates a default Directory to use
    fn default() -> Self {
        Self::new(
            Subspace::from_bytes(DEFAULT_NODE_PREFIX),
            Subspace::all(),
            false,
        )
    }
}

impl DirectoryLayer {
    pub fn new(
        node_subspace: Subspace,
        content_subspace: Subspace,
        allow_manual_prefixes: bool,
    ) -> Self {
        let root_node = node_subspace.subspace(&node_subspace.bytes());

        DirectoryLayer {
            root_node: root_node.clone(),
            node_subspace,
            content_subspace,
            allocator: HighContentionAllocator::new(root_node.subspace(&DEFAULT_HCA_PREFIX)),
            allow_manual_prefixes,
            path: vec![],
        }
    }

    pub(crate) fn new_with_path(
        node_subspace: Subspace,
        content_subspace: Subspace,
        allow_manual_prefixes: bool,
        path: Vec<String>,
    ) -> Self {
        let root_node = node_subspace.subspace(&node_subspace.bytes());

        DirectoryLayer {
            root_node: root_node.clone(),
            node_subspace,
            content_subspace,
            allocator: HighContentionAllocator::new(root_node.subspace(&DEFAULT_HCA_PREFIX)),
            allow_manual_prefixes,
            path,
        }
    }

    pub fn get_path(&self) -> Vec<String> {
        self.path.clone()
    }

    fn node_with_optional_prefix(&self, prefix: Option<FdbSlice>) -> Option<Subspace> {
        match prefix {
            None => None,
            Some(fdb_slice) => Some(self.node_with_prefix(&(&*fdb_slice))),
        }
    }

    fn node_with_prefix<T: TuplePack>(&self, prefix: &T) -> Subspace {
        self.node_subspace.subspace(prefix)
    }

    async fn find(&self, trx: &Transaction, path: Vec<String>) -> Result<Node, DirectoryError> {
        let mut node = Node {
            subspace: Some(self.root_node.clone()),
            current_path: vec![],
            target_path: path.clone(),
            layer: vec![],
            loaded_metadata: false,
        };

        // walking through the provided path
        for path_name in path.iter() {
            node.current_path.push(path_name.clone());
            let node_subspace = match node.subspace {
                // unreachable because on first iteration, it is set to root_node,
                // on other iteration, `node.exists` is checking for the subspace's value
                None => unreachable!("node's subspace is not set"),
                Some(s) => s,
            };
            let key = node_subspace.subspace(&(DEFAULT_SUB_DIRS, path_name.to_owned()));

            // finding the next node
            let fdb_slice_value = trx.get(key.bytes(), false).await?;

            node = Node {
                subspace: self.node_with_optional_prefix(fdb_slice_value),
                current_path: node.current_path.clone(),
                target_path: path.clone(),
                layer: vec![],
                loaded_metadata: false,
            };

            node.load_metadata(&trx).await?;

            if !node.exists() || node.layer.eq(PARTITION_LAYER) {
                return Ok(node);
            }
        }

        if !node.loaded_metadata {
            node.load_metadata(&trx).await?;
        }

        Ok(node)
    }

    fn to_absolute_path(&self, sub_path: &[String]) -> Vec<String> {
        let mut path: Vec<String> = Vec::with_capacity(self.path.len() + sub_path.len());

        path.extend_from_slice(&self.path);
        path.extend_from_slice(sub_path);

        path
    }

    fn contents_of_node(
        &self,
        node: Subspace,
        path: Vec<String>,
        layer: Vec<u8>,
    ) -> Result<DirectoryOutput, DirectoryError> {
        let prefix: Vec<u8> = self.node_subspace.unpack(node.bytes())?;

        if layer.eq(PARTITION_LAYER) {
            Ok(DirectoryOutput::DirectoryPartition(
                DirectoryPartition::new(self.to_absolute_path(&path), prefix, self.clone()),
            ))
        } else {
            Ok(DirectoryOutput::DirectorySubspace(DirectorySubspace::new(
                self.to_absolute_path(&path),
                prefix,
                self,
                layer.to_owned(),
            )))
        }
    }

    /// `create_or_open_internal` is the function used to open and/or create a directory.
    #[async_recursion]
    async fn create_or_open_internal(
        &self,
        trx: &Transaction,
        path: Vec<String>,
        prefix: Option<Vec<u8>>,
        layer: Option<Vec<u8>>,
        allow_create: bool,
        allow_open: bool,
    ) -> Result<DirectoryOutput, DirectoryError> {
        self.check_version(trx, allow_create).await?;

        if prefix.is_some() && !self.allow_manual_prefixes {
            if self.path.is_empty() {
                return Err(DirectoryError::PrefixNotAllowed);
            }

            return Err(DirectoryError::CannotPrefixInPartition);
        }

        if path.is_empty() {
            return Err(DirectoryError::NoPathProvided);
        }

        let node = self.find(trx, path.to_owned()).await?;

        if node.exists() {
            // TODO true or false?
            if node.is_in_partition(false) {
                let sub_path = node.get_partition_subpath();
                let subspace_node = match node.subspace {
                    // not reachable because `self.find` is creating a node with a subspace,
                    None => unreachable!("node's subspace is not set"),
                    Some(s) => s,
                };
                let dir_space =
                    self.contents_of_node(subspace_node, node.current_path, node.layer)?;
                dir_space
                    .create_or_open(trx, sub_path.to_owned(), prefix, layer)
                    .await?;
                Ok(dir_space)
            } else {
                self.open_internal(layer, &node, allow_open).await
            }
        } else {
            self.create_internal(trx, path, layer, prefix, allow_create)
                .await
        }
    }

    async fn open_internal(
        &self,
        layer: Option<Vec<u8>>,
        node: &Node,
        allow_open: bool,
    ) -> Result<DirectoryOutput, DirectoryError> {
        if !allow_open {
            return Err(DirectoryError::DirAlreadyExists);
        }

        match layer.to_owned() {
            None => {}
            Some(l) => match compare_slice(&l, &node.layer) {
                Ordering::Equal => {}
                _ => {
                    return Err(DirectoryError::IncompatibleLayer);
                }
            },
        }

        let subspace_node = match node.to_owned().subspace {
            // not reachable because `self.find` is creating a node with a subspace,
            None => unreachable!("node's subspace is not set"),
            Some(s) => s,
        };

        self.contents_of_node(
            subspace_node.clone().subspace(&()),
            node.target_path.to_owned(),
            node.layer.to_owned(),
        )
    }

    async fn create_internal(
        &self,
        trx: &Transaction,
        path: Vec<String>,
        layer: Option<Vec<u8>>,
        prefix: Option<Vec<u8>>,
        allow_create: bool,
    ) -> Result<DirectoryOutput, DirectoryError> {
        if !allow_create {
            return Err(DirectoryError::DirectoryDoesNotExists);
        }

        let layer = layer.unwrap_or(vec![]);

        self.check_version(trx, allow_create).await?;
        let new_prefix = self.get_prefix(trx, prefix.clone()).await?;

        let is_prefix_free = self
            .is_prefix_free(trx, new_prefix.to_owned(), !prefix.is_some())
            .await?;

        if !is_prefix_free {
            return Err(DirectoryError::DirectoryPrefixInUse);
        }

        let parent_node = self.get_parent_node(trx, path.to_owned()).await?;
        let node = self.node_with_prefix(&new_prefix);

        let key = parent_node.subspace(&(DEFAULT_SUB_DIRS, path.last().unwrap()));

        trx.set(&key.bytes(), &new_prefix);
        trx.set(&node.pack(&(String::from("layer"))), &layer);

        self.contents_of_node(node, path.to_owned(), layer.to_owned())
    }

    async fn get_parent_node(
        &self,
        trx: &Transaction,
        path: Vec<String>,
    ) -> Result<Subspace, DirectoryError> {
        if path.len() > 1 {
            let (_, list) = path.split_last().unwrap();

            let parent = self
                .create_or_open_internal(trx, list.to_vec(), None, None, true, true)
                .await?;
            Ok(self.node_with_prefix(&parent.bytes().to_vec()))
        } else {
            Ok(self.root_node.clone())
        }
    }

    async fn is_prefix_free(
        &self,
        trx: &Transaction,
        prefix: Vec<u8>,
        snapshot: bool,
    ) -> Result<bool, DirectoryError> {
        if prefix.is_empty() {
            return Ok(false);
        }

        let node = self
            .node_containing_key(trx, prefix.to_owned(), snapshot)
            .await?;

        if node.is_some() {
            return Ok(false);
        }

        let range_option =
            RangeOption::from(Subspace::from(self.node_subspace.pack(&prefix)).range());

        let result = trx.get_range(&range_option, 1, false).await?;

        Ok(result.is_empty())
    }

    async fn node_containing_key(
        &self,
        trx: &Transaction,
        key: Vec<u8>,
        snapshot: bool,
    ) -> Result<Option<Subspace>, DirectoryError> {
        if key.starts_with(self.node_subspace.bytes()) {
            return Ok(Some(self.root_node.clone()));
        }

        let mut key_after = key.to_vec();
        // pushing 0x00 to simulate keyAfter
        key_after.push(0);

        let range_end = self.node_subspace.pack(&key_after);

        let mut range_option = RangeOption::from((self.node_subspace.range().0, range_end));
        range_option.reverse = true;
        range_option.limit = Some(1);

        // checking range
        let fdb_values = trx.get_range(&range_option, 1, snapshot).await?;

        match fdb_values.get(0) {
            None => {}
            Some(fdb_key_value) => {
                let previous_prefix: Vec<Element> =
                    self.node_subspace.unpack(fdb_key_value.key())?;
                match previous_prefix.get(0) {
                    Some(Element::Bytes(b)) => {
                        let previous_prefix = b.to_vec();
                        if key.starts_with(&previous_prefix) {
                            return Ok(Some(self.node_with_prefix(&previous_prefix)));
                        };
                    }
                    _ => {}
                };
            }
        }
        Ok(None)
    }

    async fn get_prefix(
        &self,
        trx: &Transaction,
        prefix: Option<Vec<u8>>,
    ) -> Result<Vec<u8>, DirectoryError> {
        match prefix {
            None => {
                // no prefix provided, allocating one
                let allocator = self.allocator.allocate(trx).await?;
                let subspace = self.content_subspace.subspace(&allocator);

                // checking range
                let result = trx
                    .get_range(&RangeOption::from(subspace.range()), 1, false)
                    .await?;

                if !result.is_empty() {
                    return Err(DirectoryError::PrefixNotEmpty);
                }

                Ok(subspace.bytes().to_vec())
            }
            Some(v) => Ok(v),
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
                    return Ok(());
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
        let directory_version_key = self.root_node.subspace(&version_subspace);
        trx.set(directory_version_key.bytes(), &value);

        Ok(())
    }

    async fn get_version_value(&self, trx: &Transaction) -> FdbResult<Option<FdbSlice>> {
        let version_subspace: &[u8] = b"version";
        let version_key = self.root_node.subspace(&version_subspace);

        trx.get(version_key.bytes(), false).await
    }

    async fn exists_internal(
        &self,
        trx: &Transaction,
        path: Vec<String>,
    ) -> Result<bool, DirectoryError> {
        self.check_version(trx, false).await?;

        let node = self.find(trx, path.to_owned()).await?;

        if !node.exists() {
            println!("node does not exists");
            return Ok(false);
        }

        if node.is_in_partition(false) {
            let subspace_node = match node.subspace {
                // not reachable because `self.find` is creating a node with a subspace,
                None => unreachable!("node's subspace is not set"),
                Some(ref s) => s.clone(),
            };

            let directory_partition = self.contents_of_node(
                subspace_node,
                node.current_path.to_owned(),
                node.layer.to_owned(),
            )?;
            return directory_partition
                .exists(trx, node.to_owned().get_partition_subpath())
                .await;
        }

        Ok(true)
    }

    async fn list_internal(
        &self,
        trx: &Transaction,
        path: Vec<String>,
    ) -> Result<Vec<String>, DirectoryError> {
        dbg!(&self.path, &path, &self.root_node.bytes());
        self.check_version(trx, false).await?;

        let node = self.find(trx, path.to_owned()).await?;
        if !node.exists() {
            return Err(DirectoryError::PathDoesNotExists);
        }
        dbg!(&node);
        if node.is_in_partition(true) {
            let subspace_node = match node.subspace {
                // not reachable because `self.find` is creating a node with a subspace.
                None => unreachable!("node's subspace is not set"),
                Some(ref s) => s.clone(),
            };

            let directory_partition = self.contents_of_node(
                subspace_node,
                node.current_path.to_owned(),
                node.layer.to_owned(),
            )?;
            return directory_partition
                .list(trx, node.get_partition_subpath())
                .await;
        }

        Ok(node.list_sub_folders(trx).await?)
    }

    async fn move_to_internal(
        &self,
        trx: &Transaction,
        old_path: Vec<String>,
        new_path: Vec<String>,
    ) -> Result<DirectoryOutput, DirectoryError> {
        self.check_version(trx, true).await?;

        if old_path.len() <= new_path.len() {
            if compare_slice(&old_path[..], &new_path[..old_path.len()]) == Ordering::Equal {
                return Err(DirectoryError::CannotMoveBetweenSubdirectory);
            }
        }

        let old_node = self.find(trx, old_path.to_owned()).await?;
        let new_node = self.find(trx, new_path.to_owned()).await?;

        if !old_node.exists() {
            return Err(DirectoryError::PathDoesNotExists);
        }

        if old_node.is_in_partition(false) || new_node.is_in_partition(false) {
            if !old_node.is_in_partition(false)
                || !new_node.is_in_partition(false)
                || old_node.current_path.eq(&new_node.current_path)
            {
                return Err(DirectoryError::CannotMoveBetweenPartition);
            }

            let subspace_new_node = match new_node.subspace {
                // not reachable because `self.find` is creating a node with a subspace,
                None => unreachable!("node's subspace is not set"),
                Some(ref s) => s.clone(),
            };

            let directory_partition = self.contents_of_node(
                subspace_new_node,
                new_node.current_path.to_owned(),
                new_node.layer.to_owned(),
            )?;

            return directory_partition
                .move_to(
                    trx,
                    old_node.get_partition_subpath(),
                    new_node.get_partition_subpath(),
                )
                .await;
        }

        if new_node.exists() || new_path.is_empty() {
            return Err(DirectoryError::DirAlreadyExists);
        }

        let parent_path = match new_path.split_last() {
            None => vec![],
            Some((_, elements)) => elements.to_vec(),
        };

        let parent_node = self.find(trx, parent_path).await?;
        if !parent_node.exists() {
            return Err(DirectoryError::ParentDirDoesNotExists);
        }

        let subspace_parent_node = match parent_node.subspace {
            // not reachable because `self.find` is creating a node with a subspace,
            None => unreachable!("node's subspace is not set"),
            Some(ref s) => s.clone(),
        };

        let key =
            subspace_parent_node.subspace(&(DEFAULT_SUB_DIRS, new_path.to_owned().last().unwrap()));
        let value: Vec<u8> = self
            .node_subspace
            .unpack(old_node.subspace.clone().unwrap().bytes())?;
        trx.set(&key.bytes(), &value);

        self.remove_from_parent(trx, old_path.to_owned()).await?;

        self.contents_of_node(
            old_node.subspace.unwrap(),
            new_path.to_owned(),
            old_node.layer,
        )
    }

    async fn remove_from_parent(
        &self,
        trx: &Transaction,
        path: Vec<String>,
    ) -> Result<(), DirectoryError> {
        let (last_element, parent_path) = match path.split_last() {
            None => return Err(DirectoryError::BadDestinationDirectory),
            Some((last, elements)) => (last.clone(), elements.to_vec()),
        };

        let parent_node = self.find(trx, parent_path).await?;
        match parent_node.subspace {
            None => {}
            Some(subspace) => {
                let key = subspace.pack(&(DEFAULT_SUB_DIRS, last_element));
                trx.clear(&key);
            }
        }

        Ok(())
    }

    #[async_recursion]
    async fn remove_internal(
        &self,
        trx: &Transaction,
        path: Vec<String>,
        fail_on_nonexistent: bool,
    ) -> Result<bool, DirectoryError> {
        self.check_version(trx, true).await?;

        if path.is_empty() {
            return Err(DirectoryError::CannotModifyRootDirectory);
        }

        let node = self.find(&trx, path.to_owned()).await?;

        if !node.exists() {
            return if fail_on_nonexistent {
                Err(DirectoryError::DirectoryDoesNotExists)
            } else {
                Ok(false)
            };
        }

        if node.is_in_partition(false) {
            return self
                .remove_internal(trx, node.get_partition_subpath(), fail_on_nonexistent)
                .await;
        }

        self.remove_recursive(trx, node.subspace.unwrap().clone())
            .await?;
        self.remove_from_parent(trx, path.to_owned()).await?;

        Ok(true)
    }

    #[async_recursion]
    async fn remove_recursive(
        &self,
        trx: &Transaction,
        node_sub: Subspace,
    ) -> Result<(), DirectoryError> {
        let sub_dir = node_sub.subspace(&(DEFAULT_SUB_DIRS));
        let (mut begin, end) = sub_dir.range();

        loop {
            let range_option = RangeOption::from((begin.as_slice(), end.as_slice()));

            let range = trx.get_range(&range_option, 1024, false).await?;
            let has_more = range.more();

            for row_key in range {
                let sub_node = self.node_with_prefix(&row_key.value());
                self.remove_recursive(trx, sub_node).await?;
                begin = row_key.key().pack_to_vec();
            }

            if !has_more {
                break;
            }
        }

        let node_prefix: Vec<u8> = self.node_subspace.unpack(node_sub.bytes())?;

        trx.clear_range(node_prefix.as_slice(), &strinc(node_prefix.to_vec()));
        trx.clear_subspace_range(&node_sub);

        Ok(())
    }
}

#[async_trait]
impl Directory for DirectoryLayer {
    /// `create_or_open` opens the directory specified by path (relative to this
    /// Directory), and returns the directory and its contents as a
    /// Subspace. If the directory does not exist, it is created
    /// (creating parent directories if necessary).
    async fn create_or_open(
        &self,
        txn: &Transaction,
        path: Vec<String>,
        prefix: Option<Vec<u8>>,
        layer: Option<Vec<u8>>,
    ) -> Result<DirectoryOutput, DirectoryError> {
        self.create_or_open_internal(txn, path, prefix, layer, true, true)
            .await
    }

    async fn create(
        &self,
        txn: &Transaction,
        path: Vec<String>,
        prefix: Option<Vec<u8>>,
        layer: Option<Vec<u8>>,
    ) -> Result<DirectoryOutput, DirectoryError> {
        self.create_or_open_internal(txn, path, prefix, layer, true, false)
            .await
    }

    async fn open(
        &self,
        txn: &Transaction,
        path: Vec<String>,
        layer: Option<Vec<u8>>,
    ) -> Result<DirectoryOutput, DirectoryError> {
        self.create_or_open_internal(txn, path, None, layer, false, true)
            .await
    }

    async fn exists(&self, trx: &Transaction, path: Vec<String>) -> Result<bool, DirectoryError> {
        self.exists_internal(trx, path).await
    }

    async fn move_directory(
        &self,
        _trx: &Transaction,
        _new_path: Vec<String>,
    ) -> Result<DirectoryOutput, DirectoryError> {
        Err(DirectoryError::CannotMoveRootDirectory)
    }

    /// `move_to` the directory from old_path to new_path(both relative to this
    /// Directory), and returns the directory (at its new location) and its
    /// contents as a Subspace. Move will return an error if a directory
    /// does not exist at oldPath, a directory already exists at newPath, or the
    /// parent directory of newPath does not exist.
    async fn move_to(
        &self,
        trx: &Transaction,
        old_path: Vec<String>,
        new_path: Vec<String>,
    ) -> Result<DirectoryOutput, DirectoryError> {
        self.move_to_internal(trx, old_path, new_path).await
    }

    async fn remove(&self, trx: &Transaction, path: Vec<String>) -> Result<bool, DirectoryError> {
        self.remove_internal(trx, path.to_owned(), true).await
    }

    async fn list(
        &self,
        trx: &Transaction,
        path: Vec<String>,
    ) -> Result<Vec<String>, DirectoryError> {
        self.list_internal(trx, path).await
    }
}
