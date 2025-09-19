// -*- coding: utf-8 -*-
// ------------------------------------------------------------------------------------------------
// Copyright © 2024, stack-graphs authors.
// Licensed under either of Apache License, Version 2.0, or MIT license, at your option.
// Please see the LICENSE-APACHE or LICENSE-MIT files in this distribution for license details.
// ------------------------------------------------------------------------------------------------

#![allow(dead_code)]

use std::collections::{HashSet, VecDeque};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use bincode::error::DecodeError;
use redb::backends::InMemoryBackend;
use redb::{
    CommitError, Database as RedbDatabase, DatabaseError, MultimapTableDefinition,
    ReadableMultimapTable, ReadableTable, StorageError as RedbStorageError, TableDefinition,
    TableError, TransactionError, WriteTransaction,
};
use rusqlite::Connection;
use thiserror::Error;

use std::convert::TryInto;

use crate::arena::Handle;
use crate::graph::{File, Node, NodeID, StackGraph};
use crate::partial::{PartialPath, PartialPaths, PartialSymbolStack};
use crate::serde;
use crate::serde::FileFilter;
use crate::{CancellationError, CancellationFlag};

use super::encoding::{decode_partial_path, encode_partial_path};
use super::{
    Database, FileEntry, FileStatus, Stats, StorageComponents, StorageError, StorageFileListing,
    StorageReader, StorageWriter, SymbolStackExactVariant, SymbolStackQuery,
    SymbolStackQueryHandle, SymbolStackQueryKey, SymbolStackQueryPool, BINCODE_CONFIG, VERSION,
};

const METADATA_TABLE: TableDefinition<'static, &str, u64> = TableDefinition::new("metadata");
const GRAPHS_TABLE: TableDefinition<'static, &str, &[u8]> = TableDefinition::new("graphs");
const FILE_PATHS_TABLE: TableDefinition<'static, &[u8], &[u8]> = TableDefinition::new("file_paths");
const ROOT_PATHS_BY_FILE_TABLE: TableDefinition<'static, &[u8], &[u8]> =
    TableDefinition::new("root_paths_by_file");
const ROOT_PATHS_BY_SYMBOL_TABLE: MultimapTableDefinition<'static, &str, &str> =
    MultimapTableDefinition::new("root_paths_by_symbol");

#[derive(Debug, Error)]
pub enum RedbError {
    #[error("cancelled at {0}")]
    Cancelled(&'static str),
    #[error("unsupported database version {0}")]
    IncorrectVersion(u64),
    #[error("database does not exist {0}")]
    MissingDatabase(String),
    #[error(transparent)]
    Redb(#[from] redb::Error),
    #[error(transparent)]
    Decode(#[from] DecodeError),
    #[error(transparent)]
    Serde(#[from] serde::Error),
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error("corrupt storage data: {0}")]
    Corrupt(String),
}

impl From<CancellationError> for RedbError {
    fn from(value: CancellationError) -> Self {
        Self::Cancelled(value.0)
    }
}

impl From<StorageError> for RedbError {
    fn from(value: StorageError) -> Self {
        match value {
            StorageError::Cancelled(at) => RedbError::Cancelled(at),
            StorageError::IncorrectVersion(v) => RedbError::IncorrectVersion(v as u64),
            StorageError::MissingDatabase(path) => RedbError::MissingDatabase(path),
            StorageError::Rusqlite(err) => RedbError::Corrupt(err.to_string()),
            StorageError::Serde(err) => RedbError::Serde(err),
            StorageError::SerializeFail(err) => RedbError::Corrupt(err.to_string()),
            StorageError::DeserializeFail(err) => RedbError::Decode(err),
            StorageError::Corrupt(msg) => RedbError::Corrupt(msg),
        }
    }
}

impl From<rusqlite::Error> for RedbError {
    fn from(err: rusqlite::Error) -> Self {
        RedbError::from(StorageError::from(err))
    }
}

impl From<DatabaseError> for RedbError {
    fn from(err: DatabaseError) -> Self {
        RedbError::Redb(err.into())
    }
}

impl From<TransactionError> for RedbError {
    fn from(err: TransactionError) -> Self {
        RedbError::Redb(err.into())
    }
}

impl From<TableError> for RedbError {
    fn from(err: TableError) -> Self {
        RedbError::Redb(err.into())
    }
}

impl From<RedbStorageError> for RedbError {
    fn from(err: RedbStorageError) -> Self {
        RedbError::Redb(err.into())
    }
}

impl From<CommitError> for RedbError {
    fn from(err: CommitError) -> Self {
        RedbError::Redb(err.into())
    }
}

type Result<T> = std::result::Result<T, RedbError>;

fn ensure_metadata_initialized(db: &RedbDatabase) -> Result<()> {
    let txn = db.begin_write()?;
    {
        let mut meta = txn.open_table(METADATA_TABLE)?;
        let has_version = {
            let existing = meta.get("version")?;
            if let Some(existing) = existing {
                let version = existing.value();
                if version != VERSION as u64 {
                    return Err(RedbError::IncorrectVersion(version));
                }
                true
            } else {
                false
            }
        };
        if !has_version {
            meta.insert("version", &(VERSION as u64))?;
        }
    }
    txn.commit()?;
    Ok(())
}

fn check_metadata_version(db: &RedbDatabase) -> Result<()> {
    let txn = db.begin_read()?;
    let table = txn.open_table(METADATA_TABLE)?;
    if let Some(version) = table.get("version")? {
        let version = version.value();
        if version != VERSION as u64 {
            return Err(RedbError::IncorrectVersion(version));
        }
        return Ok(());
    }
    Err(RedbError::Corrupt("missing metadata version".into()))
}

fn read_graph_record(db: &RedbDatabase, file: &str) -> Result<Option<GraphRecord>> {
    let txn = db.begin_read()?;
    let table = txn.open_table(GRAPHS_TABLE)?;
    let record = table
        .get(file)?
        .map(|value| GraphRecord::decode(value.value()))
        .transpose()?;
    Ok(record)
}

pub struct RedbReader {
    db: RedbDatabase,
    loaded_graphs: HashSet<Handle<File>>,
    loaded_node_paths: HashSet<Handle<Node>>,
    loaded_root_paths: HashSet<SymbolStackQueryHandle>,
    node_paths_prefetched: HashSet<Handle<File>>,
    root_paths_prefetched: HashSet<Handle<File>>,
    graph: StackGraph,
    partials: PartialPaths,
    database: Database,
    stats: Stats,
    symbol_stack_queries: SymbolStackQueryPool,
}

impl RedbReader {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path_ref = path.as_ref();
        if !path_ref.exists() {
            return Err(RedbError::MissingDatabase(
                path_ref.to_string_lossy().to_string(),
            ));
        }

        let db = RedbDatabase::open(path_ref)?;
        Self::from_database(db)
    }

    pub fn from_database(db: RedbDatabase) -> Result<Self> {
        check_metadata_version(&db)?;
        Ok(Self {
            db,
            loaded_graphs: HashSet::new(),
            loaded_node_paths: HashSet::new(),
            loaded_root_paths: HashSet::new(),
            node_paths_prefetched: HashSet::new(),
            root_paths_prefetched: HashSet::new(),
            graph: StackGraph::new(),
            partials: PartialPaths::new(),
            database: Database::new(),
            stats: Stats::default(),
            symbol_stack_queries: SymbolStackQueryPool::new(),
        })
    }

    pub fn clear(&mut self) {
        self.loaded_graphs.clear();
        self.graph = StackGraph::new();

        self.loaded_node_paths.clear();
        self.loaded_root_paths.clear();
        self.node_paths_prefetched.clear();
        self.root_paths_prefetched.clear();
        self.partials.clear();
        self.database.clear();
        self.stats.clear();
        self.symbol_stack_queries.clear();
    }

    pub fn clear_paths(&mut self) {
        self.loaded_node_paths.clear();
        self.loaded_root_paths.clear();
        self.node_paths_prefetched.clear();
        self.root_paths_prefetched.clear();
        self.partials.clear();
        self.database.clear();
        self.stats.clear_paths();
        self.symbol_stack_queries.clear();
    }

    pub fn status_for_file<T: AsRef<str>>(&self, file: &str, tag: Option<T>) -> Result<FileStatus> {
        if let Some(record) = read_graph_record(&self.db, file)? {
            if let Some(expected) = tag {
                if record.tag != expected.as_ref() {
                    return Ok(FileStatus::Missing);
                }
            }
            match record.error {
                Some(error) => Ok(FileStatus::Error(error)),
                None => Ok(FileStatus::Indexed),
            }
        } else {
            Ok(FileStatus::Missing)
        }
    }

    fn load_graph_for_file(&mut self, file: &str) -> Result<Handle<File>> {
        if let Some(handle) = self.graph.get_file(file) {
            if self.loaded_graphs.contains(&handle) {
                self.stats.file_cached += 1;
                return Ok(handle);
            }
        }

        self.stats.file_loads += 1;
        let txn = self.db.begin_read()?;
        let table = txn.open_table(GRAPHS_TABLE)?;
        let value = table
            .get(file)?
            .ok_or_else(|| RedbError::Corrupt(format!("missing graph for {file}")))?;
        let record = GraphRecord::decode(value.value())?;
        let (graph, _) = bincode::decode_from_slice::<serde::StackGraph, _>(
            record.graph_blob.as_slice(),
            BINCODE_CONFIG,
        )?;
        graph.load_into(&mut self.graph)?;
        let handle = self.graph.get_file(file).expect("loaded file to exist");
        self.loaded_graphs.insert(handle);
        Ok(handle)
    }

    fn load_graphs_for_file_or_directory(
        &mut self,
        file_or_directory: &Path,
        cancellation_flag: &dyn CancellationFlag,
    ) -> Result<()> {
        let mut listing = RedbFileListing::new(&self.db, Some(file_or_directory.to_path_buf()))?;
        for entry in listing.try_iter()? {
            cancellation_flag.check("loading graphs")?;
            let entry = entry?;
            let path = entry.path.to_string_lossy().to_string();
            self.load_graph_for_file(&path)?;
        }
        Ok(())
    }

    fn preload_node_paths_for_file(
        &mut self,
        file: Handle<File>,
        cancellation_flag: &dyn CancellationFlag,
    ) -> Result<()> {
        if !self.node_paths_prefetched.insert(file) {
            return Ok(());
        }
        let file_name = self.graph[file].name().to_string();
        let (start, end) = node_range_bounds(&file_name);
        let txn = self.db.begin_read()?;
        let table = txn.open_table(FILE_PATHS_TABLE)?;
        let mut iter = table.range(start.as_slice()..=end.as_slice())?;
        while let Some(Ok((key, value))) = iter.next() {
            cancellation_flag.check("loading node paths")?;
            let local_id = decode_local_id(key.value())?;
            let path = decode_partial_path(value.value(), &mut self.graph, &mut self.partials)?;
            self.database
                .add_partial_path(&self.graph, &mut self.partials, path);
            if let Some(handle) = self.graph.node_for_id(NodeID::new_in_file(file, local_id)) {
                self.loaded_node_paths.insert(handle);
            }
        }
        Ok(())
    }

    fn preload_root_paths_for_file(
        &mut self,
        file: Handle<File>,
        cancellation_flag: &dyn CancellationFlag,
    ) -> Result<()> {
        if !self.root_paths_prefetched.insert(file) {
            return Ok(());
        }
        let file_name = self.graph[file].name().to_string();
        let (start, end) = root_file_range_bounds(&file_name);
        let txn = self.db.begin_read()?;
        let table = txn.open_table(ROOT_PATHS_BY_FILE_TABLE)?;
        let mut iter = table.range(start.as_slice()..=end.as_slice())?;
        while let Some(Ok((_key, value))) = iter.next() {
            cancellation_flag.check("loading root paths")?;
            let path = decode_partial_path(value.value(), &mut self.graph, &mut self.partials)?;
            let handles = path.symbol_stack_precondition.storage_key_queries(
                &self.graph,
                &mut self.partials,
                &mut self.symbol_stack_queries,
            );
            for handle in handles {
                let key = self.symbol_stack_queries.get_key(handle);
                if matches!(
                    key,
                    SymbolStackQueryKey::Exact {
                        variant: SymbolStackExactVariant::FullStack,
                        ..
                    }
                ) {
                    self.loaded_root_paths.insert(handle);
                }
            }
            self.database
                .add_partial_path(&self.graph, &mut self.partials, path);
        }
        Ok(())
    }

    fn files_with_exact_root_symbol_stack(
        &self,
        key: &str,
        cancellation_flag: &dyn CancellationFlag,
    ) -> Result<Vec<String>> {
        let txn = self.db.begin_read()?;
        let table = txn.open_multimap_table(ROOT_PATHS_BY_SYMBOL_TABLE)?;
        let mut iter = table.get(key)?;
        let mut files: Vec<String> = Vec::new();
        while let Some(Ok(file)) = iter.next() {
            cancellation_flag.check("loading root paths")?;
            files.push(file.value().to_string());
        }
        files.sort();
        files.dedup();
        Ok(files)
    }

    fn load_paths_for_node(
        &mut self,
        node: Handle<Node>,
        cancellation_flag: &dyn CancellationFlag,
    ) -> Result<()> {
        if !self.loaded_node_paths.insert(node) {
            self.stats.node_path_cached += 1;
            return Ok(());
        }
        self.stats.node_path_loads += 1;
        let id = self.graph[node].id();
        let file = id.file().expect("file node required");
        self.preload_node_paths_for_file(file, cancellation_flag)?;
        Ok(())
    }

    fn load_paths_for_root(
        &mut self,
        symbol_stack: PartialSymbolStack,
        cancellation_flag: &dyn CancellationFlag,
    ) -> Result<()> {
        let query_handles = symbol_stack.storage_key_queries(
            &self.graph,
            &mut self.partials,
            &mut self.symbol_stack_queries,
        );
        for handle in query_handles {
            if !self.loaded_root_paths.insert(handle) {
                self.stats.root_path_cached += 1;
                continue;
            }
            self.stats.root_path_loads += 1;
            let query = self.symbol_stack_queries.get(handle).clone();
            self.stats.record_root_path_load(&query);
            match query {
                SymbolStackQuery::Exact(key) => {
                    let files = self.files_with_exact_root_symbol_stack(&key, cancellation_flag)?;
                    for file in files {
                        cancellation_flag.check("loading root paths")?;
                        let handle = self.load_graph_for_file(&file)?;
                        self.preload_root_paths_for_file(handle, cancellation_flag)?;
                    }
                }
                SymbolStackQuery::Range { start, end } => {
                    self.load_root_paths_for_range(&start, &end, cancellation_flag)?;
                }
            }
        }
        Ok(())
    }

    fn load_root_paths_for_range(
        &mut self,
        start: &str,
        end: &str,
        cancellation_flag: &dyn CancellationFlag,
    ) -> Result<()> {
        let mut files: Vec<String> = {
            let txn = self.db.begin_read()?;
            let table = txn.open_multimap_table(ROOT_PATHS_BY_SYMBOL_TABLE)?;
            let mut range = table.range(start..end)?;
            let mut files = Vec::new();
            while let Some(Ok((_, mut values))) = range.next() {
                cancellation_flag.check("loading root paths")?;
                while let Some(Ok(value)) = values.next() {
                    cancellation_flag.check("loading root paths")?;
                    files.push(value.value().to_string());
                }
            }
            files
        };
        files.sort();
        files.dedup();
        for file in files {
            let handle = self.load_graph_for_file(&file)?;
            self.preload_root_paths_for_file(handle, cancellation_flag)?;
        }
        Ok(())
    }

    fn load_partial_path_extensions(
        &mut self,
        path: &PartialPath,
        cancellation_flag: &dyn CancellationFlag,
    ) -> Result<()> {
        let end_node = self.graph[path.end_node].id();
        if self.graph[path.end_node].file().is_some() {
            self.load_paths_for_node(path.end_node, cancellation_flag)?;
        } else if end_node.is_root() {
            self.load_paths_for_root(path.symbol_stack_postcondition, cancellation_flag)?;
        }
        Ok(())
    }

    pub fn stats(&self) -> Stats {
        self.stats.clone()
    }

    pub fn components_mut(&mut self) -> StorageComponents<'_> {
        (&mut self.graph, &mut self.partials, &mut self.database)
    }

    pub fn database(&self) -> &Database {
        &self.database
    }
}

impl StorageReader for RedbReader {
    type Error = RedbError;
    type ListAll<'a>
        = RedbFileListing<'a>
    where
        Self: 'a;
    type ListByPath<'a>
        = RedbFileListing<'a>
    where
        Self: 'a;

    fn clear(&mut self) {
        RedbReader::clear(self);
    }

    fn clear_paths(&mut self) {
        RedbReader::clear_paths(self);
    }

    fn status_for_file<T: AsRef<str>>(
        &mut self,
        file: &str,
        tag: Option<T>,
    ) -> std::result::Result<FileStatus, Self::Error> {
        RedbReader::status_for_file(self, file, tag.as_ref().map(|t| t.as_ref()))
    }

    fn list_all(&mut self) -> std::result::Result<Self::ListAll<'_>, Self::Error> {
        RedbFileListing::new(&self.db, None)
    }

    fn list_file_or_directory(
        &mut self,
        file_or_directory: &Path,
    ) -> std::result::Result<Self::ListByPath<'_>, Self::Error> {
        RedbFileListing::new(&self.db, Some(file_or_directory.to_path_buf()))
    }

    fn load_graph_for_file(
        &mut self,
        file: &str,
    ) -> std::result::Result<Handle<File>, Self::Error> {
        RedbReader::load_graph_for_file(self, file)
    }

    fn load_graphs_for_file_or_directory(
        &mut self,
        file_or_directory: &Path,
        cancellation_flag: &dyn CancellationFlag,
    ) -> std::result::Result<(), Self::Error> {
        RedbReader::load_graphs_for_file_or_directory(self, file_or_directory, cancellation_flag)
    }

    fn preload_node_paths_for_file(
        &mut self,
        file: Handle<File>,
        cancellation_flag: &dyn CancellationFlag,
    ) -> std::result::Result<(), Self::Error> {
        RedbReader::preload_node_paths_for_file(self, file, cancellation_flag)
    }

    fn preload_root_paths_for_file(
        &mut self,
        file: Handle<File>,
        cancellation_flag: &dyn CancellationFlag,
    ) -> std::result::Result<(), Self::Error> {
        RedbReader::preload_root_paths_for_file(self, file, cancellation_flag)
    }

    fn load_partial_path_extensions(
        &mut self,
        path: &PartialPath,
        cancellation_flag: &dyn CancellationFlag,
    ) -> std::result::Result<(), Self::Error> {
        RedbReader::load_partial_path_extensions(self, path, cancellation_flag)
    }

    fn graph(&self) -> &StackGraph {
        &self.graph
    }

    fn database(&self) -> &Database {
        &self.database
    }

    fn components_mut(&mut self) -> StorageComponents<'_> {
        (&mut self.graph, &mut self.partials, &mut self.database)
    }

    fn stats(&self) -> Stats {
        RedbReader::stats(self)
    }
}

pub struct RedbWriter {
    db: RedbDatabase,
    graph_buf: Vec<u8>,
    path_buf: Vec<u8>,
}

impl RedbWriter {
    pub fn open_in_memory() -> Result<Self> {
        let db = RedbDatabase::builder().create_with_backend(InMemoryBackend::new())?;
        ensure_metadata_initialized(&db)?;
        Ok(Self {
            db,
            graph_buf: Vec::new(),
            path_buf: Vec::new(),
        })
    }

    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path_ref = path.as_ref();
        if let Some(parent) = path_ref.parent() {
            fs::create_dir_all(parent)?;
        }
        let db = if path_ref.exists() {
            let db = RedbDatabase::open(path_ref)?;
            ensure_metadata_initialized(&db)?;
            db
        } else {
            let db = RedbDatabase::create(path_ref)?;
            ensure_metadata_initialized(&db)?;
            db
        };
        Ok(Self {
            db,
            graph_buf: Vec::new(),
            path_buf: Vec::new(),
        })
    }

    pub fn clean_all(&mut self) -> Result<usize> {
        let mut txn = self.db.begin_write()?;
        let count = Self::clean_all_inner(&mut txn)?;
        txn.commit()?;
        Ok(count)
    }

    pub fn clean_file(&mut self, file: &Path) -> Result<usize> {
        let mut txn = self.db.begin_write()?;
        let file_name = file.to_string_lossy().to_string();
        let count = Self::clean_file_inner(&mut txn, &file_name)?;
        txn.commit()?;
        Ok(count)
    }

    pub fn clean_file_or_directory(&mut self, file_or_directory: &Path) -> Result<usize> {
        let mut txn = self.db.begin_write()?;
        let count = Self::clean_file_or_directory_inner(&mut txn, file_or_directory)?;
        txn.commit()?;
        Ok(count)
    }

    pub fn store_error_for_file(&mut self, file: &Path, tag: &str, error: &str) -> Result<()> {
        let mut txn = self.db.begin_write()?;
        let file_name = file.to_string_lossy().to_string();
        Self::clean_file_inner(&mut txn, &file_name)?;
        {
            let mut graphs = txn.open_table(GRAPHS_TABLE)?;
            let graph = crate::serde::StackGraph::default();
            let serialized = super::encode_into_buf(&graph, &mut self.graph_buf)
                .map_err(StorageError::from)
                .map_err(RedbError::from)?;
            let encoded = GraphRecord::encode(tag, Some(error), serialized);
            graphs.insert(file_name.as_str(), encoded.as_slice())?;
        }
        txn.commit()?;
        Ok(())
    }

    pub fn store_result_for_file<'a, IP>(
        &mut self,
        graph: &StackGraph,
        file: Handle<File>,
        tag: &str,
        partials: &mut PartialPaths,
        paths: IP,
    ) -> Result<()>
    where
        IP: IntoIterator<Item = &'a PartialPath>,
    {
        let mut txn = self.db.begin_write()?;
        let file_name = graph[file].name().to_string();
        Self::clean_file_inner(&mut txn, &file_name)?;
        {
            let mut graphs = txn.open_table(GRAPHS_TABLE)?;
            let graph_value = serde::StackGraph::from_graph_filter(graph, &FileFilter(file));
            let serialized = super::encode_into_buf(&graph_value, &mut self.graph_buf)
                .map_err(StorageError::from)
                .map_err(RedbError::from)?;
            let encoded = GraphRecord::encode(tag, None, serialized);
            graphs.insert(file_name.as_str(), encoded.as_slice())?;
        }
        {
            let mut node_table = txn.open_table(FILE_PATHS_TABLE)?;
            let mut root_by_file = txn.open_table(ROOT_PATHS_BY_FILE_TABLE)?;
            let mut root_by_symbol = txn.open_multimap_table(ROOT_PATHS_BY_SYMBOL_TABLE)?;
            for path in paths {
                encode_partial_path(graph, partials, path, &mut self.path_buf)
                    .map_err(RedbError::from)?;
                let serialized = self.path_buf.as_slice();
                let start_node = graph[path.start_node].id();
                if start_node.is_root() {
                    let symbol_stack = path.symbol_stack_precondition.storage_key(graph, partials);
                    let key = encode_root_key(file_name.as_str(), &symbol_stack);
                    root_by_file.insert(key.as_slice(), serialized)?;
                    root_by_symbol.insert(symbol_stack.as_str(), file_name.as_str())?;
                } else if start_node.is_in_file(file) {
                    let key = encode_node_key(file_name.as_str(), start_node.local_id());
                    node_table.insert(key.as_slice(), serialized)?;
                } else {
                    panic!(
                        "added path {} must start in given file {} or at root",
                        path.display(graph, partials),
                        graph[file].name()
                    );
                }
            }
        }
        txn.commit()?;
        Ok(())
    }

    pub fn status_for_file(&mut self, file: &str, tag: Option<&str>) -> Result<FileStatus> {
        if let Some(record) = read_graph_record(&self.db, file)? {
            if let Some(expected) = tag {
                if record.tag != expected {
                    return Ok(FileStatus::Missing);
                }
            }
            match record.error {
                Some(error) => Ok(FileStatus::Error(error)),
                None => Ok(FileStatus::Indexed),
            }
        } else {
            Ok(FileStatus::Missing)
        }
    }

    pub fn into_reader(self) -> Result<RedbReader> {
        RedbReader::from_database(self.db)
    }

    fn clean_all_inner(txn: &mut WriteTransaction<'_>) -> Result<usize> {
        let mut graphs = txn.open_table(GRAPHS_TABLE)?;
        let mut count = 0usize;
        {
            let mut drain = graphs.drain::<&str>(..)?;
            while let Some(entry) = drain.next() {
                entry?;
                count += 1;
            }
        }
        drop(graphs);

        {
            let mut nodes = txn.open_table(FILE_PATHS_TABLE)?;
            let mut drain = nodes.drain::<&[u8]>(..)?;
            while let Some(entry) = drain.next() {
                entry?;
            }
        }

        {
            let mut roots = txn.open_table(ROOT_PATHS_BY_FILE_TABLE)?;
            let mut drain = roots.drain::<&[u8]>(..)?;
            while let Some(entry) = drain.next() {
                entry?;
            }
        }

        {
            let mut symbols = txn.open_multimap_table(ROOT_PATHS_BY_SYMBOL_TABLE)?;
            let mut keys = Vec::new();
            {
                let mut iter = symbols.iter()?;
                while let Some(entry) = iter.next() {
                    let (key, mut values) = entry?;
                    keys.push(key.value().to_string());
                    while let Some(value) = values.next() {
                        value?;
                    }
                }
            }
            for key in keys {
                let mut removed = symbols.remove_all(key.as_str())?;
                while let Some(value) = removed.next() {
                    value?;
                }
            }
        }

        Ok(count)
    }

    fn clean_file_inner(txn: &mut WriteTransaction<'_>, file: &str) -> Result<usize> {
        let mut removed = 0usize;
        {
            let mut graphs = txn.open_table(GRAPHS_TABLE)?;
            if graphs.remove(file)?.is_some() {
                removed = 1;
            }
        }

        let (start, end) = node_range_bounds(file);
        {
            let mut nodes = txn.open_table(FILE_PATHS_TABLE)?;
            let mut drain = nodes.drain(start.as_slice()..=end.as_slice())?;
            while let Some(entry) = drain.next() {
                entry?;
            }
        }

        let (start_root, end_root) = root_file_range_bounds(file);
        let mut symbols_to_remove = Vec::new();
        {
            let mut roots = txn.open_table(ROOT_PATHS_BY_FILE_TABLE)?;
            let mut drain = roots.drain(start_root.as_slice()..=end_root.as_slice())?;
            while let Some(entry) = drain.next() {
                let (key, _value) = entry?;
                symbols_to_remove.push(decode_root_symbol(key.value())?);
            }
        }

        if !symbols_to_remove.is_empty() {
            let mut symbols = txn.open_multimap_table(ROOT_PATHS_BY_SYMBOL_TABLE)?;
            for symbol in symbols_to_remove {
                while symbols.remove(symbol.as_str(), file)? {}
            }
        }

        Ok(removed)
    }

    fn clean_file_or_directory_inner(
        txn: &mut WriteTransaction<'_>,
        file_or_directory: &Path,
    ) -> Result<usize> {
        let graphs = txn.open_table(GRAPHS_TABLE)?;
        let mut to_remove = Vec::new();
        {
            let mut iter = graphs.iter()?;
            while let Some(entry) = iter.next() {
                let (key, _value) = entry?;
                let path = PathBuf::from(key.value());
                if path_descendant_of(&path, file_or_directory) {
                    to_remove.push(path);
                }
            }
        }
        drop(graphs);

        let mut removed = 0usize;
        for path in to_remove {
            let file_name = path.to_string_lossy().to_string();
            removed += Self::clean_file_inner(txn, &file_name)?;
        }
        Ok(removed)
    }
}

impl StorageWriter for RedbWriter {
    type Error = RedbError;
    type Reader = RedbReader;

    fn open_in_memory() -> std::result::Result<Self, Self::Error> {
        RedbWriter::open_in_memory()
    }

    fn open<P: AsRef<Path>>(path: P) -> std::result::Result<Self, Self::Error> {
        RedbWriter::open(path)
    }

    fn clean_all(&mut self) -> std::result::Result<usize, Self::Error> {
        RedbWriter::clean_all(self)
    }

    fn clean_file(&mut self, file: &Path) -> std::result::Result<usize, Self::Error> {
        RedbWriter::clean_file(self, file)
    }

    fn clean_file_or_directory(
        &mut self,
        file_or_directory: &Path,
    ) -> std::result::Result<usize, Self::Error> {
        RedbWriter::clean_file_or_directory(self, file_or_directory)
    }

    fn store_error_for_file(
        &mut self,
        file: &Path,
        tag: &str,
        error: &str,
    ) -> std::result::Result<(), Self::Error> {
        RedbWriter::store_error_for_file(self, file, tag, error)
    }

    fn store_result_for_file<'a, IP>(
        &mut self,
        graph: &StackGraph,
        file: Handle<File>,
        tag: &str,
        partials: &mut PartialPaths,
        paths: IP,
    ) -> std::result::Result<(), Self::Error>
    where
        IP: IntoIterator<Item = &'a PartialPath>,
    {
        RedbWriter::store_result_for_file(self, graph, file, tag, partials, paths)
    }

    fn status_for_file(
        &mut self,
        file: &str,
        tag: Option<&str>,
    ) -> std::result::Result<FileStatus, Self::Error> {
        RedbWriter::status_for_file(self, file, tag)
    }

    fn into_reader(self) -> std::result::Result<Self::Reader, Self::Error> {
        RedbWriter::into_reader(self)
    }
}

pub struct RedbFileListing<'a> {
    db: &'a RedbDatabase,
    directory: Option<PathBuf>,
}

impl<'a> RedbFileListing<'a> {
    fn new(db: &'a RedbDatabase, directory: Option<PathBuf>) -> Result<Self> {
        Ok(Self { db, directory })
    }

    fn collect_entries(&self) -> Result<Vec<FileEntry>> {
        let txn = self.db.begin_read()?;
        let table = txn.open_table(GRAPHS_TABLE)?;
        let mut entries = Vec::new();
        let mut iter = table.iter()?;
        while let Some(Ok((key, value))) = iter.next() {
            let path = key.value();
            let record = GraphRecord::decode(value.value())?;
            let path_buf = PathBuf::from(path);
            if let Some(dir) = &self.directory {
                if !path_descendant_of(&path_buf, dir) {
                    continue;
                }
            }
            let status = match &record.error {
                Some(error) => FileStatus::Error(error.clone()),
                None => FileStatus::Indexed,
            };
            entries.push(FileEntry {
                path: path_buf,
                tag: record.tag.clone(),
                status,
            });
        }
        Ok(entries)
    }
}

pub struct RedbFileEntries {
    entries: VecDeque<FileEntry>,
}

impl Iterator for RedbFileEntries {
    type Item = Result<FileEntry>;

    fn next(&mut self) -> Option<Self::Item> {
        self.entries.pop_front().map(Ok)
    }
}

impl<'a> StorageFileListing<'a> for RedbFileListing<'a> {
    type Error = RedbError;
    type Iter = RedbFileEntries;

    fn try_iter(&'a mut self) -> std::result::Result<Self::Iter, Self::Error> {
        let entries = self.collect_entries()?;
        Ok(RedbFileEntries {
            entries: entries.into(),
        })
    }
}

pub(crate) struct GraphRecord {
    tag: String,
    error: Option<String>,
    graph_blob: Vec<u8>,
}

impl GraphRecord {
    pub(crate) fn decode(data: &[u8]) -> Result<Self> {
        let mut slice = data;
        let tag_len = read_u32(&mut slice)? as usize;
        let tag = read_string(&mut slice, tag_len)?;
        let has_error = read_u8(&mut slice)? != 0;
        let error = if has_error {
            let len = read_u32(&mut slice)? as usize;
            Some(read_string(&mut slice, len)?)
        } else {
            None
        };
        let graph_len = read_u32(&mut slice)? as usize;
        if slice.len() < graph_len {
            return Err(RedbError::Corrupt("graph blob truncated".into()));
        }
        let graph_blob = slice[..graph_len].to_vec();
        Ok(Self {
            tag,
            error,
            graph_blob,
        })
    }

    pub(crate) fn encode(tag: &str, error: Option<&str>, graph_blob: &[u8]) -> Vec<u8> {
        let mut buf = Vec::with_capacity(
            4 + tag.len() + 1 + error.map(|e| 4 + e.len()).unwrap_or(0) + 4 + graph_blob.len(),
        );
        write_u32(&mut buf, tag.len() as u32);
        buf.extend_from_slice(tag.as_bytes());
        match error {
            Some(err) => {
                buf.push(1);
                write_u32(&mut buf, err.len() as u32);
                buf.extend_from_slice(err.as_bytes());
            }
            None => buf.push(0),
        }
        write_u32(&mut buf, graph_blob.len() as u32);
        buf.extend_from_slice(graph_blob);
        buf
    }
}

fn read_u32(slice: &mut &[u8]) -> Result<u32> {
    if slice.len() < 4 {
        return Err(RedbError::Corrupt("unexpected end of record".into()));
    }
    let value = u32::from_be_bytes(slice[..4].try_into().unwrap());
    *slice = &slice[4..];
    Ok(value)
}

fn read_u8(slice: &mut &[u8]) -> Result<u8> {
    if slice.is_empty() {
        return Err(RedbError::Corrupt("unexpected end of record".into()));
    }
    let value = slice[0];
    *slice = &slice[1..];
    Ok(value)
}

fn read_string(slice: &mut &[u8], len: usize) -> Result<String> {
    if slice.len() < len {
        return Err(RedbError::Corrupt("unexpected end of record".into()));
    }
    let value = std::str::from_utf8(&slice[..len])
        .map_err(|_| RedbError::Corrupt("invalid utf8".into()))?
        .to_string();
    *slice = &slice[len..];
    Ok(value)
}

fn node_range_bounds(file: &str) -> (Vec<u8>, Vec<u8>) {
    let prefix = file_path_prefix(file);
    let mut start = prefix.clone();
    start.extend_from_slice(&0u32.to_be_bytes());
    let mut end = prefix;
    end.extend_from_slice(&u32::MAX.to_be_bytes());
    (start, end)
}

fn root_file_range_bounds(file: &str) -> (Vec<u8>, Vec<u8>) {
    let prefix = root_file_prefix(file);
    let mut end = prefix.clone();
    end.push(0xFF);
    (prefix, end)
}

fn decode_local_id(key: &[u8]) -> Result<u32> {
    if key.len() < 4 {
        return Err(RedbError::Corrupt("node key truncated".into()));
    }
    let len = key.len();
    Ok(u32::from_be_bytes(key[len - 4..].try_into().unwrap()))
}

fn decode_root_symbol(key: &[u8]) -> Result<String> {
    let parts = key.split(|b| *b == 0).collect::<Vec<_>>();
    if parts.len() != 2 {
        return Err(RedbError::Corrupt("invalid root path key".into()));
    }
    let symbol = std::str::from_utf8(parts[1])
        .map_err(|_| RedbError::Corrupt("invalid utf8".into()))?
        .to_string();
    Ok(symbol)
}

fn file_path_prefix(file: &str) -> Vec<u8> {
    let mut prefix = Vec::with_capacity(file.len() + 1);
    prefix.extend_from_slice(file.as_bytes());
    prefix.push(0);
    prefix
}

fn root_file_prefix(file: &str) -> Vec<u8> {
    file_path_prefix(file)
}

fn path_descendant_of(path: &Path, parent: &Path) -> bool {
    if parent.as_os_str().is_empty() {
        return true;
    }
    path.starts_with(parent)
}

fn encode_node_key(file: &str, local_id: u32) -> Vec<u8> {
    let mut key = file_path_prefix(file);
    key.extend_from_slice(&local_id.to_be_bytes());
    key
}

fn encode_root_key(file: &str, symbol: &str) -> Vec<u8> {
    let mut key = root_file_prefix(file);
    key.extend_from_slice(symbol.as_bytes());
    key
}

fn write_u32(buf: &mut Vec<u8>, value: u32) {
    buf.extend_from_slice(&value.to_be_bytes());
}

pub fn convert_sqlite_to_redb(sqlite_path: &Path, redb_path: &Path) -> Result<()> {
    if let Some(parent) = redb_path.parent() {
        fs::create_dir_all(parent)?;
    }
    if redb_path.exists() {
        fs::remove_file(redb_path)?;
    }

    let sqlite = Connection::open(sqlite_path)?;
    let db = RedbDatabase::create(redb_path)?;
    let txn = db.begin_write()?;

    {
        let mut meta = txn.open_table(METADATA_TABLE)?;
        meta.insert("version", &(VERSION as u64))?;
    }

    {
        let mut graphs_table = txn.open_table(GRAPHS_TABLE)?;
        let mut stmt = sqlite.prepare("SELECT file, tag, error, value FROM graphs")?;
        let mut rows = stmt.query([])?;
        while let Some(row) = rows.next()? {
            let file: String = row.get(0)?;
            let tag: String = row.get(1)?;
            let error: Option<String> = row.get(2)?;
            let blob: Vec<u8> = row.get(3)?;
            let encoded = GraphRecord::encode(&tag, error.as_deref(), &blob);
            graphs_table.insert(file.as_str(), encoded.as_slice())?;
        }
    }

    {
        let mut file_table = txn.open_table(FILE_PATHS_TABLE)?;
        let mut stmt = sqlite.prepare("SELECT file, local_id, value FROM file_paths")?;
        let mut rows = stmt.query([])?;
        while let Some(row) = rows.next()? {
            let file: String = row.get(0)?;
            let local_id: u32 = row.get(1)?;
            let blob: Vec<u8> = row.get(2)?;
            let key = encode_node_key(&file, local_id);
            file_table.insert(key.as_slice(), blob.as_slice())?;
        }
    }

    {
        let mut file_table = txn.open_table(ROOT_PATHS_BY_FILE_TABLE)?;
        let mut symbol_table = txn.open_multimap_table(ROOT_PATHS_BY_SYMBOL_TABLE)?;
        let mut stmt = sqlite.prepare("SELECT file, symbol_stack, value FROM root_paths")?;
        let mut rows = stmt.query([])?;
        while let Some(row) = rows.next()? {
            let file: String = row.get(0)?;
            let symbol: String = row.get(1)?;
            let blob: Vec<u8> = row.get(2)?;
            let key = encode_root_key(&file, &symbol);
            file_table.insert(key.as_slice(), blob.as_slice())?;
            symbol_table.insert(symbol.as_str(), file.as_str())?;
        }
    }

    txn.commit()?;
    Ok(())
}
