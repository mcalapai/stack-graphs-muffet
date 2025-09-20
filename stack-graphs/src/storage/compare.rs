// -*- coding: utf-8 -*-
// ------------------------------------------------------------------------------------------------
// Copyright © 2024, stack-graphs authors.
// Licensed under either of Apache License, Version 2.0, or MIT license, at your option.
// Please see the LICENSE-APACHE or LICENSE-MIT files in this distribution for license details.
// ------------------------------------------------------------------------------------------------

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::path::Path;

use redb::{
    Database, DatabaseError, MultimapTableDefinition, ReadableMultimapTable, ReadableTable,
    StorageError as RedbStorageError, TableDefinition, TableError, TransactionError,
};
use rusqlite::Connection;
use serde::Serialize;
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::graph::StackGraph;
use crate::partial::PartialPaths;
use crate::serde::Error as SerdeGraphError;
use crate::serde::StackGraph as SerdeStackGraph;
use crate::storage::encoding::{decode_partial_path, encode_partial_path};
use crate::storage::{StorageError, BINCODE_CONFIG, VERSION};

const METADATA_TABLE: TableDefinition<'static, &str, u64> = TableDefinition::new("metadata");
const GRAPHS_TABLE: TableDefinition<'static, &str, &[u8]> = TableDefinition::new("graphs");
const FILE_PATHS_TABLE: MultimapTableDefinition<'static, &[u8], &[u8]> =
    MultimapTableDefinition::new("file_paths");
const ROOT_PATHS_BY_FILE_TABLE: MultimapTableDefinition<'static, &[u8], &[u8]> =
    MultimapTableDefinition::new("root_paths_by_file");

#[derive(Debug, Error)]
pub enum ComparisonError {
    #[error(transparent)]
    Sqlite(#[from] rusqlite::Error),
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Redb(#[from] redb::Error),
    #[error(transparent)]
    RedbStorage(#[from] RedbStorageError),
    #[error(transparent)]
    RedbDatabase(#[from] DatabaseError),
    #[error(transparent)]
    RedbTransaction(#[from] TransactionError),
    #[error(transparent)]
    RedbTable(#[from] TableError),
    #[error(transparent)]
    Decode(#[from] bincode::error::DecodeError),
    #[error(transparent)]
    Encode(#[from] bincode::error::EncodeError),
    #[error("failed to load graph {file}: {source}")]
    GraphLoad {
        file: String,
        source: SerdeGraphError,
    },
    #[error("sqlite schema version mismatch: found {found}, expected {expected}")]
    SchemaVersionMismatch { found: usize, expected: usize },
    #[error("redb schema version mismatch: found {found}, expected {expected}")]
    RedbSchemaVersionMismatch { found: u64, expected: u64 },
    #[error("invalid redb key: {0}")]
    InvalidRedbKey(String),
    #[error("corrupt redb graph record: {0}")]
    CorruptRedbGraph(String),
}

#[derive(Debug)]
struct GraphRecordData {
    file: String,
    tag: String,
    error: Option<String>,
    digest: String,
    summary: GraphSummary,
}

#[derive(Debug, Clone, Serialize)]
pub struct GraphEntry {
    pub file: String,
    pub tag: String,
    pub error: Option<String>,
    pub digest: String,
    pub file_count: usize,
    pub node_count: usize,
    pub edge_count: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct GraphMismatch {
    pub file: String,
    pub sqlite: GraphEntry,
    pub redb: GraphEntry,
    pub differences: Vec<String>,
}

#[derive(Debug)]
struct NodePathRecordData {
    file: String,
    local_id: u32,
    digest: String,
    summary: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct NodePathEntry {
    pub file: String,
    pub local_id: u32,
    pub digest: String,
    pub summary: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct NodePathMismatch {
    pub file: String,
    pub local_id: u32,
    pub sqlite: NodePathEntry,
    pub redb: NodePathEntry,
    pub differences: Vec<String>,
}

#[derive(Debug)]
struct RootPathRecordData {
    file: String,
    symbol_stack: String,
    digest: String,
    summary: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct RootPathEntry {
    pub file: String,
    pub symbol_stack: String,
    pub digest: String,
    pub summary: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct RootPathMismatch {
    pub file: String,
    pub symbol_stack: String,
    pub sqlite: RootPathEntry,
    pub redb: RootPathEntry,
}

#[derive(Debug, Serialize)]
pub struct TableDiff<R, M>
where
    R: Serialize,
    M: Serialize,
{
    pub sqlite_count: usize,
    pub redb_count: usize,
    pub missing_in_redb: Vec<R>,
    pub extra_in_redb: Vec<R>,
    pub mismatched: Vec<M>,
}

#[derive(Debug, Serialize)]
pub struct ComparisonReport {
    pub graphs: TableDiff<GraphEntry, GraphMismatch>,
    pub file_paths: TableDiff<NodePathEntry, NodePathMismatch>,
    pub root_paths: TableDiff<RootPathEntry, RootPathMismatch>,
}

impl ComparisonReport {
    pub fn has_differences(&self) -> bool {
        !self.graphs.missing_in_redb.is_empty()
            || !self.graphs.extra_in_redb.is_empty()
            || !self.graphs.mismatched.is_empty()
            || !self.file_paths.missing_in_redb.is_empty()
            || !self.file_paths.extra_in_redb.is_empty()
            || !self.file_paths.mismatched.is_empty()
            || !self.root_paths.missing_in_redb.is_empty()
            || !self.root_paths.extra_in_redb.is_empty()
            || !self.root_paths.mismatched.is_empty()
    }
}

struct BackendSnapshot {
    graphs: Vec<GraphRecordData>,
    file_paths: Vec<NodePathRecordData>,
    root_paths: Vec<RootPathRecordData>,
}

#[derive(Debug)]
struct GraphSummary {
    file_count: usize,
    node_count: usize,
    edge_count: usize,
}

impl GraphRecordData {
    fn to_entry(&self) -> GraphEntry {
        GraphEntry {
            file: self.file.clone(),
            tag: self.tag.clone(),
            error: self.error.clone(),
            digest: self.digest.clone(),
            file_count: self.summary.file_count,
            node_count: self.summary.node_count,
            edge_count: self.summary.edge_count,
        }
    }
}

impl NodePathRecordData {
    fn to_entry(&self) -> NodePathEntry {
        NodePathEntry {
            file: self.file.clone(),
            local_id: self.local_id,
            digest: self.digest.clone(),
            summary: self.summary.clone(),
        }
    }
}

impl RootPathRecordData {
    fn to_entry(&self) -> RootPathEntry {
        RootPathEntry {
            file: self.file.clone(),
            symbol_stack: self.symbol_stack.clone(),
            digest: self.digest.clone(),
            summary: self.summary.clone(),
        }
    }
}

pub fn compare_backends(
    sqlite_path: &Path,
    redb_path: &Path,
) -> Result<ComparisonReport, ComparisonError> {
    let sqlite = load_sqlite_snapshot(sqlite_path)?;
    let redb = load_redb_snapshot(redb_path)?;

    let graphs = compare_graphs(sqlite.graphs, redb.graphs);
    let file_paths = compare_file_paths(sqlite.file_paths, redb.file_paths);
    let root_paths = compare_root_paths(sqlite.root_paths, redb.root_paths);

    Ok(ComparisonReport {
        graphs,
        file_paths,
        root_paths,
    })
}

fn load_sqlite_snapshot(path: &Path) -> Result<BackendSnapshot, ComparisonError> {
    let conn = Connection::open(path)?;
    let version: usize = conn.query_row("SELECT version FROM metadata", [], |row| row.get(0))?;
    if version != VERSION {
        return Err(ComparisonError::SchemaVersionMismatch {
            found: version,
            expected: VERSION,
        });
    }

    let mut graph = StackGraph::new();
    let mut partials = PartialPaths::new();

    let mut graphs_stmt = conn.prepare("SELECT file, tag, error, value FROM graphs")?;
    let mut graph_records = Vec::new();
    let mut graph_rows = graphs_stmt.query([])?;
    while let Some(row) = graph_rows.next()? {
        let file: String = row.get(0)?;
        let tag: String = row.get(1)?;
        let error: Option<String> = row.get(2)?;
        let blob: Vec<u8> = row.get(3)?;
        let (serde_graph, normalized_blob, summary) = normalize_graph(&blob)?;
        serde_graph
            .load_into(&mut graph)
            .map_err(|source| ComparisonError::GraphLoad {
                file: file.clone(),
                source,
            })?;
        let digest = digest_bytes(&normalized_blob);
        graph_records.push(GraphRecordData {
            file,
            tag,
            error,
            digest,
            summary,
        });
    }

    let mut node_paths_stmt = conn.prepare("SELECT file, local_id, value FROM file_paths")?;
    let mut node_paths = Vec::new();
    let mut node_rows = node_paths_stmt.query([])?;
    while let Some(row) = node_rows.next()? {
        let file: String = row.get(0)?;
        let local_id: u32 = row.get(1)?;
        let blob: Vec<u8> = row.get(2)?;
        let (normalized_blob, summary) = normalize_partial_path(&blob, &mut graph, &mut partials)?;
        let digest = digest_bytes(&normalized_blob);
        node_paths.push(NodePathRecordData {
            file,
            local_id,
            digest,
            summary,
        });
    }

    let mut root_paths_stmt = conn.prepare("SELECT file, symbol_stack, value FROM root_paths")?;
    let mut root_paths = Vec::new();
    let mut root_rows = root_paths_stmt.query([])?;
    while let Some(row) = root_rows.next()? {
        let file: String = row.get(0)?;
        let symbol_stack: String = row.get(1)?;
        let blob: Vec<u8> = row.get(2)?;
        let (normalized_blob, summary) = normalize_partial_path(&blob, &mut graph, &mut partials)?;
        let digest = digest_bytes(&normalized_blob);
        root_paths.push(RootPathRecordData {
            file,
            symbol_stack,
            digest,
            summary,
        });
    }

    Ok(BackendSnapshot {
        graphs: graph_records,
        file_paths: node_paths,
        root_paths,
    })
}

fn load_redb_snapshot(path: &Path) -> Result<BackendSnapshot, ComparisonError> {
    let db = Database::open(path)?;
    {
        let txn = db.begin_read()?;
        let table = txn.open_table(METADATA_TABLE)?;
        let version = table
            .get("version")?
            .map(|value| value.value())
            .ok_or_else(|| ComparisonError::InvalidRedbKey("missing metadata version".into()))?;
        let expected = VERSION as u64;
        if version != expected {
            return Err(ComparisonError::RedbSchemaVersionMismatch {
                found: version,
                expected,
            });
        }
    }

    let mut graph = StackGraph::new();
    let mut partials = PartialPaths::new();

    let mut graph_records = Vec::new();
    {
        let txn = db.begin_read()?;
        let table = txn.open_table(GRAPHS_TABLE)?;
        let mut iter = table.iter()?;
        while let Some(entry) = iter.next() {
            let (key, value) = entry?;
            let file = key.value().to_string();
            let (tag, error, blob) = decode_redb_graph_record(value.value())?;
            let (serde_graph, normalized_blob, summary) = normalize_graph(&blob)?;
            serde_graph
                .load_into(&mut graph)
                .map_err(|source| ComparisonError::GraphLoad {
                    file: file.clone(),
                    source,
                })?;
            let digest = digest_bytes(&normalized_blob);
            graph_records.push(GraphRecordData {
                file,
                tag,
                error,
                digest,
                summary,
            });
        }
    }

    let mut node_paths = Vec::new();
    {
        let txn = db.begin_read()?;
        let table = txn.open_multimap_table(FILE_PATHS_TABLE)?;
        let mut iter = table.iter()?;
        while let Some(entry) = iter.next() {
            let (key, mut values) = entry?;
            let (file, local_id) = parse_node_key(key.value())?;
            while let Some(value) = values.next() {
                let blob = value?.value();
                let (normalized_blob, summary) =
                    normalize_partial_path(blob, &mut graph, &mut partials)?;
                let digest = digest_bytes(&normalized_blob);
                node_paths.push(NodePathRecordData {
                    file: file.clone(),
                    local_id,
                    digest,
                    summary,
                });
            }
        }
    }

    let mut root_paths = Vec::new();
    {
        let txn = db.begin_read()?;
        let table = txn.open_multimap_table(ROOT_PATHS_BY_FILE_TABLE)?;
        let mut iter = table.iter()?;
        while let Some(entry) = iter.next() {
            let (key, mut values) = entry?;
            let (file, symbol_stack) = parse_root_key(key.value())?;
            while let Some(value) = values.next() {
                let blob = value?.value();
                let (normalized_blob, summary) =
                    normalize_partial_path(blob, &mut graph, &mut partials)?;
                let digest = digest_bytes(&normalized_blob);
                root_paths.push(RootPathRecordData {
                    file: file.clone(),
                    symbol_stack: symbol_stack.clone(),
                    digest,
                    summary,
                });
            }
        }
    }

    Ok(BackendSnapshot {
        graphs: graph_records,
        file_paths: node_paths,
        root_paths,
    })
}

fn compare_graphs(
    sqlite: Vec<GraphRecordData>,
    redb: Vec<GraphRecordData>,
) -> TableDiff<GraphEntry, GraphMismatch> {
    let sqlite_count = sqlite.len();
    let redb_count = redb.len();

    let sqlite_map: BTreeMap<_, _> = sqlite
        .into_iter()
        .map(|record| (record.file.clone(), record))
        .collect();
    let redb_map: BTreeMap<_, _> = redb
        .into_iter()
        .map(|record| (record.file.clone(), record))
        .collect();

    let mut keys = BTreeSet::new();
    keys.extend(sqlite_map.keys().cloned());
    keys.extend(redb_map.keys().cloned());

    let mut missing = Vec::new();
    let mut extra = Vec::new();
    let mut mismatched = Vec::new();

    for key in keys {
        match (sqlite_map.get(&key), redb_map.get(&key)) {
            (Some(sqlite_record), Some(redb_record)) => {
                let mut differences = Vec::new();
                if sqlite_record.tag != redb_record.tag {
                    differences.push(format!(
                        "tag differs: sqlite='{}', redb='{}'",
                        sqlite_record.tag, redb_record.tag
                    ));
                }
                if sqlite_record.error != redb_record.error {
                    differences.push(format!(
                        "error status differs: sqlite={:?}, redb={:?}",
                        sqlite_record.error, redb_record.error
                    ));
                }
                if sqlite_record.digest != redb_record.digest {
                    differences.push(format!(
                        "graph payload digest differs: sqlite={}, redb={}",
                        sqlite_record.digest, redb_record.digest
                    ));
                }
                if !differences.is_empty() {
                    mismatched.push(GraphMismatch {
                        file: key.clone(),
                        sqlite: sqlite_record.to_entry(),
                        redb: redb_record.to_entry(),
                        differences,
                    });
                }
            }
            (Some(sqlite_record), None) => {
                missing.push(sqlite_record.to_entry());
            }
            (None, Some(redb_record)) => {
                extra.push(redb_record.to_entry());
            }
            (None, None) => {}
        }
    }

    TableDiff {
        sqlite_count,
        redb_count,
        missing_in_redb: missing,
        extra_in_redb: extra,
        mismatched,
    }
}

fn compare_file_paths(
    sqlite: Vec<NodePathRecordData>,
    redb: Vec<NodePathRecordData>,
) -> TableDiff<NodePathEntry, NodePathMismatch> {
    let sqlite_count = sqlite.len();
    let redb_count = redb.len();

    let sqlite_map: BTreeMap<_, _> = sqlite
        .into_iter()
        .map(|record| ((record.file.clone(), record.local_id), record))
        .collect();
    let redb_map: BTreeMap<_, _> = redb
        .into_iter()
        .map(|record| ((record.file.clone(), record.local_id), record))
        .collect();

    let mut keys = BTreeSet::new();
    keys.extend(sqlite_map.keys().cloned());
    keys.extend(redb_map.keys().cloned());

    let mut missing = Vec::new();
    let mut extra = Vec::new();
    let mut mismatched = Vec::new();

    for key in keys {
        match (sqlite_map.get(&key), redb_map.get(&key)) {
            (Some(sqlite_record), Some(redb_record)) => {
                let mut differences = Vec::new();
                if sqlite_record.digest != redb_record.digest {
                    differences.push(format!(
                        "partial path digest differs: sqlite={}, redb={}",
                        sqlite_record.digest, redb_record.digest
                    ));
                }
                if sqlite_record.summary != redb_record.summary {
                    differences.push(format!(
                        "partial path summary differs: sqlite='{}', redb='{}'",
                        sqlite_record.summary, redb_record.summary
                    ));
                }
                if !differences.is_empty() {
                    mismatched.push(NodePathMismatch {
                        file: key.0.clone(),
                        local_id: key.1,
                        sqlite: sqlite_record.to_entry(),
                        redb: redb_record.to_entry(),
                        differences,
                    });
                }
            }
            (Some(sqlite_record), None) => missing.push(sqlite_record.to_entry()),
            (None, Some(redb_record)) => extra.push(redb_record.to_entry()),
            (None, None) => {}
        }
    }

    TableDiff {
        sqlite_count,
        redb_count,
        missing_in_redb: missing,
        extra_in_redb: extra,
        mismatched,
    }
}

fn compare_root_paths(
    sqlite: Vec<RootPathRecordData>,
    redb: Vec<RootPathRecordData>,
) -> TableDiff<RootPathEntry, RootPathMismatch> {
    let sqlite_count = sqlite.len();
    let redb_count = redb.len();

    let mut sqlite_map: BTreeMap<_, Vec<RootPathRecordData>> = BTreeMap::new();
    for record in sqlite {
        sqlite_map
            .entry((record.file.clone(), record.symbol_stack.clone()))
            .or_default()
            .push(record);
    }
    for records in sqlite_map.values_mut() {
        records.sort_by(|a, b| a.digest.cmp(&b.digest));
    }

    let mut redb_map: BTreeMap<_, Vec<RootPathRecordData>> = BTreeMap::new();
    for record in redb {
        redb_map
            .entry((record.file.clone(), record.symbol_stack.clone()))
            .or_default()
            .push(record);
    }
    for records in redb_map.values_mut() {
        records.sort_by(|a, b| a.digest.cmp(&b.digest));
    }

    let mut keys = BTreeSet::new();
    keys.extend(sqlite_map.keys().cloned());
    keys.extend(redb_map.keys().cloned());

    let mut missing = Vec::new();
    let mut extra = Vec::new();
    let mut mismatched = Vec::new();

    for key in keys {
        let sqlite_records = sqlite_map.get(&key);
        let redb_records = redb_map.get(&key);
        match (sqlite_records, redb_records) {
            (Some(sqlite_entries), Some(redb_entries)) => {
                if sqlite_entries.len() == redb_entries.len() {
                    for (sqlite_entry, redb_entry) in sqlite_entries.iter().zip(redb_entries.iter())
                    {
                        if sqlite_entry.digest != redb_entry.digest
                            || sqlite_entry.summary != redb_entry.summary
                        {
                            mismatched.push(RootPathMismatch {
                                file: key.0.clone(),
                                symbol_stack: key.1.clone(),
                                sqlite: sqlite_entry.to_entry(),
                                redb: redb_entry.to_entry(),
                            });
                        }
                    }
                } else {
                    let mut si = 0usize;
                    let mut ri = 0usize;
                    while si < sqlite_entries.len() && ri < redb_entries.len() {
                        let sqlite_entry = &sqlite_entries[si];
                        let redb_entry = &redb_entries[ri];
                        match sqlite_entry.digest.cmp(&redb_entry.digest) {
                            std::cmp::Ordering::Equal => {
                                si += 1;
                                ri += 1;
                            }
                            std::cmp::Ordering::Less => {
                                missing.push(sqlite_entry.to_entry());
                                si += 1;
                            }
                            std::cmp::Ordering::Greater => {
                                extra.push(redb_entry.to_entry());
                                ri += 1;
                            }
                        }
                    }
                    while si < sqlite_entries.len() {
                        missing.push(sqlite_entries[si].to_entry());
                        si += 1;
                    }
                    while ri < redb_entries.len() {
                        extra.push(redb_entries[ri].to_entry());
                        ri += 1;
                    }
                }
            }
            (Some(sqlite_entries), None) => {
                for entry in sqlite_entries {
                    missing.push(entry.to_entry());
                }
            }
            (None, Some(redb_entries)) => {
                for entry in redb_entries {
                    extra.push(entry.to_entry());
                }
            }
            (None, None) => {}
        }
    }

    TableDiff {
        sqlite_count,
        redb_count,
        missing_in_redb: missing,
        extra_in_redb: extra,
        mismatched,
    }
}

fn normalize_graph(
    blob: &[u8],
) -> Result<(SerdeStackGraph, Vec<u8>, GraphSummary), ComparisonError> {
    let (graph, _) = bincode::decode_from_slice::<SerdeStackGraph, _>(blob, BINCODE_CONFIG)?;
    let normalized = bincode::encode_to_vec(&graph, BINCODE_CONFIG)?;
    let summary = GraphSummary {
        file_count: graph.files.data.len(),
        node_count: graph.nodes.data.len(),
        edge_count: graph.edges.data.len(),
    };
    Ok((graph, normalized, summary))
}

fn normalize_partial_path(
    blob: &[u8],
    graph: &mut StackGraph,
    partials: &mut PartialPaths,
) -> Result<(Vec<u8>, String), ComparisonError> {
    let path = decode_partial_path(blob, graph, partials)?;
    let mut normalized = Vec::with_capacity(blob.len());
    encode_partial_path(graph, partials, &path, &mut normalized)?;
    let summary = format!("{}", path.display(graph, partials));
    Ok((normalized, summary))
}

fn digest_bytes(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    let digest = hasher.finalize();
    let mut out = String::with_capacity(digest.len() * 2);
    for byte in digest {
        write!(&mut out, "{:02x}", byte).expect("failed to write digest");
    }
    out
}

fn parse_node_key(data: &[u8]) -> Result<(String, u32), ComparisonError> {
    let split = data
        .iter()
        .position(|b| *b == 0)
        .ok_or_else(|| ComparisonError::InvalidRedbKey("node key missing separator".into()))?;
    if data.len() < split + 5 {
        return Err(ComparisonError::InvalidRedbKey("node key truncated".into()));
    }
    let file = std::str::from_utf8(&data[..split])
        .map_err(|_| ComparisonError::InvalidRedbKey("node key not utf8".into()))?
        .to_string();
    let local_id = u32::from_be_bytes([
        data[split + 1],
        data[split + 2],
        data[split + 3],
        data[split + 4],
    ]);
    Ok((file, local_id))
}

fn parse_root_key(data: &[u8]) -> Result<(String, String), ComparisonError> {
    let split = data
        .iter()
        .position(|b| *b == 0)
        .ok_or_else(|| ComparisonError::InvalidRedbKey("root key missing separator".into()))?;
    let file = std::str::from_utf8(&data[..split])
        .map_err(|_| ComparisonError::InvalidRedbKey("root key file not utf8".into()))?
        .to_string();
    let symbol = std::str::from_utf8(&data[split + 1..])
        .map_err(|_| ComparisonError::InvalidRedbKey("root key symbol not utf8".into()))?
        .to_string();
    Ok((file, symbol))
}

fn decode_redb_graph_record(
    data: &[u8],
) -> Result<(String, Option<String>, Vec<u8>), ComparisonError> {
    let mut slice = data;
    let tag_len = take_u32(&mut slice)? as usize;
    let tag = take_string(&mut slice, tag_len)?;
    let has_error = take_u8(&mut slice)? != 0;
    let error = if has_error {
        let len = take_u32(&mut slice)? as usize;
        Some(take_string(&mut slice, len)?)
    } else {
        None
    };
    let graph_len = take_u32(&mut slice)? as usize;
    if slice.len() < graph_len {
        return Err(ComparisonError::CorruptRedbGraph(
            "graph blob truncated".into(),
        ));
    }
    let graph_blob = slice[..graph_len].to_vec();
    Ok((tag, error, graph_blob))
}

fn take_u32(slice: &mut &[u8]) -> Result<u32, ComparisonError> {
    if slice.len() < 4 {
        return Err(ComparisonError::CorruptRedbGraph(
            "unexpected end of record".into(),
        ));
    }
    let value = u32::from_be_bytes([slice[0], slice[1], slice[2], slice[3]]);
    *slice = &slice[4..];
    Ok(value)
}

fn take_u8(slice: &mut &[u8]) -> Result<u8, ComparisonError> {
    if slice.is_empty() {
        return Err(ComparisonError::CorruptRedbGraph(
            "unexpected end of record".into(),
        ));
    }
    let value = slice[0];
    *slice = &slice[1..];
    Ok(value)
}

fn take_string(slice: &mut &[u8], len: usize) -> Result<String, ComparisonError> {
    if slice.len() < len {
        return Err(ComparisonError::CorruptRedbGraph(
            "unexpected end of record".into(),
        ));
    }
    let value = std::str::from_utf8(&slice[..len])
        .map_err(|_| ComparisonError::CorruptRedbGraph("invalid utf8".into()))?
        .to_string();
    *slice = &slice[len..];
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn digest_bytes_produces_hex() {
        let digest = digest_bytes(b"test");
        assert_eq!(digest.len(), 64);
        assert!(digest.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn parse_node_key_decodes_file_and_id() {
        let mut key = Vec::new();
        key.extend_from_slice(b"file.py");
        key.push(0);
        key.extend_from_slice(&42u32.to_be_bytes());
        let (file, local_id) = parse_node_key(&key).unwrap();
        assert_eq!(file, "file.py");
        assert_eq!(local_id, 42);
    }

    #[test]
    fn parse_root_key_decodes_components() {
        let mut key = Vec::new();
        key.extend_from_slice(b"file.py");
        key.push(0);
        key.extend_from_slice(b"symbol");
        let (file, stack) = parse_root_key(&key).unwrap();
        assert_eq!(file, "file.py");
        assert_eq!(stack, "symbol");
    }

    #[test]
    fn decode_redb_graph_record_succeeds() {
        let mut data = Vec::new();
        data.extend_from_slice(&(4u32.to_be_bytes()));
        data.extend_from_slice(b"main");
        data.push(0);
        data.extend_from_slice(&(3u32.to_be_bytes()));
        data.extend_from_slice(b"abc");
        let (tag, error, blob) = decode_redb_graph_record(&data).unwrap();
        assert_eq!(tag, "main");
        assert!(error.is_none());
        assert_eq!(blob, b"abc");
    }
}
