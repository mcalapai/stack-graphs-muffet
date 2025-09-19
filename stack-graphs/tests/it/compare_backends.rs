// -*- coding: utf-8 -*-
// ------------------------------------------------------------------------------------------------
// Copyright © 2024, stack-graphs authors.
// Licensed under either of Apache License, Version 2.0, or MIT license, at your option.
// Please see the LICENSE-APACHE or LICENSE-MIT files in this distribution for license details.
// ------------------------------------------------------------------------------------------------

#![cfg(feature = "storage-redb")]

use redb::TableDefinition;
use stack_graphs::graph::StackGraph;
use stack_graphs::partial::PartialPaths;
use stack_graphs::storage::compare::compare_backends;
use stack_graphs::storage::redb::convert_sqlite_to_redb;
use stack_graphs::storage::{SQLiteWriter, StorageWriter};
use tempfile::TempDir;

use crate::util::{
    create_partial_path_and_edges,
    create_pop_symbol_node,
};

#[test]
fn reports_no_differences_for_matching_backends() -> Result<(), Box<dyn std::error::Error>> {
    let temp = TempDir::new()?;
    let sqlite_path = temp.path().join("store.sqlite");
    let redb_path = temp.path().join("store.redb");

    let mut sqlite_writer = SQLiteWriter::open(&sqlite_path)?;
    let mut graph = StackGraph::new();
    let file = graph.add_file("test1").unwrap();
    let mut partials = PartialPaths::new();

    let root = StackGraph::root_node();
    let foo = create_pop_symbol_node(&mut graph, file, "foo", true);
    let bar = create_pop_symbol_node(&mut graph, file, "bar", true);

    let root_path = create_partial_path_and_edges(&mut graph, &mut partials, &[root, foo])?;
    let node_path = create_partial_path_and_edges(&mut graph, &mut partials, &[foo, bar])?;

    StorageWriter::store_result_for_file(
        &mut sqlite_writer,
        &graph,
        file,
        "tag",
        &mut partials,
        vec![&root_path, &node_path],
    )?;
    drop(sqlite_writer);

    convert_sqlite_to_redb(&sqlite_path, &redb_path)?;

    let report = compare_backends(&sqlite_path, &redb_path)?;
    assert!(!report.has_differences());

    Ok(())
}

#[test]
fn detects_missing_records_in_redb() -> Result<(), Box<dyn std::error::Error>> {
    let temp = TempDir::new()?;
    let sqlite_path = temp.path().join("store.sqlite");
    let redb_path = temp.path().join("store.redb");

    let mut sqlite_writer = SQLiteWriter::open(&sqlite_path)?;
    let mut graph = StackGraph::new();
    let file = graph.add_file("test1").unwrap();
    let mut partials = PartialPaths::new();

    let root = StackGraph::root_node();
    let foo = create_pop_symbol_node(&mut graph, file, "foo", true);
    let bar = create_pop_symbol_node(&mut graph, file, "bar", true);

    let root_path = create_partial_path_and_edges(&mut graph, &mut partials, &[root, foo])?;
    let node_path = create_partial_path_and_edges(&mut graph, &mut partials, &[foo, bar])?;

    StorageWriter::store_result_for_file(
        &mut sqlite_writer,
        &graph,
        file,
        "tag",
        &mut partials,
        vec![&root_path, &node_path],
    )?;
    drop(sqlite_writer);

    convert_sqlite_to_redb(&sqlite_path, &redb_path)?;

    // Remove a node path record from the redb database.
    const FILE_PATHS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("file_paths");
    let db = redb::Database::open(&redb_path)?;
    let mut txn = db.begin_write()?;
    let mut table = txn.open_table(FILE_PATHS)?;
    let local_id = graph[foo].id().local_id();
    let key = encode_node_key("test1", local_id);
    table.remove(key.as_slice())?;
    txn.commit()?;

    let report = compare_backends(&sqlite_path, &redb_path)?;
    assert!(report.has_differences());
    assert_eq!(report.file_paths.missing_in_redb.len(), 1);
    assert_eq!(report.file_paths.extra_in_redb.len(), 0);

    Ok(())
}

fn encode_node_key(file: &str, local_id: u32) -> Vec<u8> {
    let mut key = Vec::with_capacity(file.len() + 1 + 4);
    key.extend_from_slice(file.as_bytes());
    key.push(0);
    key.extend_from_slice(&local_id.to_be_bytes());
    key
}
