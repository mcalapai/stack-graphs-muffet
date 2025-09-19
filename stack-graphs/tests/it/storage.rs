// -*- coding: utf-8 -*-
// ------------------------------------------------------------------------------------------------
// Copyright © 2023, stack-graphs authors.
// Licensed under either of Apache License, Version 2.0, or MIT license, at your option.
// Please see the LICENSE-APACHE or LICENSE-MIT files in this distribution for license details.
// ------------------------------------------------------------------------------------------------

use itertools::Itertools;
use stack_graphs::graph::StackGraph;
use stack_graphs::partial::PartialPaths;
#[cfg(feature = "storage-redb")]
use stack_graphs::storage::redb::convert_sqlite_to_redb;
use stack_graphs::storage::{
    FileStatus, SQLiteReader, SQLiteWriter, StorageError, StorageReader, StorageWriter,
};
#[cfg(feature = "storage-redb")]
use stack_graphs::storage::{RedbError, RedbReader, RedbWriter};
use stack_graphs::NoCancellation;
#[cfg(feature = "storage-redb")]
use tempfile::TempDir;

use crate::util::create_partial_path_and_edges;
use crate::util::create_pop_symbol_node;
use crate::util::create_push_symbol_node;

trait StorageTestBackend {
    type Error: std::fmt::Display;
    type Writer: StorageWriter<Error = Self::Error>;
    type Reader: StorageReader<Error = Self::Error>;

    fn name() -> &'static str;
    fn create_writer() -> Result<Self::Writer, Self::Error>;
    fn into_reader(writer: Self::Writer) -> Result<Self::Reader, Self::Error>;
}

fn expect_ok<B, T>(result: Result<T, B::Error>) -> T
where
    B: StorageTestBackend,
    B::Error: std::fmt::Display,
{
    result.unwrap_or_else(|err| panic!("{} backend failed: {}", B::name(), err))
}

struct SqliteBackend;

impl StorageTestBackend for SqliteBackend {
    type Error = StorageError;
    type Writer = SQLiteWriter;
    type Reader = SQLiteReader;

    fn name() -> &'static str {
        "sqlite"
    }

    fn create_writer() -> Result<Self::Writer, Self::Error> {
        SQLiteWriter::open_in_memory()
    }

    fn into_reader(writer: Self::Writer) -> Result<Self::Reader, Self::Error> {
        StorageWriter::into_reader(writer)
    }
}

#[cfg(feature = "storage-redb")]
struct RedbBackend;

#[cfg(feature = "storage-redb")]
impl StorageTestBackend for RedbBackend {
    type Error = RedbError;
    type Writer = RedbWriter;
    type Reader = RedbReader;

    fn name() -> &'static str {
        "redb"
    }

    fn create_writer() -> Result<Self::Writer, Self::Error> {
        RedbWriter::open_in_memory()
    }

    fn into_reader(writer: Self::Writer) -> Result<Self::Reader, Self::Error> {
        StorageWriter::into_reader(writer)
    }
}

#[cfg(feature = "storage-redb")]
#[test]
fn converts_sqlite_database_to_redb() -> Result<(), Box<dyn std::error::Error>> {
    let temp = TempDir::new()?;
    let sqlite_path = temp.path().join("store.sqlite");
    let redb_path = temp.path().join("store.redb");

    let mut sqlite_writer = SQLiteWriter::open(&sqlite_path)?;
    let mut graph = StackGraph::new();
    let file = graph.add_file("test1").unwrap();
    let mut partials = PartialPaths::new();
    let r = StackGraph::root_node();
    let foo = create_pop_symbol_node(&mut graph, file, "foo", true);
    let path = create_partial_path_and_edges(&mut graph, &mut partials, &[r, foo]).unwrap();
    StorageWriter::store_result_for_file(
        &mut sqlite_writer,
        &graph,
        file,
        "tag",
        &mut partials,
        vec![&path],
    )?;
    drop(sqlite_writer);

    convert_sqlite_to_redb(&sqlite_path, &redb_path)?;

    let mut reader = RedbReader::open(&redb_path)?;
    assert!(matches!(
        StorageReader::status_for_file(&mut reader, "test1", Some("tag"))?,
        FileStatus::Indexed
    ));

    reader.load_graph_for_file("test1")?;
    let query_path = {
        let (graph, partials, _) = reader.components_mut();
        let file = graph.add_file("query").unwrap();
        let r = StackGraph::root_node();
        let foo_ref = create_push_symbol_node(graph, file, "foo", true);
        create_partial_path_and_edges(graph, partials, &[foo_ref, r]).unwrap()
    };
    reader.load_partial_path_extensions(&query_path, &NoCancellation)?;
    let (graph, partials, db) = reader.components_mut();
    let mut results = Vec::new();
    db.find_candidate_partial_paths_from_root(
        &*graph,
        partials,
        Some(query_path.symbol_stack_postcondition),
        &mut results,
    );
    assert!(!results.is_empty());

    Ok(())
}

macro_rules! run_for_backends {
    ($test_fn:ident) => {{
        $test_fn::<SqliteBackend>();
        #[cfg(feature = "storage-redb")]
        {
            $test_fn::<RedbBackend>();
        }
    }};
}

fn test_foo_bar_root_candidate_paths<B: StorageTestBackend>(
    symbols: &[&str],
    variable: bool,
) -> usize {
    let mut writer = expect_ok::<B, _>(B::create_writer());

    let mut graph = StackGraph::new();
    let file = graph.add_file("test1").unwrap();
    let mut partials = PartialPaths::new();

    let r = StackGraph::root_node();
    let foo = create_pop_symbol_node(&mut graph, file, "foo", true);
    let bar = create_pop_symbol_node(&mut graph, file, "bar", true);

    let path_with_variable =
        create_partial_path_and_edges(&mut graph, &mut partials, &[r, foo, bar]).unwrap();

    let mut path_without_variable = path_with_variable.clone();
    path_without_variable.eliminate_precondition_stack_variables(&mut partials);

    expect_ok::<B, _>(StorageWriter::store_result_for_file(
        &mut writer,
        &graph,
        file,
        "",
        &mut partials,
        vec![&path_with_variable, &path_without_variable],
    ));

    let mut reader = expect_ok::<B, _>(B::into_reader(writer));

    let path = {
        let (graph, partials, _) = reader.components_mut();
        let file = graph.add_file("test2").unwrap();

        let r = StackGraph::root_node();
        let refs = symbols
            .iter()
            .map(|symbol| create_push_symbol_node(graph, file, *symbol, true))
            .chain(std::iter::once(r))
            .collect_vec();
        let mut path = create_partial_path_and_edges(graph, partials, &refs).unwrap();
        if !variable {
            path.eliminate_precondition_stack_variables(partials);
        }
        path
    };

    expect_ok::<B, _>(reader.load_partial_path_extensions(&path, &NoCancellation));

    let (graph, partials, db) = reader.components_mut();
    let mut results = Vec::new();
    db.find_candidate_partial_paths_from_root(
        &*graph,
        partials,
        Some(path.symbol_stack_postcondition),
        &mut results,
    );

    results.len()
}

fn find_candidates_for_exact_symbol_stack_with_variable_impl<B: StorageTestBackend>() {
    let results = test_foo_bar_root_candidate_paths::<B>(&["bar", "foo"], true);
    assert_eq!(2, results, "backend {}", B::name());
}

#[test]
fn find_candidates_for_exact_symbol_stack_with_variable() {
    run_for_backends!(find_candidates_for_exact_symbol_stack_with_variable_impl);
}

fn find_candidates_for_exact_symbol_stack_without_variable_impl<B: StorageTestBackend>() {
    let results = test_foo_bar_root_candidate_paths::<B>(&["bar", "foo"], false);
    assert_eq!(2, results, "backend {}", B::name());
}

#[test]
fn find_candidates_for_exact_symbol_stack_without_variable() {
    run_for_backends!(find_candidates_for_exact_symbol_stack_without_variable_impl);
}

fn find_candidates_for_longer_symbol_stack_with_variable_impl<B: StorageTestBackend>() {
    let results = test_foo_bar_root_candidate_paths::<B>(&["quz", "bar", "foo"], true);
    assert_eq!(1, results, "backend {}", B::name());
}

#[test]
fn find_candidates_for_longer_symbol_stack_with_variable() {
    run_for_backends!(find_candidates_for_longer_symbol_stack_with_variable_impl);
}

fn find_candidates_for_longer_symbol_stack_without_variable_impl<B: StorageTestBackend>() {
    let results = test_foo_bar_root_candidate_paths::<B>(&["quz", "bar", "foo"], false);
    assert_eq!(1, results, "backend {}", B::name());
}

#[test]
fn find_candidates_for_longer_symbol_stack_without_variable() {
    run_for_backends!(find_candidates_for_longer_symbol_stack_without_variable_impl);
}

fn find_candidates_for_shorter_symbol_stack_with_variable_impl<B: StorageTestBackend>() {
    let results = test_foo_bar_root_candidate_paths::<B>(&["foo"], true);
    assert_eq!(2, results, "backend {}", B::name());
}

#[test]
fn find_candidates_for_shorter_symbol_stack_with_variable() {
    run_for_backends!(find_candidates_for_shorter_symbol_stack_with_variable_impl);
}

fn find_candidates_for_shorter_symbol_stack_without_variable_impl<B: StorageTestBackend>() {
    let results = test_foo_bar_root_candidate_paths::<B>(&["foo"], false);
    assert_eq!(0, results, "backend {}", B::name());
}

#[test]
fn find_candidates_for_shorter_symbol_stack_without_variable() {
    run_for_backends!(find_candidates_for_shorter_symbol_stack_without_variable_impl);
}

fn find_candidates_for_symbol_stack_with_wildcard_symbols_impl<B: StorageTestBackend>() {
    let mut writer = expect_ok::<B, _>(B::create_writer());

    let mut graph = StackGraph::new();
    let file = graph.add_file("special_defs").unwrap();
    let mut partials = PartialPaths::new();

    let r = StackGraph::root_node();
    let sym_a = create_pop_symbol_node(&mut graph, file, "na_me%", true);
    let sym_b = create_pop_symbol_node(&mut graph, file, "other_%value", true);

    let path_with_variable =
        create_partial_path_and_edges(&mut graph, &mut partials, &[r, sym_a, sym_b]).unwrap();

    let mut path_without_variable = path_with_variable.clone();
    path_without_variable.eliminate_precondition_stack_variables(&mut partials);

    expect_ok::<B, _>(StorageWriter::store_result_for_file(
        &mut writer,
        &graph,
        file,
        "",
        &mut partials,
        vec![&path_with_variable, &path_without_variable],
    ));

    let mut reader = expect_ok::<B, _>(B::into_reader(writer));

    let path = {
        let (graph, partials, _) = reader.components_mut();
        let file = graph.add_file("special_refs").unwrap();

        let r = StackGraph::root_node();
        let refs = ["other_%value", "na_me%"]
            .iter()
            .map(|symbol| create_push_symbol_node(graph, file, *symbol, true))
            .chain(std::iter::once(r))
            .collect_vec();
        create_partial_path_and_edges(graph, partials, &refs).unwrap()
    };

    expect_ok::<B, _>(reader.load_partial_path_extensions(&path, &NoCancellation));

    let (graph, partials, db) = reader.components_mut();
    let mut results = Vec::new();
    db.find_candidate_partial_paths_from_root(
        &*graph,
        partials,
        Some(path.symbol_stack_postcondition),
        &mut results,
    );

    assert_eq!(2, results.len(), "backend {}", B::name());
}

#[test]
fn find_candidates_for_symbol_stack_with_wildcard_symbols() {
    run_for_backends!(find_candidates_for_symbol_stack_with_wildcard_symbols_impl);
}
