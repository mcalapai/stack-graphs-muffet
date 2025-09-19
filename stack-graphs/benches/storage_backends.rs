#![cfg(any(feature = "storage", feature = "storage-redb"))]

#[cfg(not(any(feature = "storage", feature = "storage-redb")))]
compile_error!(
    "Enable the `storage` or `storage-redb` feature to run the storage_backends benchmark."
);

use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use tempfile::TempDir;

use stack_graphs::arena::Handle;
use stack_graphs::graph::{Edge, File, Node, StackGraph};
use stack_graphs::partial::{PartialPath, PartialPaths};
use stack_graphs::stitching::{ForwardPartialPathStitcher, StitcherConfig};
#[cfg(feature = "storage-redb")]
use stack_graphs::storage::redb::convert_sqlite_to_redb;
#[cfg(feature = "storage-redb")]
use stack_graphs::storage::{RedbError, RedbReader};
use stack_graphs::storage::{
    SQLiteReader, SQLiteWriter, StorageError, StorageReader, StorageWriter,
};
use stack_graphs::NoCancellation;

#[cfg(feature = "storage-redb")]
fn build_redb_reader(size: usize) -> (RedbReader, TempDir) {
    let dir = TempDir::new().expect("tempdir");
    let sqlite_path = dir.path().join("fixture.sqlite");
    let redb_path = dir.path().join("fixture.redb");
    let mut writer = SQLiteWriter::open(&sqlite_path).expect("sqlite writer");
    populate_fixture(&mut writer, size);
    drop(writer);
    convert_sqlite_to_redb(&sqlite_path, &redb_path).expect("convert to redb");
    let reader = RedbReader::open(&redb_path).expect("redb reader");
    (reader, dir)
}

fn build_sqlite_reader(size: usize) -> (SQLiteReader, TempDir) {
    let dir = TempDir::new().expect("tempdir");
    let sqlite_path = dir.path().join("fixture.sqlite");
    let mut writer = SQLiteWriter::open(&sqlite_path).expect("sqlite writer");
    populate_fixture(&mut writer, size);
    let reader = writer.into_reader();
    (reader, dir)
}

fn populate_fixture(writer: &mut impl StorageWriter<Error = StorageError>, size: usize) {
    populate_fixture_impl(writer, size);
}

fn populate_fixture_impl<W>(writer: &mut W, size: usize)
where
    W: StorageWriter<Error = StorageError>,
{
    let mut graph = StackGraph::new();
    let file = graph.add_file("bench").expect("file");
    let mut partials = PartialPaths::new();

    let mut paths = Vec::new();
    for idx in 0..size {
        let name = format!("sym{idx}");
        let reference = create_push_symbol_node(&mut graph, file, &name, true);
        let definition = create_pop_symbol_node(&mut graph, file, &name, true);
        let path = create_partial_path_and_edges(
            &mut graph,
            &mut partials,
            &[StackGraph::root_node(), reference, definition],
        )
        .expect("partial path");
        paths.push(path);
    }

    let path_refs: Vec<_> = paths.iter().collect();
    writer
        .store_result_for_file(&graph, file, "", &mut partials, path_refs)
        .expect("store result");
}

fn exercise_reader<R>(reader: &mut R)
where
    R: StorageReader,
    R::Error: std::fmt::Debug,
{
    reader.load_graph_for_file("bench").expect("load graph");
    let file = reader.graph().get_file("bench").expect("file should exist");
    reader
        .preload_root_paths_for_file(file, &NoCancellation)
        .expect("preload roots");
    let refs: Vec<_> = reader
        .graph()
        .nodes_for_file(file)
        .filter(|node| reader.graph()[*node].is_reference())
        .collect();
    ForwardPartialPathStitcher::find_all_complete_partial_paths(
        reader,
        refs,
        StitcherConfig::default(),
        &NoCancellation,
        |_, _, _| {},
    )
    .expect("stitching");
}

pub fn storage_benchmarks(c: &mut Criterion) {
    let mut group = c.benchmark_group("storage_backends");

    group.bench_function("sqlite", |b| {
        b.iter_batched(
            || build_sqlite_reader(128),
            |(mut reader, _guard)| exercise_reader(&mut reader),
            BatchSize::SmallInput,
        );
    });

    #[cfg(feature = "storage-redb")]
    {
        group.bench_function("redb", |b| {
            b.iter_batched(
                || build_redb_reader(128),
                |(mut reader, _guard)| exercise_reader(&mut reader),
                BatchSize::SmallInput,
            );
        });
    }

    group.finish();
}

criterion_group!(benches, storage_benchmarks);
criterion_main!(benches);

fn create_push_symbol_node(
    graph: &mut StackGraph,
    file: Handle<File>,
    symbol: &str,
    is_reference: bool,
) -> Handle<Node> {
    let id = graph.new_node_id(file);
    let symbol = graph.add_symbol(symbol);
    graph
        .add_push_symbol_node(id, symbol, is_reference)
        .expect("push node")
}

fn create_pop_symbol_node(
    graph: &mut StackGraph,
    file: Handle<File>,
    symbol: &str,
    is_definition: bool,
) -> Handle<Node> {
    let id = graph.new_node_id(file);
    let symbol = graph.add_symbol(symbol);
    graph
        .add_pop_symbol_node(id, symbol, is_definition)
        .expect("pop node")
}

fn create_partial_path_and_edges(
    graph: &mut StackGraph,
    partials: &mut PartialPaths,
    nodes: &[Handle<Node>],
) -> Result<PartialPath, stack_graphs::paths::PathResolutionError> {
    let mut iter = nodes.iter();
    let mut prev = iter.next().cloned().unwrap();
    let mut path = PartialPath::from_node(graph, partials, prev);
    for next in iter {
        graph.add_edge(prev, *next, 0);
        path.append(
            graph,
            partials,
            Edge {
                source: prev,
                sink: *next,
                precedence: 0,
            },
        )?;
        prev = *next;
    }
    Ok(path)
}
