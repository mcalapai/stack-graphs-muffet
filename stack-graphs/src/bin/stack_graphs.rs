use std::fs::File;
use std::io::BufReader;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand, ValueEnum};
#[cfg(feature = "storage-redb")]
use redb::{
    Database, MultimapTableDefinition, ReadableMultimapTable, ReadableTable, TableDefinition,
};
use serde::Deserialize;
use stack_graphs::graph::StackGraph;
use stack_graphs::partial::{PartialPath, PartialPaths};
use stack_graphs::serde::{PartialPath as SerdePartialPath, StackGraph as SerdeStackGraph};
#[cfg(feature = "storage-redb")]
use stack_graphs::storage::redb::convert_sqlite_to_redb;
#[cfg(feature = "storage-redb")]
use stack_graphs::storage::RedbWriter;
use stack_graphs::storage::{SQLiteWriter, StorageWriter};

fn main() {
    if let Err(error) = run() {
        eprintln!("Error: {error}");
        for cause in error.chain().skip(1) {
            eprintln!("  caused by: {cause}");
        }
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Commands::Index(args) => handle_index(args),
        #[cfg(feature = "storage-redb")]
        Commands::Convert(args) => handle_convert(args),
        #[cfg(feature = "storage-redb")]
        Commands::InspectRedb(args) => handle_inspect(args),
    }
}

#[derive(Parser)]
#[command(author, version, about = "Index stack graphs into persistent storage", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[allow(clippy::large_enum_variant)]
#[derive(Subcommand)]
enum Commands {
    /// Index files into a storage backend using a JSON manifest
    Index(IndexArgs),
    /// Convert an existing SQLite database into redb format
    #[cfg(feature = "storage-redb")]
    Convert(ConvertArgs),
    /// Inspect a redb database and display table summaries
    #[cfg(feature = "storage-redb")]
    InspectRedb(InspectArgs),
}

#[derive(Parser)]
struct IndexArgs {
    /// Storage backend to use when writing the database
    #[arg(long, default_value = "sqlite", value_parser = parse_storage_backend)]
    storage_backend: StorageBackendKind,
    /// Path to the database file that should be updated
    #[arg(long)]
    database: PathBuf,
    /// Manifest describing the files to index
    #[arg(long)]
    manifest: PathBuf,
    /// Remove any existing records before indexing
    #[arg(long)]
    clean: bool,
}

#[cfg(feature = "storage-redb")]
#[derive(Parser)]
struct ConvertArgs {
    /// Path to the source SQLite database
    #[arg(long)]
    sqlite: PathBuf,
    /// Destination redb database path
    #[arg(long = "to-redb")]
    to_redb: PathBuf,
    /// Overwrite the destination file if it already exists
    #[arg(long)]
    overwrite: bool,
}

#[cfg(feature = "storage-redb")]
#[derive(Parser)]
struct InspectArgs {
    /// Path to the redb database to inspect
    #[arg(long)]
    database: PathBuf,
    /// Table to dump; defaults to graphs summary
    #[arg(long, value_enum, default_value = "graphs")]
    table: InspectTable,
}

#[cfg(feature = "storage-redb")]
#[derive(Copy, Clone, Debug, ValueEnum)]
enum InspectTable {
    Graphs,
    FilePaths,
    RootPathsByFile,
    RootPathsBySymbol,
}

#[derive(Clone, Copy, Debug)]
enum StorageBackendKind {
    Sqlite,
    #[cfg(feature = "storage-redb")]
    Redb,
}

fn parse_storage_backend(value: &str) -> std::result::Result<StorageBackendKind, String> {
    match value.to_ascii_lowercase().as_str() {
        "sqlite" => Ok(StorageBackendKind::Sqlite),
        "redb" => {
            #[cfg(feature = "storage-redb")]
            {
                Ok(StorageBackendKind::Redb)
            }
            #[cfg(not(feature = "storage-redb"))]
            {
                Err(
                    "backend 'redb' is unavailable; recompile with the 'storage-redb' feature"
                        .into(),
                )
            }
        }
        other => Err(format!("unsupported backend '{other}'")),
    }
}

fn handle_index(args: IndexArgs) -> Result<()> {
    let manifest = Manifest::from_path(&args.manifest)
        .with_context(|| format!("failed to read manifest {}", args.manifest.display()))?;

    match args.storage_backend {
        StorageBackendKind::Sqlite => {
            index_with_backend::<SQLiteWriter>(&args.database, &manifest, args.clean)?;
        }
        #[cfg(feature = "storage-redb")]
        StorageBackendKind::Redb => {
            index_with_backend::<RedbWriter>(&args.database, &manifest, args.clean)?;
        }
    }

    println!(
        "Indexed {} file(s) into {}",
        manifest.files.len(),
        args.database.display()
    );

    Ok(())
}

#[cfg(feature = "storage-redb")]
fn handle_convert(args: ConvertArgs) -> Result<()> {
    if args.to_redb.exists() && !args.overwrite {
        bail!(
            "destination {} already exists; pass --overwrite to replace it",
            args.to_redb.display()
        );
    }

    convert_sqlite_to_redb(&args.sqlite, &args.to_redb).with_context(|| {
        format!(
            "failed to convert {} to {}",
            args.sqlite.display(),
            args.to_redb.display()
        )
    })?;

    println!("Wrote redb database to {}", args.to_redb.display());

    Ok(())
}

#[cfg(feature = "storage-redb")]
fn handle_inspect(args: InspectArgs) -> Result<()> {
    const GRAPHS: TableDefinition<&str, &[u8]> = TableDefinition::new("graphs");
    const FILE_PATHS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("file_paths");
    const ROOT_PATHS_BY_FILE: TableDefinition<&[u8], &[u8]> =
        TableDefinition::new("root_paths_by_file");
    const ROOT_PATHS_BY_SYMBOL: MultimapTableDefinition<&str, &str> =
        MultimapTableDefinition::new("root_paths_by_symbol");

    let db = Database::open(&args.database)
        .with_context(|| format!("failed to open {}", args.database.display()))?;
    let txn = db.begin_read()?;

    match args.table {
        InspectTable::Graphs => {
            let table = txn.open_table(GRAPHS)?;
            let mut iter = table.iter()?;
            println!("file\ttag\tstatus");
            while let Some(entry) = iter.next() {
                let (key, value) = entry?;
                let (tag, error) = decode_graph_record_summary(value.value())?;
                let status = error.as_deref().unwrap_or("indexed");
                println!("{}\t{}\t{}", key.value(), tag, status);
            }
        }
        InspectTable::FilePaths => {
            let table = txn.open_table(FILE_PATHS)?;
            let mut counts: std::collections::BTreeMap<String, usize> =
                std::collections::BTreeMap::new();
            let mut iter = table.iter()?;
            while let Some(entry) = iter.next() {
                let (key, _) = entry?;
                let (file, _) = parse_node_key(key.value())?;
                *counts.entry(file).or_default() += 1;
            }
            println!("file\tnode_paths");
            for (file, count) in counts {
                println!("{}\t{}", file, count);
            }
        }
        InspectTable::RootPathsByFile => {
            let table = txn.open_table(ROOT_PATHS_BY_FILE)?;
            let mut counts: std::collections::BTreeMap<(String, String), usize> =
                std::collections::BTreeMap::new();
            let mut iter = table.iter()?;
            while let Some(entry) = iter.next() {
                let (key, _) = entry?;
                let (file, symbol) = parse_root_file_key(key.value())?;
                *counts.entry((file, symbol)).or_default() += 1;
            }
            println!("file\tsymbol\troot_paths");
            for ((file, symbol), count) in counts {
                println!("{}\t{}\t{}", file, symbol, count);
            }
        }
        InspectTable::RootPathsBySymbol => {
            let table = txn.open_multimap_table(ROOT_PATHS_BY_SYMBOL)?;
            let mut iter = table.iter()?;
            println!("symbol\tfiles");
            while let Some(entry) = iter.next() {
                let (symbol, mut values) = entry?;
                let mut files = Vec::new();
                while let Some(value) = values.next() {
                    files.push(value?.value().to_string());
                }
                files.sort();
                files.dedup();
                println!("{}\t{}", symbol.value(), files.join(", "));
            }
        }
    }

    Ok(())
}

fn index_with_backend<W>(database: &Path, manifest: &Manifest, clean: bool) -> Result<()>
where
    W: StorageWriter,
    W::Error: std::error::Error + Send + Sync + 'static,
{
    let mut writer = W::open(database)
        .with_context(|| format!("failed to open database {}", database.display()))?;

    if clean {
        writer
            .clean_all()
            .with_context(|| format!("failed to clean database {}", database.display()))?;
    }

    for entry in &manifest.files {
        let path = Path::new(&entry.path);
        if let Some(error) = &entry.error {
            if entry.graph.is_some() || !entry.partials.is_empty() {
                bail!(
                    "manifest entry for {} cannot specify both an error and graph data",
                    path.display()
                );
            }
            let tag = entry.tag.as_deref().unwrap_or("");
            writer
                .store_error_for_file(path, tag, error)
                .with_context(|| format!("failed to store error for {}", path.display()))?;
            continue;
        }

        let graph_data = entry.graph.as_ref().with_context(|| {
            format!(
                "manifest entry for {} is missing graph data",
                path.display()
            )
        })?;

        let mut graph = StackGraph::new();
        graph_data
            .load_into(&mut graph)
            .with_context(|| format!("failed to load graph for {}", path.display()))?;
        let file_handle = graph
            .get_file(&entry.path)
            .with_context(|| format!("graph does not define file {}", path.display()))?;

        let mut partials = PartialPaths::new();
        let mut concrete_paths: Vec<PartialPath> = Vec::with_capacity(entry.partials.len());
        for (index, partial) in entry.partials.iter().enumerate() {
            let converted = partial
                .to_partial_path(&mut graph, &mut partials)
                .with_context(|| {
                    format!(
                        "failed to decode partial path {index} for {}",
                        path.display()
                    )
                })?;
            concrete_paths.push(converted);
        }
        let references: Vec<&PartialPath> = concrete_paths.iter().collect();
        let tag = entry.tag.as_deref().unwrap_or("");

        writer
            .store_result_for_file(&graph, file_handle, tag, &mut partials, references)
            .with_context(|| format!("failed to store results for {}", path.display()))?;
    }

    Ok(())
}

#[cfg(feature = "storage-redb")]
fn parse_node_key(data: &[u8]) -> Result<(String, u32)> {
    let split = data
        .iter()
        .position(|b| *b == 0)
        .ok_or_else(|| anyhow::anyhow!("invalid node key"))?;
    let file = std::str::from_utf8(&data[..split])?.to_string();
    if data.len() < split + 5 {
        bail!("node key truncated");
    }
    let id = u32::from_be_bytes([
        data[split + 1],
        data[split + 2],
        data[split + 3],
        data[split + 4],
    ]);
    Ok((file, id))
}

#[cfg(feature = "storage-redb")]
fn parse_root_file_key(data: &[u8]) -> Result<(String, String)> {
    let split = data
        .iter()
        .position(|b| *b == 0)
        .ok_or_else(|| anyhow::anyhow!("invalid root key"))?;
    let file = std::str::from_utf8(&data[..split])?.to_string();
    let symbol = std::str::from_utf8(&data[split + 1..])?.to_string();
    Ok((file, symbol))
}

#[cfg(feature = "storage-redb")]
fn decode_graph_record_summary(data: &[u8]) -> Result<(String, Option<String>)> {
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
    Ok((tag, error))
}

#[cfg(feature = "storage-redb")]
fn read_u32(slice: &mut &[u8]) -> Result<u32> {
    if slice.len() < 4 {
        bail!("unexpected end of record");
    }
    let value = u32::from_be_bytes([slice[0], slice[1], slice[2], slice[3]]);
    *slice = &slice[4..];
    Ok(value)
}

#[cfg(feature = "storage-redb")]
fn read_u8(slice: &mut &[u8]) -> Result<u8> {
    if slice.is_empty() {
        bail!("unexpected end of record");
    }
    let value = slice[0];
    *slice = &slice[1..];
    Ok(value)
}

#[cfg(feature = "storage-redb")]
fn read_string(slice: &mut &[u8], len: usize) -> Result<String> {
    if slice.len() < len {
        bail!("unexpected end of record");
    }
    let value = std::str::from_utf8(&slice[..len])?.to_string();
    *slice = &slice[len..];
    Ok(value)
}

#[derive(Debug, Deserialize)]
struct Manifest {
    files: Vec<ManifestEntry>,
}

impl Manifest {
    fn from_path(path: &Path) -> Result<Self> {
        let file = File::open(path)?;
        let reader = BufReader::new(file);
        let manifest: Manifest = serde_json::from_reader(reader)?;
        Ok(manifest)
    }
}

#[derive(Debug, Deserialize)]
struct ManifestEntry {
    path: String,
    #[serde(default)]
    tag: Option<String>,
    #[serde(default)]
    error: Option<String>,
    #[serde(default)]
    graph: Option<SerdeStackGraph>,
    #[serde(default)]
    partials: Vec<SerdePartialPath>,
}
