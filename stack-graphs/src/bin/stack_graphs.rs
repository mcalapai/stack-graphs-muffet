use std::fs::File;
use std::io::BufReader;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand};
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
