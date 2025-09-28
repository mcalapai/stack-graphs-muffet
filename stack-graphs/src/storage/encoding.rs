use std::convert::TryFrom;
use std::mem::size_of;

use controlled_option::ControlledOption;

use crate::arena::Handle;
use crate::graph::{Node, NodeID, StackGraph};
use crate::partial::{
    PartialPath, PartialPathEdge, PartialPathEdgeList, PartialPaths, PartialScopeStack,
    PartialScopedSymbol, PartialSymbolStack, ScopeStackVariable, SymbolStackVariable,
};
use crate::storage::StorageError;

pub(crate) const RECORD_VERSION: u8 = 1;

pub(crate) fn encode_partial_path(
    graph: &StackGraph,
    partials: &mut PartialPaths,
    path: &PartialPath,
    buf: &mut Vec<u8>,
) -> Result<(), StorageError> {
    buf.clear();
    buf.reserve(256);
    buf.push(RECORD_VERSION);

    encode_node_id(buf, graph[path.start_node].id(), graph)?;
    encode_node_id(buf, graph[path.end_node].id(), graph)?;

    encode_symbol_stack(buf, graph, partials, &path.symbol_stack_precondition)?;
    encode_symbol_stack(buf, graph, partials, &path.symbol_stack_postcondition)?;
    encode_scope_stack(buf, graph, partials, &path.scope_stack_precondition)?;
    encode_scope_stack(buf, graph, partials, &path.scope_stack_postcondition)?;
    encode_edge_list(buf, graph, partials, &path.edges)?;

    Ok(())
}

pub(crate) fn decode_partial_path(
    blob: &[u8],
    graph: &mut StackGraph,
    partials: &mut PartialPaths,
) -> Result<PartialPath, StorageError> {
    let mut decoder = Decoder::new(blob);
    let version = decoder.read_u8()?;
    if version != RECORD_VERSION {
        return Err(StorageError::Corrupt(format!(
            "unsupported partial path record version {version}"
        )));
    }

    let start_node_id = decoder.read_node_id(graph)?;
    let end_node_id = decoder.read_node_id(graph)?;
    let start_node = resolve_node_handle(graph, start_node_id)?;
    let end_node = resolve_node_handle(graph, end_node_id)?;

    let symbol_stack_precondition = decoder.read_symbol_stack(graph, partials)?;
    let symbol_stack_postcondition = decoder.read_symbol_stack(graph, partials)?;
    let scope_stack_precondition = decoder.read_scope_stack(graph, partials)?;
    let scope_stack_postcondition = decoder.read_scope_stack(graph, partials)?;
    let edges = decoder.read_edge_list(graph, partials)?;

    decoder.ensure_finished()?;

    Ok(PartialPath {
        start_node,
        end_node,
        symbol_stack_precondition,
        symbol_stack_postcondition,
        scope_stack_precondition,
        scope_stack_postcondition,
        edges,
    })
}

pub(crate) fn validate_partial_path_blob(blob: &[u8]) -> Result<(), StorageError> {
    let mut cursor = BlobCursor::new(blob);
    let version = cursor.read_u8()?;
    if version != RECORD_VERSION {
        return Err(StorageError::Corrupt(format!(
            "unsupported partial path record version {version}"
        )));
    }

    cursor.skip_node_id()?; // start node
    cursor.skip_node_id()?; // end node
    cursor.skip_symbol_stack()?; // precondition
    cursor.skip_symbol_stack()?; // postcondition
    cursor.skip_scope_stack()?; // scope precondition
    cursor.skip_scope_stack()?; // scope postcondition
    cursor.skip_edge_list()?;
    cursor.ensure_finished()?;
    Ok(())
}

fn encode_node_id(buf: &mut Vec<u8>, id: NodeID, graph: &StackGraph) -> Result<(), StorageError> {
    if id.is_root() {
        buf.push(0);
    } else if id.is_jump_to() {
        buf.push(1);
    } else if let Some(file) = id.file() {
        buf.push(2);
        write_str(buf, graph[file].name())?;
        write_u32(buf, id.local_id());
    } else {
        return Err(StorageError::Corrupt("node id missing file".into()));
    }
    Ok(())
}

fn encode_symbol_stack(
    buf: &mut Vec<u8>,
    graph: &StackGraph,
    partials: &mut PartialPaths,
    stack: &PartialSymbolStack,
) -> Result<(), StorageError> {
    match stack.variable() {
        Some(variable) => {
            buf.push(1);
            write_u32(buf, variable.as_u32());
        }
        None => buf.push(0),
    }

    let symbol_count = stack.len();
    let symbols: Vec<_> = stack.iter(partials).collect();

    write_u32(
        buf,
        u32::try_from(symbol_count)
            .map_err(|_| StorageError::Corrupt("symbol stack overflow".into()))?,
    );

    for scoped_symbol in symbols {
        write_str(buf, graph[scoped_symbol.symbol].as_ref())?;
        match scoped_symbol.scopes.into_option() {
            Some(scopes) => {
                buf.push(1);
                encode_scope_stack(buf, graph, partials, &scopes)?;
            }
            None => buf.push(0),
        }
    }

    Ok(())
}

fn encode_scope_stack(
    buf: &mut Vec<u8>,
    graph: &StackGraph,
    partials: &mut PartialPaths,
    stack: &PartialScopeStack,
) -> Result<(), StorageError> {
    match stack.variable() {
        Some(variable) => {
            buf.push(1);
            write_u32(buf, variable.as_u32());
        }
        None => buf.push(0),
    }

    write_u32(
        buf,
        u32::try_from(stack.len())
            .map_err(|_| StorageError::Corrupt("scope stack overflow".into()))?,
    );

    for scope in stack.iter_scopes(partials) {
        encode_node_id(buf, graph[scope].id(), graph)?;
    }
    Ok(())
}

fn encode_edge_list(
    buf: &mut Vec<u8>,
    graph: &StackGraph,
    partials: &mut PartialPaths,
    edges: &PartialPathEdgeList,
) -> Result<(), StorageError> {
    write_u32(
        buf,
        u32::try_from(edges.len())
            .map_err(|_| StorageError::Corrupt("edge list overflow".into()))?,
    );
    for edge in edges.iter(partials) {
        encode_node_id(buf, edge.source_node_id, graph)?;
        write_i32(buf, edge.precedence);
    }
    Ok(())
}

fn resolve_node_handle(graph: &mut StackGraph, id: NodeID) -> Result<Handle<Node>, StorageError> {
    graph
        .node_for_id(id)
        .ok_or_else(|| StorageError::Corrupt("node missing from graph".into()))
}

struct Decoder<'a> {
    data: &'a [u8],
    offset: usize,
}

struct BlobCursor<'a> {
    data: &'a [u8],
    offset: usize,
}

impl<'a> BlobCursor<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self { data, offset: 0 }
    }

    fn read_u8(&mut self) -> Result<u8, StorageError> {
        if self.offset >= self.data.len() {
            return Err(StorageError::Corrupt("unexpected end of record".into()));
        }
        let value = self.data[self.offset];
        self.offset += 1;
        Ok(value)
    }

    fn read_u32(&mut self) -> Result<u32, StorageError> {
        if self.remaining() < size_of::<u32>() {
            return Err(StorageError::Corrupt("unexpected end of record".into()));
        }
        let mut bytes = [0u8; 4];
        bytes.copy_from_slice(&self.data[self.offset..self.offset + 4]);
        self.offset += 4;
        Ok(u32::from_le_bytes(bytes))
    }

    fn read_i32(&mut self) -> Result<i32, StorageError> {
        if self.remaining() < size_of::<i32>() {
            return Err(StorageError::Corrupt("unexpected end of record".into()));
        }
        let mut bytes = [0u8; 4];
        bytes.copy_from_slice(&self.data[self.offset..self.offset + 4]);
        self.offset += 4;
        Ok(i32::from_le_bytes(bytes))
    }

    fn read_str(&mut self) -> Result<(), StorageError> {
        let len = self.read_u32()? as usize;
        if self.remaining() < len {
            return Err(StorageError::Corrupt("unexpected end of record".into()));
        }
        self.offset += len;
        Ok(())
    }

    fn skip_node_id(&mut self) -> Result<(), StorageError> {
        match self.read_u8()? {
            0 | 1 => Ok(()),
            2 => {
                self.read_str()?;
                self.read_u32()?;
                Ok(())
            }
            tag => Err(StorageError::Corrupt(format!(
                "invalid node kind tag {tag}"
            ))),
        }
    }

    fn skip_symbol_stack(&mut self) -> Result<(), StorageError> {
        match self.read_u8()? {
            0 => {}
            1 => {
                self.read_u32()?;
            }
            tag => {
                return Err(StorageError::Corrupt(format!(
                    "invalid symbol stack variable tag {tag}"
                )))
            }
        }
        let count = self.read_u32()? as usize;
        for _ in 0..count {
            self.read_str()?;
            match self.read_u8()? {
                0 => {}
                1 => self.skip_scope_stack()?,
                tag => {
                    return Err(StorageError::Corrupt(format!(
                        "invalid scoped symbol flag {tag}"
                    )))
                }
            }
        }
        Ok(())
    }

    fn skip_scope_stack(&mut self) -> Result<(), StorageError> {
        match self.read_u8()? {
            0 => {}
            1 => {
                self.read_u32()?;
            }
            tag => {
                return Err(StorageError::Corrupt(format!(
                    "invalid scope stack variable tag {tag}"
                )))
            }
        }
        let count = self.read_u32()? as usize;
        for _ in 0..count {
            self.skip_node_id()?;
        }
        Ok(())
    }

    fn skip_edge_list(&mut self) -> Result<(), StorageError> {
        let count = self.read_u32()? as usize;
        for _ in 0..count {
            self.skip_node_id()?;
            self.read_i32()?;
        }
        Ok(())
    }

    fn ensure_finished(&self) -> Result<(), StorageError> {
        if self.offset != self.data.len() {
            return Err(StorageError::Corrupt("unexpected trailing data".into()));
        }
        Ok(())
    }

    fn remaining(&self) -> usize {
        self.data.len() - self.offset
    }
}

impl<'a> Decoder<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self { data, offset: 0 }
    }

    fn read_u8(&mut self) -> Result<u8, StorageError> {
        if self.offset >= self.data.len() {
            return Err(StorageError::Corrupt("unexpected end of record".into()));
        }
        let value = self.data[self.offset];
        self.offset += 1;
        Ok(value)
    }

    fn read_u32(&mut self) -> Result<u32, StorageError> {
        if self.remaining() < size_of::<u32>() {
            return Err(StorageError::Corrupt("unexpected end of record".into()));
        }
        let mut bytes = [0u8; 4];
        bytes.copy_from_slice(&self.data[self.offset..self.offset + 4]);
        self.offset += 4;
        Ok(u32::from_le_bytes(bytes))
    }

    fn read_i32(&mut self) -> Result<i32, StorageError> {
        if self.remaining() < size_of::<i32>() {
            return Err(StorageError::Corrupt("unexpected end of record".into()));
        }
        let mut bytes = [0u8; 4];
        bytes.copy_from_slice(&self.data[self.offset..self.offset + 4]);
        self.offset += 4;
        Ok(i32::from_le_bytes(bytes))
    }

    fn read_str(&mut self) -> Result<&'a str, StorageError> {
        let len = self.read_u32()? as usize;
        if self.remaining() < len {
            return Err(StorageError::Corrupt("unexpected end of record".into()));
        }
        let start = self.offset;
        let end = start + len;
        self.offset = end;
        std::str::from_utf8(&self.data[start..end])
            .map_err(|_| StorageError::Corrupt("invalid utf-8 in storage".into()))
    }

    fn read_node_id(&mut self, graph: &mut StackGraph) -> Result<NodeID, StorageError> {
        match self.read_u8()? {
            0 => Ok(NodeID::root()),
            1 => Ok(NodeID::jump_to()),
            2 => {
                let file_name = self.read_str()?;
                let file = graph.get_file(&file_name).ok_or_else(|| {
                    StorageError::Corrupt(format!("file '{file_name}' missing from loaded graph"))
                })?;
                let local_id = self.read_u32()?;
                Ok(NodeID::new_in_file(file, local_id))
            }
            other => Err(StorageError::Corrupt(format!(
                "invalid node id tag {other}"
            ))),
        }
    }

    fn read_symbol_stack(
        &mut self,
        graph: &mut StackGraph,
        partials: &mut PartialPaths,
    ) -> Result<PartialSymbolStack, StorageError> {
        let variable =
            match self.read_u8()? {
                0 => None,
                1 => {
                    let raw = self.read_u32()?;
                    Some(SymbolStackVariable::new(raw).ok_or_else(|| {
                        StorageError::Corrupt("invalid symbol stack variable".into())
                    })?)
                }
                other => {
                    return Err(StorageError::Corrupt(format!(
                        "invalid symbol stack variable tag {other}"
                    )))
                }
            };

        let count = self.read_u32()? as usize;
        let mut stack = match variable {
            Some(variable) => PartialSymbolStack::from_variable(variable),
            None => PartialSymbolStack::empty(),
        };

        for _ in 0..count {
            let symbol_name = self.read_str()?;
            let symbol = graph.add_symbol(symbol_name);
            let scopes = match self.read_u8()? {
                0 => None,
                1 => Some(self.read_scope_stack(graph, partials)?),
                other => {
                    return Err(StorageError::Corrupt(format!(
                        "invalid scoped symbol tag {other}"
                    )))
                }
            };
            let scoped_symbol = PartialScopedSymbol {
                symbol,
                scopes: ControlledOption::from_option(scopes),
            };
            stack.push_back(partials, scoped_symbol);
        }
        Ok(stack)
    }

    fn read_scope_stack(
        &mut self,
        graph: &mut StackGraph,
        partials: &mut PartialPaths,
    ) -> Result<PartialScopeStack, StorageError> {
        let variable =
            match self.read_u8()? {
                0 => None,
                1 => {
                    let raw = self.read_u32()?;
                    Some(ScopeStackVariable::new(raw).ok_or_else(|| {
                        StorageError::Corrupt("invalid scope stack variable".into())
                    })?)
                }
                other => {
                    return Err(StorageError::Corrupt(format!(
                        "invalid scope stack variable tag {other}"
                    )))
                }
            };
        let count = self.read_u32()? as usize;
        let mut stack = match variable {
            Some(variable) => PartialScopeStack::from_variable(variable),
            None => PartialScopeStack::empty(),
        };
        for _ in 0..count {
            let node_id = self.read_node_id(graph)?;
            let handle = resolve_node_handle(graph, node_id)?;
            stack.push_back(partials, handle);
        }
        Ok(stack)
    }

    fn read_edge_list(
        &mut self,
        graph: &mut StackGraph,
        partials: &mut PartialPaths,
    ) -> Result<PartialPathEdgeList, StorageError> {
        let count = self.read_u32()? as usize;
        let mut list = PartialPathEdgeList::empty();
        for _ in 0..count {
            let node_id = self.read_node_id(graph)?;
            let precedence = self.read_i32()?;
            list.push_back(
                partials,
                PartialPathEdge {
                    source_node_id: node_id,
                    precedence,
                },
            );
        }
        Ok(list)
    }

    fn ensure_finished(&self) -> Result<(), StorageError> {
        if self.offset != self.data.len() {
            return Err(StorageError::Corrupt("unexpected trailing data".into()));
        }
        Ok(())
    }

    fn remaining(&self) -> usize {
        self.data.len() - self.offset
    }
}

fn write_u32(buf: &mut Vec<u8>, value: u32) {
    buf.extend_from_slice(&value.to_le_bytes());
}

fn write_i32(buf: &mut Vec<u8>, value: i32) {
    buf.extend_from_slice(&value.to_le_bytes());
}

fn write_str(buf: &mut Vec<u8>, value: &str) -> Result<(), StorageError> {
    let len = value.len();
    let len_u32 = u32::try_from(len)
        .map_err(|_| StorageError::Corrupt("string too long to encode".into()))?;
    write_u32(buf, len_u32);
    buf.extend_from_slice(value.as_bytes());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::StackGraph;
    use crate::partial::{PartialPath, PartialPaths};

    #[test]
    fn round_trip_empty_path() {
        let mut graph = StackGraph::new();
        let mut partials = PartialPaths::new();
        let root = StackGraph::root_node();
        let path = PartialPath::from_node(&graph, &mut partials, root);
        let mut buf = Vec::new();
        encode_partial_path(&graph, &mut partials, &path, &mut buf).unwrap();
        let decoded = decode_partial_path(buf.as_slice(), &mut graph, &mut partials).unwrap();
        assert!(decoded.equals(&mut partials, &path));
    }
}
