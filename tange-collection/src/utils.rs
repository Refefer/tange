use std::io::prelude::*;
use std::io::{SeekFrom,BufReader,Error};
use std::fs::{File,metadata};

use tange::deferred::{Deferred, batch_apply};

use collection::memory::MemoryCollection;

#[derive(Clone)]
struct Chunk { path: String, start: u64, end: u64 }

pub fn read_text(path: &str, chunk_size: u64) -> Result<MemoryCollection<String>,Error> {
    // Read the file size
    let file_size = metadata(path)?.len();
    let mut dfs = Vec::new();
    let mut cur_offset = 0u64;
    while cur_offset < file_size {
        let chunk = Chunk {
            path: path.into(),
            start: cur_offset,
            end: cur_offset + chunk_size
        };
        dfs.push(Deferred::lift(chunk, 
                                Some(&format!("File: {}, start: {}", path, cur_offset))));
        cur_offset += chunk_size;
    }

    Ok(MemoryCollection::from_defs(batch_apply(&dfs, read)))
}

fn read(_idx: usize, chunk: &Chunk) -> Vec<String> {
    let f = File::open(&chunk.path)
        .expect("Error when opening file");
    let mut reader = BufReader::new(f);
    reader.seek(SeekFrom::Start(chunk.start))
        .expect("Error when reading file!");

    let mut start = if chunk.start > 0 { 
        // Skip first line, which is likely a partial line
        let mut s = Vec::new();
        let size = reader.read_until(b'\n', &mut s)
            .expect("Error reading line from file!");
        chunk.start + size as u64
    } else {
        0
    };

    let total = chunk.end;
    let mut lines = Vec::new();
    loop {
        let mut s = String::new();
        match reader.read_line(&mut s) {
            Ok(0) => break,
            Ok(size) => {
                start += size as u64;
                lines.push(s);
            },
            _ => break
        };
        if start > total { break; }
    }
    lines
}
