use std::fs::File;
use std::io::prelude::*;
use thrift_codec::data::{Data, Struct};
use thrift_codec::CompactDecode;

const MAGIC: &str = "PAR1";
const MAGIC_BYTES: &[u8] = MAGIC.as_bytes();

const METADATA_SIZE_LENGTH: usize = 4;

fn main() -> std::io::Result<()> {
    let mut file = File::open("example.parquet")?;
    let mut buf = Vec::new();
    let size = file.read_to_end(&mut buf)?;
    println!("file size: {}", size);

    let buf = buf;
    assert_eq!(MAGIC_BYTES, &buf[0..MAGIC_BYTES.len()]);
    // start at 4 bytes from the end of the file
    let magic_start = size - MAGIC_BYTES.len();
    assert_eq!(
        MAGIC_BYTES,
        &buf[magic_start..magic_start + MAGIC_BYTES.len()]
    );

    // go back by 4 bytes
    let metadata_size_start = magic_start - METADATA_SIZE_LENGTH;
    let metadata_size = u32::from_le_bytes(
        buf[metadata_size_start..metadata_size_start + METADATA_SIZE_LENGTH]
            .try_into()
            .unwrap(),
    );
    println!("metadata size: {}", metadata_size);

    // go back by metadata_size bytes
    let metadata_start = metadata_size_start - metadata_size as usize;
    let mut metadata_bytes = &buf[metadata_start..metadata_start + metadata_size as usize];

    let version = i32::compact_decode(&mut metadata_bytes).unwrap();
    println!("{:?}", version);
    let message = Struct::compact_decode(&mut metadata_bytes).unwrap();
    dbg!(message);

    let schema_one = &Struct::compact_decode(&mut metadata_bytes).unwrap();
    dbg!(schema_one);
    if let Data::Binary(v) = schema_one.fields()[2].data() {
        assert_eq!("one", String::from_utf8_lossy(&v));
    } else {
        panic!("unexpected data kind");
    }

    let schema_two = &Struct::compact_decode(&mut metadata_bytes).unwrap();
    dbg!(schema_two);
    if let Data::Binary(v) = schema_two.fields()[2].data() {
        assert_eq!("two", String::from_utf8_lossy(&v));
    } else {
        panic!("unexpected data kind");
    }

    let schema_three = &Struct::compact_decode(&mut metadata_bytes).unwrap();
    dbg!(schema_three);
    if let Data::Binary(v) = schema_three.fields()[2].data() {
        assert_eq!("three", String::from_utf8_lossy(&v));
    } else {
        panic!("unexpected data kind");
    }

    let schema_index = &Struct::compact_decode(&mut metadata_bytes).unwrap();
    dbg!(schema_index);
    if let Data::Binary(v) = schema_index.fields()[2].data() {
        assert_eq!("__index_level_0__", String::from_utf8_lossy(&v));
    } else {
        panic!("unexpected data kind");
    }

    let num_rows = i64::compact_decode(&mut metadata_bytes).unwrap();
    dbg!(num_rows);
    // num_rows = 11?
    // assert_eq!(3, num_rows);

    Ok(())
}
