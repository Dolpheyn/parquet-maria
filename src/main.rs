use std::cell::RefCell;
use std::io::prelude::*;
use std::{fs::File, rc::Rc};

use thrift::protocol::{self, TInputProtocol, TType};
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

    struct SharedMetadata<'a> {
        inner: RefCell<&'a [u8]>,
    }

    impl<'a> SharedMetadata<'a> {
        fn new(inner: &'a [u8]) -> Self {
            Self {
                inner: RefCell::new(inner),
            }
        }
    }

    impl Read for SharedMetadata<'_> {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            let mut a = self.inner.borrow_mut();
            (a).read(buf)
        }
    }

    // go back by metadata_size bytes
    let metadata_start = metadata_size_start - metadata_size as usize;
    let metadata_bytes = &buf[metadata_start..metadata_start + metadata_size as usize];
    let metadata_bytes = Vec::from(&metadata_bytes[..]);
    let mut shared_metadata = SharedMetadata::new(&metadata_bytes);

    let mut t = protocol::TCompactInputProtocol::new(&mut shared_metadata);

    // start parsing metadata
    t.read_struct_begin().unwrap();

    // version field - id: 1
    let field_ident = t.read_field_begin().unwrap();
    let field_id = protocol::field_id(&field_ident).unwrap();
    assert_eq!(field_id, 1);
    assert_eq!(field_ident.field_type, TType::I32);
    let version = t.read_i32().unwrap();
    dbg!(version);

    // schema list field - id: 2
    let field_ident = t.read_field_begin().unwrap();
    let field_id = protocol::field_id(&field_ident).unwrap();
    assert_eq!(field_id, 2);
    assert_eq!(field_ident.field_type, TType::List);

    let schema_list_ident = t.read_list_begin().unwrap();
    dbg!(schema_list_ident);

    let schema_schema = &Struct::compact_decode(&mut shared_metadata).unwrap();
    dbg!(schema_schema);
    if let Data::Binary(v) = schema_schema
        .fields()
        .iter()
        .find(|f| f.id() == 4)
        .unwrap()
        .data()
    {
        assert_eq!("schema", String::from_utf8_lossy(&v));
    } else {
        panic!("unexpected data kind");
    }

    let schema_one = &Struct::compact_decode(&mut shared_metadata).unwrap();
    dbg!(schema_one);
    if let Data::Binary(v) = schema_one
        .fields()
        .iter()
        .find(|f| f.id() == 4)
        .unwrap()
        .data()
    {
        assert_eq!("one", String::from_utf8_lossy(&v));
    } else {
        panic!("unexpected data kind");
    }

    let schema_two = &Struct::compact_decode(&mut shared_metadata).unwrap();
    dbg!(schema_two);
    if let Data::Binary(v) = schema_two
        .fields()
        .iter()
        .find(|f| f.id() == 4)
        .unwrap()
        .data()
    {
        assert_eq!("two", String::from_utf8_lossy(&v));
    } else {
        panic!("unexpected data kind");
    }

    let schema_three = &Struct::compact_decode(&mut shared_metadata).unwrap();
    dbg!(schema_three);
    if let Data::Binary(v) = schema_three
        .fields()
        .iter()
        .find(|f| f.id() == 4)
        .unwrap()
        .data()
    {
        assert_eq!("three", String::from_utf8_lossy(&v));
    } else {
        panic!("unexpected data kind");
    }

    let schema_idx = &Struct::compact_decode(&mut shared_metadata).unwrap();
    dbg!(schema_idx);
    if let Data::Binary(v) = schema_idx
        .fields()
        .iter()
        .find(|f| f.id() == 4)
        .unwrap()
        .data()
    {
        assert_eq!("__index_level_0__", String::from_utf8_lossy(&v));
    } else {
        panic!("unexpected data kind");
    }

    let mut t = protocol::TCompactInputProtocol::new(&mut shared_metadata);
    t.read_list_end().unwrap();

    // num_rows field
    let field_ident = t.read_field_begin().unwrap();
    let field_id = protocol::field_id(&field_ident).unwrap();
    assert_eq!(field_id, 1);
    assert_eq!(field_ident.field_type, TType::I64);

    let num_rows = i64::compact_decode(&mut shared_metadata).unwrap();
    assert_eq!(num_rows, 3);

    Ok(())
}
