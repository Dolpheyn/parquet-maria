use std::cell::RefCell;
use std::fs::File;
use std::io::prelude::*;
use std::ops::Deref;

use thrift::protocol::{self, TInputProtocol, TType};
use thrift_codec::data::{Data, Struct};
use thrift_codec::CompactDecode;

const MAGIC: &str = "PAR1";
const MAGIC_BYTES: &[u8] = MAGIC.as_bytes();

const METADATA_SIZE_LENGTH: usize = 4;

fn main() -> std::io::Result<()> {
    let mut file = File::open("example.parquet")?;
    let mut buf = Vec::new();
    let file_size = file.read_to_end(&mut buf)?;
    dbg!(file_size);

    let buf = buf;
    assert_eq!(MAGIC_BYTES, &buf[0..MAGIC_BYTES.len()]);
    // start at 4 bytes from the end of the file
    let magic_start = file_size - MAGIC_BYTES.len();
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
    dbg!(metadata_size);

    let data_size =
        file_size - metadata_size as usize - MAGIC_BYTES.len() * 2 - METADATA_SIZE_LENGTH;
    dbg!(data_size);

    // go back by metadata_size bytes
    let metadata_start = metadata_size_start - metadata_size as usize;
    let metadata_bytes = &buf[metadata_start..metadata_start + metadata_size as usize];
    let metadata_bytes = Vec::from(metadata_bytes);
    let mut shared_metadata = SharedByteSliceReader::new(&metadata_bytes);

    let starting_inner_reader_len = shared_metadata.copy_inner().len();
    dbg!(starting_inner_reader_len);
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

    let schema_schema =
        SchemaElement::try_from(&Struct::compact_decode(&mut shared_metadata).unwrap()).unwrap();
    dbg!(&schema_schema);
    assert_eq!("schema", schema_schema.name());

    let schema_one =
        SchemaElement::try_from(&Struct::compact_decode(&mut shared_metadata).unwrap()).unwrap();
    dbg!(&schema_one);
    assert_eq!("one", schema_one.name());

    let schema_two =
        SchemaElement::try_from(&Struct::compact_decode(&mut shared_metadata).unwrap()).unwrap();
    dbg!(&schema_two);
    assert_eq!("two", schema_two.name());

    let schema_three =
        SchemaElement::try_from(&Struct::compact_decode(&mut shared_metadata).unwrap()).unwrap();
    dbg!(&schema_three);
    assert_eq!("three", schema_three.name());

    let schema_idx =
        SchemaElement::try_from(&Struct::compact_decode(&mut shared_metadata).unwrap()).unwrap();
    dbg!(&schema_idx);
    assert_eq!("__index_level_0__", schema_idx.name());

    let mut t = protocol::TCompactInputProtocol::new(&mut shared_metadata);
    t.read_list_end().unwrap();

    let after_schema_inner_reader_len = shared_metadata.copy_inner().len();
    dbg!(after_schema_inner_reader_len);

    // is the reader advancing the shared slice?
    assert!(after_schema_inner_reader_len < starting_inner_reader_len);
    let metadata_read_len = starting_inner_reader_len - after_schema_inner_reader_len;
    dbg!(metadata_read_len);

    // num_rows field
    let mut t = protocol::TCompactInputProtocol::new(&mut shared_metadata);
    let field_ident = t.read_field_begin().unwrap();
    let field_id = protocol::field_id(&field_ident).unwrap();
    assert_eq!(field_id, 1); // field id should be 4 here
    assert_eq!(field_ident.field_type, TType::I64);
    let num_rows = t.read_i16().unwrap();

    // let num_rows = i64::compact_decode(&mut shared_metadata).unwrap();
    assert_eq!(num_rows, 3);

    Ok(())
}

#[derive(Debug)]
struct SchemaElement {
    name: String,
}

impl SchemaElement {
    fn name(&self) -> String {
        self.name.clone()
    }
}

impl TryFrom<&Struct> for SchemaElement {
    type Error = std::io::Error;

    fn try_from(value: &Struct) -> Result<Self, Self::Error> {
        if let Data::Binary(v) = value.fields().iter().find(|f| f.id() == 4).unwrap().data() {
            return Ok(Self {
                name: String::from_utf8_lossy(v).to_string(),
            });
        } else {
            panic!("unexpected data kind");
        }
    }
}

struct SharedByteSliceReader<'a> {
    inner: RefCell<&'a [u8]>,
}

impl<'a> SharedByteSliceReader<'a> {
    fn new(inner: &'a [u8]) -> Self {
        Self {
            inner: RefCell::new(inner),
        }
    }

    fn copy_inner(&self) -> &[u8] {
        self.inner.borrow().deref()
    }
}

impl Read for SharedByteSliceReader<'_> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let mut a = self.inner.borrow_mut();
        Read::read(&mut *a, buf)
    }
}
