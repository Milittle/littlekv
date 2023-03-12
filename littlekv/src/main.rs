use engine::engine_interface::{Put, StorageEngine, WriteOperation};
use engine::memory_engine::MemoryEngine;

fn main() {
    let engine = MemoryEngine::new(&["testtable1", "testtable2", "testtable3"]).unwrap();
    let wr_ops = vec![WriteOperation::Put(Put {
        table: "testtable1",
        key: b"key".to_vec(),
        value: b"value".to_vec(),
        sync: false,
    })];

    let _ = engine.write_batch(wr_ops).unwrap();

    let res = engine.get("testtable1", "key".as_bytes().to_vec()).unwrap();

    println!("{:?}", String::from_utf8(res.unwrap()));
}
