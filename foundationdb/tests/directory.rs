// Copyright 2018 foundationdb-rs developers, https://github.com/Clikengo/foundationdb-rs/graphs/contributors
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.

use foundationdb::directory::DirectoryLayer;
use foundationdb::*;

mod common;

async fn test_create_then_open_async(
    db: &Database,
    directory: &DirectoryLayer,
    paths: Vec<String>,
) -> FdbResult<()> {
    eprintln!("creating directory for {:?}", paths.to_owned());
    let trx = db.create_trx()?;
    let create_output = directory.create_or_open(&trx, paths.to_owned()).await;
    assert!(create_output.is_ok());

    trx.commit().await?;
    eprintln!("opening directory for {:?}", paths.to_owned());

    let trx = db.create_trx()?;
    let get_output = directory.open(&trx, paths.to_owned()).await;
    assert!(get_output.is_ok());

    assert_eq!(create_output.unwrap().bytes(), get_output.unwrap().bytes());
    Ok(())
}

async fn test_create_or_open_async(
    db: &Database,
    directory: &DirectoryLayer,
    paths: Vec<String>,
) -> FdbResult<()> {
    let trx = db.create_trx()?;
    let create_output = directory.create_or_open(&trx, paths.to_owned()).await;
    assert!(create_output.is_ok());
    Ok(())
}

#[test]
fn test_directory() {
    let _guard = unsafe { foundationdb::boot() };
    let db = futures::executor::block_on(common::database()).expect("cannot open fdb");

    eprintln!("clearing all keys");
    let trx = db.create_trx().expect("cannot create txn");
    trx.clear_range(b"\x00", b"\xff");
    futures::executor::block_on(trx.commit()).expect("could not clear keys");

    eprintln!("creating directories");
    let directory = DirectoryLayer::default();

    futures::executor::block_on(test_create_or_open_async(
        &db,
        &directory,
        vec![String::from("a")],
    ))
    .expect("failed to run");

    futures::executor::block_on(test_create_then_open_async(
        &db,
        &directory,
        vec![String::from("b"), String::from("a")],
    ))
    .expect("failed to run");
}
