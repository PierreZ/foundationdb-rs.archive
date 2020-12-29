// Copyright 2018 foundationdb-rs developers, https://github.com/Clikengo/foundationdb-rs/graphs/contributors
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.

use foundationdb::*;

mod common;

async fn test_create_then_open_async() -> FdbResult<()> {
    let paths = vec![String::from("some_path")];
    let directory = DirectoryLayer::default();

    let db = common::database().await?;
    let trx = db.create_trx()?;
    let create_output = directory.create_or_open(&trx, paths.to_owned()).await;
    assert!(create_output.is_ok());

    trx.commit().await?;

    let trx = db.create_trx()?;
    let get_output = directory.create_or_open(&trx, paths.to_owned()).await;
    assert!(get_output.is_ok());
    assert_eq!(create_output.unwrap().bytes(), get_output.unwrap().bytes());

    Ok(())
}

#[test]
fn test_create_or_open() {
    let _guard = unsafe { foundationdb::boot() };
    futures::executor::block_on(test_create_then_open_async()).expect("failed to run");
}
