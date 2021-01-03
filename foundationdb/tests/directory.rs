// Copyright 2018 foundationdb-rs developers, https://github.com/Clikengo/foundationdb-rs/graphs/contributors
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.

use foundationdb::directory::error::DirectoryError;
use foundationdb::directory::DirectoryLayer;
use foundationdb::*;

mod common;

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

    futures::executor::block_on(test_list(&db, &directory, vec![String::from("a")], 10))
        .expect("failed to run");

    futures::executor::block_on(test_children_content_subspace(
        &db,
        &directory,
        vec![String::from("c")],
    ))
    .expect("failed to run");

    futures::executor::block_on(test_bad_layer(&db)).expect("failed to run");
}

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

/// testing that we throwing Err(DirectoryError::IncompatibleLayer)
async fn test_bad_layer(db: &Database) -> Result<(), DirectoryError> {
    let directory = DirectoryLayer {
        layer: vec![0u8],
        ..Default::default()
    };
    let trx = db.create_trx()?;

    directory
        .create_or_open(&trx, vec![String::from("bad_layer")])
        .await?;

    let directory = DirectoryLayer {
        layer: vec![1u8],
        ..Default::default()
    };

    let result = directory
        .create_or_open(&trx, vec![String::from("bad_layer")])
        .await;
    match result {
        Err(DirectoryError::IncompatibleLayer) => {}
        _ => panic!("should have been an IncompatibleLayer error"),
    }

    Ok(())
}

/// testing list functionality. Will open paths and create n sub-folders.
async fn test_list(
    db: &Database,
    directory: &DirectoryLayer,
    paths: Vec<String>,
    sub_path_to_create: usize,
) -> Result<(), DirectoryError> {
    // creating directory
    let trx = db.create_trx()?;
    directory.create(&trx, paths.to_owned()).await;
    trx.commit().await.expect("could not commit");

    for i in 0..sub_path_to_create {
        let trx = db.create_trx()?;

        let mut sub_path = paths.clone();
        sub_path.push(format!("node-{}", i));
        eprintln!("creating {:?}", sub_path.to_owned());
        directory.create(&trx, sub_path.to_owned()).await;

        trx.commit().await.expect("could not commit");
    }

    let trx = db.create_trx()?;

    let sub_folders = directory.list(&trx, paths.to_owned()).await?;
    eprintln!("found {:?}", sub_folders);
    assert_eq!(sub_folders.len(), sub_path_to_create);

    for i in 0..sub_path_to_create {
        let mut sub_path = paths.clone();
        sub_path.push(format!("node-{}", i));
        assert!(sub_folders.contains(&format!("node-{}", i)));

        let trx = db.create_trx()?;
        match directory.exists(&trx, sub_path.to_owned()).await {
            Ok(_) => {}
            Err(err) => panic!("should have found {:?}: {:?}", sub_path, err),
        }
    }

    Ok(())
}

/// checks that the content_subspace of the children is inside the parent
async fn test_children_content_subspace(
    db: &Database,
    directory: &DirectoryLayer,
    paths: Vec<String>,
) -> Result<(), DirectoryError> {
    let trx = db.create_trx()?;

    eprintln!("parent = {:?}", paths.to_owned());

    let root_subspace = directory.create_or_open(&trx, paths.to_owned()).await?;

    let mut children_path = paths.clone();
    children_path.push(String::from("nested"));
    eprintln!("children = {:?}", children_path.to_owned());

    let children_subspace = directory
        .create_or_open(&trx, children_path.to_owned())
        .await?;

    assert!(
        children_subspace.bytes().starts_with(root_subspace.bytes()),
        "children subspace '{:?} does not start with parent subspace '{:?}'",
        children_subspace.bytes(),
        root_subspace.bytes()
    );

    trx.commit().await.expect("could not commit");
    let trx = db.create_trx()?;

    let open_children_subspace = directory.open(&trx, children_path.to_owned()).await?;

    assert_eq!(children_subspace.bytes(), open_children_subspace.bytes());

    Ok(())
}
