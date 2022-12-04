use futures::future;
use rusoto_core::Region;
use rusoto_s3::{GetObjectRequest, ListObjectsV2Request, S3Client, S3};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::{env, fs};
use tokio::fs::File;
use tokio::io;

#[tokio::main]
async fn main() {
    if env::var("AWS_ACCESS_KEY_ID").is_err() {
        panic!("AWS_ACCESS_KEY_ID is not set in env")
    }

    if env::var("AWS_SECRET_ACCESS_KEY").is_err() {
        panic!("AWS_SECRET_ACCESS_KEY is not set in env")
    }

    let args = std::env::args().collect::<Vec<String>>();

    // todo validate
    let bucket_name = args
        .get(1)
        .unwrap_or_else(|| panic!("bucket name is not specified"));

    //格納用ディレクトリをデスクトップに作る
    let desktop = env::var("HOME").unwrap();
    let desktop_path = Path::new(&desktop).join("Desktop");
    let dir_path = desktop_path.join(bucket_name);
    fs::create_dir(&dir_path);

    let default_region = "ap-northeast-1".to_owned();
    let region_name = args.get(2).unwrap_or(&default_region);
    let region: Region = region_name.parse().unwrap();
    let client = S3Client::new(region);
    let list_objects_request = ListObjectsV2Request {
        bucket: bucket_name.to_string(),
        ..Default::default()
    };
    let objects = client.list_objects_v2(list_objects_request).await.unwrap();

    // バケット内のオブジェクトをひとつずつダウンロード
    for object in objects.contents.unwrap() {
        let object_key = object.key.unwrap().to_owned();
        if !object_key.ends_with('/') {
            println!("{}", object_key);
            let get_object_request = GetObjectRequest {
                bucket: bucket_name.to_string(),
                key: object_key.to_string(),
                ..Default::default()
            };

            // S3オブジェクトをダウンロードする
            let object = client.get_object(get_object_request).await.unwrap();
            println!(
                "Downloading {}, whose size is {}MB",
                object_key,
                object.content_length.unwrap() / 1_000_000
            );

            // ダウンロードしたS3オブジェクトのデータをファイルに保存する
            let downloaded_path = dir_path.join(object_key.replace('/', "___"));
            println!("to: {}", downloaded_path.display());
            let mut body = object.body.unwrap().into_async_read();
            let mut file = File::create(&downloaded_path).await.unwrap();
            io::copy(&mut body, &mut file).await;
        }
    }
    archive(dir_path, bucket_name.to_string()).await;
}

use zip::write::{FileOptions, ZipWriter};
use zip::CompressionMethod;

async fn archive(files_dir_path: PathBuf, zip_prefix: String) {
    let dir_path = files_dir_path;
    let files = std::fs::read_dir(&dir_path).unwrap();
    let mut groups: Vec<Vec<PathBuf>> = Vec::new();
    let mut current_group = Vec::new();
    let mut current_group_size = 0;
    for file in files {
        let file_path = file.unwrap().path().clone();
        let file_metadata = tokio::fs::metadata(&file_path).await.unwrap();
        let file_size = file_metadata.len();

        if current_group_size + file_size > 1024 * 1024 * 1024 {
            // 1GBを超える場合は、新しいグループを作成する
            groups.push(current_group);
            current_group = Vec::new();
            current_group_size = 0;
        }

        // 現在のグループにファイルを追加する
        current_group.push(file_path);
        current_group_size += file_size;
    }

    groups.push(current_group);

    for (index, group) in groups.into_iter().enumerate() {
        let zip_file_path = dir_path.join(format!("{}_{:03}.zip", zip_prefix, index));
        println!("zip path is: {}", zip_file_path.to_str().unwrap());
        let file = std::fs::File::create(zip_file_path).unwrap();
        let mut zip = ZipWriter::new(file);

        for file_path in group {
            let options = FileOptions::default().compression_method(CompressionMethod::Stored);
            zip.start_file(file_path.file_name().unwrap().to_str().unwrap(), options)
                .unwrap();
            let mut file = std::fs::File::open(file_path).unwrap();
            std::io::copy(&mut file, &mut zip).unwrap();
        }

        zip.finish().unwrap();
    }
}
