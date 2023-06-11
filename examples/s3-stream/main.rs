use oci_distribution as oci;
use s3;
use tokio::io::duplex;
use tokio::spawn;

#[tokio::main]
pub async fn main() {
    let mut client = oci::Client::new(oci::client::ClientConfig {
        protocol: oci::client::ClientProtocol::Http,
        ..Default::default()
    });

    let (mut writer, reader) = duplex(1024 * 1024);

    let bucket = s3::Bucket::new(
        "model-bucket",
        s3::Region::Custom {
            region: "us-east-1".into(),
            endpoint: "http://localhost:9000".to_string(),
        },
        s3::creds::Credentials::new(Some("minio"), Some("minio123"), None, None, None).unwrap(),
    )
    .unwrap();

    let s3_read = spawn(async move {
        let res = bucket
            .get_object_stream(
                "model-bucket/7989be5eb6f3016cb2c15cea3eb7d27c1c5260907cd61d442e23ca1cf1c0a549.venv.tar",
                &mut writer,
            )
            .await
            .unwrap();

        println!("read stream: {:?}", res);
    });

    let reference = oci::Reference::with_digest(
        "localhost:5050".into(),
        "model2".into(),
        "6b2b155d204e34eefd25537e6908f4a5f85d939ee7b373d83d9092a6400ecf66".into(),
    );

    // let mut read_vec = Vec::new();
    // let read = reader.read_to_end(&mut read_vec).await;
    // println!("read {:?} bytes: {:?}", read, read_vec);

    let layer = oci::client::ImageLayerStream::new(
        reader,
        "application/octet-stream",
        None,
        "sha256:6b2b155d204e34eefd25537e6908f4a5f85d939ee7b373d83d9092a6400ecf66",
        638935040,
    );

    let config = oci::client::Config::new("{}".into(), "application/json".into(), None);

    let auth = oci::secrets::RegistryAuth::Anonymous;

    let res = client
        .push_stream(&reference, vec![layer], config, &auth, None)
        .await
        .unwrap();
    println!("upload:\n\t{}\n\t{}", res.config_url, res.manifest_url);

    s3_read.await.unwrap();
}
