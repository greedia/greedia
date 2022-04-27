# Greedia

Greedia is a cloud-drive mounting application with the goal of heavily caching media and serving it fast. It does this by indexing the drive upfront and smart-caching the beginning of every media file (by default, 10 seconds of content).

The Greedia cache contains a permanent cache with the first X seconds of each file (the "hard cache"), along with
a space-limited cache for the rest of the data (the "soft cache").

***Greedia is extremely alpha at the moment, and the polish and documentation are very lacking. Expect things to break.***

## Features

At the moment, Greedia supports the following cloud drives:

- Google Drive

And smart-caches the following media formats:

- MP3
- MKV/WebM

Rclone-encrypted files are also supported.

Note that Greedia does not currently support file uploading, and gives read-only access to drives by default.

## Installation

Greedia will be added to crates<span>.io eventually, however for now you can install directly from git using:

```
cargo install --git https://github.com/greedia/greedia
```

## Configuration

Greedia will eventually have an rclone-style initialization command but at the moment, configuration is manual.
As Greedia is meant to be an `rclone mount` replacement, you should be able to base the Greedia configuration on an rclone configuration under most circumstances.

For example, if you have an rclone configuration that looks like this:

```ini
[mydrive]
type = drive
client_id = client-id.apps.googleusercontent.com
client_secret = client-secret
token = {"access_token": "unimportant", "token_type": "unimportant", "refresh_token": "1/refresh-token", "expiry": "unimportant"}
scope = drive
team_drive = team_drive_id

[mydrive_crypt]
type = crypt
remote = mydrive:
filename_encryption = standard
directory_name_encryption = true
password = password1
password2 = password2
```

That will translate to a Greedia configuration that looks like this:

```toml
[caching]
db_path = "/path/to/greedia/db"  # Greedia's database
mount_point = "/path/to/mount/point"  # Where to FUSE-mount the drives
soft_cache_limit = 10_000_000_000  # Limit the soft cache to 10GB

[gdrive.mydrive]  # Make a google drive directory called "mydrive"
client_id = "client-id.apps.googleusercontent.com"
client_secret = "client-secret"
refresh_token = "1/refresh-token"
drive_id = "team_drive_id"
password = "password1"
password2 = "password2"
```

If you have service accounts you want to use with a `gdrive`, save the JSON format service accounts somewhere and add this line to the `gdrive.mydrive` section:

```toml
service_accounts = [ "/path/to/sa1.json", "/path/to/sa2.json", "path/to/sa3.json" ]
```

Rinse and repeat for each service account file you have. If you have many files, it is recommended to add at least a few service accounts, as they will speed up the initial scanning immensely.

If you want `mydrive`'s root to be a directory within the drive, add this line to the `gdrive.mydrive` section:
```toml
root_path = "/path/within/drive"
```

For more options, look at `greedia_example_config.toml`.

## Running

The command to run Greedia is:

```
greedia run -c path/to/greedia.toml
```

On first startup, Greedia will scan through all files within a drive in order to hard-cache the metadata and the beginning of each piece of media. While that's in progress, you can still explore the directory and access files.

With this README's configuration as an example, the mount point would look like this:

```
$ ls /path/to/mount/point
mydrive
$ ls /path/to/mount/point/mydrive
<google drive contents>
```

## License

Licensed under the terms of the MIT license and the Apache License (Version 2.0)

See [LICENSE-MIT](LICENSE-MIT) and [LICENSE-APACHE](LICENSE-APACHE) for details.

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be dual licensed as above, without any
additional terms or conditions.