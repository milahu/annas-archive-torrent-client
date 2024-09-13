# annas-archive-torrent-client



## why

because i hate captchas (because i hate rate-limiting in general)

> annas-archive.org
>
> Verifying you are human. This may take a few seconds.
>
> annas-archive.org needs to review the security of your connection before proceeding.

"a few seconds"? no, that shit just hangs forever.

https://old.reddit.com/r/CloudFlare/comments/x3m0ti/checking_if_the_site_connection_is_secure_page/

> The bot check is a feature that is essentially disabled by default. I've never gotten a bot check on any of my Cloudflare websites because I don't have it enabled. It's just a feature of the proxy. Sites that have it set to a high setting usually do so because they're frequently under attack or being crawled very fast. But it isn't Cloudflare just doing it randomly - it's people choosing to enable it on their sites.

seems like annas-archive.org have enabled the "anti bot" feature of cloudflare... idiots. hate them.



## hate annas-archive

the idiot-maintainer of annas-archive hates my idea so much that he
[censored multiple of my gitlab issues](http://it7otdanqu7ktntxzm427cba6i53w6wlanlh23v5i3siqmos47pzhvyd.onion/milahu/hate-maintainers/src/branch/main/hate-annas-archive),
calling me a "spammer".
so much for "anti censorship" and "open access"...
nah, that guy is greedy for money from donations,
aka "passive income", aka "let some idiots work for me"



---



# cas_torrent

a bittorrent client with content-addressed storage

download and upload torrents to/from a CAS filesystem

avoid overwriting files with same path but different content

proof of concept for qBittorrent issue: [4.2.5 overwrites files if file names are the same](https://github.com/qbittorrent/qBittorrent/issues/12842)

similar issue: [Never silently overwrite existing files](https://github.com/qbittorrent/qBittorrent/issues/127)

## example use

load torrent file

```
python3 -m cas_torrent --port 6882 input.torrent
```

load magnet link

```
python3 -m cas_torrent --port 6882 magnet:?xt=urn:btih:1234567890123456789012345678901234567890
```

## cas filesystem

all complete files are stored in the sha256 store.
sha256 is the most common file hash,
more common than the bt2r hash (bittorrent v2 merkle root hash, see `def get_bt2_root_hash_of_path(file_path)` in [src/cas_torrent/cas_torrent.py](src/cas_torrent/cas_torrent.py)).
storing files by their sha256 hash allows sharing these files with other apps.

```
cas/sha256/12/34/567890123456789012345678901234567890123456789012345678901234
```

all directories are stored in the bt2 store.
the directories contain symlinks to files in the sha256 store.
temporary files are stored here, because their sha256 hash is unknown.

```
cas/bt2/12/34/567890123456789012345678901234567890123456789012345678901234
```

if the bt2 hash is unknown (v1-only magnet links and missing metadata),
then instead of the bt2 store, the bt1 store is used.
as soon as the bt2 hash is known, files are moved to the bt2 store.
in most cases, the bt1 store holds only a symlink to the bt2 store,
because the bt2 store offers better collision-resistance (sha256 versus sha1).

```
cas/bt1/12/34/567890123456789012345678901234567890
```

## las filesystem

las = [location-addressed storage](https://en.wikipedia.org/wiki/Content-addressable_storage)

presenting torrents only by their info hashes is not user-friendly.
users expect the torrent name -- this is provided by the "las store".

```
las/some_torrent_name/some_file.txt
```

### file path collisions

problem: file path collisions between different torrents.

solution: merge by default, rename if different content.

the file contents are compared by comparing cas store paths:

if identical las file paths are linked to identical cas file paths,
then the file contents are identical, and we dont rename the las file paths.

if identical las file paths are linked to different cas file paths,
then the file contents are different, and we rename the las file paths.

#### renaming files

a simple solution is to append " (1)" or " (2)" or " (3)" etc. before the file extension.

another solution would be to append a part of the file hash before the file extension,
but that produces longer filenames.

## todo

- add tests
- allow multiple CAS filesystems on multiple hard drives, aka "soft raid"
- add this feature to other bittorrent clients
