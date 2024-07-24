#!/usr/bin/env python3

# TODO download torrents.json to f"{cache_dir}/torrents.json"
# TODO download all torrents to f"{cache_dir}/torrents/"
#   first download "torrent of torrents"
#     https://software.annas-archive.se/AnnaArchivist/annas-archive/-/issues/179
#     example: 5709 torrents with 2.1GiB
#     magnet:?xt=urn:btih:062811c31823bc800fe3e6a2e1e60fd5673c83a5&dn=annas-torrents
#   then download missing torrents from the urls in torrents.json

"""
todo query

$ sqlite3 ~/.cache/annas-archive/md5_to_btih.db "
  select lower(hex(torrents.btih_bytes))
  from torrents
  JOIN files_torrents ON torrents.id = files_torrents.torrent_id
  JOIN files ON files.id = files_torrents.file_id
  where files.md5_bytes = unhex('DB609084958BBD327053257DE0DA1E5B')
"

"""

import os
import re
import sys
import time
import glob
import base64
import sqlite3
import datetime
import subprocess
import collections

cache_dir = os.environ["HOME"] + "/.cache/annas-archive"

db_path = cache_dir + "/md5_to_btih.db"

print("db_path", db_path)

db_con = sqlite3.connect(db_path)
db_con.execute("PRAGMA foreign_keys = ON")
db_cur = db_con.cursor()

def db_has_table(db_cur, table_name):
    try:
        db_cur.execute(f"SELECT 1 FROM {table_name} limit 1")
    except sqlite3.OperationalError:
        return False
    return True

if not db_has_table(db_cur, "files"):
    query = (
        "CREATE TABLE files (\n"
        "  id INTEGER PRIMARY KEY,\n"
        "  md5_bytes BLOB UNIQUE\n"
        ")"
    )
    db_cur.execute(query)

if not db_has_table(db_cur, "torrents"):
    query = (
        "CREATE TABLE torrents (\n"
        "  id INTEGER PRIMARY KEY,\n"
        "  btih_bytes BLOB UNIQUE\n"
        ")"
    )
    db_cur.execute(query)

if not db_has_table(db_cur, "files_torrents"):
    query = (
        "CREATE TABLE files_torrents (\n"
        "  id INTEGER PRIMARY KEY,\n"
        "  file_id INTEGER REFERENCES files(id),\n"
        "  torrent_id INTEGER REFERENCES torrents(id)\n"
        ")"
    )
    db_cur.execute(query)

# no. dirname(__file__) can be read-only
#logfile_path = __file__ + ".log"
logfile_path = "parse_torrents.py.log"

done_torrent_files = set()

if 1:
    if os.path.exists(logfile_path):
        with open(logfile_path) as f:
            for line in f.readlines():
                if not line.startswith("torrent_file "):
                    continue
                if not " error: " in line:
                    continue
                parts = line.split(" ")
                torrent_basename = parts[1]
                done_torrent_files.add(torrent_basename)

def add_torrent(btih_bytes):
    query = "INSERT INTO torrents (btih_bytes) VALUES (?)"
    args = (btih_bytes,)
    try:
        db_cur.execute(query, args)
        torrent_id = db_cur.lastrowid
    except Exception as exc:
        # todo?
        raise
    """
    except sqlite3.IntegrityError as exc:
        if str(exc) != "UNIQUE constraint failed: torrents.btih_bytes":
            raise
    """
    return torrent_id

def add_file(md5_bytes, torrent_id):
    query = "INSERT INTO files (md5_bytes) VALUES (?)"
    args = (md5_bytes,)
    file_id = None
    try:
        db_cur.execute(query, args)
        file_id = db_cur.lastrowid
    except sqlite3.IntegrityError as exc:
        if str(exc) != "UNIQUE constraint failed: files.md5_bytes":
            raise
    if file_id == None:
        # file exists in db
        query = "SELECT id from files where md5_bytes = ?"
        args = (md5_bytes,)
        file_id = db_cur.execute(query, args).fetchone()[0]

    query = "INSERT INTO files_torrents (file_id, torrent_id) VALUES (?, ?)"
    args = (file_id, torrent_id)
    try:
        db_cur.execute(query, args)
    except Exception as exc:
        raise

def get_btih_bytes(torrent_file):
    args = [
        "torrenttools",
        "show",
        "infohash",
        "--protocol=1",
        torrent_file
    ]
    out = subprocess.check_output(args, text=True).strip()
    # btih: base16 sha1
    if len(out) != 40:
        raise Exception(f"torrent {torrent_file!r}: invalid btih {out!r}")
    btih_bytes = bytes.fromhex(out)
    return btih_bytes

torrent_file_count = len(glob.glob(f"{cache_dir}/torrents/**/*.torrent", recursive=True))

torrent_file_num = 0

dn = 100
times = collections.deque()
t1 = None

# TODO filter torrents with torrents.json
# ignore ...
#   "group_name": "aa_derived_mirror_metadata"
#   comics, magazines, papers, metadata
# TODO refactor with annas_archive_torrent_client.py

for torrent_file in glob.glob(f"{cache_dir}/torrents/**/*.torrent", recursive=True):

    torrent_file_num += 1

    torrent_basename = os.path.basename(torrent_file)

    if torrent_basename in done_torrent_files:
        continue

    t2 = time.time()
    eta = "?"
    if len(times) > 0:
        if len(times) >= dn:
            t1 = times.popleft()
            dt = t2 - t1
            speed = dn / dt
        else:
            t1 = times[0]
            dt = t2 - t1
            speed = len(times) / dt
        eta = torrent_file_count / speed
        eta = datetime.timedelta(seconds=eta)

    times.append(t2)

    print("torrent", torrent_file_num, "of", torrent_file_count, "=", round(torrent_file_num/torrent_file_count*100), "%", "eta", eta, torrent_basename)

    #print("torrent_file", torrent_basename)

    btih_bytes = get_btih_bytes(torrent_file)

    try:
        torrent_id = add_torrent(btih_bytes)
    except sqlite3.IntegrityError:
        # sqlite3.IntegrityError: UNIQUE constraint failed: torrents.btih_bytes
        # assume that this torrent has already been added
        continue

    args = [
        "torrenttools",
        "show",
        "files",
        torrent_file
    ]

    proc = subprocess.Popen(args, stdout=subprocess.PIPE)

    i = 0
    added_files = True

    for filepath in iter(proc.stdout.readline, b""):

        i += 1

        # debug: check first N files
        if 0:
            if i > 5:
                break

        filepath = filepath.decode("ascii").rstrip()

        # download torrents to get file hashes and positions
        # this is a braindead workaround for...
        # add md5 hashes to filenames
        # https://software.annas-archive.se/AnnaArchivist/annas-archive/-/issues/196
        # split single-file torrents to multi-file torrents
        # https://software.annas-archive.se/AnnaArchivist/annas-archive/-/issues/181
        # TODO use the braindead workaround...
        use_braindead_workaround = True
        use_braindead_workaround = False

        def download_torrent(torrent_file, on_file=None, on_piece=None):
            # TODO download torrent torrent_file
            # download in sequential order
            # on every downloaded piece, call on_piece
            # on every downloaded file, call on_file
            raise 123

        if len(filepath) < 22:
            # hashless filenames
            # TODO add md5 hashes to filenames
            # https://software.annas-archive.se/AnnaArchivist/annas-archive/-/issues/196
            if use_braindead_workaround:
                # TODO? also solve this with "def on_piece"
                #   this depends on whether we get per-file events from the torrent downloader
                #   or only per-piece events
                def on_file(file_name, file_bytes):
                    # TODO get md5 hash of file
                    # and store it in a separate table hashless_torrent_files
                    raise 123
                # TODO download torrent torrent_file
                download_torrent(torrent_file, on_file=on_file)
            else:
                print("torrent_file", torrent_basename, f"error: too short filepath {filepath!r}")
                added_files = False
                break

        if filepath.endswith(".tar"):
            # single-file torrent
            # TODO split single-file torrents to multi-file torrents
            # https://software.annas-archive.se/AnnaArchivist/annas-archive/-/issues/181
            if use_braindead_workaround:
                def on_piece(piece_idx, piece_bytes):
                    # TODO parse this block of the tar file
                    # for each file in the tar archive:
                    # get md5 hash and position of file
                    # and store it in a separate table tar_torrent_files
                    # TODO carry trailing bytes at end of piece to next piece
                    raise 123
                # TODO download torrent torrent_file
                download_torrent(torrent_file, on_piece=on_piece)
            else:
                print("torrent_file", torrent_basename, f"error: tar file {filepath!r}")
                added_files = False
                break

        # torrent_file annas_archive_data__aacid__duxiu_files__20240613T211620Z--20240613T211621Z.torrent ok? 'aacid__duxiu_files__20240613T211620Z__22i4qDCn9PUrE6UviPdfAs'
        # torrent_file annas_archive_data__aacid__duxiu_files__20240613T181252Z--20240613T181253Z.torrent ok? 'aacid__duxiu_files__20240613T181252Z__22DL9ZFXH3eaeowzxYaNj9'
        # torrent_file annas_archive_data__aacid__upload_files_alexandrina__20240510T044716Z--20240510T044717Z.torrent ok? 'aacid__upload_files_alexandrina__20240510T044716Z__278AwLFtWH2YwjtMTqGcuK'
        # TODO which base64 alphabet is used?
        # base64 alphabet is [0-9a-zA-Z] + 2 chars = 10 + 26 + 26 + 2 chars

        md5_bytes = None

        if match := re.search("_([0-9a-zA-Z_+=-]{22})$", filepath):
            # filename contains base64 md5 hash
            md5_bytes = base64.b64decode(match.group(1) + "==")
            b16 = md5_bytes.hex()
            #print("torrent_file", torrent_basename, "ok base64", repr(filepath), "->", b16)

        elif match := re.fullmatch("([0-9a-fA-F]{32})", filepath):
            # filename is base16 md5 hash only
            md5_bytes = bytes.fromhex(match.group(1))
            #print("torrent_file", torrent_basename, "ok md5", repr(filepath))

        else:
            print("torrent_file", torrent_basename, "ok?", repr(filepath))
            continue

        add_file(md5_bytes, torrent_id)

    if added_files:
        print("commit", torrent_file)
        db_con.commit()


# this index is required to speed up the query
# "select torrent_id from files_torrents where file_id = 1"
# this takes about 1 minute
# note: its better to create the index after all data has been inserted
print("creating index idx_files_torrents_file_id ...")
t1 = time.time()
query = "CREATE INDEX idx_files_torrents_file_id ON files_torrents (file_id)"
db_cur.execute(query)
db_con.commit()
t2 = time.time()
print("creating index idx_files_torrents_file_id done after", (t2 - t1), "seconds")
