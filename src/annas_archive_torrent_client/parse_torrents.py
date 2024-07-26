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
import hashlib
import datetime
import subprocess
import collections

import libtorrent as lt
import libtorrent

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
        "  md5_bytes BLOB UNIQUE,\n"
        # needed for tar torrents
        "  size INTEGER\n"
        ")"
    )
    db_cur.execute(query)

if not db_has_table(db_cur, "torrents"):
    query = (
        "CREATE TABLE torrents (\n"
        "  id INTEGER PRIMARY KEY,\n"
        "  btih_bytes BLOB UNIQUE,\n"
        "  filename TEXT\n"
        ")"
    )
    db_cur.execute(query)
    query = "CREATE INDEX idx_torrents_filename ON torrents (filename)"
    db_cur.execute(query)

if not db_has_table(db_cur, "files_torrents"):
    query = (
        "CREATE TABLE files_torrents (\n"
        "  id INTEGER PRIMARY KEY,\n"
        "  file_id INTEGER REFERENCES files(id),\n"
        "  torrent_id INTEGER REFERENCES torrents(id),\n"
        # needed for hashless torrents
        # TODO? also solve this with file_position and file size
        "  file_index INTEGER,\n"
        # needed for tar torrents
        # byte offset of content file in single-file tar torrent
        # file size is stored in the files table
        # TODO? rename to file_offset or file_byte_offset
        "  file_position INTEGER\n"
        ")"
    )
    db_cur.execute(query)

# no. dirname(__file__) can be read-only
#logfile_path = __file__ + ".log"
logfile_path = "parse_torrents.py.log"

done_torrent_files = set()

# no. this would skip torrents like pilimi-zlib-12160000-12229999.torrent
#if 1:
if 0:
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

def add_file(
        md5_bytes,
        torrent_id,
        # needed for hashless torrents
        #file_name=None,
        file_index=None,
        # needed for tar torrents
        size=None,
        file_position=None,
    ):
    query = "INSERT INTO files (md5_bytes, size) VALUES (?, ?)"
    args = (md5_bytes, size)
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

    query = "INSERT INTO files_torrents (file_id, torrent_id, file_index, file_position) VALUES (?, ?, ?, ?)"
    args = (file_id, torrent_id, file_index, file_position)
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



def btfs_mount_torrent(torrent_file, mount_dir, temp_dir):
    # this is simpler than using libtorrent
    # we want zero disk writes to save SSD lifetime
    # so all downloaded data should stay in memory
    # man btfs
    #   The contents of the files will be downloaded on-demand as they are read by applications.
    print(f"mounting torrent {torrent_file!r} on {mount_dir!r}")
    os.makedirs(mount_dir, exist_ok=True)
    os.makedirs(temp_dir, exist_ok=True)
    args = [
        "btfs",
        # default is $HOME/btfs
        f"--data-directory={temp_dir}",
        "--max-upload-rate=0",
        "--min-port=6881",
        "--max-port=6889",
        torrent_file,
        mount_dir,
    ]
    subprocess.check_call(args)



def btfs_unmount_torrent(mount_dir):
    print(f"unmounting torrent on {mount_dir!r}")
    args = [
        "fusermount",
        "-u",
        mount_dir,
    ]
    subprocess.check_call(args)



def torrent_list_files(torrent_file):

    args = [
        "torrenttools",
        "show",
        "files",
        torrent_file
    ]

    proc = subprocess.Popen(args, stdout=subprocess.PIPE, text=True)

    for filepath in iter(proc.stdout.readline, b""):
        filepath = filepath.rstrip()
        yield filepath



def torrent_get_name(torrent_file):

    args = [
        "torrenttools",
        "show",
        "name",
        torrent_file
    ]

    # TODO? use subprocess.check_output
    proc = subprocess.Popen(args, stdout=subprocess.PIPE, text=True)

    return proc.stdout.read().rstrip()



def torrent_get_piece_size(torrent_file):

    args = [
        "torrenttools",
        "show",
        "piece-size",
        torrent_file
    ]

    # TODO? use subprocess.check_output
    proc = subprocess.Popen(args, stdout=subprocess.PIPE, text=True)

    return int(proc.stdout.read())



def torrent_get_file_count(torrent_file):

    args = [
        "torrenttools",
        "info",
        torrent_file
    ]

    # TODO? use subprocess.check_output
    proc = subprocess.Popen(args, stdout=subprocess.PIPE, text=True)

    # empty line
    line1 = None
    # "  78.47 GiB in 0 directories, 61189 files"
    line2 = None

    # get the second-last line
    for line in proc.stdout.readlines():
        line2 = line1
        line1 = line

    # ['78.47', 'GiB', 'in', '0', 'directories,', '61189', 'files']
    parts = line2.split()

    assert parts[-1] == "files"
    return int(parts[-2])



def sha1sum(file_path=None, data=None):
    if data:
        return hashlib.sha1(data).digest()
    assert file_path
    # https://stackoverflow.com/questions/22058048/hashing-a-file-in-python
    # BUF_SIZE is totally arbitrary, change for your app!
    BUF_SIZE = 65536  # lets read stuff in 64kb chunks!
    #md5 = hashlib.md5()
    sha1 = hashlib.sha1()
    with open(file_path, 'rb') as f:
        while data := f.read(BUF_SIZE):
            #md5.update(data)
            sha1.update(data)
    return sha1.digest()
    #print("SHA1: {0}".format(sha1.hexdigest()))



def md5sum(file_path=None, data=None):
    if data:
        return hashlib.md5(data).digest()
    assert file_path
    # https://stackoverflow.com/questions/22058048/hashing-a-file-in-python
    # BUF_SIZE is totally arbitrary, change for your app!
    BUF_SIZE = 65536  # lets read stuff in 64kb chunks!
    #md5 = hashlib.md5()
    md5 = hashlib.md5()
    with open(file_path, 'rb') as f:
        while data := f.read(BUF_SIZE):
            #md5.update(data)
            md5.update(data)
    return md5.digest()
    #print("md5: {0}".format(md5.hexdigest()))



def libtorrent_download_torrent(torrent_file, on_file=None, on_piece=None):

    # TODO download torrent torrent_file
    # download in sequential order
    # on every downloaded piece, call on_piece
    # on every downloaded file, call on_file
    raise 123

    atp = lt.add_torrent_params()

    info_hash_v1 = None
    info_hash_v2 = None

    ti = lt.torrent_info(torrent_file)

    torrent_data = torrent_parser.parse_torrent_file(torrent_file, hash_raw=True)
    info_bytes = torrent_parser.encode(torrent_data["info"])
    # this is not part of torrent_parser
    # https://github.com/7sDream/torrent_parser/issues/14
    # add functions to calculate v1 and v2 info hashes of torrent files
    # FIXME handle v1-only and v2-only torrents
    # dont get info_hash_v1 of v2-only torrents
    # dont get info_hash_v2 of v1-only torrents
    # TODO or should we always calculate both hashes?
    info_hash_v1 = hashlib.sha1(info_bytes).hexdigest()
    info_hash_v2 = hashlib.sha256(info_bytes).hexdigest()

    resume_file = os.path.join(options.save_path, ti.name() + '.fastresume')
    try:
        atp = lt.read_resume_data(open(resume_file, 'rb').read())
    except Exception as e:
        print('failed to open resume file "%s": %s' % (resume_file, e))

    atp.ti = ti

    #atp.save_path = options.save_path
    atp.storage_mode = lt.storage_mode_t.storage_mode_sparse
    atp.flags |= lt.torrent_flags.duplicate_is_error \
        | lt.torrent_flags.auto_managed \
        | lt.torrent_flags.duplicate_is_error

    #if torrent_file.startswith('magnet:'):
    if 0:
        print("add_torrent: fetching metadata of magnet link")
        # https://github.com/arvidn/libtorrent/issues/2239 # get metadata info without downloading the complete file
        # https://github.com/snowyu/libtorrent/issues/650 # Pause after downloading metadata
        atp.flag_auto_managed = False
        atp.file_priorities = [0] * 1000 # TODO remove?
        atp.upload_mode = True
        atp.paused = False

    # download in sequential order
    # this is okay for old torrents with few leechers = mostly
    # this should be better for the filesystem, because less fragmentation
    atp.sequential_download = True

    # we always know the torrent hash or hashes
    # either from torrent file or from magnet link
    if False:
        temp_save_path = tempfile.mkdtemp(prefix="cas-torrent-temp-save-path-")
        print("temp_save_path", temp_save_path)
        atp.save_path = temp_save_path



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

    torrent_id = None
    query = "select id from torrents where filename = ?"
    args = (torrent_basename,)
    if (row := db_cur.execute(query, args).fetchone()) != None:
        torrent_id = row[0]

    # test: error: too short filepath '12184604'
    if torrent_basename == "pilimi-zlib-12160000-12229999.torrent": torrent_id = None
    #if torrent_basename == "pilimi-zlib2-19330000-21079999.torrent": torrent_id = None
    #if torrent_basename == "pilimi-zlib2-17600000-17689999.torrent": torrent_id = None

    if torrent_id != None:
        # done already
        #print("skip", torrent_basename)
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

    #print("torrent_file", torrent_basename)

    btih_bytes = get_btih_bytes(torrent_file)

    try:
        torrent_id = add_torrent(btih_bytes)
    except sqlite3.IntegrityError:
        # sqlite3.IntegrityError: UNIQUE constraint failed: torrents.btih_bytes
        # assume that this torrent has already been added -> continue

        if torrent_basename == "pilimi-zlib-12160000-12229999.torrent":
            pass
        else:
            # migrate: add column torrents.filename
            #query = "update torrents set filename = ? where id = ?"
            query = "update torrents set filename = ? where btih_bytes = ?"
            args = (torrent_basename, btih_bytes)
            try:
                db_cur.execute(query, args)
            except Exception as exc:
                print("query failed:", query, args, exc)
                raise
            print("skipping torrent", torrent_file_num, "of", torrent_file_count, "=", round(torrent_file_num/torrent_file_count*100), "%", "eta", eta, torrent_basename)
            db_con.commit()
            continue

    print("torrent", torrent_file_num, "of", torrent_file_count, "=", round(torrent_file_num/torrent_file_count*100), "%", "eta", eta, torrent_basename)

    i = 0
    added_files = True

    for filepath in torrent_list_files(torrent_file):

        i += 1

        # debug: check first N files
        if 0:
            if i > 5:
                break

        # download torrents to get file hashes and positions
        # this is a braindead workaround for...
        # add md5 hashes to filenames
        # https://software.annas-archive.se/AnnaArchivist/annas-archive/-/issues/196
        # split single-file torrents to multi-file torrents
        # https://software.annas-archive.se/AnnaArchivist/annas-archive/-/issues/181

        use_braindead_workaround = True
        #use_braindead_workaround = False

        # problem: btfs is slow
        # FIXME fetch pieces ahead for sequential access
        # https://github.com/johang/btfs/issues/89

        use_btfs = True
        use_libtorrent = False

        if len(filepath) < 22:

            # hashless torrent with hashless filenames
            # TODO add md5 hashes to filenames
            # https://software.annas-archive.se/AnnaArchivist/annas-archive/-/issues/196

            if use_braindead_workaround and use_btfs:

                # problem: btfs does not list files in sequential order
                # solution: use "torrenttools show files" to get the file list
                # https://github.com/johang/btfs/issues/87
                # preserve the original file order for sequential access
                # TODO check if already mounted
                mount_dir = "/run/user/1000/annas_archive_torrent_client_mount_dir"
                temp_dir = "/run/user/1000/annas_archive_torrent_client_temp_dir"
                btfs_mount_torrent(torrent_file, mount_dir, temp_dir)
                # TODO atexit
                # unmount_torrent(mount_dir)

                torrent_name = torrent_get_name(torrent_file)
                torrent_root_dir = mount_dir + "/" + torrent_name
                temp_root_dir = temp_dir + "/" + os.listdir(temp_dir)[0] + "/files/" + torrent_name
                piece_size = torrent_get_piece_size(torrent_file)
                num_files = torrent_get_file_count(torrent_file)

                print(f"waiting for torrent_root_dir {torrent_root_dir!r} ...")
                for retry_idx in range(100):
                    if os.path.exists(torrent_root_dir):
                        break
                    time.sleep(1)

                print(f"waiting for {num_files} files in torrent_root_dir {torrent_root_dir!r} ...")
                for retry_idx in range(100):
                    # FIXME stat is slow -> glob is slow (recursive listdir)
                    # https://github.com/johang/btfs/issues/88
                    # NOTE this (non-recursive listdir) only works when all files are in root_dir
                    num_files_actual = len(os.listdir(torrent_root_dir))
                    print(f"found {num_files_actual} of {num_files} files in {torrent_root_dir!r}")
                    if num_files_actual == num_files:
                        break
                    time.sleep(1)
                print(f"waiting for {num_files} files in torrent_root_dir {torrent_root_dir!r} done")

                # fix: "looping files" can hang
                # TODO dynamic sleep
                print(f"waiting for btfs init")
                time.sleep(5)

                keep_n_tempfiles = 100
                #keep_n_pieces = 10 # ?
                #piece_files = []
                piece_files = collections.deque()

                #for file_name in os.listdir(mount_dir): # no! not sequential access
                # TODO re-use the outer iterator torrent_list_files(torrent_file)
                print(f"looping files in torrent_root_dir {torrent_root_dir!r} ...")
                loop_t1 = time.time()
                last_t2 = time.time()
                sum_size = 0
                last_piece_idx = 0

                for file_index, file_path in enumerate(torrent_list_files(torrent_file)):

                    file_name = os.path.basename(file_path)
                    temp_file_path = temp_root_dir + "/" + file_path
                    file_path = torrent_root_dir + "/" + file_path
                    #print("reading file", file_path)
                    md5_bytes = md5sum(file_path) # slow
                    t2 = time.time()
                    loop_dt = t2 - loop_t1
                    dt = t2 - last_t2
                    file_size = os.path.getsize(file_path)
                    sum_size += file_size
                    loop_speed = (sum_size / 1024) / loop_dt
                    piece_idx = sum_size // piece_size
                    print("md5", md5_bytes.hex(), "file", (file_index + 1), "of", num_files, "name", file_name, "dt", round(dt), "loop_speed", round(loop_speed, 1), "KiB/s", "size", file_size, "piece", piece_idx)
                    # TODO? also solve this with file_position and file size
                    add_file(md5_bytes, torrent_id, file_index=file_index)
                    # no. still getting OSError: [Errno 5] Input/output error
                    # no. deleting tempfiles too early causes OSError: [Errno 5] Input/output error
                    #os.unlink(temp_file_path)
                    piece_files.append(temp_file_path)
                    """
                    if piece_idx != last_piece_idx:
                        for path in piece_files:
                            os.unlink(path)
                        piece_files = []
                    """
                    while len(piece_files) > keep_n_tempfiles:
                        path = piece_files.popleft()
                        os.unlink(path)
                    last_t2 = t2
                    last_piece_idx = piece_idx

                for path in piece_files:
                    os.unlink(path)
                piece_files = []

                print(f"looping files in torrent_root_dir {torrent_root_dir!r} done")
                btfs_unmount_torrent(mount_dir)

            elif use_braindead_workaround and use_libtorrent:

                # FIXME use libtorrent like https://github.com/XayOn/torrentstream

                libtorrent_download_torrent()

            else:
                print("torrent_file", torrent_basename, f"error: too short filepath {filepath!r}")
                added_files = False

            # stop after first file
            break

        if filepath.endswith(".tar"):
            # single-file torrent
            # TODO split single-file torrents to multi-file torrents
            # https://software.annas-archive.se/AnnaArchivist/annas-archive/-/issues/181
            if use_braindead_workaround:

                #db_con.commit() # commit pending update queries

                def on_piece(piece_idx, piece_bytes):
                    # TODO parse this block of the tar file
                    # for each file in the tar archive:
                    # get md5 hash and position of file
                    # and store it in a separate table tar_torrent_files
                    # TODO carry trailing bytes at end of piece to next piece
                    raise 123
                # TODO download torrent torrent_file
                download_torrent(torrent_file, on_piece=on_piece)
                # add_file(md5_bytes, torrent_id, file_index=None, size=None, file_position=None)
            else:
                print("torrent_file", torrent_basename, f"error: tar file {filepath!r}")
                added_files = False
            # stop after first file
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
