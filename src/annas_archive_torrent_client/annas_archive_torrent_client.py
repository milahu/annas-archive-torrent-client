#!/usr/bin/env python3

# annas_archive_torrent_client

# a bittorrent client with content-addressed storage
# download and upload torrents to/from a CAS filesystem
# avoid overwriting files with same path but different content

# based on https://github.com/arvidn/libtorrent/blob/RC_2_0/bindings/python/client.py

import sys
import time
import os.path
import tempfile
import shutil
import hashlib
import math

import libtorrent as lt
import watchdog
import watchdog.events
import watchdog.observers

#import binascii
#import json

# https://github.com/google/casfs/blob/master/casfs/util.py
from . import casfs_util

# https://github.com/7sDream/torrent_parser/blob/master/torrent_parser.py
from . import torrent_parser


# also in setup.py
# FIXME single source
annas_archive_torrent_client_version = "0.0.1"


def add_suffix(val):
    prefix = ['B', 'kB', 'MB', 'GB', 'TB']
    for i in range(len(prefix)):
        if abs(val) < 1000:
            if i == 0:
                return '%5.3g%s' % (val, prefix[i])
            else:
                return '%4.3g%s' % (val, prefix[i])
        val /= 1000

    return '%6.3gPB' % val


def is_empty_hash(h):
    # TODO libtorrent should return None for empty hashes
    # FIXME avoid str. store hashes as bytes = use 2x less memory
    if h == None:
        return True
    if len(h) == 40:
        # v1
        return h == "0000000000000000000000000000000000000000"
    # v2
    return h == "0000000000000000000000000000000000000000000000000000000000000000"


def get_bt2_root_hash_of_path(file_path):
    """
    get bittorrent v2 merkle root hash of file path
    """
    # sha256 performance https://stackoverflow.com/questions/67355203/how-to-improve-the-speed-of-merkle-root-calculation
    nodes = []
    chunk_size = 16 * 1024

    with open(file_path, "rb") as f:
        # TODO better. this needs much memory for large files
        while chunk := f.read(chunk_size):
            leaf_node = hashlib.sha256(chunk).digest()
            nodes.append(leaf_node)

        # pad tree to binary tree
        # TODO better. use less memory
        empty_digest = b"\x00" * 32
        num_missing_nodes = 2**math.ceil(math.log2(len(nodes))) - len(nodes)
        nodes += [empty_digest] * num_missing_nodes

        while len(nodes) != 1:
            next_nodes = []
            for i in range(0, len(nodes), 2):
                node1 = nodes[i]
                # tree was padded to binary tree, so nodes[i+1] is always defined
                node2 = nodes[i+1]
                parent_node = hashlib.sha256(node1 + node2).digest()
                next_nodes.append(parent_node)
            nodes = next_nodes
        return nodes[0]


def create_relative_symlink(link_target, link_path, target_is_directory=False):
    # debug
    print(f"creating symlink from {repr(link_path)} to {repr(link_target)}")
    os.makedirs(os.path.dirname(link_path), exist_ok=True)
    link_target_relative = os.path.relpath(link_target, os.path.dirname(link_path))
    os.symlink(link_target_relative, link_path, target_is_directory)


def symlink_las_cas(file_las_path, file_cas_path_list):
    """
    create symlink from las to cas

    accept multiple cas paths to detect duplicate files

    if a file has same las path but different content (different cas paths)
    then rename the las path and return the new las path
    otherwise return None

    rename las paths by appending " (1)" or " (2)" or " (3)" etc
    before the file extension
    """
    # FIXME pass more cas paths to symlink_las_cas
    # FIXME check all paths in file_cas_path_list
    file_cas_path = file_cas_path_list[0]
    if os.path.islink(file_las_path):
        print("symlink exists: file_las_path:", file_las_path)
        # FIXME handle absolute symlinks
        # os.path.realpath should not resolve symlinks, only remove "x/../" path components
        # -> use os.path.abspath
        #link_target = os.path.realpath(os.path.join(os.path.dirname(file_las_path), os.readlink(file_las_path)))
        link_target = os.path.abspath(os.path.join(os.path.dirname(file_las_path), os.readlink(file_las_path)))
        if link_target == file_cas_path:
            return
        # FIXME handle other cases of "symlink exists to identical content"
        # check both bt1 and bt2 stores
        print("symlink exists: link_target:", link_target)
        raise Exception("FIXME handle existing files in las store")
        # FIXME check link_target. if its the same content, ignore
        # if the content is different, rename the symlink
        # by appending " (1)" or " (2)" or " (3)" etc before the file extension
        return new_file_las_path
    elif os.path.exists(file_las_path):
        print("file exists: file_las_path:", file_las_path)
        raise Exception("FIXME handle existing files in las store")
        return new_file_las_path
    # create symlink from las to bt2 store
    create_relative_symlink(file_cas_path, file_las_path)



def add_torrent(ses, filename, options):
    atp = lt.add_torrent_params()

    info_hash_v1 = None
    info_hash_v2 = None

    if filename.startswith('magnet:'):
        print("add_torrent: parsing magnet link:", filename)
        atp = lt.parse_magnet_uri(filename)
        # currently, atp.info_hashes works only for magnet links
        # TODO avoid str
        # TODO test v1-only and v2-only torrents
        info_hash_v1 = str(atp.info_hashes.v1)
        info_hash_v2 = str(atp.info_hashes.v2)
    else:
        print("add_torrent: parsing torrent file:", filename)
        # https://www.libtorrent.org/reference-Torrent_Info.html#torrent-info-1
        # libtorrent/bindings/python/src/torrent_info.cpp
        # .def("__init__", make_constructor(&file_constructor0))
        # FIXME all hashes are zero. is lt.torrent_info async?
        ti = lt.torrent_info(filename)
        # workaround: parse the torrent file in python to get the hashes
        # https://github.com/7sDream/torrent_parser # 140 stars, 2022
        # https://github.com/fuzeman/bencode.py # 40 stars, 2020
        # note: bt1 and bt2 info hashes are not in the torrent file
        # cat input.torrent | xxd -ps -c0 | grep 1234567890123456789012345678901234567890
        # the info hashes are derived from the contents of the torrent file
        # https://github.com/7sDream/torrent_parser/blob/master/tests/test_info_hash.py
        # torrent = parse_torrent_file(self.REAL_FILE, hash_raw=True)
        # info_bytes = encode(torrent["info"])
        # info_hash = binascii.hexlify(hashlib.sha1(info_bytes).digest()).decode()
        # # print(f"info_hash: {info_hash}")
        # self.assertEqual(info_hash, "f435d2324f313bad7ff941633320fe4d1c9c3079")
        # TODO does parse_torrent_file preserve the sort order of "info"?
        # https://stackoverflow.com/questions/19749085/calculating-the-info-hash-of-a-torrent-file
        #   Be observant that the example torrent file given by Arvid, both the root-dictionary and the info-dictionary is unsorted.
        #   According to the bencode specification a dictionary must be sorted.
        #   However the agreed convention when a info-dictionary for some reason is unsorted,
        #   is to hash the info-dictionary raw as it is (unsorted), as explained by Arvid above.
        # https://stackoverflow.com/questions/28348678/what-exactly-is-the-info-hash-in-a-torrent-file
        # hash_raw=True is needed for lossless parsing and encoding, to preserve the info hash
        torrent_data = torrent_parser.parse_torrent_file(filename, hash_raw=True)
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

    print("add_torrent: info_hash_v1", info_hash_v1)
    print("add_torrent: info_hash_v2", info_hash_v2)

    if False:
        # libtorrent.torrent_info
        print("add_torrent: atp.ti", atp.ti)
        # FIXME all hashes are zero. is lt.torrent_info async?
        print("add_torrent: atp.info_hash", atp.info_hash)
        print("add_torrent: atp.info_hashes", atp.info_hashes)
        print("add_torrent: atp.info_hashes.v1", atp.info_hashes.v1)
        print("add_torrent: atp.info_hashes.v2", atp.info_hashes.v2)
        time.sleep(1)
        # no, hashes are still zero
        print("add_torrent: atp.info_hashes.v1", atp.info_hashes.v1)
        print("add_torrent: atp.info_hashes.v2", atp.info_hashes.v2)

    #atp.save_path = options.save_path
    atp.storage_mode = lt.storage_mode_t.storage_mode_sparse
    atp.flags |= lt.torrent_flags.duplicate_is_error \
        | lt.torrent_flags.auto_managed \
        | lt.torrent_flags.duplicate_is_error

    if filename.startswith('magnet:'):
        print("add_torrent: fetching metadata of magnet link")
        # https://github.com/arvidn/libtorrent/issues/2239 # get metadata info without downloading the complete file
        # https://github.com/snowyu/libtorrent/issues/650 # Pause after downloading metadata
        atp.flag_auto_managed = False
        # https://gist.github.com/johncf/f1606e33562b51f67aa53ffdddf2183c
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
    #raise Exception("TODO set atp.save_path from info_hash_v2 or info_hash_v1")
    #print("add_torrent: setting save path ...")
    # for torrent files, this prefers the v2 hash to avoid collisions
    # for v1-only magnet links, this uses the v1 hash
    # FIXME magnet links: later with metadata, move files from bt1 to bt2 store
    store_path = get_store_path_from_hashes(info_hash_v1, info_hash_v2)
    atp.save_path = store_path
    print("add_torrent: save path:", atp.save_path)

    if not is_empty_hash(info_hash_v1) and not is_empty_hash(info_hash_v1):
        # v1 torrents: create symlink from bt1 to bt2 store
        # FIXME for magnet links, do this later with metadata
        store_path_v1 = get_store_path_from_hashes(info_hash_v1, None)
        # note: os.path.exists returns False on broken symlinks
        if not os.path.exists(store_path_v1) and not os.path.islink(store_path_v1):
            create_relative_symlink(store_path, store_path_v1)

    # use complete files from the bt2r store

    # populate the bt2r store from the sha256 store
    print("populating bt2r store from sha256 store")
    for sha256_root, _dirs, sha256_files in os.walk(os.path.join(store_prefix, "sha256")):
        for sha256_file in sha256_files:
            sha256_file_path = os.path.join(sha256_root, sha256_file)
            bt2_root_hash = get_bt2_root_hash_of_path(sha256_file_path).hex()
            #print("sha256 file:", sha256_file_path)
            bt2_root_file_path = get_file_store_path(bt2_root_hash, "bt2r")
            if os.path.exists(bt2_root_file_path):
                continue
            create_relative_symlink(sha256_file_path, bt2_root_file_path)

    #print("torrent_data:")
    # TypeError: Object of type bytes is not JSON serializable
    #print(json.dumps(torrent_data, indent=2))
    # ugly...
    #import pprint; pprint.pprint(torrent_data, indent=2, width=100, sort_dicts=False)
    #import pprint; pprint.pprint(torrent_data['info']['file tree'], indent=2, width=100, sort_dicts=False)
    #raise Exception("todo")

    #torrent_piece_length = torrent_data['info']['piece length']

    if 'file tree' in torrent_data['info']:
        def walk_file_tree(file_tree, entry_path=[]):
            for entry_name, entry in file_tree.items():
                if entry_name != "":
                    # branch node == directory
                    # recurse
                    walk_file_tree(entry, entry_path + [entry_name])
                    continue

                # leaf node == file

                #print("walk_file_tree: file:", os.path.join(*entry_path), entry['length'], entry['pieces root'].hex())

                file_path = os.path.join(store_path, *entry_path)

                if os.path.exists(file_path):
                    continue

                # search for existing file by bt2r hash

                # bt2r = bittorrent root hash
                file_bt2r_hash = entry['pieces root'].hex()
                file_bt2r_store_path = get_file_store_path(file_bt2r_hash, "bt2r")
                #print("walk_file_tree: file_bt2r_store_path", file_bt2r_store_path)

                if not os.path.exists(file_bt2r_store_path):
                    continue

                """
                    # not needed. now we can lookup files by bt2r hash

                    # FIXME try to find complete file by file size and file hash

                    file_size = entry['length']

                    # bash: find cas/sha256/ -size 6186712c

                    for sha256_root, _dirs, sha256_files in os.walk(os.path.join(store_prefix, "sha256")):
                        for sha256_file in sha256_files:
                            sha256_file_path = os.path.join(sha256_root, sha256_file)
                            print("sha256 file:", sha256_file_path)
                            if os.path.getsize(sha256_file_path) != file_size:
                                continue

                    raise Exception("todo")

                    continue
                """

                # get sha256 store path
                # this assumes that bt2r store files are always symlinked to sha256 store files
                file_sha256_store_path = os.path.normpath(os.path.join(os.path.dirname(file_bt2r_store_path), os.readlink(file_bt2r_store_path)))
                #print("walk_file_tree: file_sha256_store_path", file_sha256_store_path)

                # create symlink from torrent to sha256 store
                print(f"walk_file_tree: found complete file: creating symlink from {repr(file_path)} to {repr(file_sha256_store_path)}")
                os.makedirs(os.path.dirname(file_path), exist_ok=True)
                link_target = os.path.relpath(file_sha256_store_path, os.path.dirname(file_path))
                #print(f"walk_file_tree: symlink({repr(link_target)}, {repr(file_bt2r_store_path)}")
                os.symlink(link_target, file_path, target_is_directory=False)
                #create_relative_symlink(file_sha256_store_path, file_path)

        torrent_name = torrent_data['info']['name']

        # FIXME verify this for single file torrents
        walk_file_tree(torrent_data['info']['file tree'], [torrent_name])

    #raise Exception("todo")



    # FIXME create las (location-addressed store) and handle filepath collisions
    # chromium handles filepath collisions like "f.txt" and "f (1).txt" and "f (2).txt"
    # FIXME for magnet links, do this later with metadata
    # fix: 4.2.5 overwrites files if file names are the same
    # https://github.com/qbittorrent/qBittorrent/issues/12842
    # https://en.wikipedia.org/wiki/Content-addressable_storage
    # In the context of CAS, these traditional approaches are referred to as "location-addressed",
    # as each file is represented by a list of one or more locations, the path and filename, on the physical storage.

    if 'file tree' in torrent_data['info']:
        def walk_file_tree(file_tree, entry_path=[]):
            for entry_name, entry in file_tree.items():
                if entry_name != "":
                    # branch node == directory
                    # recurse
                    walk_file_tree(entry, entry_path + [entry_name])
                    continue

                # leaf node == file

                #print("walk_file_tree: file:", os.path.join(*entry_path), entry['length'], entry['pieces root'].hex())

                file_path = os.path.join(store_path, *entry_path)

                # note: entry_path[0] == torrent_name
                file_las_path = os.path.join(las_store_prefix, *entry_path)

                print("file_path:", file_path)
                print("file_las_path:", file_las_path)

                symlink_las_cas(file_las_path, [file_path])

                # search for existing file by bt2r hash

                # bt2r = bittorrent root hash
                file_bt2r_hash = entry['pieces root'].hex()
                file_bt2r_store_path = get_file_store_path(file_bt2r_hash, "bt2r")
                #print("walk_file_tree: file_bt2r_store_path", file_bt2r_store_path)

                if not os.path.exists(file_bt2r_store_path):
                    continue

                if os.path.islink(file_path):
                    # TODO? check if the symlink points to the sha256 store
                    continue

                """
                    # find file by size and bt2r hash
                    # not needed
                    # with populated bt2r store, we can lookup files by bt2r hash

                    # FIXME try to find complete file by file size and file hash

                    file_size = entry['length']

                    # bash: find cas/sha256/ -size 6186712c

                    for sha256_root, _dirs, sha256_files in os.walk(os.path.join(store_prefix, "sha256")):
                        for sha256_file in sha256_files:
                            sha256_file_path = os.path.join(sha256_root, sha256_file)
                            print("sha256 file:", sha256_file_path)
                            if os.path.getsize(sha256_file_path) != file_size:
                                continue

                    raise Exception("todo")

                    continue
                """

                # get sha256 store path
                # this assumes that bt2r store files are always symlinked to sha256 store files
                file_sha256_store_path = os.path.normpath(os.path.join(os.path.dirname(file_bt2r_store_path), os.readlink(file_bt2r_store_path)))
                #print("walk_file_tree: file_sha256_store_path", file_sha256_store_path)

                # create symlink from torrent to sha256 store
                print(f"walk_file_tree: found complete file: creating symlink from {repr(file_path)} to {repr(file_sha256_store_path)}")
                """
                os.makedirs(os.path.dirname(file_path), exist_ok=True)
                link_target = os.path.relpath(file_sha256_store_path, os.path.dirname(file_path))
                #print(f"walk_file_tree: symlink({repr(link_target)}, {repr(file_bt2r_store_path)}")
                os.symlink(link_target, file_path, target_is_directory=False)
                """
                create_relative_symlink(file_sha256_store_path, file_path)

        torrent_name = torrent_data['info']['name']

        # FIXME verify this for single file torrents
        walk_file_tree(torrent_data['info']['file tree'], [torrent_name])
    elif 'files' in torrent_data['info']:
        #print(torrent_data['info'])
        torrent_name = torrent_data['info']['name']
        # create one symlink per file, so we can merge directories
        for file_entry in torrent_data['info']['files']:
            # FIXME handle single file torrents
            file_path = os.path.join(store_path, torrent_name, *file_entry['path'])
            print("file_path:", file_path)
            file_las_path = os.path.join(las_store_prefix, torrent_name, *file_entry['path'])
            #file_size = file_entry['length']
            print("file_las_path:", file_las_path)
            symlink_las_cas(file_las_path, [file_path])
    else:
        # single file torrent
        #print(torrent_data['info'])
        torrent_name = torrent_data['info']['name']
        file_path = os.path.join(store_path, torrent_name)
        print("file_path:", file_path)
        file_las_path = os.path.join(las_store_prefix, torrent_name)
        print("file_las_path:", file_las_path)
        symlink_las_cas(file_las_path, [file_path])

    if not is_empty_hash(info_hash_v2):
        store_dirs_v2.add(info_hash_v2)
    else:
        store_dirs_v1.add(info_hash_v1)

    # TODO handle v2 and hybrid torrents: also create the v1 store path

    ses.async_add_torrent(atp)


# global state
# TODO better?
store_prefix = None
las_store_prefix = None
store_dirs_v1 = None
store_dirs_v2 = None
store_files_v2 = None

def get_store_path_from_hashes(info_hash_v1, info_hash_v2):
    global store_prefix
    global las_store_prefix
    use_v2 = not is_empty_hash(info_hash_v2)
    hashid = info_hash_v2 if use_v2 else info_hash_v1
    store_dir = "bt2" if use_v2 else "bt1"
    assert hashid != None
    shard_depth = 2
    shard_width = 2
    store_shard = casfs_util.shard(hashid, shard_depth, shard_width)
    #print("store shard:", repr(store_shard))
    # "".join(map(lambda n: str(n % 10), range(1, 65)))
    # cas/bt2/12/34/567890123456789012345678901234567890123456789012345678901234
    #print("get_store_path_from_hashes: store_prefix:", store_prefix)
    #print("get_store_path_from_hashes: store_dir:", store_dir)
    #print("get_store_path_from_hashes: hashid:", hashid)
    #print("get_store_path_from_hashes: join paths:", [store_prefix, store_dir, *store_shard])
    store_path = os.path.join(store_prefix, store_dir, *store_shard)
    return store_path


def get_file_store_path(file_hash, store_dir="sha256"):
    global store_prefix
    global las_store_prefix
    #print("file_hash", repr(file_hash))
    assert len(file_hash) == 64
    assert not is_empty_hash(file_hash)
    shard_depth = 2
    shard_width = 2
    store_shard = casfs_util.shard(file_hash, shard_depth, shard_width)
    # "".join(map(lambda n: str(n % 10), range(1, 65)))
    # cas/sha256/12/34/567890123456789012345678901234567890123456789012345678901234
    store_path = os.path.join(store_prefix, store_dir, *store_shard)
    return store_path


# https://stackoverflow.com/questions/1131220/get-the-md5-hash-of-big-files-in-python
def get_sha256_of_path(file_path, chunk_size=8192):
    hash = hashlib.sha256()
    with open(file_path, "rb") as f:
        while chunk := f.read(chunk_size):
            hash.update(chunk)
    return hash.digest()


class WatchdogHandler(watchdog.events.FileSystemEventHandler):
    def __init__(self, handle_new_file):
        self.handle_new_file = handle_new_file
        super().__init__()
    def dispatch(self, event):
        print("WatchdogHandler dispatch event", event)
        events = (
            watchdog.events.FileCreatedEvent,
            watchdog.events.FileModifiedEvent,
        )
        if isinstance(event, events):
            self.handle_new_file(event.src_path)


def main():

    # global state
    global store_prefix
    global las_store_prefix
    global store_dirs_v1
    global store_dirs_v2
    global store_files_v2



    import argparse

    parser = argparse.ArgumentParser(
        description='bittorrent client for annas-archive'
    )

    parser.add_argument(
        '-p', '--port', type=int, default=6881,
        help='set listening port'
    )

    parser.add_argument(
        '-i', '--listen-interface', type=str, default='0.0.0.0',
        help='set interface for incoming connections'
    )

    parser.add_argument(
        '-o', '--outgoing-interface', type=str, default='',
        help='set interface for outgoing connections'
    )

    parser.add_argument(
        '-d', '--max-download-rate', type=float, default=0,
        help='the maximum download rate given in kB/s. 0 means infinite.'
    )

    parser.add_argument(
        '-u', '--max-upload-rate', type=float, default=0,
        help='the maximum upload rate given in kB/s. 0 means infinite.'
    )

    parser.add_argument(
        '-s', '--save-path', type=str, default='.',
        help='the path where the downloaded file/folder should be placed'
    )

    parser.add_argument(
        '-r', '--proxy-host', type=str, default='',
        help='sets HTTP proxy host and port (separated by ":")'
    )

    parser.add_argument(
        '--requests-watch-dir', type=str,
        action="append",
        dest='requests_watch_dirs',
        help='directory to watch for request.txt files'
    )

    parser.add_argument(
        'torrent_file',
        nargs='*',
    )

    options = parser.parse_args()

    if options.port < 0 or options.port > 65525:
        options.port = 6881

    options.max_upload_rate *= 1000
    options.max_download_rate *= 1000

    if options.max_upload_rate <= 0:
        options.max_upload_rate = -1
    if options.max_download_rate <= 0:
        options.max_download_rate = -1

    settings = {
        'user_agent': f'annas_archive_torrent_client/{annas_archive_torrent_client_version} libtorrent/{lt.__version__}',
        'listen_interfaces': '%s:%d' % (options.listen_interface, options.port),
        'download_rate_limit': int(options.max_download_rate),
        'upload_rate_limit': int(options.max_upload_rate),
        # By default, only errors are reported. settings_pack::alert_mask can be used to specify which kinds of events should be reported. The alert mask is a combination of the alert_category_t flags in the alert class.
        'alert_mask': lt.alert.category_t.all_categories,
        'outgoing_interfaces': options.outgoing_interface,
    }

    if options.proxy_host != '':
        settings['proxy_hostname'] = options.proxy_host.split(':')[0]
        settings['proxy_type'] = lt.proxy_type_t.http
        settings['proxy_port'] = options.proxy_host.split(':')[1]

    ses = lt.session(settings)

    # map torrent_handle to torrent_status
    torrents = {}
    alerts_log = []

    # init global state
    store_prefix = os.path.join(os.getcwd(), "cas")
    print("main: store_prefix:", store_prefix)
    las_store_prefix = os.path.join(os.getcwd(), "las")
    print("main: las_store_prefix:", las_store_prefix)
    store_dirs_v1 = set()
    store_dirs_v2 = set()
    store_files_v2 = set()

    for f in (options.torrent_file or []):
        add_torrent(ses, f, options)

    child_threads = []

    sys.path.append(os.path.dirname(__file__) + "/annas-py")
    import annas_py

    annas_torrents_json_url = "https://annas-archive.org/dyn/torrents.json"

    annas_torrents_json_path = os.environ["HOME"] + "/.cache/annas-archive/torrents.json"

    cache_dt_max = 60*60*24*10 # 10 days

    import requests
    import json
    import re

    def needs_update(path):
        if not os.path.exists(path):
            return True
        t1 = os.path.getmtime(path)
        t2 = time.time()
        dt = t2 - t1
        return dt > cache_dt_max

    requests_session = requests.Session()

    if needs_update(annas_torrents_json_path):
        print("writing", annas_torrents_json_path)
        response = requests_session.get(annas_torrents_json_url)
        os.makedirs(os.path.dirname(annas_torrents_json_path), exist_ok=True)
        with open(annas_torrents_json_path, "wb") as f:
            f.write(response.content)

    # download non-metadata torrent files
    print("downloading torrent files ...")
    with open(annas_torrents_json_path) as f:
        annas_torrents = json.load(f)

    for torrent in annas_torrents:

        if torrent["is_metadata"]:
            continue

        if torrent["obsolete"]:
            continue

        # ignore comic books
        if torrent["group_name"] == "libgen_li_comics":
            continue

        # ignore fiction books
        if torrent["group_name"] in ("libgen_li_fic", "libgen_rs_fic"):
            continue

        # ignore magazines
        if torrent["group_name"] == "libgen_li_magazines":
            continue

        # ignore science papers
        if torrent["group_name"] == "scihub":
            continue

        # ignore metadata with torrent["is_metadata"] == False
        if torrent["group_name"] == "aa_derived_mirror_metadata":
            continue

        old_torrent_path = None

        """
        # migrate
        old_torrent_path = os.path.join(
            os.environ["HOME"],
            ".cache/annas-archive/torrents",
            torrent["top_level_group_name"],
            torrent["group_name"],
            torrent["display_name"],
        )

        # migrate
        old_torrent_path = os.path.join(
            os.environ["HOME"],
            ".cache/annas-archive/torrents",
            re.sub("^https?://", "", torrent["url"]),
            #torrent["top_level_group_name"],
            #torrent["group_name"],
            #torrent["display_name"],
        )
        """

        torrent_path = os.path.join(
            os.environ["HOME"],
            ".cache/annas-archive/torrents",
            re.sub("^annas-archive.org/dyn/small_file/torrents/", "",
                re.sub("^https?://", "", torrent["url"])
            ),
            #torrent["top_level_group_name"],
            #torrent["group_name"],
            #torrent["display_name"],
        )

        expected_size = torrent["torrent_size"]

        if old_torrent_path:
            # migrate
            if os.path.exists(old_torrent_path):
                os.makedirs(os.path.dirname(torrent_path), exist_ok=True)
                os.rename(old_torrent_path, torrent_path)

        if os.path.exists(torrent_path):
            actual_size = os.path.getsize(torrent_path)
            if actual_size != expected_size:
                # file exists but has wrong size
                os.unlink(torrent_path)

        if os.path.exists(torrent_path):
            continue

        print("writing", torrent_path)

        response = requests_session.get(torrent["url"])
        os.makedirs(os.path.dirname(torrent_path), exist_ok=True)
        with open(torrent_path, "wb") as f:
            f.write(response.content)

        actual_size = os.path.getsize(torrent_path)

        if actual_size != expected_size:
            print(f"FIXME size mismatch of torrent file {torrent_path!r}: {actual_size} != {expected_size}")
            os.rename(torrent_path, torrent_path + ".broken")

    print("downloading torrent files done")

    def handle_new_file(path):
        print("new file", path)
        name = os.path.basename(path)
        query = None
        args = dict(
            #query: str,
            #language: Language = Language.ANY,
            #file_type: FileType = FileType.ANY,
            #order_by: OrderBy = OrderBy.MOST_RELEVANT,
        )

        if name == "request.txt":
            with open(path) as f:
                query = f.read()

        #elif name in ("request.yml", "request.yaml"):
        # TODO parse yaml file

        if query is None:
            return

        results = annas_py.search(query, **args)

        """
        return SearchResult(
            id=id,
            title=html_unescape(title),
            authors=html_unescape(authors),
            file_info=file_info,
            thumbnail=thumbnail,
            publisher=html_unescape(publisher) if publisher else None,
            publish_date=publish_date,
        )
        """

        for r in results:
            print("result", dict(
                md5=r.id,
                title=r.title,
                authors=r.authors,
                extension=r.file_info.extension,
                size=r.file_info.size,
                language=r.file_info.language,
                library=r.file_info.library,
                date=r.publish_date,
            ))

    # watch for new files
    for watchdir in (options.requests_watch_dirs or []):
        os.makedirs(watchdir, exist_ok=True)
        event_handler = WatchdogHandler(handle_new_file)
        watchdog_thread = watchdog.observers.Observer()
        watchdog_thread.schedule(event_handler, watchdir, recursive=True)
        watchdog_thread.start()
        child_threads.append(watchdog_thread)

    done_connect_peer = False

    # todo? stop child threads on exit
    # https://stackoverflow.com/questions/1635080/terminate-a-multi-thread-python-program
    """
    try:
        while True:
            time.sleep(1)
    finally:
        for thread in child_threads:
            thread.stop()
            thread.join()
    """

    alive = True
    while alive:

        out = ''

        for h, t in torrents.items():

            # https://www.libtorrent.org/reference-Torrent_Handle.html
            # libtorrent/bindings/python/src/torrent_status.cpp

            if done_connect_peer == False:
                # debug: add localhost peer
                debug_extra_peer = ("127.0.0.1", 6881)
                print("manually connecting to peer:", debug_extra_peer)
                h.connect_peer(debug_extra_peer)
                done_connect_peer = True

            t_status = t

            #print("torrent status", t.status)
            print("torrent status.paused", t_status.paused)
            print("torrent status.state", t_status.state)

            print("torrent save_path", t.save_path)

            torrent_info = h.get_torrent_info()
            file_storage = torrent_info.files()

            if False:
                print("torrent files", file_storage)
                print("torrent files count", file_storage.num_files())
                print("torrent files v2", file_storage.v2()) # True or False

            v2_torrent_started = False
            has_info_hash_v2 = False
            v1_store_path = None
            v2_store_path = None
            store_path = None

            if t.has_metadata:
                #print("torrent has metadata", t)
                # libtorrent/bindings/python/src/info_hash.cpp
                #print("t.info_hashes", t.info_hashes)
                #print("h.info_hashes()", h.info_hashes())
                #if t.info_hashes.has_v2: # always true
                # TODO avoid str()

                info_hash_v2 = str(t.info_hashes.v2)

                if is_empty_hash(info_hash_v2):
                    # v1-only torrent: get v2 hash of info dict
                    torrent_info = h.get_torrent_info()
                    torrent_metadata = torrent_info.metadata()
                    info_hash_v2 = hashlib.sha256(torrent_metadata).hexdigest()

                #if not is_empty_hash(str(t.info_hashes.v2)):
                # always use v2 hash
                if True:
                    has_info_hash_v2 = True
                    #print("t.info_hashes has v2")
                    #print("t.info_hashes.v2", t.info_hashes.v2)
                    print("info_hash_v2", info_hash_v2)

                    hashid = info_hash_v2
                    # "".join(map(lambda n: str(n % 10), range(1, 65)))
                    # cas/bt2/12/34/567890123456789012345678901234567890123456789012345678901234
                    v2_store_path = get_store_path_from_hashes(None, hashid)
                    print("v2 store path:", v2_store_path)
                    store_path = v2_store_path

                    # TODO avoid str()
                    #if not str(t.info_hashes.v2) in store_dirs_v2:
                    if not info_hash_v2 in store_dirs_v2:
                        # set download path and start torrent
                        # TODO dont makedirs if torrent is a single file
                        # not needed?
                        #os.makedirs(store_path, exist_ok=True)

                        # set new save_path
                        old_save_path = t.save_path
                        print("torrent old save_path", t.save_path)
                        print("torrent new save_path", v2_store_path)
                        #t.save_path = store_path # no, read only
                        #t.move_storage(store_path) # missing
                        h.move_storage(v2_store_path)

                        # start download
                        h.resume()
                        v2_torrent_started = True

                        # TODO avoid str()
                        #store_dirs_v2.add(str(t.info_hashes.v2))
                        store_dirs_v2.add(info_hash_v2)

                        # TODO loop files: create symlinks to sha256 files store: cas/sha256/xx/xx/xxxxx...
                        # TODO how to handle empty and temporary files?
                        # create them like xxxxxx.temp? rather no...

                #if t.info_hashes.has_v1: # always true
                # TODO avoid str()
                #if not is_empty_hash(str(t.info_hashes.v1)):
                if False:
                    #print("t.info_hashes has v1")
                    print("t.info_hashes.v1", t.info_hashes.v1)

                    hashid = str(t.info_hashes.v1)
                    # "".join(map(lambda n: str(n % 10), range(1, 41)))
                    # cas/bt1/12/34/567890123456789012345678901234567890
                    v1_store_path = get_store_path_from_hashes(hashid, None)
                    print("v1 store path:", v1_store_path)

                    # prefer v2_store_path
                    if v2_store_path == None:
                        store_path = v1_store_path

                    # TODO avoid str()
                    if not str(t.info_hashes.v1) in store_dirs_v1:
                        # set download path and start torrent
                        # TODO dont makedirs if torrent is a single file
                        # not needed?
                        #os.makedirs(store_path, exist_ok=True)
                        # TODO avoid str()
                        store_dirs_v1.add(str(t.info_hashes.v1))
                        #if not v2_torrent_started:

                        if not has_info_hash_v2:
                            # set new save_path
                            old_save_path = t.save_path
                            print("torrent old save_path", t.save_path)
                            print("torrent new save_path", v1_store_path)
                            #t.save_path = store_path # no, read only
                            #t.move_storage(store_path) # missing
                            h.move_storage(v1_store_path)
                            #raise Exception("todo")
                            # no such file, tempdir was removed by h.move_storage
                            #if old_save_path.startswith("/tmp/cas-torrent-temp-save-path-"):
                            #    print("removing old_save_path", old_save_path)
                            #    #shutil.rmtree(old_save_path, ignore_errors=True)
                            #    shutil.rmtree(old_save_path)
                        else:
                            # v1 and v2 torrent = "hybrid" torrent
                            # TODO link files between stores: bt1, bt2, sha256
                            # note: os.path.exists "Returns False for broken symbolic links"
                            if not os.path.exists(v1_store_path) and not os.path.islink(v1_store_path):
                                print(f"creating symlink from {v1_store_path} to {v2_store_path}")
                                os.makedirs(os.path.dirname(v1_store_path), exist_ok=True)
                                link_target = os.path.relpath(v2_store_path, os.path.dirname(v1_store_path))
                                print(f"symlink({repr(link_target)}, {repr(v1_store_path)}")
                                os.symlink(link_target, v1_store_path, target_is_directory=True)
                                #create_relative_symlink(file_store_path, file_path)

                        if not v2_torrent_started:
                            # start download
                            h.resume()

                if file_storage.v2():
                    # we have all bt2r files hashes
                    # for complete files, create symlinks to sha256 file store
                    for file_idx in range(file_storage.num_files()):
                        file_flags = file_storage.file_flags(file_idx)
                        if file_flags & 1 == 1:
                            # pad file
                            continue
                        file_path = os.path.join(store_path, file_storage.file_path(file_idx))
                        file_bt2r_hash = str(file_storage.root(file_idx))
                        file_bt2r_store_path = get_file_store_path(file_bt2r_hash, "bt2r")
                        if False:
                            print(f"file {file_idx} path:", file_path)
                            print(f"file {file_idx} root:", file_storage.root(file_idx))
                            print(f"file {file_idx} size:", file_storage.file_size(file_idx))
                            #print(f"file {file_idx} flags:", file_flags)
                            #print(f"file {file_idx} hash:", file_storage.hash(file_idx))

                        if os.path.exists(file_bt2r_store_path) and not os.path.exists(file_path):
                            # FIXME readlink file_bt2r_store_path to create symlink to sha256 store
                            create_relative_symlink(file_bt2r_store_path, file_path)


            out += 'name: %-40s\n' % t.name[:40]

            if t.state != lt.torrent_status.seeding:
                state_str = ['queued', 'checking', 'downloading metadata',
                             'downloading', 'finished', 'seeding',
                             '', 'checking fastresume']
                out += state_str[t.state] + ' '

                out += 'total downloaded: %d Bytes\n' % t.total_done
                out += 'peers: %d seeds: %d distributed copies: %d\n' % \
                    (t.num_peers, t.num_seeds, t.distributed_copies)
                out += '\n'

            out += 'download: %s/s (%s) ' \
                % (add_suffix(t.download_rate), add_suffix(t.total_download))

            out += 'upload: %s/s (%s) ' \
                % (add_suffix(t.upload_rate), add_suffix(t.total_upload))

            if t.state != lt.torrent_status.seeding:
                out += 'info-hash: %s\n' % t.info_hashes
                out += 'next announce: %s\n' % t.next_announce
                out += 'tracker: %s\n' % t.current_tracker

            print(out, end="")

        alerts = ses.pop_alerts()

        for a in alerts:

            #alerts_log.append(a.message())

            # add new torrents to our list of torrent_status
            if isinstance(a, lt.add_torrent_alert):
                # https://www.libtorrent.org/reference-Torrent_Handle.html
                h = a.handle
                h.set_max_connections(60)
                h.set_max_uploads(-1)
                torrents[h] = h.status()

            if isinstance(a, lt.metadata_received_alert):
                # https://www.libtorrent.org/reference-Torrent_Handle.html
                h = a.handle
                # TODO write .torrent file to
                # cas/bt1/12/34/567890123456789012345678901234567890.torrent
                # and/or
                # cas/bt2/12/34/567890123456789012345678901234567890123456789012345678901234.torrent
                # for hybrid torrents, create bt2 torrent file and symlink from bt1 to bt2 torrent file
                # TODO start download
                # This alert is generated when the metadata has been completely received and the torrent can start downloading. It is not generated on torrents that are started with metadata, but only those that needs to download it from peers (when utilizing the libtorrent extension).
                # https://www.libtorrent.org/reference-Alerts.html#metadata_received_alert

            if (
              isinstance(a, lt.file_completed_alert) or
              isinstance(a, lt.torrent_finished_alert)
            ):
                h = a.handle

                # get store_path
                # this is wrong when add_torrent sets save_path in bt2 store
                """
                store_path = None
                if not is_empty_hash(str(h.info_hashes().v2)):
                    hashid = str(h.info_hashes().v2)
                    store_path = get_store_path_from_hashes(None, hashid)
                elif not is_empty_hash(str(h.info_hashes().v1)):
                    hashid = str(h.info_hashes().v1)
                    store_path = get_store_path_from_hashes(hashid, None)
                """
                store_path = h.save_path()
                print("store_path:", store_path)

                # get file_storage
                torrent_info = h.get_torrent_info()
                file_storage = torrent_info.files()

                file_idx_list = None

                if isinstance(a, lt.file_completed_alert):
                    # one file
                    file_idx = a.index
                    file_idx_list = [file_idx]
                else:
                    # multiple files
                    print("torrent finished. moving all files to the sha256 files store")
                    file_idx_list = range(file_storage.num_files())

                for file_idx in file_idx_list:

                    file_flags = file_storage.file_flags(file_idx)

                    # skip pad files
                    if file_flags & 1 == 1:
                        continue

                    print("file completed: id:", file_idx)
                    print("file completed: handle:", h)

                    file_path = os.path.join(store_path, file_storage.file_path(file_idx))
                    print("file completed: path:", file_path)
                    # https://www.libtorrent.org/reference-Alerts.html#file-completed-alert
                    # TODO move file to the sha256 files store
                    # then create symlinks to other stores
                    # os.symlink(
                    #create_relative_symlink(file_store_path, file_path)

                    print(f"file completed: making file read-only: {repr(file_path)}")
                    os.chmod(file_path, 0o444)

                    # verify file size
                    print(f"file completed: checking file size")
                    file_size_actual = os.path.getsize(file_path)
                    file_size = file_storage.file_size(file_idx)
                    # TODO better
                    assert file_size_actual == file_size

                    if os.path.islink(file_path):
                        # keep all symlinks
                        # move only regular files to the sha256 store
                        continue

                    # move file
                    file_sha256 = get_sha256_of_path(file_path).hex()
                    file_sha256_store_path = get_file_store_path(file_sha256)

                    # FIXME handle truncated SHA-256 hashes https://blog.libtorrent.org/2020/09/bittorrent-v2/

                    #print(f"file {file_idx} hash:", file_storage.hash(file_idx))

                    if os.path.exists(file_sha256_store_path):
                        # file exists in sha256 store
                        # delete duplicate file in torrent store
                        print("file completed: file exists in sha256 store:", file_sha256_store_path)
                        os.unlink(file_path)
                    else:
                        # move file from torrent to store
                        print(f"file completed: moving file from {repr(file_path)} to {repr(file_sha256_store_path)}")
                        os.makedirs(os.path.dirname(file_sha256_store_path), exist_ok=True)
                        os.rename(file_path, file_sha256_store_path)

                    # TODO better
                    assert os.path.exists(file_sha256_store_path) == True
                    assert os.path.exists(file_path) == False

                    # create symlink from torrent to sha256 file store
                    create_relative_symlink(file_sha256_store_path, file_path)

                    # create symlink from root hash to sha256 file store
                    # FIXME handle v1-only torrents
                    # TODO verify root hash
                    # note: file_bt2r_hash != file_sha256
                    file_bt2r_hash = str(file_storage.root(file_idx))
                    # TODO better check for v2 torrents
                    if not is_empty_hash(file_bt2r_hash):
                        file_bt2r_store_path = get_file_store_path(file_bt2r_hash, "bt2r")
                        create_relative_symlink(file_store_path, file_bt2r_store_path)



            # TODO file_progress_alert -> a.files

            # update our torrent_status array for torrents that have
            # changed some of their state
            if isinstance(a, lt.state_update_alert):
                for s in a.status:
                    torrents[s.handle] = s

            #if len(alerts_log) > 20:
            #    alerts_log = alerts_log[-20:]

            #for a in alerts_log:

            # ignore some alerts

            m = a.message()

            # filter alerts by type
            if isinstance(a, lt.log_alert) and (m.startswith("<== LSD: ") or m.startswith("==> LSD: ")):
                continue
            if isinstance(a, lt.torrent_log_alert):
                continue
            if isinstance(a, lt.stats_alert):
                continue
            if isinstance(a, lt.tracker_error_alert):
                continue
            if isinstance(a, lt.tracker_announce_alert):
                continue
            if isinstance(a, lt.dht_pkt_alert):
                continue
            if isinstance(a, lt.dht_reply_alert):
                continue
            if isinstance(a, lt.dht_outgoing_get_peers_alert):
                continue
            if isinstance(a, lt.peer_log_alert):
                continue
            if isinstance(a, lt.dht_log_alert):
                continue
            if isinstance(a, lt.portmap_log_alert):
                continue
            if isinstance(a, lt.block_finished_alert): # TODO keep? share blocks...
                continue
            if isinstance(a, lt.piece_finished_alert): # TODO keep? share blocks...
                continue
            if isinstance(a, lt.block_downloading_alert):
                continue
            if isinstance(a, lt.picker_log_alert):
                continue
            """
            if isinstance(a, lt.):
                continue
            """

            # filter alerts by message
            #if "finished downloading" in m:
            #    continue
            #if "m_checking_piece" in m:
            #    continue

            print(type(a).__name__ + ': ' + m)

        print("-" * 80)

        time.sleep(5)
        #c = console.sleep_and_input(0.5)
        c = None

        ses.post_torrent_updates()
        if not c:
            continue

        if c == 'r':
            for h in torrents:
                h.force_reannounce()
        elif c == 'q':
            alive = False
        elif c == 'p':
            for h in torrents:
                h.pause()
        elif c == 'u':
            for h in torrents:
                h.resume()

    ses.pause()
    for h, t in torrents.items():
        if not h.is_valid() or not t.has_metadata:
            continue
        h.save_resume_data()

    while len(torrents) > 0:
        alerts = ses.pop_alerts()
        for a in alerts:
            if isinstance(a, lt.save_resume_data_alert):
                print(a)
                data = lt.write_resume_data_buf(a.params)
                h = a.handle
                # https://www.libtorrent.org/reference-Torrent_Handle.html
                if h in torrents:
                    open(os.path.join(options.save_path, torrents[h].name + '.fastresume'), 'wb').write(data)
                    del torrents[h]

            if isinstance(a, lt.save_resume_data_failed_alert):
                # https://www.libtorrent.org/reference-Torrent_Handle.html
                h = a.handle
                if h in torrents:
                    print('failed to save resume data for ', torrents[h].name)
                    del torrents[h]
        time.sleep(0.5)


main()
