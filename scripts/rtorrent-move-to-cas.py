#!/usr/bin/env python3

# move all torrents to ~/cas/btih/{btih}

# https://pypi.org/project/rtorrent-rpc/
# https://rtorrent-rpc.readthedocs.io/
# https://rtorrent-docs.readthedocs.io/en/latest/cmd-ref.html



import os
import sys
import subprocess
import dataclasses

from rtorrent_rpc import RTorrent
from rtorrent_rpc.helper import parse_comment, parse_tags



# config
home_dir = os.environ["HOME"]
rtorrent_socket = home_dir + "/rtorrent/.socket"
src_dir = home_dir + "/rtorrent/data"
dst_dir = home_dir + "/cas"



r = RTorrent(address='scgi://' + rtorrent_socket)

if 0:
    print(r.system_list_methods())
    print(r.rpc.system.listMethods())
    raise 123

# only if your rtorrent support jsonrpc!
#print(r.jsonrpc.call("system.listMethods"))

@dataclasses.dataclass
class Torrent:
    name: str
    info_hash: str
    directory_base: str
    tags: set[str]
    comment: str
    is_open: bool
    is_private: bool
    is_complete: bool
    is_hashing: bool
    state: int

    size_bytes: int


def get_torrents() -> dict[str, Torrent]:
    return {
        x[1]: Torrent(
            name=x[0],
            info_hash=x[1],
            directory_base=x[2],
            tags=parse_tags(x[3]),
            comment=parse_comment(x[4]),
            is_open=x[5],
            size_bytes=x[6],
            is_private=x[7],
            state=x[8],
            is_complete=x[9],
            is_hashing=x[10],
        )
        for x in r.d.multicall2(
            "",  # required by rpc, doesn't know why
            "default",
            "d.name=",
            "d.hash=",
            "d.directory_base=",
            "d.custom1=",
            "d.custom2=",
            "d.is_open=",
            "d.size_bytes=",
            "d.is_private=",
            "d.state=",
            "d.complete=",
            "d.hashing=",
        )
    }

@dataclasses.dataclass(kw_only=False)
class File:
    name: str
    size: int


def get_files(info_hash: str) -> list[File]:
    """use json rpc incase there are emoji in filename"""

    files = r.f.multicall(info_hash, "", "f.path=", "f.size_bytes=")

    return [File(name=f[0], size=f[1]) for f in files]


@dataclasses.dataclass
class Tracker:
    info_hash: str
    index: int
    enabled: bool
    url: str


def get_trackers(info_hash: str) -> list[Tracker]:
    return [
        Tracker(
            info_hash=info_hash,
            index=i,
            enabled=x[0],
            url=x[1],
        )
        for i, x in enumerate(r.t.multicall(info_hash, "", "t.is_enabled=", "t.url="))
    ]



def main():

    for btih, torrent in get_torrents().items():
        src = torrent.directory_base
        print("torrent", torrent.info_hash, torrent.name, src)
        if src.startswith(dst_dir + "/"):
            continue
        # note: can be equal: src == src_dir
        if not src.startswith(src_dir):
            print("unexpected src", src)
            continue
        dst2 = r.d.directory_base(btih)
        dst3 = r.d.directory(btih)
        assert dst2 == dst3, f"btih {btih}: dst2 != dst3: {dst2} != {dst3}"

        # must stop before move
        # https://github.com/rakshasa/rtorrent/issues/1203
        print("stopping", btih)
        r.d.stop(btih)
        r.d.close(btih)

        # copy by hardlink
        dst = dst_dir + "/btih/" + btih.lower() + src[len(src_dir):]
        if not os.path.exists(dst):
            os.makedirs(os.path.dirname(dst), exist_ok=True)
            args = ["cp", "-r", "-l", src, dst]
            print(args)
            subprocess.run(args)
        else:
            print("dst exists", dst)
        r.d.directory_base.set(btih, dst)
        #r.d.directory.set(btih, dst) # ?
        # verify
        # TODO rtorrent: whats the difference between d.directory_base and d.directory
        dst2 = r.d.directory_base(btih)
        dst3 = r.d.directory(btih)
        print("src", src)
        print("dst", dst)
        print("dst2", dst2)
        print("dst3", dst3)
        assert src != dst2, f"btih {btih}: src == dst2: {src} == {dst2}"
        assert src != dst3, f"btih {btih}: src == dst3: {src} == {dst3}"

        # delete duplicate files
        args = ["find", src, "-type", "f", "-links", "+1", "-delete"]
        print(args); subprocess.run(args)

        # delete empty directories
        args = ["find", src, "-depth", "-type", "d", "-delete"]
        print(args); subprocess.run(args)

        print("starting", btih)
        r.d.start(btih)

        #break # debug



if __name__ == "__main__":

    main()
