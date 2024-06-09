#!/usr/bin/env python3

# move all finished torrents to ~/cas/btih/{btih}

# if the destination exists, qbittorrent checks the existing files
# uses the new location, but keeps the old files
#
# todo: delete old files if they are part of the torrent
# and if the file is complete in the new location
# dont delete extra files added by the user



# https://github.com/rmartin16/qbittorrent-api
# https://github.com/qbittorrent/qBittorrent/wiki/WebUI-API-(qBittorrent-4.1)
# https://qbittorrent-api.readthedocs.io/en/latest/
# https://github.com/qbittorrent/qBittorrent/wiki/WebUI-API-(qBittorrent-4.1)#set-torrent-location
# https://qbittorrent-api.readthedocs.io/en/latest/apidoc/torrents.html#qbittorrentapi.torrents.TorrentDictionary.set_location
# https://github.com/rmartin16/qbittorrent-api/raw/main/src/qbittorrentapi/torrents.py

import os
import sys
import time
import qbittorrentapi



# config
home_dir = os.environ["HOME"]
src_dir = home_dir + "/qbittorrent/data"
dst_dir = home_dir + "/cas"
conn_info = dict(
    host="localhost",
    # grep '^WebUI\\Port' qBittorrent.conf
    port=9001,
    username="user",
    password="pass",
)



with qbittorrentapi.Client(**conn_info) as qbt_client:

    #if qbt_client.torrents_add(urls="...") != "Ok.":
    #    raise Exception("Failed to add torrent.")

    # display qBittorrent info
    if False:
        print(f"qBittorrent: {qbt_client.app.version}")
        print(f"qBittorrent Web API: {qbt_client.app.web_api_version}")
        for k, v in qbt_client.app.build_info.items():
            print(f"{k}: {v}")

    # state
    finished_states = (
        "uploading", # Torrent is being seeded and data is being transferred
        "pausedUP",  # Torrent is paused and has finished downloading
        "queuedUP",  # Queuing is enabled and torrent is queued for upload
        "stalledUP", # Torrent is being seeded, but no connection were made
        "forcedUP",  # Torrent is forced to uploading and ignore queue limit
    )

    for torrent in qbt_client.torrents_info():

        #print(f"torrent {torrent.hash} {torrent.name} {torrent.state} {torrent.content_path}")

        # TODO remove? move all torrents
        if not torrent.state in finished_states:
            continue

        # usually, save_path is the parent directory of content_path
        src = torrent.save_path
        src2 = torrent.content_path

        if src.startswith(dst_dir + "/"):
            continue

        if not src.startswith(src_dir):
            print("unexpected src", src)
            print("src_dir", src_dir)
            continue

        print(f"torrent {torrent.hash} {torrent.name} {torrent.state} {torrent.content_path}")

        btih = torrent.info.hash
        dst = dst_dir + "/btih/" + btih.lower()
        dst2 = dst_dir + "/btih/" + btih.lower() + src2[len(src_dir):]

        wait_for_check = False

        if os.path.exists(dst):
            print("note: dst exists. qbittorrent will check files.", dst)
            wait_for_check = True

        os.makedirs(os.path.dirname(dst), exist_ok=True)

        # move torrent files
        # https://github.com/rmartin16/qbittorrent-api/raw/main/src/qbittorrentapi/torrents.py
        # def set_location
        print("src", src)
        print("src2", src2)
        print("dst", dst)
        print("dst2", dst2)
        #print("dst_parent", dst_parent)
        #torrent.set_location(dst_parent)
        torrent.set_location(dst)

        checking_states = (
            "checkingUP", # Torrent has finished downloading and is being checked
            "checkingDL", # Same as checkingUP, but torrent has NOT finished downloading
            "checkingResumeData", # Checking resume data on qBt startup
        )

        # only one moving state:
        # moving  Torrent is moving to another location

        def get_state():
            # TODO better. get state of one torrent
            for torrent2 in qbt_client.torrents_info():
                if torrent2.info.hash != torrent.info.hash:
                    continue
                return torrent2.state

        # TODO refactor checking and moving

        if get_state() in checking_states:
            print("waiting: qbittorrent is checking files ", end="")
            sys.stdout.flush()
            time.sleep(2)
            # todo timeout
            while get_state() in checking_states:
                print(".", end="")
                sys.stdout.flush()
                time.sleep(2)
            print(" ok")

        if get_state() == "moving":
            print("waiting: qbittorrent is moving files ", end="")
            sys.stdout.flush()
            time.sleep(2)
            # todo timeout
            while get_state() == "moving":
                print(".", end="")
                sys.stdout.flush()
                time.sleep(2)
            print(" ok")
