{ pkgs ? import <nixpkgs> { } }:

let
  #rtorrent-rpc = pkgs.python3.pkgs.callPackage ./nix/rtorrent-rpc {
  #  bencode2 = pkgs.python3.pkgs.callPackage ./nix/bencode2 { };
  #};
in

pkgs.mkShell {
  buildInputs = with pkgs; [
    btfs
    (python3.withPackages (pp: with pp; [
      #qbittorrent-api
      #rtorrent-rpc
      libtorrent-rasterbar
      watchdog
      # annas-py
      beautifulsoup4
      # annas_py/utils.py
      # soup = BeautifulSoup(html, "lxml")
      lxml
      requests
    ]))
  ];
}
