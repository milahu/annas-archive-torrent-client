{ pkgs ? import <nixpkgs> { } }:

let
  rtorrent-rpc = pkgs.python3.pkgs.callPackage ./nix/rtorrent-rpc {
    bencode2 = pkgs.python3.pkgs.callPackage ./nix/bencode2 { };
  };
in

pkgs.mkShell {
  buildInputs = with pkgs; [
    (python3.withPackages (pp: with pp; [
      qbittorrent-api
      rtorrent-rpc
    ]))
  ];
}
