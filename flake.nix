{
  description = "A lazy, effectful streaming library for Rust — FS2-inspired Pipe<B>";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    crane.url = "github:ipetkov/crane";
    fenix.url = "github:nix-community/fenix";
    flake-parts.url = "github:hercules-ci/flake-parts";
  };

  outputs =
    inputs@{ self, nixpkgs, crane, fenix, flake-parts, ... }:
    flake-parts.lib.mkFlake { inherit inputs; } {
      systems = [
        "aarch64-darwin"
        "x86_64-darwin"
        "aarch64-linux"
        "x86_64-linux"
      ];

      perSystem =
        { pkgs, system, ... }:
        let
          rustToolchain = fenix.packages.${system}.combine (
            with fenix.packages.${system}.stable;
            [
              cargo
              clippy
              rustc
              rustfmt
              rust-src
            ]
          );
          craneLib = (crane.mkLib pkgs).overrideToolchain rustToolchain;

          commonArgs = {
            pname = "pipe";
            version = "0.1.0";
            src = craneLib.cleanCargoSource ./.;
            strictDeps = true;
            buildInputs =
              pkgs.lib.optionals pkgs.stdenv.isDarwin [
                pkgs.apple-sdk_15
              ];
          };

          cargoArtifacts = craneLib.buildDepsOnly commonArgs;

          pipe = craneLib.buildPackage (
            commonArgs
            // {
              inherit cargoArtifacts;
              doCheck = false;
            }
          );
        in
        {
          devShells.default = pkgs.mkShell {
            buildInputs = [
              rustToolchain
              pkgs.rust-analyzer
              pkgs.protobuf
            ] ++ pkgs.lib.optionals pkgs.stdenv.isDarwin [
              pkgs.apple-sdk_15
            ];

            RUST_SRC_PATH = "${fenix.packages.${system}.stable.rust-src}/lib/rustlib/src/rust/library";
            CARGO_NET_GIT_FETCH_WITH_CLI = "true";
          };

          packages = {
            inherit pipe;
            default = pipe;
          };
        };
    };
}
