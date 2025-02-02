{
  description = "Nix flake for the DuckDB Airport extension";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs?ref=nixpkgs-unstable";
    flake-utils = {
      url = "github:numtide/flake-utils";
    };
    duckdb-nix = {
      url = "github:rupurt/duckdb-nix?rev=720cd4650a8517b6921154b6c814fcfe6ea1d72d";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = {
    self,
    nixpkgs,
    flake-utils,
    duckdb-nix,
    ...
  }: let
    systems = ["x86_64-linux" "aarch64-linux" "x86_64-darwin" "aarch64-darwin"];
    outputs = flake-utils.lib.eachSystem systems (system: let
      pkgs = import nixpkgs {
        inherit system;
        overlays = [
          duckdb-nix.overlay
          self.overlay
          (final: prev: {
            arrow-cpp = prev.arrow-cpp.override { enableShared = false; };
            boost = prev.boost.override { enableStatic = true; };
            snappy = prev.snappy.override { static = true; };
            brotli = prev.brotli.override { staticOnly = true; };
          })
        ];
      };
    in {
      # packages exported by the flake
      packages = rec {
        duckdb-airport-extension = pkgs.stdenv.mkDerivation rec {
          pname = "duckdb-airport-extenion";
          version = "v0.0.0";
          src = self;

          nativeBuildInputs = [
            pkgs.cmake
            # pkgs.ninja
            pkgs.pkg-config
            pkgs.python312
            # pkgs.vcpkg
          ];

          buildInputs = [
            pkgs.arrow-cpp
            pkgs.boost
            pkgs.curl
            pkgs.grpc
            pkgs.msgpack-cxx
            pkgs.openssl
          ];

          cmakeFlags = [
            "-DOVERRIDE_GIT_DESCRIBE=v${version}"
          ];

          configurePhase = ''
            echo 'ls -l $src'
            echo $src
            ls -l $src
            echo 'ls -l $src/duckdb'
            ls -l $src/duckdb
          '';
        };
        default = duckdb-airport-extension;
      };

      # nix fmt
      formatter = pkgs.alejandra;

      # dev shell
      devShells.default = pkgs.mkShell {
        packages = [
          # pkgs.cmake
          # pkgs.pkg-config
          #
          # pkgs.arrow-cpp
          # pkgs.boost
          # pkgs.curl
          # pkgs.grpc
          # pkgs.msgpack-cxx
          # pkgs.openssl

          # pkgs.duckdb-pkgs.v1_2_0-dev
          # pkgs.duckdb-airport-extension-pkgs.default
        ];
      };
    });
  in
    outputs
    // {
      # Overlay that can be imported so you can access the packages
      # using duckdb-nix.overlay
      overlay = final: prev: {
        duckdb-airport-extension-pkgs = outputs.packages.${prev.system};
      };
    };
}
