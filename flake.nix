{
  description = "Bitagnis Bitaxe optimizer";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";

  outputs =
    { nixpkgs, ... }:
    let
      systems = [
        "aarch64-darwin"
        "aarch64-linux"
        "x86_64-linux"
      ];
      forAllSystems = nixpkgs.lib.genAttrs systems;
    in
    {
      packages = forAllSystems (
        system:
        let
          pkgs = nixpkgs.legacyPackages.${system};
          bitagnis = pkgs.buildGo125Module {
            pname = "bitagnis";
            version = "unstable";
            src = ./.;
            vendorHash = "sha256-bMn47yiyxPdgioJJvUnDYrqaWMGHFkRPxlq/g1gBlxo=";

            meta.mainProgram = "bitagnis";
          };
        in
        {
          inherit bitagnis;
          default = bitagnis;
        }
      );
    };
}
