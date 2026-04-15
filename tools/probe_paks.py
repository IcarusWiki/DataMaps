"""Probe each pak individually to find which contain Terrain_021_DLC2 sublevels."""
import glob
import os
import shutil
import subprocess
import sys
import tempfile

UE4EXPORT = os.path.join(os.path.dirname(__file__), "Ue4Export", "Ue4Export.exe")
PAKS_DIR = r"D:\Games\Steam\steamapps\common\Icarus\Icarus\Content\Paks"
ENGINE_VERSION = "UE4_27"

# Probe asset: a sublevel that should exist if the pak has T021 data
PROBE_ASSETS = "Icarus/Content/Maps/Terrain_021_DLC2/Sublevels/*"


def probe_pak(pak_path: str, work_dir: str) -> list[str]:
    """Copy a single pak to a temp dir and probe for T021 sublevels.
    Returns list of sublevel names found."""
    pak_name = os.path.basename(pak_path)
    iso_dir = os.path.join(work_dir, "iso")
    out_dir = os.path.join(work_dir, "out")
    os.makedirs(iso_dir, exist_ok=True)
    os.makedirs(out_dir, exist_ok=True)

    # Copy pak (and .sig if present) to isolated dir
    shutil.copy2(pak_path, iso_dir)
    sig = pak_path + ".sig"
    if os.path.exists(sig):
        shutil.copy2(sig, iso_dir)

    # Write asset list
    asset_list = os.path.join(work_dir, "probe.txt")
    with open(asset_list, "w") as f:
        f.write("[Text]\n")
        f.write(PROBE_ASSETS + "\n")

    # Run Ue4Export
    cmd = [UE4EXPORT, "--quiet", iso_dir, ENGINE_VERSION, asset_list, out_dir]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)

    # Find any JSON output
    found = []
    for json_file in glob.glob(os.path.join(out_dir, "**", "*.json"), recursive=True):
        name = os.path.basename(json_file).replace(".json", "")
        found.append(name)

    # Cleanup
    shutil.rmtree(iso_dir, ignore_errors=True)
    shutil.rmtree(out_dir, ignore_errors=True)

    return found


def main():
    paks = sorted(glob.glob(os.path.join(PAKS_DIR, "*.pak")))
    print(f"Found {len(paks)} pak files to probe\n")

    hits = {}
    for i, pak in enumerate(paks):
        pak_name = os.path.basename(pak)
        sys.stdout.write(f"[{i+1}/{len(paks)}] {pak_name}... ")
        sys.stdout.flush()

        with tempfile.TemporaryDirectory() as work_dir:
            found = probe_pak(pak, work_dir)

        if found:
            hits[pak_name] = found
            print(f"FOUND {len(found)} sublevels")
        else:
            print("no T021 data")

    print(f"\n{'='*60}")
    print(f"Summary: {len(hits)} paks contain Terrain_021_DLC2 data\n")
    for pak_name, sublevels in sorted(hits.items()):
        print(f"  {pak_name}: {len(sublevels)} sublevels")
        for sl in sorted(sublevels)[:5]:
            print(f"    - {sl}")
        if len(sublevels) > 5:
            print(f"    ... and {len(sublevels) - 5} more")


if __name__ == "__main__":
    main()
