"""Full local extraction: run Ue4Export on both T021 paks and extract plant positions."""
import glob
import json
import os
import shutil
import subprocess
import sys
import tempfile

# Add consumerScripts to path for import
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "consumerScripts"))
from generate_elysium_plants import PLANT_GROUPS, process_sublevel, SUBLEVEL_PATTERNS

UE4EXPORT = os.path.join(os.path.dirname(__file__), "Ue4Export", "Ue4Export.exe")
PAKS_DIR = r"D:\Games\Steam\steamapps\common\Icarus\Icarus\Content\Paks"
ENGINE_VERSION = "UE4_27"

TARGET_PAKS = [
    "pakchunk0_s30-WindowsNoEditor.pak",
    "pakchunk0_s31-WindowsNoEditor.pak",
]


def extract_from_pak(pak_name: str, work_dir: str) -> dict[str, list[list[float]]]:
    """Extract plant positions from a single pak."""
    pak_path = os.path.join(PAKS_DIR, pak_name)
    iso_dir = os.path.join(work_dir, "iso")
    text_dir = os.path.join(work_dir, "text")
    raw_dir = os.path.join(work_dir, "raw")
    os.makedirs(iso_dir, exist_ok=True)

    # Copy pak to isolated dir
    shutil.copy2(pak_path, iso_dir)
    sig = pak_path + ".sig"
    if os.path.exists(sig):
        shutil.copy2(sig, iso_dir)

    # Export Text + Raw
    for mode, out in [("Text", text_dir), ("Raw", raw_dir)]:
        os.makedirs(out, exist_ok=True)
        asset_list = os.path.join(work_dir, f"{mode.lower()}_list.txt")
        with open(asset_list, "w") as f:
            f.write(f"[{mode}]\n")
            f.write("Icarus/Content/Maps/Terrain_021_DLC2/Sublevels/*\n")

        cmd = [UE4EXPORT, "--quiet", iso_dir, ENGINE_VERSION, asset_list, out]
        print(f"  Running Ue4Export [{mode}]...")
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
        if result.returncode != 0:
            print(f"  Ue4Export [{mode}] failed: {result.stderr}")
            return {}

    # Process sublevels
    sublevels_root = os.path.join(text_dir, "Icarus", "Content", "Maps", "Terrain_021_DLC2", "Sublevels")
    raw_sublevels_root = os.path.join(raw_dir, "Icarus", "Content", "Maps", "Terrain_021_DLC2", "Sublevels")

    json_files = []
    for pattern in SUBLEVEL_PATTERNS:
        for subdir in ("Generated", "Developer"):
            json_files.extend(
                glob.glob(os.path.join(sublevels_root, subdir, pattern + ".json"))
            )

    from collections import defaultdict
    all_positions: dict[str, list[list[float]]] = defaultdict(list)

    for json_path in sorted(json_files):
        sublevel_name = os.path.basename(json_path).replace(".json", "")
        rel = os.path.relpath(json_path, sublevels_root)
        uexp_path = os.path.join(raw_sublevels_root, rel.replace(".json", ".uexp"))
        if not os.path.isfile(uexp_path):
            print(f"    {sublevel_name}: no binary, skipping")
            continue

        positions = process_sublevel(json_path, uexp_path)
        n_total = sum(len(v) for v in positions.values())
        if n_total > 0:
            groups = ", ".join(f"{g}:{len(v)}" for g, v in sorted(positions.items()))
            print(f"    {sublevel_name}: {n_total} instances ({groups})")
        for group, pos_list in positions.items():
            all_positions[group].extend(
                [round(x, 2), round(y, 2), round(z, 2)] for x, y, z in pos_list
            )

    return dict(all_positions)


def main():
    all_combined: dict[str, list[list[float]]] = {}

    for pak_name in TARGET_PAKS:
        print(f"\n{'='*60}")
        print(f"Extracting from {pak_name}")
        print(f"{'='*60}")

        with tempfile.TemporaryDirectory() as work_dir:
            positions = extract_from_pak(pak_name, work_dir)

        for group, pos_list in positions.items():
            if group not in all_combined:
                all_combined[group] = []
            all_combined[group].extend(pos_list)

    # Summary
    print(f"\n{'='*60}")
    print(f"COMBINED RESULTS")
    print(f"{'='*60}")
    for group in sorted(all_combined.keys()):
        print(f"  {group}: {len(all_combined[group])} instances")
    total = sum(len(v) for v in all_combined.values())
    print(f"\n  Total: {total} instances across {len(all_combined)} plant types")

    # Save combined raw positions
    out_path = os.path.join(os.path.dirname(__file__), "..", "tmp_raw_positions_new.json")
    with open(out_path, "w") as f:
        json.dump(all_combined, f, separators=(",", ":"))
    print(f"\nSaved to {out_path}")

    # Compare with existing
    old_path = os.path.join(os.path.dirname(__file__), "..", "tmp_raw_positions.json")
    if os.path.exists(old_path):
        with open(old_path) as f:
            old = json.load(f)
        print(f"\nComparison with previous extraction:")
        all_groups = sorted(set(list(old.keys()) + list(all_combined.keys())))
        for group in all_groups:
            old_n = len(old.get(group, []))
            new_n = len(all_combined.get(group, []))
            diff = new_n - old_n
            marker = " <-- DIFF" if diff != 0 else ""
            print(f"  {group}: {old_n} -> {new_n} ({'+' if diff > 0 else ''}{diff}){marker}")


if __name__ == "__main__":
    main()
