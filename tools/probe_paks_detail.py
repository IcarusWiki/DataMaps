"""Get detailed sublevel breakdown for T021 paks."""
import glob
import os
import shutil
import subprocess
import sys
import tempfile

UE4EXPORT = os.path.join(os.path.dirname(__file__), "Ue4Export", "Ue4Export.exe")
PAKS_DIR = r"D:\Games\Steam\steamapps\common\Icarus\Icarus\Content\Paks"
ENGINE_VERSION = "UE4_27"

TARGET_PAKS = [
    "pakchunk0_s30-WindowsNoEditor.pak",
    "pakchunk0_s31-WindowsNoEditor.pak",
]


def probe_pak_detail(pak_path: str, work_dir: str) -> dict[str, list[str]]:
    """Extract sublevel listing from a single pak."""
    iso_dir = os.path.join(work_dir, "iso")
    out_dir = os.path.join(work_dir, "out")
    os.makedirs(iso_dir, exist_ok=True)
    os.makedirs(out_dir, exist_ok=True)

    shutil.copy2(pak_path, iso_dir)
    sig = pak_path + ".sig"
    if os.path.exists(sig):
        shutil.copy2(sig, iso_dir)

    asset_list = os.path.join(work_dir, "probe.txt")
    with open(asset_list, "w") as f:
        f.write("[Text]\n")
        f.write("Icarus/Content/Maps/Terrain_021_DLC2/Sublevels/*\n")

    cmd = [UE4EXPORT, "--quiet", iso_dir, ENGINE_VERSION, asset_list, out_dir]
    subprocess.run(cmd, capture_output=True, text=True, timeout=120)

    # Categorize found sublevels
    categories: dict[str, list[str]] = {
        "Generated (main tiles)": [],
        "Generated (Vista)": [],
        "Generated (LOD)": [],
        "Generated (Proxy)": [],
        "Developer": [],
        "Other": [],
    }

    for json_file in glob.glob(os.path.join(out_dir, "**", "*.json"), recursive=True):
        name = os.path.basename(json_file).replace(".json", "")
        rel = os.path.relpath(json_file, out_dir)

        if "LOD" in name:
            if "PROXY" in name.upper():
                categories["Generated (Proxy)"].append(name)
            else:
                categories["Generated (LOD)"].append(name)
        elif "PROXY" in name.upper() or "M_PROXY" in name:
            categories["Generated (Proxy)"].append(name)
        elif "Vista" in name:
            categories["Generated (Vista)"].append(name)
        elif "Developer" in rel:
            categories["Developer"].append(name)
        elif "Generated" in rel and "x" in name and "y" in name:
            categories["Generated (main tiles)"].append(name)
        else:
            categories["Other"].append(name)

    shutil.rmtree(iso_dir, ignore_errors=True)
    shutil.rmtree(out_dir, ignore_errors=True)

    return {k: sorted(v) for k, v in categories.items() if v}


def main():
    for pak_name in TARGET_PAKS:
        pak_path = os.path.join(PAKS_DIR, pak_name)
        print(f"\n{'='*60}")
        print(f"  {pak_name}")
        print(f"{'='*60}")

        with tempfile.TemporaryDirectory() as work_dir:
            cats = probe_pak_detail(pak_path, work_dir)

        for cat, names in cats.items():
            print(f"\n  {cat} ({len(names)}):")
            for n in names:
                print(f"    {n}")


if __name__ == "__main__":
    main()
