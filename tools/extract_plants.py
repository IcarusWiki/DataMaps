"""Extract plant foliage positions from Ue4Export JSON + binary data."""
import json
import struct
import os
import glob
from collections import defaultdict

# Mapping from foliage type names to plant group names for the wiki map
PLANT_GROUPS = {
    "FT_DC_Agave_Var1": "Agave", "FT_DC_Agave_Var2": "Agave", "FT_DC_Agave_Var3": "Agave",
    "FT_Avocado_Var1": "Avocado", "FT_Avocado_Var2": "Avocado", "FT_Avocado_Var3": "Avocado",
    "FT_HRB_Banana_Var1": "Banana", "FT_HRB_Banana_Var2": "Banana", "FT_HRB_Banana_Var3": "Banana",
    "FT_BerryBush": "Berries",
    "FT_Carrot": "Carrot",
    "FT_Cocoa": "Cocoa", "FT_LC_Cocoa": "Cocoa",
    "FT_Coffee": "Coffee",
    "FT_CornCob_01": "Corn",
    "FT_Garlic_Var1": "Garlic", "FT_Garlic_Var2": "Garlic", "FT_Garlic_Var3": "Garlic",
    "FT_KiwiFruit_Var1": "Kiwi", "FT_KiwiFruit_Var2": "Kiwi", "FT_KiwiFruit_Var3": "Kiwi",
    "FT_Kumara_Var1": "Kumara", "FT_Kumara_Var2": "Kumara", "FT_Kumara_Var3": "Kumara",
    "FT_LC_Kumara": "Kumara",
    "FT_MushroomA": "Mushroom",
    "FT_Onion_Var1": "Onion", "FT_Onion_Var2": "Onion", "FT_Onion_Var3": "Onion",
    "FT_SW_Potato_Wild": "Potato", "FT_TU_Potato_Wild": "Potato",
    "FT_SW_Potato": "Potato", "FT_TU_Potato": "Potato",
    "FT_PricklyPear_Var1": "PricklyPear", "FT_PricklyPear_Var2": "PricklyPear",
    "FT_PricklyPear_Var3": "PricklyPear", "FT_PricklyPear_Var4": "PricklyPear",
    "FT_DC_Prickly_Pear_Var1": "PricklyPear", "FT_DC_Prickly_Pear_Var2": "PricklyPear",
    "FT_DC_Prickly_Pear_Var3": "PricklyPear", "FT_DC_Prickly_Pear_Var4": "PricklyPear",
    "FT_Pumpkin": "Pumpkin", "FT_TU_Pumpkin": "Pumpkin",
    "FT_ReedFlower_01": "Reed",
    "FT_Rhubarb_Var1": "Rhubarb", "FT_Rhubarb_Var2": "Rhubarb", "FT_Rhubarb_Var3": "Rhubarb",
    "FT_Beans": "Beans", "FT_LC_Beans": "Beans",
    "FT_Squash": "Squash", "FT_LC_Squash": "Squash",
    "FT_HRB_Sugar_Cane_Var1": "SugarCane", "FT_HRB_Sugar_Cane_Var2": "SugarCane", "FT_HRB_Sugar_Cane_Var3": "SugarCane",
    "FT_WildTea": "Tea", "FT_LC_WildTea": "Tea", "FT_GreenTea": "Tea",
    "FT_Tomatoes_Wild": "Tomato",
    "FT_Truffle_Var1": "Truffle", "FT_Truffle_Var2": "Truffle", "FT_Truffle_Var3": "Truffle",
    "FT_DC_TrufflePlant_Var1_Temp": "Truffle", "FT_DC_TrufflePlant_Var2_Temp": "Truffle",
    "FT_DC_TrufflePlant_Var3_Temp": "Truffle",
    "FT_Watermelon": "Watermelon",
    "FT_Wheat_03": "Wheat",
    "FT_YeastPlant_01": "Yeast",
    "FT_Alpine_Lily_01": "Lily",
    "FT_Sponge_01": "Sponge",
}


def extract_positions_from_binary(bindata, count, bounds):
    """Search binary data for an FMatrix instance array with the given count."""
    count_bytes = struct.pack("<I", count)
    bmin = bounds.get("Min", {})
    bmax = bounds.get("Max", {})

    pos = 0
    while True:
        pos = bindata.find(count_bytes, pos)
        if pos < 0:
            return None

        # Check we have enough data for all instances
        if pos + 4 + count * 64 > len(bindata):
            pos += 4
            continue

        # Read first instance translation (floats 12-14 of the FMatrix)
        trans_offset = pos + 4 + 48
        tx = struct.unpack("<f", bindata[trans_offset : trans_offset + 4])[0]
        ty = struct.unpack("<f", bindata[trans_offset + 4 : trans_offset + 8])[0]
        tz = struct.unpack("<f", bindata[trans_offset + 8 : trans_offset + 12])[0]
        w = struct.unpack("<f", bindata[trans_offset + 12 : trans_offset + 16])[0]

        # Validate: w should be ~1.0 and coords in reasonable range
        if abs(w - 1.0) >= 0.01 or not (-400000 < tx < 400000) or not (-400000 < ty < 400000):
            pos += 4
            continue

        # Check against bounds if available
        if bmin and bmax:
            margin = 2000
            if not (
                bmin.get("X", -1e9) - margin < tx < bmax.get("X", 1e9) + margin
                and bmin.get("Y", -1e9) - margin < ty < bmax.get("Y", 1e9) + margin
            ):
                pos += 4
                continue

        # Extract all instances
        positions = []
        for i in range(count):
            inst_offset = pos + 4 + i * 64 + 48
            ix = struct.unpack("<f", bindata[inst_offset : inst_offset + 4])[0]
            iy = struct.unpack("<f", bindata[inst_offset + 4 : inst_offset + 8])[0]
            iz = struct.unpack("<f", bindata[inst_offset + 8 : inst_offset + 12])[0]
            positions.append((ix, iy, iz))

        return positions

    return None


def process_sublevel(json_path, uexp_path):
    """Process a single sublevel and return plant positions."""
    with open(json_path, "r", encoding="utf-8-sig") as f:
        try:
            jdata = json.load(f)
        except (json.JSONDecodeError, UnicodeDecodeError):
            return {}

    with open(uexp_path, "rb") as f:
        bindata = f.read()

    # Find InstancedFoliageActor and components
    foliage_actor = None
    components = {}
    for obj in jdata:
        if isinstance(obj, dict):
            if (
                obj.get("Name", "").startswith("InstancedFoliageActor")
                and "FoliageInfos" in obj
            ):
                foliage_actor = obj
            name = obj.get("Name", "")
            if "FISMComponent" in name or "FoliageInstanced" in name:
                components[name] = obj

    if not foliage_actor:
        return {}

    positions = defaultdict(list)

    for ft_path, info in foliage_actor["FoliageInfos"].items():
        ft_name = ft_path.split("/")[-1].split(".")[0]

        if ft_name not in PLANT_GROUPS:
            continue

        plant_group = PLANT_GROUPS[ft_name]

        # Get component info
        comp_ref = (info.get("Implementation") or {}).get("Component")
        if not comp_ref:
            continue
        comp_obj_name = comp_ref.get("ObjectName", "")
        comp_name = comp_obj_name.split(".")[-1].rstrip("'")

        comp = components.get(comp_name)
        if not comp:
            continue

        props = comp.get("Properties", {})

        # Try PerInstanceSMData first (standard components)
        if "PerInstanceSMData" in comp:
            for inst in comp["PerInstanceSMData"]:
                trans = inst.get("TransformData", {}).get("Translation", {})
                x, y, z = trans.get("X", 0), trans.get("Y", 0), trans.get("Z", 0)
                positions[plant_group].append((x, y, z))
            continue

        # For FLOD components, search binary
        count = props.get("NumBuiltInstances", 0)
        if count == 0:
            continue

        bounds = props.get("BuiltInstanceBounds", {})
        result = extract_positions_from_binary(bindata, count, bounds)
        if result:
            positions[plant_group].extend(result)
        else:
            sublevel_name = os.path.basename(json_path).replace(".json", "")
            print(f"  WARNING: {sublevel_name}/{ft_name}: count={count} NOT FOUND")

    return positions


def main():
    text_dir = r"C:\Users\viach\AppData\Local\Temp\elysium_text\Icarus\Content\Maps\Terrain_021_DLC2\Sublevels"
    raw_dir = r"C:\Users\viach\AppData\Local\Temp\elysium_raw\Icarus\Content\Maps\Terrain_021_DLC2\Sublevels"

    all_positions = defaultdict(list)

    # Only process main sublevels, not LOD or proxy files
    json_files = []
    for pattern in [
        os.path.join(text_dir, "Generated", "T021_Generated_x?_y?.json"),
        os.path.join(text_dir, "Generated", "T021_Generated_Vista.json"),
        os.path.join(text_dir, "Developer", "T021_Developer_*.json"),
    ]:
        json_files.extend(glob.glob(pattern))
    print(f"Found {len(json_files)} sublevel JSON files to process")

    for json_path in sorted(json_files):
        rel_path = os.path.relpath(json_path, text_dir)
        uexp_path = os.path.join(raw_dir, rel_path.replace(".json", ".uexp"))

        if not os.path.exists(uexp_path):
            continue

        positions = process_sublevel(json_path, uexp_path)
        for group, pos_list in positions.items():
            all_positions[group].extend(pos_list)

    print(f"\n=== Plant extraction summary ===")
    for group in sorted(all_positions.keys()):
        print(f"  {group}: {len(all_positions[group])} instances")

    total = sum(len(v) for v in all_positions.values())
    print(f"\nTotal plant instances: {total}")

    # Save raw positions
    output = {
        k: [(round(x, 2), round(y, 2), round(z, 2)) for x, y, z in v]
        for k, v in all_positions.items()
    }
    out_path = os.path.join(os.path.dirname(__file__), "..", "tmp_raw_positions.json")
    with open(out_path, "w") as f:
        json.dump(output, f)
    print(f"\nSaved raw positions to {out_path}")


if __name__ == "__main__":
    main()
