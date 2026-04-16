#!/usr/bin/env python3
from __future__ import annotations

import json
import math
import os
import re
import struct
import subprocess
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path

ENGINE_VERSION = "UE4_27"

WORLD_TILE_SIZE = 100800
WORLD_CELL_SIZE = WORLD_TILE_SIZE // 2  # 50400
CLUSTER_DISTANCE_THRESHOLD = WORLD_CELL_SIZE * 0.1  # 5040
PARTITION_SIZE = WORLD_CELL_SIZE * 0.25  # 12600
AUTO_CLUSTER_TARGET_PIXELS = 16.0
AUTO_CLUSTER_MIN_THRESHOLD = 800.0
AUTO_CLUSTER_MAX_THRESHOLD = 3200.0
DEFAULT_MAX_CLUSTER_COUNT = 20
DEFAULT_OUTPUT_DIR = Path(__file__).resolve().parents[1] / "plantMaps"

INCLUDED_WORLD_IDS = {
    "Terrain_016",
    "Terrain_017",
    "Terrain_019",
    "Terrain_021",
    "Outpost_002",
    "Outpost_003",
    "Outpost_004",
    "Outpost_005",
    "Outpost_006",
    "Outpost_007",
    "Outpost_008",
    "Outpost_009",
    "Outpost_011",
    "Outpost_012",
    "Outpost_013",
    "Outpost_DEV",
}


@dataclass(frozen=True)
class PlantGroupMeta:
    display_name: str
    icon: str
    size: tuple[int, int]
    article: str | None = None
    blueprint_patterns: tuple[str, ...] = ()


@dataclass(frozen=True)
class WorldConfig:
    world_id: str
    display_name: str
    output_stem: str
    bounds: tuple[float, float, float, float]
    background_image: str
    package_refs: tuple[str, ...]


PLANT_GROUPS: dict[str, PlantGroupMeta] = {
    "Agave": PlantGroupMeta(
        display_name="Agave",
        icon="T ITEM Agave.png",
        size=(40, 40),
        blueprint_patterns=(r"^BP_DC_Agave_",),
    ),
    "Aloe": PlantGroupMeta(
        display_name="Aloe",
        icon="ITEM_Fibre.png",
        size=(40, 40),
        blueprint_patterns=(r"^BP_Herb_Aloe_Vera$",),
    ),
    "Avocado": PlantGroupMeta(
        display_name="Avocados",
        icon="ITEM_Avocado.png",
        size=(40, 40),
        blueprint_patterns=(r"^BP_Avocado_Var\d+$",),
    ),
    "Banana": PlantGroupMeta(
        display_name="Bananas",
        icon="T_ITEM_Banana.png",
        size=(40, 40),
        blueprint_patterns=(r"^BP_HRB_Banana_Var\d+$",),
    ),
    "Beans": PlantGroupMeta(
        display_name="Soy Beans",
        icon="ITEM Bean.png",
        size=(40, 40),
        article="Soy Beans",
        blueprint_patterns=(r"^BP_(?:LC_)?Beans$",),
    ),
    "Berries": PlantGroupMeta(
        display_name="Wild Berries",
        icon="ITEM Wild Berry.png",
        size=(40, 40),
        article="Wild Berries",
        blueprint_patterns=(r"^BP_BerryBush$",),
    ),
    "Cactus": PlantGroupMeta(
        display_name="Cactus",
        icon="ITEM_Fibre.png",
        size=(40, 40),
        blueprint_patterns=(r"^BP_Cactus[AB]_\d+$",),
    ),
    "Carrot": PlantGroupMeta(
        display_name="Carrots",
        icon="ITEM Carrot.png",
        size=(40, 40),
        blueprint_patterns=(r"^BP_Carrot$",),
    ),
    "Cocoa": PlantGroupMeta(
        display_name="Cocoa",
        icon="ITEM Cocoa.png",
        size=(40, 40),
        blueprint_patterns=(r"^BP_(?:LC_)?Cocoa$",),
    ),
    "Coffee": PlantGroupMeta(
        display_name="Coffee Beans",
        icon="ITEM Coffee Bean.png",
        size=(40, 40),
        article="Coffee Beans",
        blueprint_patterns=(r"^BP_Coffee$",),
    ),
    "ConiferFlower": PlantGroupMeta(
        display_name="Conifer Flower",
        icon="ITEM_Fibre.png",
        size=(40, 40),
        article="Conifer Flower",
        blueprint_patterns=(r"^BP_Herb_Conifer_Flower$",),
    ),
    "Corn": PlantGroupMeta(
        display_name="Corn",
        icon="ITEM_Corn_Cob.png",
        size=(40, 40),
        blueprint_patterns=(r"^BP_CornCob$", r"^BP_Corn$", r"^BP_Corn_Crops_Large$"),
    ),
    "Garlic": PlantGroupMeta(
        display_name="Garlic",
        icon="T_ITEM_Garlic.png",
        size=(40, 40),
        blueprint_patterns=(r"^BP_HRB_Garlic_Var\d+$",),
    ),
    "Kiwi": PlantGroupMeta(
        display_name="Kiwifruit",
        icon="ITEM_Kiwi_Fruit.png",
        size=(40, 40),
        blueprint_patterns=(r"^BP_KiwiFruit_Var\d+$",),
    ),
    "Kumara": PlantGroupMeta(
        display_name="Kumara",
        icon="ITEM_Kumara.png",
        size=(40, 40),
        blueprint_patterns=(r"^BP_Kumara_Var\d+$", r"^BP_LC_Kumara$"),
    ),
    "Lily": PlantGroupMeta(
        display_name="Lily",
        icon="ITEM_Alpine_Lily.png",
        size=(40, 40),
        blueprint_patterns=(r"^BP_Lily$",),
    ),
    "Mushroom": PlantGroupMeta(
        display_name="Mushrooms",
        icon="ITEM Mushroom.png",
        size=(40, 40),
        blueprint_patterns=(r"^BP_Herb_Mushroom_A$", r"^BP_SW_Mushroom_Shelf_A_Var\d+$"),
    ),
    "Onion": PlantGroupMeta(
        display_name="Onions",
        icon="T_ITEM_Onion.png",
        size=(35, 35),
        blueprint_patterns=(r"^BP_HRB_Onion_Var\d+$",),
    ),
    "PalmBush": PlantGroupMeta(
        display_name="Palm Bush",
        icon="ITEM_Fibre.png",
        size=(40, 40),
        article="Palm Bush",
        blueprint_patterns=(r"^BP_PalmBush_\d+$",),
    ),
    "Potato": PlantGroupMeta(
        display_name="Potatoes",
        icon="ITEM Potato.png",
        size=(40, 40),
        blueprint_patterns=(r"^BP_(?:SW|TU)_Crop_Potato_Wild$",),
    ),
    "PricklyPear": PlantGroupMeta(
        display_name="Prickly Pear",
        icon="ITEM_Prickly_Pear.png",
        size=(40, 40),
        article="Prickly Pear",
        blueprint_patterns=(r"^BP_HRB_Prickly_Pear_Var\d+$",),
    ),
    "Pumpkin": PlantGroupMeta(
        display_name="Pumpkin",
        icon="ITEM_Pumpkin.png",
        size=(40, 40),
        blueprint_patterns=(r"^BP_(?:TU_)?Pumpkin$",),
    ),
    "Reed": PlantGroupMeta(
        display_name="Reed Flowers",
        icon="ITEM_Reeds.png",
        size=(40, 40),
        article="Reed Flowers",
        blueprint_patterns=(r"^BP_Reed_Flower$",),
    ),
    "Rhubarb": PlantGroupMeta(
        display_name="Rhubarb",
        icon="ITEM_Rhubarb.png",
        size=(40, 40),
        blueprint_patterns=(r"^BP_Rhubarb_Var\d+$",),
    ),
    "Sponge": PlantGroupMeta(
        display_name="Sponge",
        icon="ITEM_Sponge.png",
        size=(40, 40),
        blueprint_patterns=(r"^BP_Sponge$",),
    ),
    "Squash": PlantGroupMeta(
        display_name="Squash",
        icon="ITEM_Squash.png",
        size=(40, 40),
        blueprint_patterns=(r"^BP_(?:LC_)?Squash$",),
    ),
    "SugarCane": PlantGroupMeta(
        display_name="Sugar Cane",
        icon="ITEM_Sugar_Cane.png",
        size=(40, 40),
        article="Sugar Cane",
        blueprint_patterns=(r"^BP_HRB_SugarCane_Var\d+$",),
    ),
    "Tea": PlantGroupMeta(
        display_name="Tea",
        icon="ITEM Tea.png",
        size=(40, 40),
        blueprint_patterns=(r"^BP_(?:GreenTea|WildTea|LC_WildTea)$",),
    ),
    "Tomato": PlantGroupMeta(
        display_name="Tomato",
        icon="ITEM_Tomato.png",
        size=(35, 35),
        blueprint_patterns=(r"^BP_HRB_Tomatoes_Wild$",),
    ),
    "Truffle": PlantGroupMeta(
        display_name="Truffles",
        icon="T_ITEM_Truffle.png",
        size=(35, 35),
        blueprint_patterns=(r"^BP_HRB_Truffle_Var\d+$",),
    ),
    "Watermelon": PlantGroupMeta(
        display_name="Watermelon",
        icon="ITEM_Watermelon.png",
        size=(40, 40),
        blueprint_patterns=(r"^BP_Watermelon$",),
    ),
    "Wheat": PlantGroupMeta(
        display_name="Wheat",
        icon="ITEM_Wheat.png",
        size=(40, 40),
        blueprint_patterns=(r"^BP_Wheat$",),
    ),
    "Yeast": PlantGroupMeta(
        display_name="Yeast",
        icon="ITEM_Yeast.png",
        size=(40, 40),
        blueprint_patterns=(r"^BP_Yeast$",),
    ),
}

STATIC_MESH_GROUPS: dict[str, str] = {
    "HRB_Crop_Tomatoes_Stage5_Var1": "Tomato",
}

MANUAL_FOLIAGE_GROUPS: dict[str, str] = {
    "FT_SW_Potato": "Potato",
    "FT_TU_Potato": "Potato",
    "FT_LC_Kumara": "Kumara",
}

_GROUP_PATTERNS: dict[str, tuple[re.Pattern[str], ...]] = {
    group_id: tuple(re.compile(pattern) for pattern in meta.blueprint_patterns)
    for group_id, meta in PLANT_GROUPS.items()
}


def fail(message: str) -> None:
    print(f"Error: {message}", file=sys.stderr)
    raise SystemExit(1)


def parse_nsloctext(value: str) -> str:
    match = re.search(r'NSLOCTEXT\([^,]+,\s*"[^"]+",\s*"([^"]+)"\)', value)
    return match.group(1) if match else value


def normalize_output_stem(name: str) -> str:
    stem = re.sub(r"[^A-Za-z0-9]+", "", name)
    return stem or "World"


def _load_json(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _infer_bounds_from_grid(row: dict) -> tuple[float, float, float, float]:
    grid_bounds = row.get("GridBounds", [])
    if not grid_bounds:
        return (-400000.0, -400000.0, 400000.0, 400000.0)

    min_x = min(cell["Origin"]["X"] - cell["BoxExtent"]["X"] for cell in grid_bounds)
    min_y = min(cell["Origin"]["Y"] - cell["BoxExtent"]["Y"] for cell in grid_bounds)
    max_x = max(cell["Origin"]["X"] + cell["BoxExtent"]["X"] for cell in grid_bounds)
    max_y = max(cell["Origin"]["Y"] + cell["BoxExtent"]["Y"] for cell in grid_bounds)
    return (float(min_x), float(min_y), float(max_x), float(max_y))


def _extract_bounds(row: dict) -> tuple[float, float, float, float]:
    minimap = row.get("MinimapData", {})
    min_bound = minimap.get("WorldBoundaryMin", {})
    max_bound = minimap.get("WorldBoundaryMax", {})

    min_x = float(min_bound.get("X", 0))
    min_y = float(min_bound.get("Y", 0))
    max_x = float(max_bound.get("X", 0))
    max_y = float(max_bound.get("Y", 0))
    if any(value != 0 for value in (min_x, min_y, max_x, max_y)):
        return (min_x, min_y, max_x, max_y)
    return _infer_bounds_from_grid(row)


def _as_package_ref(level_ref: str | None) -> str | None:
    if not level_ref or level_ref == "None":
        return None
    return level_ref.split(".", 1)[0]


def _package_ref_to_asset_path(package_ref: str) -> str:
    if not package_ref.startswith("/Game/"):
        fail(f"Unexpected package ref: {package_ref}")
    return "Icarus/Content/" + package_ref[len("/Game/") :]


def find_data_root(explicit_root: str | None = None) -> Path:
    if explicit_root:
        path = Path(explicit_root).resolve()
        if path.joinpath("InGameFiles").is_dir():
            return path
        fail(f"Data root does not contain InGameFiles: {path}")

    candidates: list[Path] = []
    github_workspace = os.environ.get("GITHUB_WORKSPACE")
    if github_workspace:
        candidates.append(Path(github_workspace))
    candidates.extend(
        [
            Path.cwd(),
            Path(__file__).resolve().parents[2],
            Path(__file__).resolve().parents[3] / "Data",
            Path.cwd().parent / "Data",
        ]
    )
    for candidate in candidates:
        if candidate.joinpath("InGameFiles").is_dir():
            return candidate.resolve()
    fail("Could not find the Data repo root; pass --data-root")


def load_world_configs(data_root: str | Path, selected_world_ids: list[str] | None = None) -> list[WorldConfig]:
    data_root = Path(data_root)
    world_data = _load_json(data_root / "InGameFiles" / "World" / "D_WorldData.json")
    terrain_data = _load_json(data_root / "InGameFiles" / "Prospects" / "D_Terrains.json")
    terrain_rows = {row["Name"]: row for row in terrain_data["Rows"]}

    wanted_ids = set(selected_world_ids or INCLUDED_WORLD_IDS)
    worlds: list[WorldConfig] = []

    for row in world_data["Rows"]:
        world_id = row["Name"]
        if world_id not in wanted_ids:
            continue

        terrain_row = terrain_rows.get(world_id)
        display_name = world_id
        if terrain_row and isinstance(terrain_row.get("TerrainName"), str):
            display_name = parse_nsloctext(terrain_row["TerrainName"])

        package_refs: list[str] = []
        for value in (
            _as_package_ref(row.get("MainLevel")),
            *(_as_package_ref(ref) for ref in row.get("HeightmapLevels", [])),
            *(_as_package_ref(ref) for ref in row.get("GeneratedLevels", [])),
            _as_package_ref(row.get("GeneratedVistaLevel")),
            *(_as_package_ref(ref) for ref in row.get("DeveloperLevels", [])),
        ):
            if value and value not in package_refs:
                package_refs.append(value)

        worlds.append(
            WorldConfig(
                world_id=world_id,
                display_name=display_name,
                output_stem=normalize_output_stem(display_name),
                bounds=_extract_bounds(row),
                background_image=f"MAP {display_name}.jpg",
                package_refs=tuple(package_refs),
            )
        )

    worlds.sort(key=lambda world: world.display_name)
    return worlds


def _actor_name(actor_path: str) -> str:
    if not actor_path:
        return ""
    return actor_path.split("/")[-1].split(".")[0]


def _matches_group(actor_name: str) -> str | None:
    for group_id, patterns in _GROUP_PATTERNS.items():
        if any(pattern.match(actor_name) for pattern in patterns):
            return group_id
    return None


def build_foliage_group_map(
    data_root: str | Path,
) -> tuple[dict[str, str], dict[str, str]]:
    data_root = Path(data_root)
    flod_data = _load_json(data_root / "InGameFiles" / "FLOD" / "D_FLODDescriptions.json")

    foliage_to_group: dict[str, str] = dict(MANUAL_FOLIAGE_GROUPS)
    resource_actor_by_foliage: dict[str, str] = {}

    for row in flod_data["Rows"]:
        actor_path = row.get("ViewTraceActor", "")
        if "/Game/BP/Objects/World/Resources/Nodes/" not in actor_path:
            continue

        actor_name = _actor_name(actor_path)
        aliases = {
            row.get("Name", ""),
            Path(str(row.get("FoliageType", ""))).name.split(".")[0],
        }
        for alias in tuple(aliases):
            if alias:
                resource_actor_by_foliage[alias] = actor_name

        group_id = _matches_group(actor_name)
        if not group_id:
            continue
        for alias in aliases:
            if alias:
                foliage_to_group[alias] = group_id

    return foliage_to_group, resource_actor_by_foliage


def run_ue4export(
    ue4export_exe: str,
    paks_dir: str | Path,
    asset_list_path: str | Path,
    output_dir: str | Path,
    *,
    quiet: bool = True,
) -> None:
    cmd = [str(ue4export_exe)]
    if quiet:
        cmd.append("--quiet")
    cmd.append("--mix-output")
    cmd.extend([str(paks_dir), ENGINE_VERSION, str(asset_list_path), str(output_dir)])
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        if result.stderr:
            print(result.stderr, file=sys.stderr)
        fail(f"Ue4Export failed with code {result.returncode}")


def write_asset_list(asset_list_path: str | Path, mode: str, package_refs: list[str]) -> None:
    asset_paths = sorted({_package_ref_to_asset_path(package_ref) for package_ref in package_refs})
    with Path(asset_list_path).open("w", encoding="utf-8", newline="\n") as handle:
        handle.write(f"[{mode}]\n")
        for asset_path in asset_paths:
            handle.write(asset_path)
            handle.write("\n")


def collect_package_refs(worlds: list[WorldConfig]) -> list[str]:
    package_refs: list[str] = []
    for world in worlds:
        for package_ref in world.package_refs:
            if package_ref not in package_refs:
                package_refs.append(package_ref)
    return package_refs


def extract_positions_from_binary(
    bindata: bytes,
    count: int,
    bounds: dict,
) -> list[tuple[float, float, float]] | None:
    count_bytes = struct.pack("<I", count)
    min_bound = bounds.get("Min", {})
    max_bound = bounds.get("Max", {})

    position = 0
    while True:
        position = bindata.find(count_bytes, position)
        if position < 0:
            return None
        if position + 4 + count * 64 > len(bindata):
            position += 4
            continue

        offset = position + 4 + 48
        first_x = struct.unpack("<f", bindata[offset : offset + 4])[0]
        first_y = struct.unpack("<f", bindata[offset + 4 : offset + 8])[0]
        first_z = struct.unpack("<f", bindata[offset + 8 : offset + 12])[0]
        first_w = struct.unpack("<f", bindata[offset + 12 : offset + 16])[0]

        if abs(first_w - 1.0) >= 0.01:
            position += 4
            continue
        if not (-600000.0 < first_x < 600000.0 and -600000.0 < first_y < 600000.0):
            position += 4
            continue

        if min_bound and max_bound:
            margin = 2000.0
            if not (
                min_bound.get("X", -1e9) - margin < first_x < max_bound.get("X", 1e9) + margin
                and min_bound.get("Y", -1e9) - margin < first_y < max_bound.get("Y", 1e9) + margin
            ):
                position += 4
                continue

        positions: list[tuple[float, float, float]] = []
        for index in range(count):
            instance_offset = position + 4 + index * 64 + 48
            inst_x = struct.unpack("<f", bindata[instance_offset : instance_offset + 4])[0]
            inst_y = struct.unpack("<f", bindata[instance_offset + 4 : instance_offset + 8])[0]
            inst_z = struct.unpack("<f", bindata[instance_offset + 8 : instance_offset + 12])[0]
            positions.append((inst_x, inst_y, inst_z))
        return positions


def process_sublevel(
    json_path: str | Path,
    uexp_path: str | Path,
    foliage_to_group: dict[str, str],
    resource_actor_by_foliage: dict[str, str],
    static_mesh_groups: dict[str, str] | None = None,
) -> tuple[dict[str, list[tuple[float, float, float]]], Counter[str]]:
    static_mesh_groups = static_mesh_groups or STATIC_MESH_GROUPS

    with Path(json_path).open("r", encoding="utf-8-sig") as handle:
        try:
            objects = json.load(handle)
        except (json.JSONDecodeError, UnicodeDecodeError):
            return {}, Counter()

    with Path(uexp_path).open("rb") as handle:
        bindata = handle.read()

    foliage_actor = None
    components: dict[str, dict] = {}
    for obj in objects:
        if not isinstance(obj, dict):
            continue
        name = obj.get("Name", "")
        if name.startswith("InstancedFoliageActor") and "FoliageInfos" in obj:
            foliage_actor = obj
        if "FISMComponent" in name or "FoliageInstanced" in name:
            components[name] = obj

    positions: dict[str, list[tuple[float, float, float]]] = defaultdict(list)
    unknown_foliage: Counter[str] = Counter()

    if foliage_actor is not None:
        for foliage_path, info in foliage_actor["FoliageInfos"].items():
            foliage_name = foliage_path.split("/")[-1].split(".")[0]
            group_id = foliage_to_group.get(foliage_name)
            if not group_id:
                actor_name = resource_actor_by_foliage.get(foliage_name)
                if actor_name:
                    unknown_foliage[f"{foliage_name} ({actor_name})"] += 1
                continue

            component_ref = (info.get("Implementation") or {}).get("Component")
            if not component_ref:
                continue
            component_name = component_ref.get("ObjectName", "").split(".")[-1].rstrip("'")
            component = components.get(component_name)
            if not component:
                continue

            if "PerInstanceSMData" in component:
                for instance in component["PerInstanceSMData"]:
                    translation = instance.get("TransformData", {}).get("Translation", {})
                    positions[group_id].append(
                        (
                            float(translation.get("X", 0.0)),
                            float(translation.get("Y", 0.0)),
                            float(translation.get("Z", 0.0)),
                        )
                    )
                continue

            props = component.get("Properties", {})
            count = props.get("NumBuiltInstances", 0) or props.get("InstanceCountToRender", 0)
            if not count:
                count = len(props.get("SortedInstances", []))
            if count <= 0:
                continue

            result = extract_positions_from_binary(bindata, count, props.get("BuiltInstanceBounds", {}))
            if result:
                positions[group_id].extend(result)

    for obj in objects:
        if not isinstance(obj, dict) or obj.get("Type") != "StaticMeshComponent":
            continue
        if obj.get("Outer", "").startswith("InstancedFoliageActor"):
            continue

        props = obj.get("Properties", {})
        if not isinstance(props, dict):
            continue

        mesh_ref = props.get("StaticMesh")
        if not isinstance(mesh_ref, dict):
            continue

        mesh_name = mesh_ref.get("ObjectName", "")
        if "'" in mesh_name:
            mesh_name = mesh_name.split("'")[1]
        else:
            mesh_name = mesh_name.split(".")[-1]
        group_id = static_mesh_groups.get(mesh_name)
        if not group_id:
            continue

        location = props.get("RelativeLocation", {})
        if not isinstance(location, dict):
            continue
        positions[group_id].append(
            (
                float(location.get("X", 0.0)),
                float(location.get("Y", 0.0)),
                float(location.get("Z", 0.0)),
            )
        )

    return dict(positions), unknown_foliage


class _Cluster:
    __slots__ = (
        "min_x",
        "max_x",
        "min_y",
        "max_y",
        "min_z",
        "max_z",
        "sum_x",
        "sum_y",
        "sum_z",
        "count",
        "threshold",
        "max_count",
    )

    def __init__(self, x: float, y: float, z: float, threshold: float, max_count: int) -> None:
        self.min_x = self.max_x = x
        self.min_y = self.max_y = y
        self.min_z = self.max_z = z
        self.sum_x = x
        self.sum_y = y
        self.sum_z = z
        self.count = 1
        self.threshold = threshold
        self.max_count = max_count

    def can_accept(self, x: float, y: float) -> bool:
        if self.count >= self.max_count:
            return False

        threshold = self.threshold
        if not (
            x < self.min_x + threshold
            and x > self.max_x - threshold
            and y < self.min_y + threshold
            and y > self.max_y - threshold
        ):
            return False

        dx = x - self.center_x
        dy = y - self.center_y
        return dx * dx + dy * dy <= threshold * threshold

    def add(self, x: float, y: float, z: float) -> None:
        self.min_x = min(self.min_x, x)
        self.max_x = max(self.max_x, x)
        self.min_y = min(self.min_y, y)
        self.max_y = max(self.max_y, y)
        self.min_z = min(self.min_z, z)
        self.max_z = max(self.max_z, z)
        self.sum_x += x
        self.sum_y += y
        self.sum_z += z
        self.count += 1

    def try_add(self, x: float, y: float, z: float) -> bool:
        if not self.can_accept(x, y):
            return False
        self.add(x, y, z)
        return True

    def try_combine(self, other: "_Cluster") -> bool:
        if self.count + other.count > self.max_count:
            return False

        threshold = self.threshold
        combined_min_x = min(self.min_x, other.min_x)
        combined_max_x = max(self.max_x, other.max_x)
        combined_min_y = min(self.min_y, other.min_y)
        combined_max_y = max(self.max_y, other.max_y)
        if combined_max_x - combined_min_x > threshold or combined_max_y - combined_min_y > threshold:
            return False

        dx = self.center_x - other.center_x
        dy = self.center_y - other.center_y
        if dx * dx + dy * dy > threshold * threshold:
            return False

        self.min_x = combined_min_x
        self.max_x = combined_max_x
        self.min_y = combined_min_y
        self.max_y = combined_max_y
        self.min_z = min(self.min_z, other.min_z)
        self.max_z = max(self.max_z, other.max_z)
        self.sum_x += other.sum_x
        self.sum_y += other.sum_y
        self.sum_z += other.sum_z
        self.count += other.count
        return True

    @property
    def center_x(self) -> float:
        return self.sum_x / self.count

    @property
    def center_y(self) -> float:
        return self.sum_y / self.count

    def distance_sq(self, x: float, y: float) -> float:
        dx = x - self.center_x
        dy = y - self.center_y
        return dx * dx + dy * dy


def resolve_cluster_threshold(
    min_x: float,
    min_y: float,
    max_x: float,
    max_y: float,
    threshold: float | None,
) -> float:
    if threshold is not None and threshold > 0:
        return threshold

    world_span = max(max_x - min_x, max_y - min_y)
    pixels_to_world = world_span / 4096.0
    resolved = pixels_to_world * AUTO_CLUSTER_TARGET_PIXELS
    return max(AUTO_CLUSTER_MIN_THRESHOLD, min(AUTO_CLUSTER_MAX_THRESHOLD, resolved))


def resolve_partition_size(threshold: float, partition: float | None) -> float:
    if partition is not None and partition > 0:
        return partition
    return max(threshold * 3.0, 2500.0)


def cluster_positions(
    positions: list[tuple[float, float, float]],
    min_x: float,
    min_y: float,
    max_x: float,
    max_y: float,
    threshold: float | None = None,
    partition: float | None = None,
    max_cluster_count: int = DEFAULT_MAX_CLUSTER_COUNT,
) -> list[tuple[float, float, int]]:
    threshold = resolve_cluster_threshold(min_x, min_y, max_x, max_y, threshold)
    partition = resolve_partition_size(threshold, partition)
    margin = max(threshold, 500.0)
    positions = [
        position
        for position in positions
        if (
            math.isfinite(position[0])
            and math.isfinite(position[1])
            and math.isfinite(position[2])
            and min_x - margin <= position[0] <= max_x + margin
            and min_y - margin <= position[1] <= max_y + margin
        )
    ]
    if not positions:
        return []
    positions.sort(key=lambda position: (position[0], position[1], position[2]))

    width = max(max_x - min_x, partition)
    height = max(max_y - min_y, partition)
    cells_x = max(1, int(math.ceil(width / partition)))
    cells_y = max(1, int(math.ceil(height / partition)))
    cells: list[list[_Cluster]] = [[] for _ in range(cells_x * cells_y)]

    def cell_index(x: float, y: float) -> int:
        cx = min(max(int(math.floor((x - min_x) / partition)), 0), cells_x - 1)
        cy = min(max(int(math.floor((y - min_y) / partition)), 0), cells_y - 1)
        return cy * cells_x + cx

    for x, y, z in positions:
        index = cell_index(x, y)
        clusters = cells[index]
        best_cluster: _Cluster | None = None
        best_distance = math.inf
        for cluster in clusters:
            if not cluster.can_accept(x, y):
                continue
            distance = cluster.distance_sq(x, y)
            if distance < best_distance:
                best_distance = distance
                best_cluster = cluster
        if best_cluster is None:
            clusters.append(_Cluster(x, y, z, threshold, max_cluster_count))
        else:
            best_cluster.add(x, y, z)

    for cy in range(cells_y):
        for cx in range(cells_x):
            targets = cells[cy * cells_x + cx]
            neighbor_offsets = ((1, 0), (0, 1), (1, 1), (-1, 1))
            for dx, dy in neighbor_offsets:
                nx = cx + dx
                ny = cy + dy
                if not (0 <= nx < cells_x and 0 <= ny < cells_y):
                    continue
                sources = cells[ny * cells_x + nx]
                for target in targets:
                    source_index = 0
                    while source_index < len(sources):
                        if target.try_combine(sources[source_index]):
                            sources.pop(source_index)
                        else:
                            source_index += 1

    result: list[tuple[float, float, int]] = []
    for clusters in cells:
        for cluster in clusters:
            result.append((cluster.center_x, cluster.center_y, cluster.count))
    result.sort(key=lambda cluster: (cluster[0], cluster[1]))
    return result


def build_datamaps_json(
    world: WorldConfig,
    clustered: dict[str, list[tuple[float, float, int]]],
) -> dict:
    min_x, min_y, max_x, max_y = world.bounds
    output: dict = {
        "$schema": "https://icarus.wiki.gg/extensions/DataMaps/schemas/v17.3.json",
        "crs": {
            "order": "xy",
            "topLeft": [min_x, min_y],
            "bottomRight": [max_x, max_y],
        },
        "backgrounds": [
            {
                "name": world.display_name,
                "associatedLayer": None,
                "image": world.background_image,
                "overlays": [
                    {
                        "name": "Grid",
                        "image": "MAP Grid 4096.png",
                        "at": [[min_x, min_y], [max_x, max_y]],
                    }
                ],
            }
        ],
        "settings": {
            "enableSearch": True,
            "showCoordinates": True,
        },
    }

    groups: dict[str, dict] = {}
    for group_id in sorted(clustered):
        meta = PLANT_GROUPS.get(group_id)
        if not meta:
            continue
        groups[group_id] = {
            "name": meta.display_name,
            "size": list(meta.size),
            "icon": meta.icon,
        }
    output["groups"] = groups

    markers: dict[str, list[dict]] = {}
    for group_id in sorted(clustered):
        meta = PLANT_GROUPS.get(group_id)
        if not meta or group_id not in groups:
            continue
        article = meta.article or meta.display_name
        group_markers: list[dict] = []
        for index, (center_x, center_y, count) in enumerate(clustered[group_id], start=1):
            group_markers.append(
                {
                    "x": round(center_x, 2),
                    "y": round(center_y, 2),
                    "id": f"{group_id}-{index}",
                    "name": meta.display_name,
                    "description": f"Amount of {meta.display_name}: {count}",
                    "article": article,
                }
            )
        markers[group_id] = group_markers
    output["markers"] = markers
    return output


def write_datamaps_json(path: str | Path, data: dict) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as handle:
        json.dump(data, handle, indent="\t", ensure_ascii=False)
        handle.write("\n")


def dedupe_exact_positions(
    positions: list[tuple[float, float, float]],
) -> list[tuple[float, float, float]]:
    seen: set[tuple[float, float, float]] = set()
    deduped: list[tuple[float, float, float]] = []
    for position in positions:
        key = (round(position[0], 2), round(position[1], 2), round(position[2], 2))
        if key in seen:
            continue
        seen.add(key)
        deduped.append(position)
    return deduped


def extract_world_positions(
    worlds: list[WorldConfig],
    text_dir: str | Path,
    raw_dir: str | Path,
    foliage_to_group: dict[str, str],
    resource_actor_by_foliage: dict[str, str],
    static_mesh_groups: dict[str, str] | None = None,
) -> tuple[
    dict[str, dict[str, list[tuple[float, float, float]]]],
    dict[str, Counter[str]],
    dict[str, int],
]:
    static_mesh_groups = static_mesh_groups or STATIC_MESH_GROUPS
    dedupe_group_ids = set(static_mesh_groups.values())

    positions_by_world: dict[str, dict[str, list[tuple[float, float, float]]]] = {
        world.world_id: defaultdict(list) for world in worlds
    }
    unknown_by_world: dict[str, Counter[str]] = {
        world.world_id: Counter() for world in worlds
    }
    processed_counts: dict[str, int] = {}

    for world in worlds:
        grouped_positions = positions_by_world[world.world_id]
        processed_levels = 0
        for package_ref in world.package_refs:
            json_path = exported_json_path(text_dir, package_ref)
            uexp_path = exported_uexp_path(raw_dir, package_ref)
            if not json_path.is_file() or not uexp_path.is_file():
                continue

            level_positions, unknown = process_sublevel(
                json_path,
                uexp_path,
                foliage_to_group,
                resource_actor_by_foliage,
                static_mesh_groups,
            )
            processed_levels += 1
            for group_id, group_positions in level_positions.items():
                grouped_positions[group_id].extend(group_positions)
            unknown_by_world[world.world_id].update(unknown)

        for group_id in dedupe_group_ids:
            if group_id in grouped_positions:
                grouped_positions[group_id] = dedupe_exact_positions(
                    grouped_positions[group_id]
                )

        positions_by_world[world.world_id] = dict(grouped_positions)
        processed_counts[world.world_id] = processed_levels

    return positions_by_world, unknown_by_world, processed_counts


def serialize_position_groups(
    grouped_positions: dict[str, list[tuple[float, float, float]]],
) -> dict[str, list[list[float]]]:
    return {
        group_id: [
            [round(x, 2), round(y, 2), round(z, 2)]
            for x, y, z in positions
        ]
        for group_id, positions in sorted(grouped_positions.items())
        if positions
    }


def serialize_positions_by_world(
    positions_by_world: dict[str, dict[str, list[tuple[float, float, float]]]],
) -> dict[str, dict[str, list[list[float]]]]:
    serialized: dict[str, dict[str, list[list[float]]]] = {}
    for world_id, grouped_positions in sorted(positions_by_world.items()):
        world_data = serialize_position_groups(grouped_positions)
        if world_data:
            serialized[world_id] = world_data
    return serialized


def load_partial_positions(
    partials_dir: str | Path,
) -> dict[str, dict[str, list[tuple[float, float, float]]]]:
    merged: dict[str, dict[str, list[tuple[float, float, float]]]] = defaultdict(
        lambda: defaultdict(list)
    )
    partial_paths = sorted(Path(partials_dir).rglob("*.json"))
    for partial_path in partial_paths:
        data = _load_json(partial_path)
        worlds = data.get("worlds", data) if isinstance(data, dict) else {}
        if not isinstance(worlds, dict):
            continue
        for world_id, grouped_positions in worlds.items():
            if not isinstance(grouped_positions, dict):
                continue
            for group_id, positions in grouped_positions.items():
                if not isinstance(positions, list):
                    continue
                merged[world_id][group_id].extend(
                    (
                        float(position[0]),
                        float(position[1]),
                        float(position[2]) if len(position) > 2 else 0.0,
                    )
                    for position in positions
                    if isinstance(position, (list, tuple)) and len(position) >= 2
                )
    return {
        world_id: {group_id: list(positions) for group_id, positions in grouped.items()}
        for world_id, grouped in merged.items()
    }


def exported_json_path(output_root: str | Path, package_ref: str) -> Path:
    asset_path = _package_ref_to_asset_path(package_ref)
    return Path(output_root, *asset_path.split("/")).with_suffix(".json")


def exported_uexp_path(output_root: str | Path, package_ref: str) -> Path:
    asset_path = _package_ref_to_asset_path(package_ref)
    return Path(output_root, *asset_path.split("/")).with_suffix(".uexp")
