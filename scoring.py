from __future__ import annotations

import csv
import io
import json
import math
import zipfile
from collections import Counter
from dataclasses import dataclass
from statistics import mean
from typing import Any
from xml.etree import ElementTree as ET


NS = {
    "main": "http://schemas.openxmlformats.org/spreadsheetml/2006/main",
    "rel": "http://schemas.openxmlformats.org/officeDocument/2006/relationships",
}
MAIN_NS = f"{{{NS['main']}}}"


NUMERIC_NA = {"", "N", "NA", "None", "null"}


PRESENCE_LEGEND = [
    {"value": 1, "label": "Mangroves Present", "color": "#1a7f37"},
    {"value": 0, "label": "Mangroves Absent", "color": "#9ca3af"},
]

MODIFICATION_LEGEND = [
    {"value": 0, "label": "Natural", "color": "#1a7f37"},
    {"value": 1, "label": "Modified", "color": "#f59e0b"},
    {"value": 2, "label": "Impervious", "color": "#b91c1c"},
]

DAMAGE_LEGEND = [
    {"value": 0, "label": "No Damage", "color": "#1a7f37"},
    {"value": 1, "label": "Minor Damage", "color": "#f59e0b"},
    {"value": 2, "label": "Moderate Damage", "color": "#ea580c"},
    {"value": 3, "label": "Major Damage", "color": "#b91c1c"},
]

DENSITY_LEGEND = [
    {"value": 1, "label": "1 Sparse", "color": "#f97316"},
    {"value": 2, "label": "2 Isolated Stand/Patch", "color": "#facc15"},
    {"value": 3, "label": "3 Open Continuous Forest", "color": "#65a30d"},
    {"value": 4, "label": "4 Closed Continuous Forest", "color": "#166534"},
]

MATURITY_LEGEND = [
    {"value": 1, "label": "1 Seedlings", "color": "#f97316"},
    {"value": 2, "label": "2 Saplings", "color": "#facc15"},
    {"value": 3, "label": "3 Young Mature", "color": "#65a30d"},
    {"value": 4, "label": "4 Mature Established Trees", "color": "#166534"},
]

CONDITION_LEGEND = [
    {"value": 0, "label": "0 Dead or Almost Dead", "color": "#111827"},
    {"value": 1, "label": "1 Very Poor Condition", "color": "#dc2626"},
    {"value": 2, "label": "2 Poor Condition", "color": "#f97316"},
    {"value": 3, "label": "3 Moderate Condition", "color": "#facc15"},
    {"value": 4, "label": "4 Healthy", "color": "#166534"},
]


@dataclass
class Dataset:
    points_10m: list[dict[str, Any]]
    points_50m: list[dict[str, Any]]
    metadata: dict[str, Any]


class ReportError(Exception):
    pass


def _clean(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, float) and math.isnan(value):
        return ""
    return str(value).strip()


def _parse_float(value: Any) -> float | None:
    text = _clean(value)
    if text in NUMERIC_NA:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _parse_int(value: Any) -> int | None:
    number = _parse_float(value)
    if number is None:
        return None
    return int(round(number))


def _sort_key(value: str) -> tuple[int, Any]:
    text = _clean(value)
    try:
        return (0, int(float(text)))
    except ValueError:
        return (1, text)


def parse_sections(selection: str) -> list[str] | None:
    text = _clean(selection)
    if not text or text.lower() == "all":
        return None
    sections = []
    for chunk in text.split(","):
        item = chunk.strip()
        if item:
            sections.append(item)
    if not sections:
        return None
    return sorted(set(sections), key=_sort_key)


def _shared_strings(zf: zipfile.ZipFile) -> list[str]:
    if "xl/sharedStrings.xml" not in zf.namelist():
        return []
    root = ET.fromstring(zf.read("xl/sharedStrings.xml"))
    out: list[str] = []
    for item in root.findall("main:si", NS):
        out.append("".join(text.text or "" for text in item.findall(".//main:t", NS)))
    return out


def _cell_value(cell: ET.Element, shared: list[str]) -> str:
    cell_type = cell.attrib.get("t")
    if cell_type == "s":
        index = int(cell.findtext("main:v", default="0", namespaces=NS) or 0)
        return shared[index]
    if cell_type == "inlineStr":
        return "".join(text.text or "" for text in cell.findall(".//main:t", NS))
    return cell.findtext("main:v", default="", namespaces=NS)


def _column_name(cell_ref: str) -> str:
    return "".join(ch for ch in cell_ref if ch.isalpha())


def _sheet_xml_path_by_name(zf: zipfile.ZipFile) -> dict[str, str]:
    workbook = ET.fromstring(zf.read("xl/workbook.xml"))
    rels = ET.fromstring(zf.read("xl/_rels/workbook.xml.rels"))
    rel_map = {rel.attrib["Id"]: rel.attrib["Target"] for rel in rels}
    result: dict[str, str] = {}
    for sheet in workbook.find("main:sheets", NS):
        rel_id = sheet.attrib["{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id"]
        result[sheet.attrib["name"]] = "xl/" + rel_map[rel_id].lstrip("/")
    return result


def _iter_sheet_rows(zf: zipfile.ZipFile, sheet_path: str, shared: list[str]):
    with zf.open(sheet_path) as sheet_file:
        for _, row in ET.iterparse(sheet_file, events=("end",)):
            if row.tag != f"{MAIN_NS}row":
                continue
            row_number = int(row.attrib.get("r", "0") or 0)
            values: dict[str, str] = {}
            for cell in row.findall(f"{MAIN_NS}c"):
                column = _column_name(cell.attrib.get("r", ""))
                if not column:
                    continue
                values[column] = _clean(_cell_value(cell, shared))
            yield row_number, values
            row.clear()


def _sheet_headers(zf: zipfile.ZipFile, sheet_path: str, shared: list[str]) -> dict[str, str]:
    for row_number, values in _iter_sheet_rows(zf, sheet_path, shared):
        if row_number == 3:
            return values
        if row_number > 3:
            break
    return {}


def _read_sheet_rows(zf: zipfile.ZipFile, sheet_path: str, shared: list[str]) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    header_by_col: dict[str, str] = {}
    for row_number, values in _iter_sheet_rows(zf, sheet_path, shared):
        if row_number == 3:
            header_by_col = values
            continue
        if row_number < 4:
            continue
        record = {header_by_col.get(column, column): value for column, value in values.items()}
        if any(_clean(value) for value in record.values()):
            rows.append(record)
    return rows


def _find_sheet(sheet_map: dict[str, str], expected_headers: set[str]) -> str:
    for _, path in sheet_map.items():
        # Fast path by name is unreliable because workbook names vary by estuary/year.
        pass
    return ""


def _sheet_with_headers(zf: zipfile.ZipFile, expected_headers: set[str], shared: list[str]) -> str:
    for _, path in _sheet_xml_path_by_name(zf).items():
        headers = set(_sheet_headers(zf, path, shared).values())
        if not headers:
            continue
        if expected_headers.issubset(headers):
            return path
    raise ReportError(f"Could not find worksheet with headers: {sorted(expected_headers)}")


def load_workbook_dataset(file_bytes: bytes, filename: str = "") -> Dataset:
    try:
        with zipfile.ZipFile(io.BytesIO(file_bytes)) as zf:
            shared = _shared_strings(zf)
            sheet_10m = _sheet_with_headers(
                zf,
                {"id_10m", "Section", "Assessed25", "Mangrove_Presence_25", "Naturalness25", "Physical_Damage25"},
                shared,
            )
            sheet_50m = _sheet_with_headers(
                zf,
                {"id_10m", "Section", "Assessed25", "Mangrove_Presence25_50", "Density25", "Maturity25", "Condition_Score25"},
                shared,
            )
            points_10m = _read_sheet_rows(zf, sheet_10m, shared)
            points_50m = _read_sheet_rows(zf, sheet_50m, shared)
    except zipfile.BadZipFile as exc:
        raise ReportError(
            "Workbook upload must be an .xlsx or .xlsm file. Legacy .xls files are not supported."
        ) from exc
    return _finalize_dataset(points_10m, points_50m, source_name=filename or "Workbook")


def _read_csv(file_bytes: bytes) -> list[dict[str, str]]:
    text = file_bytes.decode("utf-8-sig")
    reader = csv.DictReader(io.StringIO(text))
    return [{key: _clean(value) for key, value in row.items()} for row in reader]


def load_csv_dataset(csv_10m_bytes: bytes, csv_50m_bytes: bytes, name_10m: str = "", name_50m: str = "") -> Dataset:
    points_10m = _read_csv(csv_10m_bytes)
    points_50m = _read_csv(csv_50m_bytes)
    required_10m = {"id_10m", "Section", "Assessed25", "Mangrove_Presence_25", "Naturalness25", "Physical_Damage25"}
    required_50m = {"id_10m", "Section", "Assessed25", "Mangrove_Presence25_50", "Density25", "Maturity25", "Condition_Score25"}
    if not points_10m or not required_10m.issubset(points_10m[0].keys()):
        raise ReportError(f"10m CSV must contain headers: {sorted(required_10m)}")
    if not points_50m or not required_50m.issubset(points_50m[0].keys()):
        raise ReportError(f"50m CSV must contain headers: {sorted(required_50m)}")
    return _finalize_dataset(points_10m, points_50m, source_name=f"{name_10m or '10m CSV'} + {name_50m or '50m CSV'}")


def _finalize_dataset(points_10m: list[dict[str, str]], points_50m: list[dict[str, str]], source_name: str) -> Dataset:
    coords_by_id: dict[str, tuple[float | None, float | None]] = {}
    sections = Counter()
    for row in points_10m:
        point_id = _clean(row.get("id_10m"))
        lon = _parse_float(row.get("lon_10m_point"))
        lat = _parse_float(row.get("lat_10m_point"))
        if point_id:
            coords_by_id[point_id] = (lon, lat)
        if _clean(row.get("Section")):
            sections[_clean(row.get("Section"))] += 1

    for row in points_50m:
        if _clean(row.get("lon_10m_point")) and _clean(row.get("lat_10m_point")):
            continue
        point_id = _clean(row.get("id_10m"))
        if point_id in coords_by_id:
            lon, lat = coords_by_id[point_id]
            if lon is not None:
                row["lon_10m_point"] = str(lon)
            if lat is not None:
                row["lat_10m_point"] = str(lat)

    sections_available = sorted({_clean(row.get("Section")) for row in points_10m if _clean(row.get("Section"))}, key=_sort_key)
    return Dataset(
        points_10m=points_10m,
        points_50m=points_50m,
        metadata={"source_name": source_name, "sections_available": sections_available},
    )


def _is_assessed_2025(row: dict[str, Any]) -> bool:
    return _clean(row.get("Assessed25")).upper() != "X"


def _filter_rows(rows: list[dict[str, Any]], sections: list[str] | None) -> list[dict[str, Any]]:
    selected = []
    section_set = set(sections or [])
    for row in rows:
        section = _clean(row.get("Section"))
        if sections is not None and section not in section_set:
            continue
        if not _is_assessed_2025(row):
            continue
        selected.append(row)
    return selected


def _modification_class(value: Any) -> int | None:
    raw = _parse_int(value)
    if raw is None:
        return None
    if raw <= 1:
        return 0
    if raw == 2:
        return 1
    return 2


def _presence_class(value: Any) -> int | None:
    raw = _parse_int(value)
    if raw is None:
        return None
    if raw not in {0, 1}:
        return None
    return raw


def _mean_or_none(values: list[float]) -> float | None:
    if not values:
        return None
    return mean(values)


def _band_grade(score: float | None) -> str | None:
    if score is None:
        return None
    if score >= 81:
        return "Very Good"
    if score >= 61:
        return "Good"
    if score >= 41:
        return "Moderate"
    if score >= 21:
        return "Poor"
    return "Very Poor"


def _positive_grade(value: float | None, bands: list[tuple[float, str]]) -> str | None:
    if value is None:
        return None
    for threshold, label in bands:
        if value >= threshold:
            return label
    return bands[-1][1]


def _inverse_grade(value: float | None, bands: list[tuple[float, str]]) -> str | None:
    if value is None:
        return None
    for threshold, label in bands:
        if value <= threshold:
            return label
    return bands[-1][1]


def _scale_up(value: float, bands: list[tuple[float, float, float, float]]) -> float:
    for lower, upper, score_low, score_high in bands:
        if value <= upper or upper == bands[-1][1]:
            if upper == lower:
                return score_high
            ratio = (value - lower) / (upper - lower)
            return score_low + (score_high - score_low) * max(0.0, min(1.0, ratio))
    return bands[-1][3]


def _scale_down(value: float, bands: list[tuple[float, float, float, float]]) -> float:
    for lower, upper, score_high, score_low in bands:
        if value <= upper:
            if upper == lower:
                return score_low
            ratio = (value - lower) / (upper - lower)
            return score_high + (score_low - score_high) * max(0.0, min(1.0, ratio))
    lower, upper, score_high, score_low = bands[-1]
    capped = min(value, upper)
    if upper == lower:
        return score_low
    ratio = (capped - lower) / (upper - lower)
    return score_high + (score_low - score_high) * max(0.0, min(1.0, ratio))


def standardize_cover(value: float | None) -> float | None:
    if value is None:
        return None
    value = max(0.0, min(value, 100.0))
    return _scale_up(
        value,
        [
            (0.0, 49.9, 0.0, 20.9),
            (50.0, 70.0, 21.0, 40.9),
            (70.1, 80.0, 41.0, 60.9),
            (80.1, 90.0, 61.0, 80.9),
            (90.1, 100.0, 81.0, 100.0),
        ],
    )


def standardize_density_or_maturity(value: float | None) -> float | None:
    if value is None:
        return None
    value = max(0.0, min(value, 4.0))
    return _scale_up(
        value,
        [
            (0.0, 1.99, 0.0, 20.9),
            (2.0, 2.5, 21.0, 40.9),
            (2.51, 3.25, 41.0, 60.9),
            (3.26, 3.75, 61.0, 80.9),
            (3.76, 4.0, 81.0, 100.0),
        ],
    )


def standardize_condition(value: float | None) -> float | None:
    if value is None:
        return None
    value = max(0.0, min(value, 4.0))
    return _scale_up(
        value,
        [
            (0.0, 1.99, 0.0, 20.9),
            (2.0, 2.5, 21.0, 40.9),
            (2.51, 3.0, 41.0, 60.9),
            (3.01, 3.5, 61.0, 80.9),
            (3.51, 4.0, 81.0, 100.0),
        ],
    )


def standardize_damage(value: float | None) -> float | None:
    if value is None:
        return None
    value = max(0.0, min(value, 100.0))
    return _scale_down(
        value,
        [
            (0.0, 1.0, 100.0, 81.0),
            (1.01, 3.0, 80.9, 61.0),
            (3.01, 7.0, 60.9, 41.0),
            (7.01, 15.0, 40.9, 21.0),
            (15.01, 100.0, 20.9, 0.0),
        ],
    )


def standardize_modification(value: float | None) -> float | None:
    if value is None:
        return None
    value = max(0.0, min(value, 100.0))
    return _scale_down(
        value,
        [
            (0.0, 2.0, 100.0, 81.0),
            (2.01, 6.0, 80.9, 61.0),
            (6.01, 14.0, 60.9, 41.0),
            (14.01, 30.0, 40.9, 21.0),
            (30.01, 100.0, 20.9, 0.0),
        ],
    )


def _weighted_impact_raw(classes: list[int], weight_map: dict[int, float]) -> float | None:
    if not classes:
        return None
    impacted = [value for value in classes if value > 0]
    if not impacted:
        return 0.0
    weighted_count = sum(weight_map.get(value, 0.0) for value in impacted)
    return weighted_count * (len(impacted) / len(classes))


def _presence_points(rows: list[dict[str, Any]], value_key: str) -> tuple[list[int], list[dict[str, Any]]]:
    values = []
    valid_rows = []
    for row in rows:
        value = _presence_class(row.get(value_key))
        if value is None:
            continue
        values.append(value)
        valid_rows.append(row)
    return values, valid_rows


def _section_label(section_values: list[str] | None) -> str:
    if not section_values:
        return "All sections"
    return ", ".join(section_values)


def _score_group(
    label: str,
    rows_10m: list[dict[str, Any]],
    rows_50m: list[dict[str, Any]],
) -> dict[str, Any]:
    presence_values, presence_rows = _presence_points(rows_10m, "Mangrove_Presence_25")
    cover_percent = None
    if presence_values:
        cover_percent = (sum(1 for value in presence_values if value == 1) / len(presence_values)) * 100.0

    density_values: list[float] = []
    maturity_values: list[float] = []
    condition_values: list[float] = []
    for row in rows_50m:
        if _presence_class(row.get("Mangrove_Presence25_50")) != 1:
            continue
        density = _parse_float(row.get("Density25"))
        maturity = _parse_float(row.get("Maturity25"))
        condition = _parse_float(row.get("Condition_Score25"))
        if density is not None:
            density_values.append(density)
        if maturity is not None:
            maturity_values.append(maturity)
        if condition is not None:
            condition_values.append(condition)

    damage_classes: list[int] = []
    for row in rows_10m:
        if _presence_class(row.get("Mangrove_Presence_25")) != 1:
            continue
        damage = _parse_int(row.get("Physical_Damage25"))
        if damage is None:
            continue
        damage_classes.append(max(0, min(damage, 3)))

    modification_classes: list[int] = []
    for row in rows_10m:
        modification = _modification_class(row.get("Naturalness25"))
        if modification is None:
            continue
        modification_classes.append(modification)

    density_mean = _mean_or_none(density_values)
    maturity_mean = _mean_or_none(maturity_values)
    condition_mean = _mean_or_none(condition_values)
    damage_raw = _weighted_impact_raw(damage_classes, {1: 1.0, 2: 1.5, 3: 2.0})
    modification_raw = _weighted_impact_raw(modification_classes, {1: 0.5, 2: 1.0})

    cover_score = standardize_cover(cover_percent)
    density_score = standardize_density_or_maturity(density_mean)
    maturity_score = standardize_density_or_maturity(maturity_mean)
    condition_score = standardize_condition(condition_mean)
    damage_score = standardize_damage(damage_raw)
    modification_score = standardize_modification(modification_raw)

    structure_score = _mean_or_none([score for score in [cover_score, density_score, maturity_score] if score is not None])
    impact_score = _mean_or_none([score for score in [damage_score, modification_score] if score is not None])
    indicator_score = _mean_or_none([score for score in [structure_score, condition_score, impact_score] if score is not None])

    return {
        "label": label,
        "section_label": label,
        "sample_counts": {
            "points_10m": len(rows_10m),
            "points_50m": len(rows_50m),
            "cover_points": len(presence_values),
            "density_points": len(density_values),
            "maturity_points": len(maturity_values),
            "condition_points": len(condition_values),
            "damage_points": len(damage_classes),
            "modification_points": len(modification_classes),
        },
        "metrics": {
            "cover": {
                "raw": cover_percent,
                "raw_grade": _positive_grade(
                    cover_percent,
                    [(90.0, "Very Good"), (80.0, "Good"), (70.0, "Moderate"), (50.0, "Poor"), (0.0, "Very Poor")],
                ),
                "score": cover_score,
                "score_grade": _band_grade(cover_score),
            },
            "density": {
                "raw": density_mean,
                "raw_grade": _positive_grade(
                    density_mean,
                    [(3.75, "Very Good"), (3.25, "Good"), (2.5, "Moderate"), (2.0, "Poor"), (0.0, "Very Poor")],
                ),
                "score": density_score,
                "score_grade": _band_grade(density_score),
            },
            "maturity": {
                "raw": maturity_mean,
                "raw_grade": _positive_grade(
                    maturity_mean,
                    [(3.75, "Very Good"), (3.25, "Good"), (2.5, "Moderate"), (2.0, "Poor"), (0.0, "Very Poor")],
                ),
                "score": maturity_score,
                "score_grade": _band_grade(maturity_score),
            },
            "condition": {
                "raw": condition_mean,
                "raw_grade": _positive_grade(
                    condition_mean,
                    [(3.5, "Very Good"), (3.0, "Good"), (2.5, "Moderate"), (2.0, "Poor"), (0.0, "Very Poor")],
                ),
                "score": condition_score,
                "score_grade": _band_grade(condition_score),
            },
            "damage": {
                "raw": damage_raw,
                "raw_grade": _inverse_grade(
                    damage_raw,
                    [(1.0, "Very Good"), (3.0, "Good"), (7.0, "Moderate"), (15.0, "Poor"), (100.0, "Very Poor")],
                ),
                "score": damage_score,
                "score_grade": _band_grade(damage_score),
            },
            "modification": {
                "raw": modification_raw,
                "raw_grade": _inverse_grade(
                    modification_raw,
                    [(2.0, "Very Good"), (6.0, "Good"), (14.0, "Moderate"), (30.0, "Poor"), (100.0, "Very Poor")],
                ),
                "score": modification_score,
                "score_grade": _band_grade(modification_score),
            },
            "structure": {
                "raw": None,
                "raw_grade": None,
                "score": structure_score,
                "score_grade": _band_grade(structure_score),
            },
            "impact": {
                "raw": None,
                "raw_grade": None,
                "score": impact_score,
                "score_grade": _band_grade(impact_score),
            },
            "indicator": {
                "raw": None,
                "raw_grade": None,
                "score": indicator_score,
                "score_grade": _band_grade(indicator_score),
            },
        },
    }


def _map_points_10m(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    points = []
    for row in rows:
        lon = _parse_float(row.get("lon_10m_point"))
        lat = _parse_float(row.get("lat_10m_point"))
        if lon is None or lat is None:
            continue
        presence = _presence_class(row.get("Mangrove_Presence_25"))
        damage = _parse_int(row.get("Physical_Damage25"))
        modification = _modification_class(row.get("Naturalness25"))
        base = {
            "lat": lat,
            "lon": lon,
            "section": _clean(row.get("Section")),
            "point_id": _clean(row.get("id_10m")),
        }
        if presence is not None:
            points.append({**base, "metric": "presence", "value": presence})
        if damage is not None:
            points.append({**base, "metric": "damage", "value": max(0, min(damage, 3))})
        if modification is not None:
            points.append({**base, "metric": "modification", "value": modification})
    return points


def _map_points_50m(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    points = []
    for row in rows:
        lon = _parse_float(row.get("lon_10m_point"))
        lat = _parse_float(row.get("lat_10m_point"))
        if lon is None or lat is None:
            continue
        base = {
            "lat": lat,
            "lon": lon,
            "section": _clean(row.get("Section")),
            "point_id": _clean(row.get("id_10m")),
        }
        density = _parse_int(row.get("Density25"))
        maturity = _parse_int(row.get("Maturity25"))
        condition = _parse_int(row.get("Condition_Score25"))
        if density is not None:
            points.append({**base, "metric": "density", "value": max(1, min(density, 4))})
        if maturity is not None:
            points.append({**base, "metric": "maturity", "value": max(1, min(maturity, 4))})
        if condition is not None:
            points.append({**base, "metric": "condition", "value": max(0, min(condition, 4))})
    return points


def _metric_map_configs(points_10m: list[dict[str, Any]], points_50m: list[dict[str, Any]]) -> list[dict[str, Any]]:
    configs = [
        ("presence", "10 m Mangrove Presence", PRESENCE_LEGEND),
        ("modification", "10 m Shoreline Modification", MODIFICATION_LEGEND),
        ("damage", "10 m Mangrove Damage", DAMAGE_LEGEND),
        ("density", "50 m Mangrove Density", DENSITY_LEGEND),
        ("maturity", "50 m Mangrove Maturity", MATURITY_LEGEND),
        ("condition", "50 m Mangrove Condition", CONDITION_LEGEND),
    ]
    all_points = points_10m + points_50m
    output = []
    for metric_key, title, legend in configs:
        metric_points = [point for point in all_points if point["metric"] == metric_key]
        if not metric_points:
            continue
        color_map = {entry["value"]: entry["color"] for entry in legend}
        for point in metric_points:
            point["color"] = color_map.get(point["value"], "#6b7280")
        output.append(
            {
                "id": f"map-{metric_key}",
                "metric": metric_key,
                "title": title,
                "legend": legend,
                "points": metric_points,
            }
        )
    return output


def _format_number(value: float | None, digits: int = 1) -> str:
    if value is None:
        return "N/A"
    return f"{value:.{digits}f}"


def _table_rows(score_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    output = []
    for row in score_rows:
        metrics = row["metrics"]
        output.append(
            {
                "label": row["label"],
                "cover": _format_number(metrics["cover"]["score"]),
                "cover_grade": metrics["cover"]["score_grade"] or "N/A",
                "cover_raw": _format_number(metrics["cover"]["raw"]),
                "density": _format_number(metrics["density"]["score"]),
                "density_grade": metrics["density"]["score_grade"] or "N/A",
                "maturity": _format_number(metrics["maturity"]["score"]),
                "maturity_grade": metrics["maturity"]["score_grade"] or "N/A",
                "condition": _format_number(metrics["condition"]["score"]),
                "condition_grade": metrics["condition"]["score_grade"] or "N/A",
                "canopy_cover_score": _format_number(metrics["condition"]["score"]),
                "damage": _format_number(metrics["damage"]["score"]),
                "damage_grade": metrics["damage"]["score_grade"] or "N/A",
                "modification": _format_number(metrics["modification"]["score"]),
                "modification_grade": metrics["modification"]["score_grade"] or "N/A",
                "structure_score": _format_number(metrics["structure"]["score"]),
                "structure_grade": metrics["structure"]["score_grade"] or "N/A",
                "impact_score": _format_number(metrics["impact"]["score"]),
                "impact_grade": metrics["impact"]["score_grade"] or "N/A",
                "indicator_score": _format_number(metrics["indicator"]["score"]),
                "indicator_grade": metrics["indicator"]["score_grade"] or "N/A",
            }
        )
    return output


def generate_report(dataset: Dataset, sections: list[str] | None = None, output_mode: str = "both") -> dict[str, Any]:
    rows_10m = _filter_rows(dataset.points_10m, sections)
    rows_50m = _filter_rows(dataset.points_50m, sections)
    if not rows_10m and not rows_50m:
        raise ReportError("No assessed 2025 rows remain after filtering. Check the section selection and Assessed25 values.")

    available_sections = sorted({_clean(row.get("Section")) for row in rows_10m if _clean(row.get("Section"))}, key=_sort_key)

    score_rows: list[dict[str, Any]] = []
    if output_mode in {"pooled", "both"}:
        pooled_label = f"Combined ({_section_label(sections)})" if sections else "All sections Combined"
        score_rows.append(_score_group(pooled_label, rows_10m, rows_50m))
    if output_mode in {"by_section", "both"}:
        section_values = sections if sections is not None else available_sections
        for section in sorted(section_values, key=_sort_key):
            section_10m = [row for row in rows_10m if _clean(row.get("Section")) == section]
            section_50m = [row for row in rows_50m if _clean(row.get("Section")) == section]
            if section_10m or section_50m:
                score_rows.append(_score_group(f"Section {section}", section_10m, section_50m))

    warnings = [
        "Naturalness25 is interpreted as shoreline modification using workbook-compatible mapping: 0-1 natural, 2 modified, 3 impervious."
    ]

    point_maps = _metric_map_configs(_map_points_10m(rows_10m), _map_points_50m(rows_50m))
    return {
        "source_name": dataset.metadata["source_name"],
        "sections_available": dataset.metadata["sections_available"],
        "selected_sections": sections or dataset.metadata["sections_available"],
        "selected_section_label": _section_label(sections),
        "output_mode": output_mode,
        "score_rows": score_rows,
        "table_rows": _table_rows(score_rows),
        "point_maps": point_maps,
        "warnings": warnings,
        "sample_counts": {
            "rows_10m": len(rows_10m),
            "rows_50m": len(rows_50m),
        },
    }


def maps_json(report: dict[str, Any]) -> str:
    return json.dumps(report["point_maps"])
