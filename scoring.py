from __future__ import annotations

import csv
import io
import json
import math
import re
import zipfile
from collections import Counter
from dataclasses import dataclass
from statistics import mean, median
from typing import Any
from xml.etree import ElementTree as ET


NS = {
    "main": "http://schemas.openxmlformats.org/spreadsheetml/2006/main",
    "rel": "http://schemas.openxmlformats.org/officeDocument/2006/relationships",
}
MAIN_NS = f"{{{NS['main']}}}"


NUMERIC_NA = {"", "N", "NA", "None", "null"}
HEADER_ALIASES = {
    "ReportCard": "Section",
}
SHEET_SCHEMAS = {
    "10m": {
        "static_fields": {
            "id_10m": ("id_10m",),
            "Section": ("Section",),
        },
        "year_fields": {
            "Assessed": ("Assessed{yy}",),
            "Mangrove_Presence_10m": ("Mangrove_Presence_{yy}", "Mangrove_Presence{yy}"),
            "Naturalness": ("Naturalness{yy}",),
            "Physical_Damage": ("Physical_Damage{yy}",),
        },
    },
    "50m": {
        "static_fields": {
            "id_10m": ("id_10m",),
            "Section": ("Section",),
        },
        "year_fields": {
            "Assessed": ("Assessed{yy}",),
            "Mangrove_Presence_50m": ("Mangrove_Presence{yy}_50", "Mangrove_Presence_{yy}_50"),
            "Density": ("Density{yy}",),
            "Maturity": ("Maturity{yy}",),
            "Condition_Score": ("Condition_Score{yy}",),
        },
    },
}


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


def _normalize_header(value: str, available_headers: set[str] | None = None) -> str:
    text = _clean(value)
    if text == "ReportCard" and available_headers and "Section" in available_headers:
        return text
    return HEADER_ALIASES.get(text, text)


def _candidate_years(headers: set[str]) -> list[str]:
    years = set()
    for header in headers:
        for match in re.finditer(r"(?<!\d)(\d{2})(?!\d)", header):
            years.add(match.group(1))
    return sorted(years, key=int, reverse=True)


def _header_match(headers: set[str], candidates: tuple[str, ...]) -> str | None:
    for candidate in candidates:
        if candidate in headers:
            return candidate
    return None


def _schema_field_map(headers: set[str], schema_name: str) -> dict[str, Any] | None:
    schema = SHEET_SCHEMAS[schema_name]
    field_map: dict[str, str] = {}

    for canonical, candidates in schema["static_fields"].items():
        header = _header_match(headers, candidates)
        if header is None:
            return None
        field_map[canonical] = header

    for year_suffix in _candidate_years(headers):
        year_field_map = dict(field_map)
        for canonical, templates in schema["year_fields"].items():
            header = _header_match(headers, tuple(template.format(yy=year_suffix) for template in templates))
            if header is None:
                break
            year_field_map[canonical] = header
        else:
            return {
                "sheet_type": schema_name,
                "assessment_year": 2000 + int(year_suffix),
                "field_map": year_field_map,
            }

    return None


def _normalize_rows(rows: list[dict[str, str]], field_map: dict[str, str]) -> list[dict[str, str]]:
    normalized_rows = []
    for row in rows:
        normalized = dict(row)
        for canonical, actual in field_map.items():
            normalized[canonical] = row.get(actual, "")
        normalized_rows.append(normalized)
    return normalized_rows


def _indicator_mapping(metadata: dict[str, Any]) -> list[dict[str, str]]:
    fields_10m = metadata["field_maps"]["10m"]["field_map"]
    fields_50m = metadata["field_maps"]["50m"]["field_map"]
    return [
        {
            "indicator": "Section Grouping",
            "used_for": "By-section rows and map labels",
            "source_columns": (
                f'10 m: {fields_10m["Section"]}; '
                f'50 m: {fields_50m["Section"]} with 10 m section backfill by id_10m when present'
            ),
        },
        {
            "indicator": "%Cover",
            "used_for": "Cover score and cover grade",
            "source_columns": f'10 m: {fields_10m["Mangrove_Presence_10m"]}',
        },
        {
            "indicator": "Density",
            "used_for": "Density score and grade",
            "source_columns": f'50 m: {fields_50m["Density"]}',
        },
        {
            "indicator": "Maturity",
            "used_for": "Maturity score and grade",
            "source_columns": f'50 m: {fields_50m["Maturity"]}',
        },
        {
            "indicator": "Condition / Canopy Cover",
            "used_for": "Condition score, canopy cover score, and grades",
            "source_columns": f'50 m: {fields_50m["Condition_Score"]}',
        },
        {
            "indicator": "Mangrove Damage",
            "used_for": "Damage score and grade",
            "source_columns": f'10 m: {fields_10m["Physical_Damage"]}',
        },
        {
            "indicator": "Shoreline Modification",
            "used_for": "Modification score and grade",
            "source_columns": f'10 m: {fields_10m["Naturalness"]}',
        },
        {
            "indicator": "Assessed Filter",
            "used_for": "Rows included in the report",
            "source_columns": f'10 m: {fields_10m["Assessed"]}; 50 m: {fields_50m["Assessed"]}',
        },
    ]


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


def _sheet_headers(
    zf: zipfile.ZipFile,
    sheet_path: str,
    shared: list[str],
    expected_headers: set[str] | None = None,
) -> tuple[int, dict[str, str]]:
    fallback_row = 0
    fallback_headers: dict[str, str] = {}
    for row_number, values in _iter_sheet_rows(zf, sheet_path, shared):
        raw_headers = {column: _clean(value) for column, value in values.items() if _clean(value)}
        available_headers = set(raw_headers.values())
        headers = {column: _normalize_header(value, available_headers) for column, value in raw_headers.items()}
        if not headers:
            continue
        if row_number == 3:
            fallback_row = row_number
            fallback_headers = headers
        elif not fallback_row:
            fallback_row = row_number
            fallback_headers = headers
        if expected_headers is not None and expected_headers.issubset(set(headers.values())):
            return row_number, headers
        if row_number >= 10:
            break
    return fallback_row, fallback_headers


def _sheet_header_set(
    zf: zipfile.ZipFile,
    sheet_path: str,
    shared: list[str],
    expected_headers: set[str] | None = None,
) -> set[str]:
    _, headers = _sheet_headers(zf, sheet_path, shared, expected_headers=expected_headers)
    return set(headers.values())


def _sheet_match_info(zf: zipfile.ZipFile, sheet_path: str, shared: list[str], schema_name: str) -> dict[str, Any] | None:
    for row_number, values in _iter_sheet_rows(zf, sheet_path, shared):
        raw_headers = {column: _clean(value) for column, value in values.items() if _clean(value)}
        available_headers = set(raw_headers.values())
        headers_by_col = {column: _normalize_header(value, available_headers) for column, value in raw_headers.items()}
        if not headers_by_col:
            continue
        match = _schema_field_map(set(headers_by_col.values()), schema_name)
        if match is not None:
            return {
                "header_row": row_number,
                "headers_by_col": headers_by_col,
                "match": match,
            }
        if row_number >= 10:
            break
    return None


def _read_sheet_rows(
    zf: zipfile.ZipFile,
    sheet_path: str,
    shared: list[str],
    expected_headers: set[str] | None = None,
    header_row: int | None = None,
    header_by_col: dict[str, str] | None = None,
) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    if header_row is None or header_by_col is None:
        header_row, header_by_col = _sheet_headers(zf, sheet_path, shared, expected_headers=expected_headers)
    if not header_row or not header_by_col:
        return rows
    for row_number, values in _iter_sheet_rows(zf, sheet_path, shared):
        if row_number <= header_row:
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


def _sheet_with_headers(zf: zipfile.ZipFile, schema_name: str, shared: list[str]) -> tuple[str, dict[str, Any]]:
    for _, path in _sheet_xml_path_by_name(zf).items():
        info = _sheet_match_info(zf, path, shared, schema_name)
        if info is not None:
            return path, info
    raise ReportError(f'Could not find worksheet matching the required {schema_name} headers.')


def _selected_sheet_path(
    zf: zipfile.ZipFile,
    shared: list[str],
    schema_name: str,
    selected_name: str | None,
    label: str,
) -> tuple[str, dict[str, Any]]:
    sheet_map = _sheet_xml_path_by_name(zf)
    if not selected_name:
        return _sheet_with_headers(zf, schema_name, shared)
    if selected_name not in sheet_map:
        raise ReportError(f'Selected {label} sheet "{selected_name}" was not found in the workbook.')

    sheet_path = sheet_map[selected_name]
    info = _sheet_match_info(zf, sheet_path, shared, schema_name)
    if info is None:
        raise ReportError(f'Selected {label} sheet "{selected_name}" does not contain the required headers.')
    return sheet_path, info


def workbook_sheet_options(file_bytes: bytes) -> dict[str, Any]:
    try:
        with zipfile.ZipFile(io.BytesIO(file_bytes)) as zf:
            shared = _shared_strings(zf)
            sheet_map = _sheet_xml_path_by_name(zf)
            sheets = []
            default_10m = ""
            default_50m = ""
            for name, path in sheet_map.items():
                matches_10m = _sheet_match_info(zf, path, shared, "10m") is not None
                matches_50m = _sheet_match_info(zf, path, shared, "50m") is not None
                if matches_10m and not default_10m:
                    default_10m = name
                if matches_50m and not default_50m:
                    default_50m = name
                sheets.append(
                    {
                        "name": name,
                        "matches_10m": matches_10m,
                        "matches_50m": matches_50m,
                    }
                )
    except zipfile.BadZipFile as exc:
        raise ReportError(
            "Workbook upload must be an .xlsx or .xlsm file. Legacy .xls files are not supported."
        ) from exc

    return {"sheets": sheets, "default_10m": default_10m, "default_50m": default_50m}


def load_workbook_dataset(
    file_bytes: bytes,
    filename: str = "",
    sheet_10m_name: str | None = None,
    sheet_50m_name: str | None = None,
) -> Dataset:
    try:
        with zipfile.ZipFile(io.BytesIO(file_bytes)) as zf:
            shared = _shared_strings(zf)
            sheet_map = _sheet_xml_path_by_name(zf)
            sheet_10m, info_10m = _selected_sheet_path(
                zf,
                shared,
                "10m",
                _clean(sheet_10m_name),
                "10 m",
            )
            sheet_50m, info_50m = _selected_sheet_path(
                zf,
                shared,
                "50m",
                _clean(sheet_50m_name),
                "50 m",
            )
            match_10m = info_10m["match"]
            match_50m = info_50m["match"]
            points_10m = _normalize_rows(
                _read_sheet_rows(
                    zf,
                    sheet_10m,
                    shared,
                    header_row=info_10m["header_row"],
                    header_by_col=info_10m["headers_by_col"],
                ),
                match_10m["field_map"],
            )
            points_50m = _normalize_rows(
                _read_sheet_rows(
                    zf,
                    sheet_50m,
                    shared,
                    header_row=info_50m["header_row"],
                    header_by_col=info_50m["headers_by_col"],
                ),
                match_50m["field_map"],
            )
            sheet_names = {
                "10m": _clean(sheet_10m_name) or next(name for name, path in sheet_map.items() if path == sheet_10m),
                "50m": _clean(sheet_50m_name) or next(name for name, path in sheet_map.items() if path == sheet_50m),
            }
    except zipfile.BadZipFile as exc:
        raise ReportError(
            "Workbook upload must be an .xlsx or .xlsm file. Legacy .xls files are not supported."
        ) from exc
    return _finalize_dataset(
        points_10m,
        points_50m,
        source_name=filename or "Workbook",
        metadata={
            "assessment_year": min(match_10m["assessment_year"], match_50m["assessment_year"]),
            "field_maps": {"10m": match_10m, "50m": match_50m},
            "sheet_names": sheet_names,
        },
    )


def _read_csv(file_bytes: bytes) -> list[dict[str, str]]:
    text = file_bytes.decode("utf-8-sig")
    reader = csv.DictReader(io.StringIO(text))
    return [{key: _clean(value) for key, value in row.items()} for row in reader]


def load_csv_dataset(csv_10m_bytes: bytes, csv_50m_bytes: bytes, name_10m: str = "", name_50m: str = "") -> Dataset:
    points_10m = _read_csv(csv_10m_bytes)
    points_50m = _read_csv(csv_50m_bytes)
    headers_10m = set(points_10m[0].keys()) if points_10m else set()
    headers_50m = set(points_50m[0].keys()) if points_50m else set()
    match_10m = _schema_field_map(headers_10m, "10m")
    match_50m = _schema_field_map(headers_50m, "50m")
    if not points_10m or match_10m is None:
        raise ReportError("10m CSV does not contain the required Mangrove Watch 10 m headers.")
    if not points_50m or match_50m is None:
        raise ReportError("50m CSV does not contain the required Mangrove Watch 50 m headers.")
    return _finalize_dataset(
        _normalize_rows(points_10m, match_10m["field_map"]),
        _normalize_rows(points_50m, match_50m["field_map"]),
        source_name=f"{name_10m or '10m CSV'} + {name_50m or '50m CSV'}",
        metadata={
            "assessment_year": min(match_10m["assessment_year"], match_50m["assessment_year"]),
            "field_maps": {"10m": match_10m, "50m": match_50m},
            "sheet_names": {"10m": name_10m or "10m CSV", "50m": name_50m or "50m CSV"},
        },
    )


def _finalize_dataset(
    points_10m: list[dict[str, str]],
    points_50m: list[dict[str, str]],
    source_name: str,
    metadata: dict[str, Any] | None = None,
) -> Dataset:
    coords_by_id: dict[str, tuple[float | None, float | None]] = {}
    section_by_id: dict[str, str] = {}
    sections = Counter()
    for row in points_10m:
        point_id = _clean(row.get("id_10m"))
        lon = _parse_float(row.get("lon_10m_point"))
        lat = _parse_float(row.get("lat_10m_point"))
        section = _clean(row.get("Section"))
        if point_id:
            coords_by_id[point_id] = (lon, lat)
            if section:
                section_by_id[point_id] = section
        if section:
            sections[section] += 1

    for row in points_50m:
        point_id = _clean(row.get("id_10m"))
        if point_id in coords_by_id and (not _clean(row.get("lon_10m_point")) or not _clean(row.get("lat_10m_point"))):
            lon, lat = coords_by_id[point_id]
            if lon is not None:
                row["lon_10m_point"] = str(lon)
            if lat is not None:
                row["lat_10m_point"] = str(lat)
        if point_id in section_by_id:
            row["Section"] = section_by_id[point_id]

    sections_available = sorted({_clean(row.get("Section")) for row in points_10m if _clean(row.get("Section"))}, key=_sort_key)
    base_metadata = dict(metadata or {})
    base_metadata.update(
        {
            "source_name": source_name,
            "sections_available": sections_available,
        }
    )
    if "indicator_mapping" not in base_metadata and "field_maps" in base_metadata:
        base_metadata["indicator_mapping"] = _indicator_mapping(base_metadata)
    return Dataset(
        points_10m=points_10m,
        points_50m=points_50m,
        metadata=base_metadata,
    )


def _is_assessed(row: dict[str, Any]) -> bool:
    return _clean(row.get("Assessed")).upper() != "X"


def _filter_rows(rows: list[dict[str, Any]], sections: list[str] | None) -> list[dict[str, Any]]:
    selected = []
    section_set = set(sections or [])
    for row in rows:
        section = _clean(row.get("Section"))
        if sections is not None and section not in section_set:
            continue
        if not _is_assessed(row):
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


def _raw_summary(values: list[float]) -> dict[str, float | None]:
    if not values:
        return {"mean": None, "median": None, "min": None, "max": None}
    return {
        "mean": mean(values),
        "median": median(values),
        "min": min(values),
        "max": max(values),
    }


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
    presence_values, presence_rows = _presence_points(rows_10m, "Mangrove_Presence_10m")
    cover_raw_values = [value * 100.0 for value in presence_values]
    cover_percent = None
    if presence_values:
        cover_percent = (sum(1 for value in presence_values if value == 1) / len(presence_values)) * 100.0

    density_values: list[float] = []
    maturity_values: list[float] = []
    condition_values: list[float] = []
    for row in rows_50m:
        if _presence_class(row.get("Mangrove_Presence_50m")) != 1:
            continue
        density = _parse_float(row.get("Density"))
        maturity = _parse_float(row.get("Maturity"))
        condition = _parse_float(row.get("Condition_Score"))
        if density is not None:
            density_values.append(density)
        if maturity is not None:
            maturity_values.append(maturity)
        if condition is not None:
            condition_values.append(condition)

    damage_classes: list[int] = []
    for row in rows_10m:
        if _presence_class(row.get("Mangrove_Presence_10m")) != 1:
            continue
        damage = _parse_int(row.get("Physical_Damage"))
        if damage is None:
            continue
        damage_classes.append(max(0, min(damage, 3)))

    modification_classes: list[int] = []
    for row in rows_10m:
        modification = _modification_class(row.get("Naturalness"))
        if modification is None:
            continue
        modification_classes.append(modification)

    density_mean = _mean_or_none(density_values)
    maturity_mean = _mean_or_none(maturity_values)
    condition_mean = _mean_or_none(condition_values)
    damage_raw = _weighted_impact_raw(damage_classes, {1: 1.0, 2: 1.5, 3: 2.0})
    modification_raw = _weighted_impact_raw(modification_classes, {1: 0.5, 2: 1.0})
    raw_summaries = {
        "cover": _raw_summary(cover_raw_values),
        "density": _raw_summary(density_values),
        "maturity": _raw_summary(maturity_values),
        "condition": _raw_summary(condition_values),
        "damage": _raw_summary([float(value) for value in damage_classes]),
        "modification": _raw_summary([float(value) for value in modification_classes]),
    }

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
                "raw_summary": raw_summaries["cover"],
            },
            "density": {
                "raw": density_mean,
                "raw_grade": _positive_grade(
                    density_mean,
                    [(3.75, "Very Good"), (3.25, "Good"), (2.5, "Moderate"), (2.0, "Poor"), (0.0, "Very Poor")],
                ),
                "score": density_score,
                "score_grade": _band_grade(density_score),
                "raw_summary": raw_summaries["density"],
            },
            "maturity": {
                "raw": maturity_mean,
                "raw_grade": _positive_grade(
                    maturity_mean,
                    [(3.75, "Very Good"), (3.25, "Good"), (2.5, "Moderate"), (2.0, "Poor"), (0.0, "Very Poor")],
                ),
                "score": maturity_score,
                "score_grade": _band_grade(maturity_score),
                "raw_summary": raw_summaries["maturity"],
            },
            "condition": {
                "raw": condition_mean,
                "raw_grade": _positive_grade(
                    condition_mean,
                    [(3.5, "Very Good"), (3.0, "Good"), (2.5, "Moderate"), (2.0, "Poor"), (0.0, "Very Poor")],
                ),
                "score": condition_score,
                "score_grade": _band_grade(condition_score),
                "raw_summary": raw_summaries["condition"],
            },
            "damage": {
                "raw": damage_raw,
                "raw_grade": _inverse_grade(
                    damage_raw,
                    [(1.0, "Very Good"), (3.0, "Good"), (7.0, "Moderate"), (15.0, "Poor"), (100.0, "Very Poor")],
                ),
                "score": damage_score,
                "score_grade": _band_grade(damage_score),
                "raw_summary": raw_summaries["damage"],
            },
            "modification": {
                "raw": modification_raw,
                "raw_grade": _inverse_grade(
                    modification_raw,
                    [(2.0, "Very Good"), (6.0, "Good"), (14.0, "Moderate"), (30.0, "Poor"), (100.0, "Very Poor")],
                ),
                "score": modification_score,
                "score_grade": _band_grade(modification_score),
                "raw_summary": raw_summaries["modification"],
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
        presence = _presence_class(row.get("Mangrove_Presence_10m"))
        damage = _parse_int(row.get("Physical_Damage"))
        modification = _modification_class(row.get("Naturalness"))
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
        density = _parse_int(row.get("Density"))
        maturity = _parse_int(row.get("Maturity"))
        condition = _parse_int(row.get("Condition_Score"))
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
                "cover": _format_number(metrics["cover"]["raw"]),
                "cover_grade": metrics["cover"]["raw_grade"] or "N/A",
                "density": _format_number(metrics["density"]["score"]),
                "density_grade": metrics["density"]["score_grade"] or "N/A",
                "maturity": _format_number(metrics["maturity"]["score"]),
                "maturity_grade": metrics["maturity"]["score_grade"] or "N/A",
                "condition": _format_number(metrics["condition"]["score"]),
                "condition_grade": metrics["condition"]["score_grade"] or "N/A",
                "canopy_cover_score": _format_number(metrics["condition"]["score"]),
                "canopy_cover_grade": metrics["condition"]["score_grade"] or "N/A",
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
        year = dataset.metadata.get("assessment_year", "selected")
        raise ReportError(f"No assessed {year} rows remain after filtering. Check the section selection and assessed values.")

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

    point_maps = _metric_map_configs(_map_points_10m(rows_10m), _map_points_50m(rows_50m))
    return {
        "source_name": dataset.metadata["source_name"],
        "sections_available": dataset.metadata["sections_available"],
        "assessment_year": dataset.metadata.get("assessment_year"),
        "sheet_names": dataset.metadata.get("sheet_names", {}),
        "indicator_mapping": dataset.metadata.get("indicator_mapping", []),
        "selected_sections": sections or dataset.metadata["sections_available"],
        "selected_section_label": _section_label(sections),
        "output_mode": output_mode,
        "score_rows": score_rows,
        "table_rows": _table_rows(score_rows),
        "point_maps": point_maps,
        "warnings": [],
        "sample_counts": {
            "rows_10m": len(rows_10m),
            "rows_50m": len(rows_50m),
        },
    }


def maps_json(report: dict[str, Any]) -> str:
    return json.dumps(report["point_maps"])


def _csv_text(fieldnames: list[str], rows: list[dict[str, Any]]) -> str:
    buffer = io.StringIO(newline="")
    writer = csv.DictWriter(buffer, fieldnames=fieldnames)
    writer.writeheader()
    for row in rows:
        writer.writerow(row)
    return buffer.getvalue()


def report_table_csv(report: dict[str, Any]) -> str:
    fieldnames = [
        "Result",
        "%Cover",
        "%Cover Grade",
        "Density",
        "Density Grade",
        "Maturity",
        "Maturity Grade",
        "Condition",
        "Condition Grade",
        "Mangrove Damage",
        "Damage Grade",
        "Shoreline Modification",
        "Modification Grade",
        "Habitat Structure",
        "Structure Grade",
        "Canopy Cover Score",
        "Canopy Cover Grade",
        "Habitat Impact",
        "Impact Grade",
        "Indicator Score",
        "Indicator Grade",
        "Cover Raw Mean",
        "Cover Raw Median",
        "Cover Raw Min",
        "Cover Raw Max",
        "Density Raw Mean",
        "Density Raw Median",
        "Density Raw Min",
        "Density Raw Max",
        "Maturity Raw Mean",
        "Maturity Raw Median",
        "Maturity Raw Min",
        "Maturity Raw Max",
        "Condition Raw Mean",
        "Condition Raw Median",
        "Condition Raw Min",
        "Condition Raw Max",
        "Damage Raw Mean",
        "Damage Raw Median",
        "Damage Raw Min",
        "Damage Raw Max",
        "Modification Raw Mean",
        "Modification Raw Median",
        "Modification Raw Min",
        "Modification Raw Max",
    ]
    rows = []
    for score_row, row in zip(report["score_rows"], report["table_rows"], strict=False):
        metrics = score_row["metrics"]
        rows.append(
            {
                "Result": row["label"],
                "%Cover": row["cover"],
                "%Cover Grade": row["cover_grade"],
                "Density": row["density"],
                "Density Grade": row["density_grade"],
                "Maturity": row["maturity"],
                "Maturity Grade": row["maturity_grade"],
                "Condition": row["condition"],
                "Condition Grade": row["condition_grade"],
                "Mangrove Damage": row["damage"],
                "Damage Grade": row["damage_grade"],
                "Shoreline Modification": row["modification"],
                "Modification Grade": row["modification_grade"],
                "Habitat Structure": row["structure_score"],
                "Structure Grade": row["structure_grade"],
                "Canopy Cover Score": row["canopy_cover_score"],
                "Canopy Cover Grade": row["canopy_cover_grade"],
                "Habitat Impact": row["impact_score"],
                "Impact Grade": row["impact_grade"],
                "Indicator Score": row["indicator_score"],
                "Indicator Grade": row["indicator_grade"],
                "Cover Raw Mean": _format_number(metrics["cover"]["raw_summary"]["mean"]),
                "Cover Raw Median": _format_number(metrics["cover"]["raw_summary"]["median"]),
                "Cover Raw Min": _format_number(metrics["cover"]["raw_summary"]["min"]),
                "Cover Raw Max": _format_number(metrics["cover"]["raw_summary"]["max"]),
                "Density Raw Mean": _format_number(metrics["density"]["raw_summary"]["mean"]),
                "Density Raw Median": _format_number(metrics["density"]["raw_summary"]["median"]),
                "Density Raw Min": _format_number(metrics["density"]["raw_summary"]["min"]),
                "Density Raw Max": _format_number(metrics["density"]["raw_summary"]["max"]),
                "Maturity Raw Mean": _format_number(metrics["maturity"]["raw_summary"]["mean"]),
                "Maturity Raw Median": _format_number(metrics["maturity"]["raw_summary"]["median"]),
                "Maturity Raw Min": _format_number(metrics["maturity"]["raw_summary"]["min"]),
                "Maturity Raw Max": _format_number(metrics["maturity"]["raw_summary"]["max"]),
                "Condition Raw Mean": _format_number(metrics["condition"]["raw_summary"]["mean"]),
                "Condition Raw Median": _format_number(metrics["condition"]["raw_summary"]["median"]),
                "Condition Raw Min": _format_number(metrics["condition"]["raw_summary"]["min"]),
                "Condition Raw Max": _format_number(metrics["condition"]["raw_summary"]["max"]),
                "Damage Raw Mean": _format_number(metrics["damage"]["raw_summary"]["mean"]),
                "Damage Raw Median": _format_number(metrics["damage"]["raw_summary"]["median"]),
                "Damage Raw Min": _format_number(metrics["damage"]["raw_summary"]["min"]),
                "Damage Raw Max": _format_number(metrics["damage"]["raw_summary"]["max"]),
                "Modification Raw Mean": _format_number(metrics["modification"]["raw_summary"]["mean"]),
                "Modification Raw Median": _format_number(metrics["modification"]["raw_summary"]["median"]),
                "Modification Raw Min": _format_number(metrics["modification"]["raw_summary"]["min"]),
                "Modification Raw Max": _format_number(metrics["modification"]["raw_summary"]["max"]),
            }
        )
    return _csv_text(fieldnames, rows)


def map_points_csv(map_config: dict[str, Any]) -> str:
    value_labels = {entry["value"]: entry["label"] for entry in map_config.get("legend", [])}
    fieldnames = ["Metric", "Section", "Point ID", "Latitude", "Longitude", "Value", "Value Label", "Color"]
    rows = []
    for point in map_config.get("points", []):
        rows.append(
            {
                "Metric": map_config.get("title", map_config.get("metric", "")),
                "Section": point.get("section", ""),
                "Point ID": point.get("point_id", ""),
                "Latitude": point.get("lat", ""),
                "Longitude": point.get("lon", ""),
                "Value": point.get("value", ""),
                "Value Label": value_labels.get(point.get("value"), ""),
                "Color": point.get("color", ""),
            }
        )
    return _csv_text(fieldnames, rows)
