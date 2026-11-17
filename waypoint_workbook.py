"""Turn GPX waypoints into a manually classified, georeferenced Excel form."""

from __future__ import annotations

import hashlib
import io
import json
import math
from pathlib import Path

from defusedxml import ElementTree as ET
from defusedxml.common import DefusedXmlException
from openpyxl import Workbook
from openpyxl.styles import Alignment, Font, PatternFill
from openpyxl.formatting.rule import FormulaRule
from openpyxl.workbook.defined_name import DefinedName
from openpyxl.worksheet.datavalidation import DataValidation


CATEGORIES = json.loads((Path(__file__).with_name("poi_categories.json")).read_text())["categories"]
HEADERS = ["FID", "time", "name", "POI", "Longitude", "Latitude", "Notes", "Watercourse", "Subsection"]
MAX_WAYPOINTS = 20_000


class WaypointError(ValueError):
    pass


def parse_gpx(contents: bytes) -> list[list]:
    if len(contents) > 16 * 1024 * 1024:
        raise WaypointError("The GPX file must be smaller than 16 MB.")
    try:
        root = ET.fromstring(contents)
    except (ET.ParseError, DefusedXmlException) as exc:
        raise WaypointError("Choose a valid GPX waypoint file.") from exc
    if root.tag.rsplit("}", 1)[-1] != "gpx":
        raise WaypointError("Choose a GPX file containing waypoint (wpt) records.")
    points = []
    for waypoint in root:
        if waypoint.tag.rsplit("}", 1)[-1] != "wpt":
            continue
        if len(points) >= MAX_WAYPOINTS:
            raise WaypointError(f"A form supports up to {MAX_WAYPOINTS:,} waypoints.")
        try:
            lon, lat = float(waypoint.attrib["lon"]), float(waypoint.attrib["lat"])
        except (KeyError, ValueError) as exc:
            raise WaypointError(f"Waypoint {len(points) + 1} needs valid longitude and latitude.") from exc
        if not (math.isfinite(lon) and math.isfinite(lat) and -180 <= lon <= 180 and -90 <= lat <= 90):
            raise WaypointError(f"Waypoint {len(points) + 1} has coordinates outside the valid range.")
        fields = {child.tag.rsplit("}", 1)[-1]: (child.text or "").strip() for child in waypoint}
        points.append([
            len(points), fields.get("time", ""), fields.get("name", ""), "", lon, lat,
            fields.get("desc") or fields.get("cmt", ""), "", "",
        ])
    if not points:
        raise WaypointError("No waypoints were found. Export GPX waypoints, rather than a track-only file.")
    return points


def build_waypoint_workbook(contents: bytes, filename: str) -> bytes:
    rows = parse_gpx(contents)
    workbook = Workbook()
    sheet = workbook.active
    sheet.title = "Waypoints"
    sheet.append(HEADERS)
    for row in rows:
        sheet.append(row)
        # Keep GPX text literal, including names or notes starting with '='.
        for column in (2, 3, 4, 7, 8, 9):
            sheet.cell(sheet.max_row, column).data_type = "s"
    sheet.freeze_panes = "D2"
    sheet.auto_filter.ref = sheet.dimensions
    sheet.row_dimensions[1].height = 28
    for cell in sheet[1]:
        cell.fill = PatternFill("solid", fgColor="1F6D49")
        cell.font = Font(color="FFFFFF", bold=True)
    for column, width in zip("ABCDEFGHI", (9, 28, 20, 20, 16, 16, 48, 30, 18)):
        sheet.column_dimensions[column].width = width
    for row in sheet.iter_rows(min_row=2):
        for cell in row:
            cell.alignment = Alignment(vertical="top", wrap_text=True)
        for cell in row[4:6]:
            cell.number_format = "0.000000"
        for column in (4, 7, 8, 9):
            row[column - 1].fill = PatternFill("solid", fgColor="FFF2CC")
    choices = workbook.create_sheet("POI categories")
    choices.append(["POI", "Instructions"])
    for category in CATEGORIES:
        choices.append([category["label"]])
        cell = choices.cell(choices.max_row, 1)
        cell.fill = PatternFill("solid", fgColor=category["color"].lstrip("#"))
        sheet.conditional_formatting.add(
            f"D2:D{len(rows) + 1}",
            FormulaRule(formula=[f'$D2="{category["label"]}"'],
                        fill=PatternFill("solid", fgColor=category["color"].lstrip("#"))),
        )
    choices["B2"] = "Choose POI manually on the Waypoints sheet. Blank POIs remain unclassified."
    choices["B3"] = "Longitude and Latitude come from the GPX. Preserve them for the dashboard map."
    choices["B4"] = "Optional Watercourse and Subsection must match the dashboard names to use those filters."
    choices["B5"] = "Times retain the original GPX time and timezone."
    choices["B6"] = "Mangroves is an explicit choice. A blank POI does not mean Mangroves."
    choices.column_dimensions["A"].width = 20
    choices.column_dimensions["B"].width = 100
    validation = DataValidation(type="list", formula1=f"'POI categories'!$A$2:$A${len(CATEGORIES) + 1}", allow_blank=True)
    # A defined name makes cross-sheet validation work in desktop Excel.
    workbook.defined_names.add(DefinedName("POICategories", attr_text=validation.formula1))
    validation.formula1 = "POICategories"
    validation.showDropDown = False
    validation.showErrorMessage = True
    validation.errorTitle = "Choose a POI category"
    validation.error = "Choose one of the supplied categories, or leave this cell blank."
    validation.errorStyle = "stop"
    sheet.add_data_validation(validation)
    validation.add(f"D2:D{len(rows) + 1}")
    metadata = workbook.create_sheet("_Survey")
    metadata.append(["survey_id", hashlib.sha256(contents).hexdigest()])
    metadata.append(["source_gpx", Path(filename).name])
    metadata["B2"].data_type = "s"
    metadata.sheet_state = "hidden"
    output = io.BytesIO()
    workbook.save(output)
    return output.getvalue()
