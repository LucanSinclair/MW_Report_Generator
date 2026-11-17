import io
import unittest

from openpyxl import load_workbook

from app import app
from waypoint_workbook import CATEGORIES, WaypointError, build_waypoint_workbook, parse_gpx


GPX = b'''<gpx xmlns="http://www.topografix.com/GPX/1/1" version="1.1">
<wpt lat="-19.2" lon="146.7"><time>2024-10-29T01:02:03Z</time><name>929</name><desc>Review shoreline</desc></wpt>
<wpt lat="-19.21" lon="146.71"><name>=1+1</name></wpt></gpx>'''


class WaypointWorkbookTests(unittest.TestCase):
    def test_gpx_to_excel_preserves_template_and_leaves_poi_for_manual_review(self):
        contents = build_waypoint_workbook(GPX, "Survey.gpx")
        workbook = load_workbook(io.BytesIO(contents))
        sheet = workbook["Waypoints"]
        self.assertEqual([cell.value for cell in sheet[1]][:4], ["FID", "time", "name", "POI"])
        self.assertEqual(sheet["B2"].value, "2024-10-29T01:02:03Z")
        self.assertIsNone(sheet["D2"].value)
        self.assertEqual(sheet["E2"].value, 146.7)
        self.assertEqual(sheet["F2"].value, -19.2)
        self.assertEqual(sheet["C3"].data_type, "s")
        self.assertEqual(sheet["C3"].value, "=1+1")
        validation = sheet.data_validations.dataValidation[0]
        self.assertEqual(str(validation.sqref), "D2:D3")
        self.assertEqual(validation.formula1, "POICategories")
        self.assertTrue(validation.showErrorMessage)
        self.assertFalse(validation.showDropDown)
        self.assertEqual([row[0] for row in workbook["POI categories"].iter_rows(min_row=2, values_only=True)], [category["label"] for category in CATEGORIES])
        self.assertEqual(workbook["_Survey"].sheet_state, "hidden")
        self.assertEqual(workbook["_Survey"]["B1"].value, load_workbook(io.BytesIO(build_waypoint_workbook(GPX, "renamed.gpx")))["_Survey"]["B1"].value)

    def test_invalid_gpx_and_external_entities_are_rejected(self):
        for contents in (b"not XML", b"<gpx><trk><trkseg><trkpt lat='1' lon='2'/></trkseg></trk></gpx>", b"<gpx><wpt lat='NaN' lon='1'/></gpx>", b'<!DOCTYPE gpx [<!ENTITY x SYSTEM "file:///etc/passwd">]><gpx>&x;</gpx>'):
            with self.subTest(contents=contents), self.assertRaises(WaypointError):
                parse_gpx(contents)

    def test_route_download_and_validation_errors(self):
        client = app.test_client()
        self.assertIn(b"GPX waypoint file", client.get("/waypoints").data)
        response = client.post("/waypoints", data={"gpx_file": (io.BytesIO(GPX), "Survey.gpx")})
        self.assertEqual(response.status_code, 200)
        self.assertIn("Survey_POI.xlsx", response.headers["Content-Disposition"])
        self.assertEqual(client.post("/waypoints").status_code, 400)
        self.assertEqual(client.post("/waypoints", data={"gpx_file": (io.BytesIO(b"invalid"), "bad.gpx")}).status_code, 400)


if __name__ == "__main__":
    unittest.main()
