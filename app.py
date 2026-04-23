from __future__ import annotations

import logging
import os
import re
import uuid
from collections import OrderedDict
from typing import Any

from flask import Flask, Response, abort, jsonify, redirect, render_template, request, url_for

from scoring import (
    ReportError,
    generate_report,
    load_csv_dataset,
    load_workbook_dataset,
    map_points_csv,
    maps_json,
    parse_sections,
    report_table_csv,
    workbook_sheet_options,
)


app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = 64 * 1024 * 1024
app.logger.setLevel(logging.INFO)


REPORT_CACHE_LIMIT = 32
REPORT_CACHE: OrderedDict[str, dict[str, Any]] = OrderedDict()


def _cache_report(report: dict[str, Any]) -> str:
    report_id = uuid.uuid4().hex
    REPORT_CACHE[report_id] = report
    REPORT_CACHE.move_to_end(report_id)
    while len(REPORT_CACHE) > REPORT_CACHE_LIMIT:
        REPORT_CACHE.popitem(last=False)
    return report_id


def _cached_report_or_404(report_id: str) -> dict[str, Any]:
    report = REPORT_CACHE.get(report_id)
    if report is None:
        abort(404, description="That download is no longer available. Generate the report again.")
    REPORT_CACHE.move_to_end(report_id)
    return report


def _download_name(value: str, fallback: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "-", (value or "").strip()).strip("-.")
    return cleaned or fallback


def _csv_response(csv_text: str, filename: str) -> Response:
    return Response(
        csv_text,
        content_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@app.route("/", methods=["GET"])
def index():
    return render_template("index.html")


@app.route("/workbook-sheets", methods=["POST"])
def workbook_sheets():
    workbook = request.files.get("workbook")
    if workbook is None or not workbook.filename:
        return jsonify({"error": "Choose a workbook file first."}), 400

    try:
        options = workbook_sheet_options(workbook.read())
    except ReportError as exc:
        return jsonify({"error": str(exc)}), 400

    return jsonify(options)


@app.route("/report", methods=["GET", "POST"])
def report():
    if request.method == "GET":
        return redirect(url_for("index"))

    sections = parse_sections(request.form.get("sections", "all"))
    output_mode = request.form.get("output_mode", "both")
    workbook_sheet_10m = request.form.get("workbook_sheet_10m", "")
    workbook_sheet_50m = request.form.get("workbook_sheet_50m", "")
    workbook = request.files.get("workbook")
    csv_10m = request.files.get("csv_10m")
    csv_50m = request.files.get("csv_50m")

    try:
        if workbook and workbook.filename:
            dataset = load_workbook_dataset(
                workbook.read(),
                workbook.filename,
                sheet_10m_name=workbook_sheet_10m,
                sheet_50m_name=workbook_sheet_50m,
            )
        elif csv_10m and csv_10m.filename and csv_50m and csv_50m.filename:
            dataset = load_csv_dataset(csv_10m.read(), csv_50m.read(), csv_10m.filename, csv_50m.filename)
        else:
            raise ReportError("Upload either one workbook (.xlsm/.xlsx) or both a 10m CSV and 50m CSV.")
        result = generate_report(dataset, sections=sections, output_mode=output_mode)
    except ReportError as exc:
        return render_template("index.html", error=str(exc), previous=request.form), 400
    except Exception:
        app.logger.exception("Unexpected error while generating report")
        return render_template(
            "index.html",
            error="The upload could not be processed. Use an .xlsx/.xlsm workbook or matching CSV files, then try again.",
            previous=request.form,
        ), 500

    report_id = _cache_report(result)
    return render_template("results.html", report=result, report_id=report_id, maps_json=maps_json(result))


@app.route("/download/report/<report_id>.csv", methods=["GET"])
def download_report_csv(report_id: str):
    report = _cached_report_or_404(report_id)
    filename = _download_name(report.get("source_name", ""), "mangrove-watch-report")
    return _csv_response(report_table_csv(report), f"{filename}-scores.csv")


@app.route("/download/map/<report_id>/<metric>.csv", methods=["GET"])
def download_map_csv(report_id: str, metric: str):
    report = _cached_report_or_404(report_id)
    map_config = next((item for item in report.get("point_maps", []) if item.get("metric") == metric), None)
    if map_config is None:
        abort(404, description="That map export is not available for this report.")
    filename = _download_name(report.get("source_name", ""), "mangrove-watch-report")
    metric_name = _download_name(metric, "map")
    return _csv_response(map_points_csv(map_config), f"{filename}-{metric_name}-map.csv")


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "5000"))
    app.run(host="127.0.0.1", port=port, debug=True)
