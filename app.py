from __future__ import annotations

import logging
import os
import re
import uuid
from collections import OrderedDict
from typing import Any

from flask import Flask, Response, abort, jsonify, render_template, request, url_for

from assessment_workbook import (
    append_assessment_to_archive_workbook,
    build_assessment_workbook,
)
from scoring import (
    ReportError,
    generate_report,
    load_archive_dataset,
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


def _file_token(value: Any, fallback: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9]+", "_", str(value or "").strip()).strip("_")
    return (cleaned or fallback).upper()


def _year_token(value: Any, fallback: str = "YEAR") -> str:
    match = re.search(r"20\d{2}", str(value or ""))
    return match.group(0) if match else fallback


def _estuary_name_from_archive_filename(filename: str) -> str:
    base = os.path.splitext(os.path.basename(filename or ""))[0]
    base = re.sub(r"(?i)^DATA[_ -]+", "", base)
    tokens = [token for token in re.split(r"[_\s-]+", base) if token]
    estuary_tokens = []
    stop_tokens = {"ALL", "Archive", "Assessment", "Review", "JM", "Final", "Draft"}
    normalized_stop_tokens = {value.upper() for value in stop_tokens}
    for token in tokens:
        if re.fullmatch(r"20\d{2}|\d{3,4}", token):
            break
        if token.upper() in normalized_stop_tokens:
            break
        estuary_tokens.append(token)
    estuary = " ".join(estuary_tokens) or base
    return estuary.title() if estuary.isupper() else estuary


def _assessment_workbook_filename(estuary_name: str, assessment_year: Any) -> str:
    return f"{_file_token(estuary_name, 'MANGROVE_WATCH')}_{_year_token(assessment_year)}_ASSESSMENT.xlsx"


def _archive_workbook_filename(report: dict[str, Any]) -> str:
    estuary_name = report.get("estuary_name") or _estuary_name_from_archive_filename(report.get("source_name", ""))
    return f"DATA_{_file_token(estuary_name, 'MANGROVE_WATCH')}_{_year_token(report.get('assessment_year'))}_ARCHIVE.xlsx"


def _csv_response(csv_text: str, filename: str) -> Response:
    return Response(
        csv_text,
        content_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


def _xlsx_response(file_bytes: bytes, filename: str) -> Response:
    return Response(
        file_bytes,
        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@app.route("/", methods=["GET"])
def index():
    return render_template("index.html")


@app.route("/assessment-workbook", methods=["POST"])
def assessment_workbook():
    current_csv = request.files.get("assessment_csv")
    archive_workbook = request.files.get("archive_workbook")
    assessment_year = request.form.get("assessment_year", "")

    if current_csv is None or not current_csv.filename:
        return render_template(
            "index.html",
            error="Choose the current-year raw CSV before building the assessment workbook.",
            previous=request.form,
        ), 400
    if archive_workbook is None or not archive_workbook.filename:
        return render_template(
            "index.html",
            error="Choose the archive workbook. It is required so the previous two years can be loaded.",
            previous=request.form,
        ), 400

    try:
        estuary_name = _estuary_name_from_archive_filename(archive_workbook.filename)
        workbook_bytes = build_assessment_workbook(
            current_csv.read(),
            assessment_year=assessment_year,
            estuary_name=estuary_name,
            archive_workbook_bytes=archive_workbook.read(),
        )
    except ReportError as exc:
        return render_template("index.html", error=str(exc), previous=request.form), 400
    except Exception:
        app.logger.exception("Unexpected error while building assessment workbook")
        return render_template(
            "index.html",
            error="The assessment workbook could not be built from that CSV/archive combination.",
            previous=request.form,
        ), 500

    return _xlsx_response(workbook_bytes, _assessment_workbook_filename(estuary_name, assessment_year))


@app.route("/archive-workbook", methods=["POST"])
def archive_workbook():
    completed_workbook = request.files.get("completed_assessment_workbook")
    if completed_workbook is None or not completed_workbook.filename:
        return render_template(
            "index.html",
            error="Choose the completed assessment workbook before creating the archive workbook.",
            previous=request.form,
        ), 400

    workbook_bytes = completed_workbook.read()
    try:
        dataset = load_workbook_dataset(workbook_bytes, completed_workbook.filename)
        archive_bytes, _ = append_assessment_to_archive_workbook(workbook_bytes)
    except ReportError as exc:
        return render_template("index.html", error=str(exc), previous=request.form), 400
    except Exception:
        app.logger.exception("Unexpected error while creating archive workbook")
        return render_template(
            "index.html",
            error="The archive workbook could not be created from that completed assessment workbook.",
            previous=request.form,
        ), 500

    filename = _archive_workbook_filename(
        {
            "estuary_name": dataset.metadata.get("estuary_name", ""),
            "assessment_year": dataset.metadata.get("assessment_year", ""),
            "source_name": completed_workbook.filename,
        }
    )
    return _xlsx_response(archive_bytes, filename)


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
        return render_template("report.html")

    sections = parse_sections(request.form.get("sections", "all"))
    output_mode = request.form.get("output_mode", "both")
    archive_report_year = request.form.get("archive_report_year", "")
    archive_workbook = request.files.get("archive_report_workbook")
    workbook_sheet_10m = request.form.get("workbook_sheet_10m", "")
    workbook_sheet_50m = request.form.get("workbook_sheet_50m", "")
    workbook = request.files.get("workbook")
    csv_10m = request.files.get("csv_10m")
    csv_50m = request.files.get("csv_50m")

    try:
        if archive_workbook and archive_workbook.filename:
            dataset = load_archive_dataset(
                archive_workbook.read(),
                archive_workbook.filename,
                assessment_year=archive_report_year,
            )
        elif workbook and workbook.filename:
            dataset = load_workbook_dataset(
                workbook.read(),
                workbook.filename,
                sheet_10m_name=workbook_sheet_10m,
                sheet_50m_name=workbook_sheet_50m,
            )
        elif csv_10m and csv_10m.filename and csv_50m and csv_50m.filename:
            dataset = load_csv_dataset(csv_10m.read(), csv_50m.read(), csv_10m.filename, csv_50m.filename)
        else:
            raise ReportError("Upload an archive workbook and choose the report year.")
        result = generate_report(dataset, sections=sections, output_mode=output_mode)
    except ReportError as exc:
        return render_template("report.html", error=str(exc), previous=request.form), 400
    except Exception:
        app.logger.exception("Unexpected error while generating report")
        return render_template(
            "report.html",
            error="The upload could not be processed. Use an archive workbook and report year, then try again.",
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
