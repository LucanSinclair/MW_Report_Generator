from __future__ import annotations

import os

from flask import Flask, redirect, render_template, request, url_for

from scoring import ReportError, generate_report, load_csv_dataset, load_workbook_dataset, maps_json, parse_sections


app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = 64 * 1024 * 1024


@app.route("/", methods=["GET"])
def index():
    return render_template("index.html")


@app.route("/report", methods=["POST"])
def report():
    sections = parse_sections(request.form.get("sections", "all"))
    output_mode = request.form.get("output_mode", "both")
    workbook = request.files.get("workbook")
    csv_10m = request.files.get("csv_10m")
    csv_50m = request.files.get("csv_50m")

    try:
        if workbook and workbook.filename:
            dataset = load_workbook_dataset(workbook.read(), workbook.filename)
        elif csv_10m and csv_10m.filename and csv_50m and csv_50m.filename:
            dataset = load_csv_dataset(csv_10m.read(), csv_50m.read(), csv_10m.filename, csv_50m.filename)
        else:
            raise ReportError("Upload either one workbook (.xlsm/.xlsx) or both a 10m CSV and 50m CSV.")
        result = generate_report(dataset, sections=sections, output_mode=output_mode)
    except ReportError as exc:
        return render_template("index.html", error=str(exc), previous=request.form), 400

    return render_template("results.html", report=result, maps_json=maps_json(result))


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "5000"))
    app.run(host="127.0.0.1", port=port, debug=True)
