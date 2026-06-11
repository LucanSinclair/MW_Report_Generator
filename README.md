# Mangrove Watch Report Generator

This app supports the Mangrove Watch assessment workflow after the video/GPS processing step has created the current-year assessment CSV.

It does three main jobs:

1. Build an assessment workbook from the current-year CSV and an existing archive workbook.
2. Convert the completed assessment workbook back into an updated archive workbook.
3. Generate report tables, scores, and maps from an archive workbook.

## Workflow

```text
Current-year CSV + existing archive workbook
        |
        v
Assessment workbook with previous years included
        |
        v
Manual review and scoring in Excel
        |
        v
Updated archive workbook
        |
        v
Report tables, scores, maps, and downloads
```

## Run The App

Install dependencies:

```powershell
pip install -r requirements.txt
```

Start the local web app:

```powershell
python app.py
```

Then open:

```text
http://127.0.0.1:5000
```

## Step 1: Build Assessment Workbook

Open the main page and upload:

- the current-year raw CSV
- the latest archive workbook
- the assessment year

The current-year raw CSV should contain columns like:

```text
ID,id_10m,lon_10m_point,lat_10m_point,INT_50,Hyperlink,Section,Assessed
```

The GoPro frame overlay tool creates this format as:

```text
DATA_<video>_ALL.csv
DATA_<video>_50.csv
```

The app currently expects the current-year raw CSV used to build the assessment workbook. The `INT_50` column is used to create the 50 m assessment rows.

The archive workbook provides previous assessment years. It can contain:

- `Archive_10m` and `Archive_50m` sheets, or
- older `DATA_*_ALL` and `DATA_*_50` style sheets that the app can read.

The app downloads an assessment workbook named like:

```text
ESTUARY_YEAR_ASSESSMENT.xlsx
```

That workbook contains:

- `Archive_10m`
- `Archive_50m`
- `Assessment_10m`
- `Assessment_50m`

## Step 2: Manually Assess In Excel

Open the downloaded assessment workbook and review/fill the current-year columns.

The 10 m assessment sheet is used for:

- mangrove presence
- naturalness / shoreline modification
- physical damage

The 50 m assessment sheet is used for:

- mangrove presence at 50 m
- density
- maturity
- canopy cover / condition score
- connectivity

Previous years are included for context. Where the current point is not marked `X`, the app can prefill values from the previous year.

## Meaning Of `Assessed`

The app treats `Assessed = X` as not assessed / not included.

In practice:

- blank `Assessed` means the row is available for assessment and can be included in reporting
- `Assessed = X` means the row is excluded from scoring and reports

This matches the old R workflow:

```r
Assessed <- ifelse(Hyperlink == "", "X", "")
```

So if a point has no matched image hyperlink, it is marked `X`.

## Step 3: Create Updated Archive

After completing the assessment workbook, upload it on the main page under "Create Updated Archive".

The app reads:

- `Assessment_10m`
- `Assessment_50m`

It appends the current-year assessment values into:

- `Archive_10m`
- `Archive_50m`

Then it removes the working assessment sheets and downloads a clean archive workbook named like:

```text
DATA_ESTUARY_YEAR_ARCHIVE.xlsx
```

Use this archive workbook as the source for future years and for reporting.

## Step 4: Generate Reports

Open the Report Generator page and upload an archive workbook.

Choose:

- report year
- sections, such as `all` or `1,2,6`
- output mode: pooled, by section, or both

The report page shows:

- indicator mapping
- score table
- point maps
- downloadable report CSV
- downloadable map CSVs
- downloadable map PNGs from the browser

## Scoring Summary

The report excludes rows where `Assessed = X`.

It calculates:

- percent mangrove cover from 10 m mangrove presence
- density from 50 m rows
- maturity from 50 m rows
- condition / canopy cover from 50 m rows
- mangrove damage from 10 m rows
- shoreline modification from 10 m naturalness

Those are converted to standardized scores and grades:

- Very Good
- Good
- Moderate
- Poor
- Very Poor

The final outputs include:

- habitat structure score
- habitat impact score
- overall indicator score

## Main Code Files

- `app.py`: Flask routes, uploads, downloads, and page rendering.
- `assessment_workbook.py`: builds assessment workbooks and updated archive workbooks.
- `scoring.py`: reads workbooks/CSVs, filters assessed rows, calculates scores, and prepares map data.
- `templates/`: browser pages.
- `static/style.css`: page styling.
- `data/`: local sample/input data.

## Common Issues

If no report rows are produced, check:

- the selected report year exists in the archive workbook
- the selected sections match the values in the workbook
- rows are not all marked `Assessed = X`
- required scoring columns have values

If the assessment workbook cannot be built, check:

- the current CSV has the expected columns
- the archive workbook has readable archive sheets
- the assessment year is a four-digit year, such as `2025`
