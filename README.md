# AWR Assessment

Extract performance and sizing metrics from Oracle **AWR (Automatic Workload Repository)** HTML reports into a single Excel workbook. The script processes existing reports offline; it does not connect to or modify Oracle databases.

Use [AWR_reader.py](AWR_reader.py), the more complete implementation. [main.py](main.py) is an alternative with fewer fields and debug messages.

## Setup

Requires Python 3 and English-language AWR HTML reports. In Windows PowerShell:

```powershell
git clone https://github.com/faustinopalma/awr_assessment.git
Set-Location awr_assessment
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install beautifulsoup4 "pandas<3" lxml openpyxl
New-Item -ItemType Directory -Force -Path AWR, output | Out-Null
```

The pandas version limit avoids pandas 3 incompatibilities with legacy conversions in the code. On Linux/macOS, use `.venv/bin/python` and create the folders with `mkdir -p AWR output`.

## Usage

Place at least one AWR HTML report in `AWR/`. Subfolders are supported; every file is processed, so keep unrelated files out of this directory.

Run from the project root:

```powershell
.\.venv\Scripts\python.exe .\AWR_reader.py
```

The script writes **`output/awr_data.xlsx`**, with one row per report. The `output` folder must already exist. Each run overwrites the workbook; close it in Excel before running again. Paths are relative to the current working directory. No command-line arguments are required.

## Output

- Database, instance and host details, Oracle release, RAC, platform and snapshot times.
- Exadata model when its report section is present.
- CPU count, cores, DB CPU time, host CPU statistics, physical memory, SGA and PGA usage.
- Read/write throughput and IOPS, totals, CPU capacity ratios and estimated Oracle memory usage.

## Limitations

- Report layouts and required section names must match the parser. Missing sections or an invalid file can stop the entire run; not all Oracle report versions are supported.
- `%DB Time of Elapsed Time` and `%DB CPU of server capacity` are fractions, not percentages: `0.25` means 25%. DB Time can exceed elapsed time and is not CPU utilization.
- Read throughput uses MiB/s, write throughput uses MB/s, but both are labeled `MB/s` and added together. Memory labeled `GB` uses binary GiB conversions.
- I/O rates are interval averages, and memory values are not necessarily peaks. Validate results before making sizing decisions.
- Reports and output may contain sensitive infrastructure details; do not publish them without review.

## License

[MIT](LICENSE).
