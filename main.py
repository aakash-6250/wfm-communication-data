import csv
import re
import sys
import math
from datetime import datetime, timedelta
import polars as pl

from pathlib import Path

ROOT_DIR = Path.cwd()

INPUT_DIR = ROOT_DIR / "Input"
OUTPUT_DIR = ROOT_DIR / "Output"

INPUT_PATH = INPUT_DIR / "consumer.csv"
RAW_DATA_PATH = INPUT_DIR / "Total Meter Details.csv"
METER_COMM_STATUS_PATH = INPUT_DIR / "Meters Comm Status.csv"
DISCOM_LAT_LONG_PATH = INPUT_DIR / "Discom Lat Long.csv"
LATEST_METER_SLA_PATH = INPUT_DIR / "SAT Status.csv"
BLP_PATH = INPUT_DIR / "Yesterday BLP.csv"

OUTPUT_PATH = OUTPUT_DIR / "consumer_qgis.csv"

# //////////////////////////////////////////////////////////////////////////
# //////////   COMMUNICATION DATA PROCESSING SCRIPT   //////////////////////
# //////////////////////////////////////////////////////////////////////////

# Map input column names to desired output names.
COLUMN_MAP = {
    "New_Meter_No": "MSN",
    "Installation_Date": "Install Date",
    "IVRS_No": "IVRS",
    "DC_Name": "DC",
    "circle": "Circle",
    "division": "Division",
    "latitude": "MI Lat",
    "longitude": "MI Long",
    "Feeder_name": "Feeder",
    "Feeder_Code": "Feeder Code",
    "DT_code": "DT Code",
    "DT_name": "DT Name",
    "HES_Type": "HES Type",
}


def read_csv_header(path: Path, skip_rows: int = 0) -> list[str]:
    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        for _ in range(skip_rows):
            next(reader, None)
        header = next(reader)
    return [col.lstrip("\ufeff") for col in header]


def load_selected_columns(path: Path, columns: list[str], skip_rows: int = 0) -> pl.DataFrame:
    schema_overrides = {col: pl.Utf8 for col in columns}
    return pl.read_csv(path, columns=columns, schema_overrides=schema_overrides, skip_rows=skip_rows)


def convert_feeder_code(df: pl.DataFrame) -> pl.DataFrame:
    if "Feeder Code" in df.columns:
        def normalize_value(value: str) -> str:
            if value is None:
                return None
            cleaned = "".join(value.split())
            try:
                number = float(cleaned)
            except ValueError:
                return value

            if number.is_integer():
                return "'" + str(int(number))
            return "'" + str(number)

        df = df.with_columns(
            pl.col("Feeder Code")
            .map_elements(normalize_value, return_dtype=pl.Utf8)
            .alias("Feeder Code")
        )
    return df


def format_datetime_column(df: pl.DataFrame, column_name: str) -> pl.DataFrame:
    if column_name not in df.columns:
        return df

    return df.with_columns(
        pl.when(
            pl.col(column_name).is_null()
            | pl.col(column_name).eq("")
            | pl.col(column_name).eq("-")
        )
        .then(pl.lit(None))
        .otherwise(
            pl.col(column_name)
            .cast(pl.Utf8)
            .str.strptime(pl.Datetime, strict=False)
            .dt.strftime("%m/%d/%Y %H:%M:%S")
        )
        .cast(pl.Utf8)
        .alias(column_name)
    )


def add_comm_status_and_ageing(df: pl.DataFrame) -> pl.DataFrame:
    now = datetime.now()

    df = df.with_columns([
        pl.col("Install Date").str.strptime(
            pl.Date,
            format="%Y-%m-%d",
            strict=False
        ).cast(pl.Datetime)
        .alias("hes_dt"),
        pl.col("Last Comm").str.strptime(pl.Datetime, strict=False).alias("lc_dt"),
    ])
    

    lc_invalid = pl.col("Last Comm").is_null() | pl.col("Last Comm").eq("") | pl.col("Last Comm").eq("-") | pl.col("lc_dt").is_null()
    comm_type_upper = pl.col("Comm Type").cast(pl.Utf8).str.to_uppercase()

    df = df.with_columns(
        pl.when(lc_invalid)
        .then(pl.lit("Never Comm"))
        .when(pl.col("lc_dt") < pl.col("hes_dt"))
        .then(pl.lit("Never Comm"))
        .when(pl.col("lc_dt").dt.date() == pl.lit(now.date()))
        .then(pl.lit("Comm"))
        .otherwise(pl.lit("Non Comm"))
        .alias("Comm Status"),
        pl.when(lc_invalid)
        .then(
            pl.when(pl.col("hes_dt").is_null())
            .then(pl.lit(9999))
            .otherwise(((pl.lit(now) - pl.col("hes_dt")) / pl.duration(days=1)).cast(pl.Int64))
        )
        .when(pl.col("lc_dt") < pl.col("hes_dt"))
        .then(((pl.lit(now) - pl.col("hes_dt")) / pl.duration(days=1)).cast(pl.Int64))
        .otherwise(((pl.lit(now) - pl.col("lc_dt")) / pl.duration(days=1)).cast(pl.Int64))
        .alias("age_days")
    )

    df = df.with_columns(
        pl.when(comm_type_upper == "KIMBAL")
        .then(pl.lit("KIMBAL"))
        .when(comm_type_upper == "ASSET NOT IN HES")
        .then(pl.lit("Asset Not In HES"))
        .otherwise(pl.col("Comm Status"))
        .alias("Comm Status"),
        pl.when(comm_type_upper == "KIMBAL")
        .then(pl.lit("KIMBAL"))
        .when(comm_type_upper == "ASSET NOT IN HES")
        .then(pl.lit("Asset Not In HES"))
        .when(comm_type_upper.is_in(["GPRS", "MESH"]))
        .then(
            pl.when(pl.col("age_days") <= 3)
            .then(pl.lit("0-3 Days"))
            .when(pl.col("age_days") <= 7)
            .then(pl.lit("4-7 Days"))
            .when(pl.col("age_days") <= 15)
            .then(pl.lit("8-15 Days"))
            .when(pl.col("age_days") <= 30)
            .then(pl.lit("16-30 Days"))
            .when(pl.col("age_days") <= 60)
            .then(pl.lit("31-60 Days"))
            .when(pl.col("age_days") <= 90)
            .then(pl.lit("61-90 Days"))
            .otherwise(pl.lit("90+ Days"))
        )
        .otherwise(pl.lit(None))
        .alias("Ageing")
    )

    return df.drop(["hes_dt", "lc_dt", "age_days"])


def load_meter_comm_status(path: Path) -> pl.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Communication status file not found: {path}")

    header = read_csv_header(path, skip_rows=1)
    columns = [col for col in ["meter_number", "last_comm", "com_type", "gw_id", "active_last_gasp_time", "time_stamp"] if col in header]
    if "meter_number" not in columns:
        raise RuntimeError(f"Communication status file is missing meter_number column: {path}")

    df = load_selected_columns(path, columns, skip_rows=1)
    rename_map = {
        "meter_number": "MSN",
        "last_comm": "Last Comm",
        "com_type": "Comm Type",
        "gw_id": "GW ID",
        "active_last_gasp_time": "Active Last Gasp Time",
        "time_stamp": "HES Time Stamp",
    }
    
    df = df.rename(rename_map)
    df = df.unique(subset=["MSN"])
    return df


def load_gw_summary(df: pl.DataFrame) -> pl.DataFrame:
    if "GW ID" not in df.columns:
        return pl.DataFrame(schema={"MSN": pl.Utf8, "GW ID": pl.Utf8, "Total Nodes": pl.Int64})

    summary = (
        df.unique(subset=["MSN"])
        .filter(
            pl.col("GW ID").is_not_null()
            & ~pl.col("GW ID").eq("")
            & ~pl.col("GW ID").eq("-")
        )
        .group_by("GW ID")
        .agg(pl.count("MSN").alias("Total Nodes"))
    )
    return summary


def load_discom_lat_long(path: Path) -> pl.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Discom lat/long file not found: {path}")

    header = read_csv_header(path)
    columns = [col for col in ["IVRS", "Discom Lat", "Discom Long"] if col in header]
    if "IVRS" not in columns:
        raise RuntimeError(f"Discom lat/long file is missing IVRS column: {path}")

    df = load_selected_columns(path, columns)
    return df


def load_latest_meter_sla(path: Path) -> pl.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Latest SLA meter file not found: {path}")

    header = read_csv_header(path)
    columns = [col for col in ["ID", "SAT Status"] if col in header]
    if "ID" not in columns:
        raise RuntimeError(f"Latest SLA meter file is missing ID column: {path}")

    df = load_selected_columns(path, columns)
    return df.rename({"ID": "IVRS"})


def load_blp_file(path: Path) -> pl.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"BLP file not found: {path}")

    header = read_csv_header(path, skip_rows=1)
    columns = [col for col in ["meter_number", "total_blocks"] if col in header]
    if "meter_number" not in columns or "total_blocks" not in columns:
        raise RuntimeError(f"BLP file is missing required columns: {path}")

    df = load_selected_columns(path, columns, skip_rows=1)
    return df.rename({"meter_number": "MSN", "total_blocks": "BLP"})


def communication_data(debug: bool = True) -> pl.DataFrame:
    if not INPUT_PATH.exists():
        raise FileNotFoundError(f"Input file not found: {INPUT_PATH}")
    if not RAW_DATA_PATH.exists():
        raise FileNotFoundError(f"Raw data file not found: {RAW_DATA_PATH}")

    header = read_csv_header(INPUT_PATH)
    # Load all columns from consumer.csv
    df = load_selected_columns(INPUT_PATH, header)
    
    # Rename only the columns that are in COLUMN_MAP
    rename_map = {col: COLUMN_MAP[col] for col in header if col in COLUMN_MAP}
    df = df.rename(rename_map)
    
    # Convert Feeder Code to numeric
    df = convert_feeder_code(df)
    
    selected_columns = list(rename_map.keys())  # For debug output

    raw_header = read_csv_header(RAW_DATA_PATH, skip_rows=1)
    raw_columns = [col for col in ["meter_number", "com_type"] if col in raw_header]
    if "meter_number" not in raw_columns:
        raise RuntimeError("Raw data file is missing meter_number column")

    raw_df = load_selected_columns(RAW_DATA_PATH, raw_columns, skip_rows=1)
    if "com_type" in raw_columns:
        raw_df = raw_df.rename({"meter_number": "MSN", "com_type": "HES Comm Type"})
    else:
        raw_df = raw_df.rename({"meter_number": "MSN"})

    raw_df = raw_df.unique(subset=["MSN"])
    df = df.join(raw_df, on="MSN", how="left")

    comm_status_df = load_meter_comm_status(METER_COMM_STATUS_PATH)
    comm_status_df = comm_status_df.unique(subset=["MSN"])
    df = df.join(comm_status_df, on="MSN", how="left")

    discom_df = load_discom_lat_long(DISCOM_LAT_LONG_PATH)
    df = df.join(discom_df, on="IVRS", how="left")

    latest_meter_df = load_latest_meter_sla(LATEST_METER_SLA_PATH)
    df = df.join(latest_meter_df, on="IVRS", how="left")

    blp_df = load_blp_file(BLP_PATH)
    df = df.join(blp_df, on="MSN", how="left")
    
    if "Comm Type" not in df.columns:
        df = df.with_columns(pl.lit(None).alias("Comm Type"))
    if "HES Comm Type" not in df.columns:
        df = df.with_columns(pl.lit(None).alias("HES Comm Type"))

    # Comm Type resolution:
    # Priority: Meter Comm Status -> Total Meter Details (HES Comm Type) -> KIMBAL for HES Type KIMBAL/0 -> Asset Not In HES
    hes_kimbal_or_zero = (
        pl.col("HES Type").cast(pl.Utf8).str.to_uppercase() == "KIMBAL"
    ) | (pl.col("HES Type").cast(pl.Utf8) == "0")

    df = df.with_columns(
        pl.when(
            pl.col("Comm Type").is_null()
            | pl.col("Comm Type").eq("")
            | pl.col("Comm Type").eq("-")
        )
        .then(
            pl.when(pl.col("HES Comm Type").is_not_null() & ~pl.col("HES Comm Type").eq("") & ~pl.col("HES Comm Type").eq("-"))
            .then(pl.col("HES Comm Type"))
            .when(hes_kimbal_or_zero)
            .then(pl.lit("KIMBAL"))
            .otherwise(pl.lit("Asset Not In HES"))
        )
        .otherwise(pl.col("Comm Type"))
        .alias("Comm Type")
    )

    # SAT Status: if IVRS found in SLA file use that value, else if Comm Type is KIMBAL use KIMBAL, else Non SAT
    df = df.with_columns(
        pl.when(
            pl.col("SAT Status").is_null()
            | pl.col("SAT Status").eq("")
            | pl.col("SAT Status").eq("-")
        )
        .then(
            pl.when(pl.col("Comm Type") == "KIMBAL")
            .then(pl.lit("KIMBAL"))
            .otherwise(pl.lit("Non SAT"))
        )
        .otherwise(pl.col("SAT Status"))
        .alias("SAT Status")
    )

    if "BLP" in df.columns:
        df = df.with_columns(
            pl.when(pl.col("BLP").is_null())
            .then(
                pl.when(pl.col("Comm Type") == "KIMBAL")
                .then(pl.lit(97))
                .otherwise(pl.lit(0))
            )
            .otherwise(pl.col("BLP"))
            .alias("BLP")
        )

    gw_summary = load_gw_summary(comm_status_df)
    if gw_summary.height > 0:
        df = df.join(gw_summary, on="GW ID", how="left")
    else:
        df = df.with_columns(
            pl.lit(None).alias("GW ID"),
            pl.lit(0).alias("Total Nodes"),
        )

    # GW ID: from Meters Comm Status, if null/empty then fallback based on Comm Type
    if "GW ID" in df.columns:
        df = df.with_columns(
            pl.when(
                pl.col("GW ID").is_null() 
                | pl.col("GW ID").eq("") 
                | pl.col("GW ID").eq("-")
            )
            .then(
                pl.when(pl.col("Comm Type") == "MESH").then(pl.lit("Not Found"))
                .when(pl.col("Comm Type") == "GPRS").then(pl.lit("Cellular"))
                .when(pl.col("Comm Type") == "Not In HES").then(pl.lit("Not Found"))
                .when(pl.col("Comm Type") == "KIMBAL").then(pl.lit("KIMBAL"))
                .otherwise(pl.lit("Not Found"))
            )
            .otherwise(pl.col("GW ID"))
            .alias("GW ID"),
            pl.when(
                pl.col("Total Nodes").is_null()
                | pl.col("GW ID").eq("Cellular")
                | pl.col("GW ID").eq("Not Found")
                | pl.col("GW ID").eq("KIMBAL")
            )
            .then(pl.lit(0))
            .otherwise(pl.col("Total Nodes"))
            .alias("Total Nodes"),
        )

    df = df.drop(["HES Comm Type"])
    df = add_comm_status_and_ageing(df)
    df = format_datetime_column(df, "HES Time Stamp")
    df = format_datetime_column(df, "Last Comm")

    # MI Lat and MI Long are now directly from latitude and longitude columns
    # No need to extract from Location field anymore

    if "Discom Lat" in df.columns or "Discom Long" in df.columns:
        invalid_discom = (
            pl.col("Discom Lat").is_null()
            | pl.col("Discom Lat").eq("")
            | pl.col("Discom Lat").eq("-")
            | pl.col("Discom Lat").eq("0")
            | pl.col("Discom Lat").eq("0.0")
            | pl.col("Discom Long").is_null()
            | pl.col("Discom Long").eq("")
            | pl.col("Discom Long").eq("-")
            | pl.col("Discom Long").eq("0")
            | pl.col("Discom Long").eq("0.0")
        )
        df = df.with_columns(
            pl.when(invalid_discom)
            .then(pl.col("MI Lat"))
            .otherwise(pl.col("Discom Lat"))
            .alias("Discom Lat"),
            pl.when(invalid_discom)
            .then(pl.col("MI Long"))
            .otherwise(pl.col("Discom Long"))
            .alias("Discom Long"),
            pl.when(invalid_discom)
            .then(pl.lit("MI"))
            .otherwise(pl.lit("Discom"))
            .alias("Discom Lat Long Source"),
        )

    # Power Off Remark Logic
    if "Active Last Gasp Time" in df.columns:
        now = datetime.now()
        df = df.with_columns(
            pl.col("Active Last Gasp Time").str.strptime(pl.Datetime, strict=False).alias("gasp_dt")
        )
        
        df = df.with_columns(
            pl.when(pl.col("Comm Status") == "Non Comm")
            .then(
                pl.when(
                    pl.col("gasp_dt").is_not_null()
                )
                .then(
                    pl.when(pl.col("Comm Type") == "GPRS")
                    .then(
                        pl.when(((pl.lit(now) - pl.col("gasp_dt")) / pl.duration(days=1)).cast(pl.Int64) >= 21)
                        .then(pl.lit("GPRS Power Off (+30 Days)"))
                        .when(((pl.lit(now) - pl.col("gasp_dt")) / pl.duration(days=1)).cast(pl.Int64) >= 11)
                        .then(pl.lit("GPRS Power Off (+20 Days)"))
                        .when(((pl.lit(now) - pl.col("gasp_dt")) / pl.duration(days=1)).cast(pl.Int64) >= 4)
                        .then(pl.lit("GPRS Power Off (+10 Days)"))
                        .when(((pl.lit(now) - pl.col("gasp_dt")) / pl.duration(days=1)).cast(pl.Int64) >= 0)
                        .then(pl.lit("GPRS Power Off (+3 Days)"))
                        .otherwise(pl.lit(None))
                    )
                    .when(pl.col("Comm Type") == "MESH")
                    .then(
                        pl.when(((pl.lit(now) - pl.col("gasp_dt")) / pl.duration(days=1)).cast(pl.Int64) >= 21)
                        .then(pl.lit("RF Power Off (+30 Days)"))
                        .when(((pl.lit(now) - pl.col("gasp_dt")) / pl.duration(days=1)).cast(pl.Int64) >= 11)
                        .then(pl.lit("RF Power Off (+20 Days)"))
                        .when(((pl.lit(now) - pl.col("gasp_dt")) / pl.duration(days=1)).cast(pl.Int64) >= 4)
                        .then(pl.lit("RF Power Off (+10 Days)"))
                        .when(((pl.lit(now) - pl.col("gasp_dt")) / pl.duration(days=1)).cast(pl.Int64) >= 0)
                        .then(pl.lit("RF Power Off (+3 Days)"))
                        .otherwise(pl.lit(None))
                    )
                    .otherwise(pl.lit(None))
                )
                .otherwise(pl.lit(None))
            )
            .otherwise(pl.lit(None))
            .alias("Power Off Remark")
        )
        
        df = df.drop("gasp_dt")
    else:
        df = df.with_columns(pl.lit(None).alias("Power Off Remark"))

    # Reorder columns: put priority columns first, then all remaining columns
    priority_columns = [
        "MSN",
        "IVRS",
        "SAT Status",
        "Circle",
        "Division",
        "DC",
        "MI Lat",
        "MI Long",
        "Discom Lat",
        "Discom Long",
        "Discom Lat Long Source",
        "DT Name",
        "DT Code",
        "Feeder",
        "Feeder Code",
        "new_meter_phase",
        "MI_Category",
        "installer_name",       
        "HES Type",
        "Comm Type",
        "GW ID",
        "Total Nodes",
        "Install Date",
        "HES Time Stamp",
        "Last Comm",
        "Comm Status",
        "Ageing",
        "Active Last Gasp Time",
        "Power Off Remark",
        "BLP",
    ]
    
    # Get all columns in dataframe
    all_cols = df.columns
    
    # Order: priority columns first (if they exist), then remaining columns
    ordered_columns = [col for col in priority_columns if col in all_cols]
    remaining_columns = [col for col in all_cols if col not in priority_columns]
    final_columns = ordered_columns + remaining_columns
    
    df = df.select(final_columns)

    if debug:
        print(f"Selected columns from consumer.csv: {selected_columns}")
        print(f"Total output columns: {len(final_columns)}")
        print(f"Data shape: {df.shape}")
        try:
            print(df.head(5))
        except UnicodeEncodeError:
            safe_text = str(df.head(5).to_dicts())
            sys.stdout.buffer.write((safe_text + "\n").encode("utf-8", errors="replace"))

    try:
        df.write_csv(OUTPUT_PATH)
    except OSError as exc:
        raise OSError(
            f"Unable to write output file {OUTPUT_PATH}. Close it if open and rerun."
        ) from exc

    print(f"Wrote {OUTPUT_PATH} with shape {df.shape}.")
    return df


# //////////////////////////////////////////////////////////////////////////
# ////////////////   GATEWAY DATA PROCESSING SCRIPT   //////////////////////
# //////////////////////////////////////////////////////////////////////////

def gateway_data(debug: bool = True) -> pl.DataFrame | None:
    """Process gateway plan and incorrect locations from consumer_qgis.csv.

    - Filters Comm Type containing 'MESH' and Comm Status containing 'Non' or 'Never'.
    - Rows with missing/zero MI Lat or MI Long are written to incorrect_location.csv.
    - Remaining meters are clustered by proximity (<=100 m) using haversine distance.
    - Clusters with size >= 10 are written to gateway_plan.csv with `cluster_id`.
    """
    def parse_float(val):
        if val is None:
            return None
        v = str(val).strip()
        if v == '' or v == '0' or v == '0.0':
            return None
        try:
            return float(v)
        except Exception:
            return None

    def haversine_meters(lat1, lon1, lat2, lon2):
        R = 6371000.0
        phi1 = math.radians(lat1)
        phi2 = math.radians(lat2)
        dphi = math.radians(lat2 - lat1)
        dlambda = math.radians(lon2 - lon1)
        a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dlambda/2)**2
        return R * (2 * math.atan2(math.sqrt(a), math.sqrt(1-a)))

    input_path = OUTPUT_PATH
    gateway_out = OUTPUT_DIR / "gateway_plan.csv"
    incorrect_out = OUTPUT_DIR / "incorrect_location.csv"

    if not input_path.exists():
        print(f"Input file not found: {input_path}")
        return None

    # Read CSV treating all columns as text to avoid parse errors on mixed types
    header = read_csv_header(input_path)
    schema_overrides = {col: pl.Utf8 for col in header}
    df = pl.read_csv(
        input_path,
        infer_schema_length=10000,
        schema_overrides=schema_overrides,
        null_values=["null"],
        try_parse_dates=False,
    )

    if "MI Lat" not in df.columns or "MI Long" not in df.columns:
        print("MI Lat / MI Long columns not found in output; skipping gateway processing.")
        return None

    # Filter Comm Type MESH (case-insensitive)
    comm_type_col = pl.col("Comm Type").cast(pl.Utf8).str.to_lowercase()
    comm_status_col = pl.col("Comm Status").cast(pl.Utf8).str.to_lowercase()

    mask = comm_type_col.str.contains("mesh") & (
        comm_status_col.str.contains("non") | comm_status_col.str.contains("never")
    )

    # Skip meters that are non-comm due to power-off/alarm conditions, as these are not suitable for gateway planning.
    power_off_col = pl.col("Power Off Remark").cast(pl.Utf8)
    mask = mask & (
        power_off_col.is_null() | power_off_col.eq("") | power_off_col.eq("null")
    )

    df_sel = df.filter(mask)

    dicts = df_sel.to_dicts()

    valid = []
    incorrect = []
    for r in dicts:
        lat = parse_float(r.get("MI Lat"))
        lon = parse_float(r.get("MI Long"))
        if lat is None or lon is None:
            incorrect.append(r)
        else:
            r["_mi_lat"] = lat
            r["_mi_lon"] = lon
            valid.append(r)

    if debug:
        print(f"Filtered {len(valid)} MESH Non/Never Comm meters with valid coords; {len(incorrect)} incorrect")

    # Build adjacency (distance <= 100 m)
    eps = 100.0
    coords = [(r["_mi_lat"], r["_mi_lon"]) for r in valid]
    adj = [[] for _ in range(len(coords))]
    for i in range(len(coords)):
        for j in range(i + 1, len(coords)):
            d = haversine_meters(coords[i][0], coords[i][1], coords[j][0], coords[j][1])
            if d <= eps:
                adj[i].append(j)
                adj[j].append(i)

    # Connected components to limit cluster search scope
    seen = [False] * len(coords)
    components = []
    for i in range(len(coords)):
        if seen[i]:
            continue
        stack = [i]
        comp = []
        seen[i] = True
        while stack:
            u = stack.pop()
            comp.append(u)
            for v in adj[u]:
                if not seen[v]:
                    seen[v] = True
                    stack.append(v)
        components.append(comp)

    # Use a strict gateway radius (50m) so the diameter is 100m.
    gateway_radius = 50.0
    clusters = []
    min_group = 10

    for comp in components:
        if len(comp) < min_group:
            continue
        remaining = set(comp)
        while remaining:
            best_center = None
            best_cluster = None
            best_size = 0
            for i in remaining:
                # determine all points within gateway radius of candidate center i
                center_lat, center_lon = coords[i]
                cluster_points = [j for j in remaining if haversine_meters(center_lat, center_lon, coords[j][0], coords[j][1]) <= gateway_radius]
                if len(cluster_points) > best_size:
                    best_size = len(cluster_points)
                    best_center = i
                    best_cluster = cluster_points
            if best_center is None or best_size < min_group:
                break
            clusters.append((best_center, sorted(best_cluster)))
            remaining -= set(best_cluster)
            # if remaining points are fewer than min_group, stop
            if len(remaining) < min_group:
                break

    # Build meter-level rows and assign Gateway Location IDs
    from collections import Counter
    gateway_rows = []
    cluster_mappings = []  # list of (gateway_id, member_rows)

    # Build prefix like GW_JUN24 using current month abbrev and day
    now = datetime.now()
    prefix = f"GW_{now.strftime('%b').upper()}{now.day:02d}"

    for i, (center_idx, c) in enumerate(clusters):
        gw_id = f"{prefix}_{i:04d}"
        center_lat, center_lon = coords[center_idx]
        members = []
        for idx in c:
            row = dict(valid[idx])
            row.pop("_mi_lat", None)
            row.pop("_mi_lon", None)
            row["Gateway Location ID"] = gw_id
            members.append(row)
            gateway_rows.append(row)
        cluster_mappings.append((gw_id, members, center_lat, center_lon))

    # Helper to clean feeder code (remove leading apostrophe and non-digit noise)
    def clean_feeder_code(val):
        if val is None:
            return ""
        s = str(val).strip()
        # remove leading apostrophe
        if s.startswith("'"):
            s = s[1:]
        # if decimal like 12345.0, keep integer part
        if "." in s:
            try:
                f = float(s)
                if f.is_integer():
                    return str(int(f))
            except Exception:
                pass
        # remove any non-digit prefix/suffix
        import re as _re
        m = _re.search(r"(\d+)", s)
        return m.group(1) if m else s

    # Prepare aggregated gateway_plan rows
    agg_rows = []
    for gw_id, members, center_lat, center_lon in cluster_mappings:
        circles = []
        dcs = []
        feeders = []
        feeder_codes = []
        for m in members:
            if m.get("Circle"):
                circles.append(m.get("Circle"))
            # DC column may be named 'DC' or 'DC Name'
            dc_val = m.get("DC") or m.get("DC Name") or m.get("DC_Name")
            if dc_val:
                dcs.append(dc_val)
            fn = m.get("Feeder") or m.get("Feeder Name") or m.get("Feeder_name")
            if fn:
                feeders.append(fn)
            fc = m.get("Feeder Code")
            if fc:
                feeder_codes.append(fc)

        lat_planned = center_lat
        lon_planned = center_lon

        # pick most common values
        circle_v = Counter([c for c in circles if c]).most_common(1)
        circle_v = circle_v[0][0] if circle_v else ""
        dc_v = Counter([c for c in dcs if c]).most_common(1)
        dc_v = dc_v[0][0] if dc_v else ""
        feeder_name_v = Counter([f for f in feeders if f]).most_common(1)
        feeder_name_v = feeder_name_v[0][0] if feeder_name_v else ""
        feeder_code_v = Counter([fc for fc in feeder_codes if fc]).most_common(1)
        feeder_code_v = feeder_code_v[0][0] if feeder_code_v else ""
        feeder_code_v = clean_feeder_code(feeder_code_v)

        agg_rows.append({
            "Circle": circle_v,
            "DC Name": dc_v,
            "Feeder Name": feeder_name_v,
            "Feeder Code": feeder_code_v,
            "Gateway Location ID": gw_id,
            "Latitude Planned": lat_planned,
            "Longitude Planned": lon_planned,
            "cluster count": len(members),
        })

    # Write outputs
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # gateway_plan_meters: meter-level list with Gateway Location ID
    meters_out = OUTPUT_DIR / "gateway_plan_meters.csv"
    if gateway_rows:
        meters_df = pl.DataFrame(gateway_rows)
        # ensure Gateway Location ID column exists and write
        meters_df.write_csv(meters_out)
    else:
        empty_m = pl.DataFrame({h: [] for h in df_sel.columns + ["Gateway Location ID"]})
        empty_m.write_csv(meters_out)

    # gateway_plan: aggregated plan
    if agg_rows:
        agg_df = pl.DataFrame(agg_rows)
        # Ensure column order as requested
        cols = ["Circle", "DC Name", "Feeder Name", "Feeder Code", "Gateway Location ID", "Latitude Planned", "Longitude Planned", "cluster count"]
        agg_df = agg_df.select(cols)
        agg_df.write_csv(gateway_out)
    else:
        empty_a = pl.DataFrame({"Circle": [], "DC Name": [], "Feeder Name": [], "Feeder Code": [], "Gateway Location ID": [], "Latitude Planned": [], "Longitude Planned": []})
        empty_a.write_csv(gateway_out)

    # incorrect locations
    if incorrect:
        inc_df = pl.DataFrame(incorrect)
        inc_df.write_csv(incorrect_out)
    else:
        empty2 = pl.DataFrame({h: [] for h in df_sel.columns})
        empty2.write_csv(incorrect_out)

    print(f"Wrote {gateway_out} ({len(agg_rows)} rows), {meters_out} ({len(gateway_rows)} rows) and {incorrect_out} ({len(incorrect)} rows)")
    return pl.DataFrame(agg_rows) if agg_rows else None


def main(debug: bool = True) -> None:
    communication_data(debug=debug)
    gateway_data(debug=debug)


if __name__ == "__main__":
    main(debug=True)
