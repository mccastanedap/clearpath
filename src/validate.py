import pandas as pd

REQUIRED_COLUMNS = ['date', 'product_name', 'category', 'size', 'quantity', 'price']


class CSVValidationError(Exception):
    """
    Raised when an uploaded sales CSV cannot be processed.
    client_message: friendly message to email to the client.
    detail: technical detail for the logs (never shown to the client).
    """
    def __init__(self, client_message, detail=""):
        self.client_message = client_message
        self.detail = detail
        super().__init__(detail or client_message)


def validate_sales_df(df):
    """
    Validate an already-read sales DataFrame (e.g. the one returned by
    read_csv_from_s3). Normalizes column names to lowercase in place so
    clean_sales_data sees them, and returns an optional warning string
    (or None). Raises CSVValidationError with a client_message if the
    file cannot be processed.
    """
    if df is None:
        raise CSVValidationError(
            "We couldn't read this file. Please make sure it's a valid CSV "
            "(exported from Excel or your point-of-sale system) and not an Excel, "
            "PDF, or other format.",
            detail="read returned None",
        )

    # Normalize column names (trim spaces, lowercase) to be tolerant
    df.columns = [str(c).strip().lower() for c in df.columns]

    # A single column means a wrong separator or not a CSV (Excel/PDF renamed)
    if len(df.columns) <= 1:
        raise CSVValidationError(
            "This file doesn't look like a comma-separated CSV. Please make sure the "
            "columns are separated by commas (not semicolons) and that the file is a CSV, "
            "not an Excel or PDF.",
            detail=f"Single column detected: {list(df.columns)}",
        )

    # Has a header but no data rows
    if df.empty:
        raise CSVValidationError(
            "The file has no data rows. Please make sure your CSV includes your sales."
        )

    # Required columns
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise CSVValidationError(
            "Your file is missing these columns: " + ", ".join(missing) + ". "
            "The CSV must have exactly these columns: " + ", ".join(REQUIRED_COLUMNS) + ".",
            detail=f"Columns found: {list(df.columns)}",
        )

    # Is there at least one usable row? (checked on a copy; the returned df is untouched)
    rows_total = len(df)
    check = df.copy()
    check['date'] = pd.to_datetime(check['date'], errors='coerce')
    check['quantity'] = pd.to_numeric(check['quantity'], errors='coerce')
    check['price'] = pd.to_numeric(check['price'], errors='coerce')
    check = check.dropna(subset=['date', 'product_name', 'quantity', 'price'])
    check = check[check['quantity'] > 0]

    if check.empty:
        raise CSVValidationError(
            "Your file has the right columns, but none of the rows have valid data. "
            "Please check that the dates, quantities, and prices are entered correctly.",
            detail=f"{rows_total} rows read, 0 valid after cleaning.",
        )

    # Soft warning (does not stop the pipeline): how many rows will be dropped
    dropped = rows_total - len(check)
    if dropped > 0:
        pct = round(100 * dropped / rows_total)
        return (f"{dropped} of {rows_total} rows ({pct}%) were skipped due to "
                f"invalid data (dates, quantities, or prices).")
    return None


def _read_csv(path):
    """Read a CSV from disk, tolerating Excel encodings. For local testing only."""
    try:
        return pd.read_csv(path)
    except pd.errors.EmptyDataError:
        raise CSVValidationError(
            "The file is empty. Please export your CSV with data before uploading."
        )
    except UnicodeDecodeError:
        try:
            return pd.read_csv(path, encoding='latin-1')
        except Exception as e:
            raise CSVValidationError(
                "We couldn't read this file. Please make sure it's a valid CSV.",
                detail=f"read_csv failed after latin-1 retry: {type(e).__name__}: {e}",
            )
    except pd.errors.ParserError as e:
        raise CSVValidationError(
            "We couldn't read this file. Please make sure it's a valid CSV and not corrupted.",
            detail=f"ParserError: {e}",
        )


def load_and_validate_csv(path):
    """Read a CSV from disk and validate it. For local testing. Returns (df, warning)."""
    df = _read_csv(path)
    warning = validate_sales_df(df)
    return df, warning