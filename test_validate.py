import sys
sys.path.insert(0, 'src')
from validate import load_and_validate_csv, CSVValidationError

for f in ['data/good.csv', 'data/bad.csv']:  # use your own test files
    try:
        df, warning = load_and_validate_csv(f)
        print(f, "-> PASSED,", len(df), "rows", "| " + warning if warning else "")
    except CSVValidationError as e:
        print(f, "-> REJECTED:", e.client_message)