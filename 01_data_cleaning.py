"""
SaaS Funnel Analysis — Step 1 & 2: Data Cleaning + Validation

"""
import pandas as pd
import numpy as np
from pathlib import Path

RAW_PATH   = Path("data/raw")
CLEAN_PATH = Path("data/clean")
CLEAN_PATH.mkdir(parents=True, exist_ok=True)


def clean_users(df: pd.DataFrame) -> tuple:
    """Clean users table. Returns (clean_df, quality_report)."""
    report_before = {
        "rows":              len(df),
        "null_signup_date":  df["signup_date"].isna().sum(),
        "null_age":          df["age"].isna().sum(),
        "null_plan_type":    df["plan_type"].isna().sum(),
        "duplicate_user_ids": df.duplicated("user_id").sum(),
    }

    df_clean = (df
        .assign(
            country     = lambda x: x["country"].str.strip().str.upper(),
            device      = lambda x: (x["device"].str.strip().str.lower()
                                       .str.replace(r"^mob$", "mobile", regex=True)),
            signup_date = lambda x: pd.to_datetime(x["signup_date"], errors="coerce"),
            age         = lambda x: pd.to_numeric(x["age"], errors="coerce"),
            plan_type   = lambda x: x["plan_type"].fillna("unknown"),
        )
        .drop_duplicates("user_id")
        .dropna(subset=["signup_date"])
    )

    report_after = {
        "rows":              len(df_clean),
        "null_signup_date":  0,
        "null_age":          df_clean["age"].isna().sum(),
        "null_plan_type":    0,
        "duplicate_user_ids": 0,
    }

    return df_clean, {"before": report_before, "after": report_after}


def clean_events(df: pd.DataFrame) -> tuple:
    """Remove duplicates, fix negative values, validate event types."""
    VALID_EVENTS = {
        "signup", "onboarding_start", "onboarding_complete",
        "trial_start", "payment_initiated", "payment_success", "payment_failed"
    }

    report_before = {
        "rows":              len(df),
        "duplicates":        df.duplicated(["user_id", "event_type", "event_time"]).sum(),
        "negative_values":   (df["event_value"].fillna(0) < 0).sum(),
        "invalid_types":     (~df["event_type"].isin(VALID_EVENTS)).sum(),
    }

    df_clean = (df
        .assign(event_time = lambda x: pd.to_datetime(x["event_time"], errors="coerce"))
        .dropna(subset=["event_time"])
        .drop_duplicates(["user_id", "event_type", "event_time"])
        .query("event_type in @VALID_EVENTS")
        .assign(event_value = lambda x: np.where(x["event_value"] < 0, np.nan, x["event_value"]))
    )

    report_after = {"rows": len(df_clean), "duplicates": 0, "negative_values": 0, "invalid_types": 0}

    return df_clean, {"before": report_before, "after": report_after}


def clean_sessions(df: pd.DataFrame) -> pd.DataFrame:
    return (df
        .assign(
            session_start    = lambda x: pd.to_datetime(x["session_start"], errors="coerce"),
            duration_seconds = lambda x: np.where(x["duration_seconds"] < 0, np.nan, x["duration_seconds"]),
            pages_viewed     = lambda x: np.where(x["pages_viewed"] < 0, np.nan, x["pages_viewed"]),
        )
        .dropna(subset=["session_start"])
        .assign(
            duration_seconds = lambda x: x["duration_seconds"].fillna(x["duration_seconds"].median()),
            pages_viewed     = lambda x: x["pages_viewed"].fillna(x["pages_viewed"].median()),
        )
    )


def clean_payments(df: pd.DataFrame) -> pd.DataFrame:
    return (df
        .assign(
            payment_date = lambda x: pd.to_datetime(x["payment_date"], errors="coerce"),
            status       = lambda x: x["status"].str.strip().str.lower(),
            amount       = lambda x: np.where(x["amount"] < 0, np.nan, x["amount"]),
        )
        .dropna(subset=["payment_date"])
    )


def print_report(name, report):
    print(f"\n{'='*50}")
    print(f"  {name} DATA QUALITY REPORT")
    print(f"{'='*50}")
    print(f"  {'Metric':<30} {'Before':>10} {'After':>10}")
    print(f"  {'-'*50}")
    for k in report["before"]:
        b = report["before"][k]
        a = report["after"].get(k, "—")
        flag = " ✓" if a == 0 or a == report["after"].get("rows") else ""
        print(f"  {k:<30} {str(b):>10} {str(a):>10}{flag}")


def run():
    users_raw    = pd.read_csv(RAW_PATH / "users.csv")
    events_raw   = pd.read_csv(RAW_PATH / "events.csv")
    sessions_raw = pd.read_csv(RAW_PATH / "sessions.csv")
    payments_raw = pd.read_csv(RAW_PATH / "payments.csv")

    users_clean,    u_rpt = clean_users(users_raw)
    events_clean,   e_rpt = clean_events(events_raw)
    sessions_clean        = clean_sessions(sessions_raw)
    payments_clean        = clean_payments(payments_raw)

    print_report("USERS",  u_rpt)
    print_report("EVENTS", e_rpt)

    users_clean.to_csv(CLEAN_PATH / "users_clean.csv",       index=False)
    events_clean.to_csv(CLEAN_PATH / "events_clean.csv",     index=False)
    sessions_clean.to_csv(CLEAN_PATH / "sessions_clean.csv", index=False)
    payments_clean.to_csv(CLEAN_PATH / "payments_clean.csv", index=False)

    print("\n  Clean datasets saved to data/clean/")


if __name__ == "__main__":
    run()
