#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
from pathlib import Path
 
CLEAN = Path("data/clean")
 


# In[2]:


def build_funnel(events: pd.DataFrame) -> pd.DataFrame:
    """One-row-per-stage funnel with drop-off rates."""
    stages = {
        "signup":               events.query("event_type=='signup'")["user_id"].nunique(),
        "onboarding_started":   events.query("event_type=='onboarding_start'")["user_id"].nunique(),
        "onboarding_completed": events.query("event_type=='onboarding_complete'")["user_id"].nunique(),
        "trial_started":        events.query("event_type=='trial_start'")["user_id"].nunique(),
        "payment_initiated":    events.query("event_type=='payment_initiated'")["user_id"].nunique(),
        "payment_success":      events.query("event_type=='payment_success'")["user_id"].nunique(),
    }
    return (pd.DataFrame({"stage": stages.keys(), "users": stages.values()})
        .assign(
            pct_of_signup = lambda x: (x["users"] / x["users"].iloc[0] * 100).round(1),
            dropoff_pct   = lambda x: (100 - x["users"].shift(-1) / x["users"] * 100).round(1),
        ))


# In[3]:


def segment_analysis(users: pd.DataFrame, paid_uids: set, by: str) -> pd.DataFrame:
    return (users
        .assign(converted = lambda x: x["user_id"].isin(paid_uids).astype(int))
        .groupby(by)
        .agg(total=("user_id", "count"), converted=("converted", "sum"))
        .assign(
            conv_rate  = lambda x: (x["converted"] / x["total"] * 100).round(1),
            cac_proxy  = lambda x: (x["total"] / x["converted"].replace(0, 1)).round(1),
        )
        .sort_values("conv_rate")
    )
 


# In[4]:


def behavioral_analysis(users, sessions, paid_uids):
    sess_agg = (sessions
        .groupby("user_id")
        .agg(
            session_count = ("session_id",  "nunique"),
            avg_duration  = ("session_duration_sec", "mean"),
            total_pages   = ("pages_viewed", "sum"),
        )
        .reset_index()
    )
    return (users[["user_id"]]
        .assign(converted = lambda x: x["user_id"].isin(paid_uids).astype(int))
        .merge(sess_agg, on="user_id", how="left")
        .fillna(0)
        .groupby("converted")
        .agg(
            avg_sessions     = ("session_count", "mean"),
            avg_duration_sec = ("avg_duration",  "mean"),
            avg_pages        = ("total_pages",    "mean"),
        )
        .round(2)
    )
 


# In[5]:


def find_activation_moment(users, events, sessions,
                            paid_uids, session_threshold=3, day_window=2):
    """
    Identify the behavioral threshold that maximally separates converters.
    Default: 3+ sessions within 2 days of onboarding completion.
    """
    onb = (events
        .query("event_type=='onboarding_complete'")[["user_id", "event_time"]]
        .rename(columns={"event_time": "onb_time"})
    )
 
    early = (sessions
        .merge(onb, on="user_id")
        .assign(
            days_from_onb = lambda x:
                (x["session_start"] - x["onb_time"]).dt.total_seconds() / 86400
        )
        .query(f"days_from_onb >= 0 and days_from_onb <= {day_window}")
        .groupby("user_id")["session_id"]
        .nunique()
        .reset_index()
        .rename(columns={"session_id": "early_sessions"})
    )
 
    result = (users[["user_id"]]
        .assign(converted = lambda x: x["user_id"].isin(paid_uids).astype(int))
        .merge(early, on="user_id", how="left")
        .fillna(0)
        .assign(activated = lambda x: x["early_sessions"] >= session_threshold)
    )
 
    conv_rates = result.groupby("activated")["converted"].mean()
    not_act    = conv_rates.get(False, 0.001)
    activated  = conv_rates.get(True,  0)
    lift       = activated / max(not_act, 0.001)
 
    return {
        "threshold":        f"{session_threshold}+ sessions in {day_window} days post-onboarding",
        "non_activated_cr": round(not_act,  3),
        "activated_cr":     round(activated, 3),
        "lift":             round(lift, 2),
    }
 


# In[15]:


def validate_revenue(events, payments, users):
    pay_event_uids = set(events.query("event_type=='payment_success'")["user_id"])

    pay_table_uids = set(
        payments.loc[payments["payment_status"] == "successful", "user_id"]
    )

    payments_merged = payments.merge(
    users[["user_id", "traffic_source"]],
    on="user_id",
    how="left"
)

    payments_success = payments_merged[
        payments_merged["payment_status"] == "successful"
    ]

    return {
        "orphan_payments": len(pay_table_uids - pay_event_uids),
        "events_without_records": len(pay_event_uids - pay_table_uids),
        "failed_payment_attempts": int(
            payments[payments["payment_status"] == "failed"]["user_id"].count()
        ),
        "total_revenue": float(payments_success["amount"].sum()),
        "revenue_by_plan": (
            payments_success.groupby("traffic_source")["amount"] \
    .agg(["sum","count","mean"]) \
    .rename(columns={
        "sum": "revenue",
        "count": "customers",
        "mean": "avg_ticket"
    }))
            .sort_values("revenue", ascending=False)
            .round(2)
        
    }


# In[16]:


def run():
    users    = pd.read_csv(CLEAN / "users_clean.csv",    parse_dates=["signup_date"])
    events   = pd.read_csv(CLEAN / "events_clean.csv",   parse_dates=["event_time"])
    sessions = pd.read_csv(CLEAN / "sessions_clean.csv", parse_dates=["session_start"])
    payments = pd.read_csv(CLEAN / "payments_clean.csv", parse_dates=["payment_date"])

    paid_uids = set(events.query("event_type=='payment_success'")["user_id"])

    print("\n====  FUNNEL  ====")
    print(build_funnel(events).to_string(index=False))

    for seg in ["device", "traffic_source", "country"]:
        print(f"\n====  SEGMENTATION by {seg.upper()}  ====")
        print(segment_analysis(users, paid_uids, seg).to_string())

    print("\n====  BEHAVIORAL (converted=1 vs 0)  ====")
    print(behavioral_analysis(users, sessions, paid_uids).to_string())

    act = find_activation_moment(users, events, sessions, paid_uids)
    print(f"\n====  ACTIVATION MOMENT  ====")
    for k, v in act.items():
        print(f"  {k}: {v}")

    # ✅ FIXED LINE (this was your issue)
    rv = validate_revenue(events, payments, users)

    print(f"\n====  REVENUE VALIDATION  ====")
    print(f"  Total revenue:          ${rv['total_revenue']:,.0f}")
    print(f"  Orphan payments:        {rv['orphan_payments']}")
    print(f"  Events without records: {rv['events_without_records']}")
    print(f"  Failed attempts:        {rv['failed_payment_attempts']}")
    print(f"\n  Revenue by plan:\n{rv['revenue_by_plan'].to_string()}")


if __name__ == "__main__":
    run()


# In[14]:





# In[ ]:




