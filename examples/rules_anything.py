# examples/rules_anything.py
from __future__ import annotations
import pandas as pd
from formiq.core import qtask, qcheck, CheckResult

@qtask(id="build_dataset")
def build_dataset(ctx):
    """Make a DataFrame from any source: DB reflection, CSV path, or injected DataFrame."""
    if ctx.env.get("session_factory") and ctx.env.get("tables"):
        Session = ctx.env["session_factory"]; tables = ctx.env["tables"]
        table_name = ctx.params.get("table_name")
        if not table_name or table_name not in tables: return pd.DataFrame()
        t = tables[table_name]
        with Session() as s:
            rows = s.execute(t.select()).mappings().all()
        return pd.DataFrame([dict(r) for r in rows])

    if "csv_path" in ctx.params:
        return pd.read_csv(ctx.params["csv_path"])

    if "dataframe" in ctx.env:
        return ctx.env["dataframe"]

    return pd.DataFrame()

@qtask(id="summarize", requires=["build_dataset"])
def summarize(ctx):
    df: pd.DataFrame = ctx.get("build_dataset")
    if df.empty: return pd.DataFrame()
    key = ctx.params.get("group_key") or df.columns[0]
    num_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
    if not num_cols: return df[[key]].drop_duplicates().assign(_rows=1)
    agg = {c: ["min","max","mean"] for c in num_cols}
    summary = df.groupby(key).agg(agg)
    summary.columns = [f"{a}_{b}" for a,b in summary.columns]
    return summary.reset_index()

@qcheck(id="qc_basic", requires=["build_dataset"], severity="error")
def qc_basic(ctx):
    df: pd.DataFrame = ctx.get("build_dataset")
    must_have = ctx.params.get("required_columns", [])
    missing_cols = [c for c in must_have if c not in df.columns]
    status = "pass" if df.shape[0] > 0 and not missing_cols else "fail"
    return CheckResult(
        id="qc_basic",
        status=status,
        metrics={"rowcount": int(df.shape[0]), "missing_columns": missing_cols},
        description="Dataset non-empty and contains required columns."
    )

@qcheck(id="recap", requires=["summarize"], severity="info")
def recap(ctx):
    summary: pd.DataFrame = ctx.get("summarize")
    sample = summary.head(5).to_dict(orient="records") if not summary.empty else []
    return CheckResult(
        id="recap",
        status="pass",
        severity="info",
        metrics={"summary_preview_rows": len(sample), "sample": sample},
        description="Preview of summary."
    )
