# formiq/reporting/markdown_reporter.py
from __future__ import annotations
from typing import Dict, Any

def print_markdown(results: Dict[str, Any]) -> None:
    print("# Formiq results\n")
    for name, (kind, val) in results.items():
        if kind == "task":
            print(f"## Task: `{name}`")
            preview = str(val)
            if len(preview) > 400: preview = preview[:400] + "…"
            print(f"```\n{preview}\n```")
        else:
            print(f"## Check: `{name}` — **{val.status.upper()}** ({val.severity})")
            if val.description: print(f"{val.description}\n")
            if val.metrics:
                print("**Metrics**")
                for k,v in val.metrics.items():
                    pv = str(v)
                    if len(pv) > 200: pv = pv[:200] + "…"
                    print(f"- `{k}`: {pv}")
            if val.samples:
                print("\n**Samples (first 5)**")
                for row in val.samples[:5]:
                    print(f"- {row}")
            print()
