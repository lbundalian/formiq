# formiq/reporting/json_to_junit.py
from __future__ import annotations
from typing import Dict, Any
import sys
import xml.etree.ElementTree as ET

def print_junit(results: Dict[str, Any]) -> None:
    checks = [(k,v[1]) for k,v in results.items() if v[0]=="check"]
    suite = ET.Element("testsuite", name="formiq", tests=str(len(checks)))
    for _, res in checks:
        tc = ET.SubElement(suite, "testcase", name=res.id, classname=res.metrics.get("model","formiq"))
        if res.status in ("fail","error"):
            tag = "failure" if res.status == "fail" else "error"
            ET.SubElement(tc, tag, message=res.description or res.error or res.id)
    ET.ElementTree(suite).write(sys.stdout.buffer, encoding="utf-8", xml_declaration=True)
