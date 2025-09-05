# README
# Formiq

A tiny, Snakemake/Nextflow-style framework for building **Python-first** DAGs with **data quality checks**.

- Config via `formiq.yml`: profiles/envs, params, targets
- Nodes are plain Python: `@qtask` for data, `@qcheck` for assertions
- Caching & DAG execution; JSON/JUnit/Markdown output
- Optional SQLAlchemy reflection for DB tables

## Install

```bash
pip install -e .[pandas,db]   # dev
# or:
pipx install .
