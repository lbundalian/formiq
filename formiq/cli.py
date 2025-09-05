# formiq/cli.py
from __future__ import annotations
import argparse, importlib, importlib.util, json, os, sys, textwrap, pathlib
from typing import Any, Dict
import yaml
from .core import Runner, list_nodes

def import_module_from_path(module_name, module_path):
    """Dynamically import a module from a path"""
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module

def load_config(path: str = "formiq.yml"):
    with open(path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    profile = os.environ.get("FORMIQ_PROFILE") or cfg.get("profile", "dev")
    env_cfg: Dict[str, Any] = cfg.get("envs", {}).get(profile, {})
    params: Dict[str, Any] = cfg.get("params", {})
    def expand(v): return os.path.expandvars(v) if isinstance(v, str) else v
    params = {k: expand(v) for k, v in params.items()}
    return cfg, env_cfg, params, profile

def build_env(env_cfg: Dict[str, Any]):
    """If 'env_module' provided, call build_env(**env_cfg); else return env_cfg."""
    module_name = env_cfg.get("env_module")
    if module_name:
        # Convert module name to file path (e.g., "examples.sqlalchemy_env" -> "examples/sqlalchemy_env.py")
        module_path = module_name.replace(".", "/") + ".py"
        if os.path.exists(module_path):
            mod = import_module_from_path(module_name, module_path)
        else:
            # Fallback to standard import if file doesn't exist
            parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            if parent_dir not in sys.path:
                sys.path.insert(0, parent_dir)
            mod = importlib.import_module(module_name)
        
        func = getattr(mod, "build_env")
        kwargs = {k:v for k,v in env_cfg.items() if k != "env_module"}
        return func(**kwargs)
    return dict(env_cfg)

def cmd_run(args):
    ## testing
    # args.config = 'examples/formiq.yml'
    cfg, env_cfg, params, _ = load_config(args.config)
    for m in cfg.get("modules", []):
        # Convert module name to file path (e.g., "examples.rules_anything" -> "examples/rules_anything.py")
        module_path = m.replace(".", "/") + ".py"
        if os.path.exists(module_path):
            import_module_from_path(m, module_path)
        else:
            # Fallback to standard import if file doesn't exist
            parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            if parent_dir not in sys.path:
                sys.path.insert(0, parent_dir)
            importlib.import_module(m)

    if args.targets:
        targets = []
        for t in args.targets:
            if t in cfg.get("targets", {}):
                targets.extend(cfg["targets"][t])
            else:
                targets.append(t)
    else:
        targets = cfg["targets"].get("daily", [])

    env = build_env(env_cfg)
    max_workers = env_cfg.get("resources", {}).get("max_workers", os.cpu_count() or 4)
    nodes = list_nodes()
    runner = Runner(env=env, params=params, workdir=args.workdir, max_workers=max_workers)
    results = runner.run(targets, parallel=args.parallel)

    failures = [n for n,(kind,res) in results.items()
                if kind=="check" and res.status in ("fail","error") and getattr(res,"severity","error")=="error"]

    if args.reporter == "json":
        print(json.dumps({k:(v[1].__dict__ if v[0]=="check" else v[1]) for k,v in results.items()},
                         default=str, indent=2))
    elif args.reporter == "junit":
        from .reporting.json_to_junit import print_junit
        print_junit(results)
    else:
        from .reporting.markdown_reporter import print_markdown
        print_markdown(results)

    sys.exit(1 if failures else 0)

def cmd_list(args):
    ## testing
    # args.config = 'examples/formiq.yml'
    if args.config and pathlib.Path(args.config).exists():
        
        cfg, _, _, _ = load_config(args.config)
        for m in cfg.get("modules", []):
            # Convert module name to file path (e.g., "examples.rules_anything" -> "examples/rules_anything.py")
            module_path = m.replace(".", "/") + ".py"
            if os.path.exists(module_path):
                import_module_from_path(m, module_path)
            else:
                # Fallback to standard import if file doesn't exist
                parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
                if parent_dir not in sys.path:
                    sys.path.insert(0, parent_dir)            
                importlib.import_module(m)
    nodes = list_nodes()
    print("# Tasks"); [print("-", n) for n in sorted(nodes["tasks"])]
    print("\n# Checks"); [print("-", n) for n in sorted(nodes["checks"])]

def _write(path: pathlib.Path, content: str, overwrite: bool = False):
    if path.exists() and not overwrite: return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(textwrap.dedent(content).lstrip(), encoding="utf-8")

def cmd_init(args):
    """Scaffold a minimal Formiq project in the current directory."""
    project = pathlib.Path.cwd().name
    _write(pathlib.Path("formiq.yml"), f"""
    project: {project}
    profile: ${{FORMIQ_PROFILE:-dev}}

    envs:
      dev:
        env_module: examples.sqlalchemy_env
        db_url: ${{DB_URL:-}}
        resources: {{ max_workers: 8 }}

    params:
      # choose a source:
      # table_name: your_table
      # csv_path: ./data/file.csv
      required_columns: ["id"]
      group_key: id

    targets:
      daily: [build_dataset, summarize, qc_basic, recap]

    modules:
      - examples.rules_anything
    """, overwrite=args.force)

    _write(pathlib.Path("examples/__init__.py"), "")
    _write(pathlib.Path("examples/sqlalchemy_env.py"), """
    from sqlalchemy import create_engine, MetaData
    from sqlalchemy.orm import sessionmaker
    def build_env(db_url: str = ""):
        env = {}
        if db_url:
            engine = create_engine(db_url, pool_pre_ping=True, future=True)
            Session = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)
            md = MetaData(); md.reflect(bind=engine)
            env.update({"session_factory": Session, "metadata": md, "tables": md.tables})
        return env
    """)
    _write(pathlib.Path("examples/rules_anything.py"), """
    import pandas as pd
    from formiq.core import qtask, qcheck, CheckResult

    @qtask(id="build_dataset")
    def build_dataset(ctx):
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
        return pd.DataFrame()

    @qtask(id="summarize", requires=["build_dataset"])
    def summarize(ctx):
        df: pd.DataFrame = ctx.get("build_dataset")
        if df.empty: return pd.DataFrame()
        key = ctx.params.get("group_key") or df.columns[0]
        num_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
        if not num_cols: return df[[key]].drop_duplicates().assign(_rows=1)
        agg = {c: ["min","max","mean"] for c in num_cols}
        summary = df.groupby(key).agg(agg); summary.columns = [f"{a}_{b}" for a,b in summary.columns]; summary = summary.reset_index()
        return summary

    @qcheck(id="qc_basic", requires=["build_dataset"], severity="error")
    def qc_basic(ctx):
        df = ctx.get("build_dataset")
        must_have = ctx.params.get("required_columns", [])
        missing_cols = [c for c in must_have if c not in df.columns]
        status = "pass" if df.shape[0] > 0 and not missing_cols else "fail"
        return CheckResult(id="qc_basic", status=status, metrics={"rowcount": int(df.shape[0]), "missing_columns": missing_cols})

    @qcheck(id="recap", requires=["summarize"], severity="info")
    def recap(ctx):
        summary = ctx.get("summarize")
        sample = summary.head(5).to_dict(orient="records") if not summary.empty else []
        return CheckResult(id="recap", status="pass", severity="info", metrics={"sample": sample})
    """)
    print("✔ Initialized Formiq project. Try:\n  formiq run daily --reporter markdown")

def main():
    parser = argparse.ArgumentParser("formiq", description="Formiq: minimal workflow & data-quality framework")
    sub = parser.add_subparsers(dest="cmd")

    p_run = sub.add_parser("run", help="Run targets or node names")
    p_run.add_argument("targets", nargs="*", help="target groups or node names; default: 'daily' from config")
    p_run.add_argument("--config", default="formiq.yml")
    p_run.add_argument("--workdir", default=".formiq")
    p_run.add_argument("--reporter", default="json", choices=["json","junit","markdown"])
    p_run.add_argument("--parallel", action="store_true")
    p_run.set_defaults(func=cmd_run)

    p_list = sub.add_parser("list", help="List registered tasks/checks")
    p_list.add_argument("--config", default="formiq.yml")
    p_list.set_defaults(func=cmd_list)

    p_init = sub.add_parser("init", help="Scaffold a minimal Formiq project here")
    p_init.add_argument("-f","--force", action="store_true", help="overwrite existing files")
    p_init.set_defaults(func=cmd_init)

    ## testing
    # sys.argv.append("run")

    # default to run if user types: formiq daily summarize
    if len(sys.argv) > 1 and sys.argv[1] not in {"run","list","init","-h","--help"}:
        sys.argv.insert(1, "run")

    
    args = parser.parse_args()
    if not getattr(args, "func", None):
        parser.print_help(); sys.exit(0)
    args.func(args)