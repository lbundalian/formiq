# formiq/core.py
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Callable, Iterable
import inspect, functools, hashlib, json, time, pathlib
from concurrent.futures import ThreadPoolExecutor, as_completed

# ---------- Results ----------
@dataclass
class CheckResult:
    id: str
    status: str                   # "pass" | "fail" | "error" | "skip"
    severity: str = "error"       # "info" | "warn" | "error"
    metrics: Dict[str, Any] = field(default_factory=dict)
    samples: List[Dict[str, Any]] = field(default_factory=list)
    description: Optional[str] = None
    error: Optional[str] = None
    started_at: float = field(default_factory=time.time)
    finished_at: float = 0.0

# ---------- Context ----------
@dataclass
class FContext:
    """Generic dependency container (DB sessions, clients, file paths, flags, etc.)."""
    env: Dict[str, Any]
    params: Dict[str, Any]
    artifacts: Dict[str, Any]
    workdir: str
    def put(self, key, value): self.artifacts[key] = value
    def get(self, key): return self.artifacts.get(key)

# ---------- Registries ----------
_TASKS: Dict[str, Callable[[FContext], Any]] = {}
_CHECKS: Dict[str, Callable[[FContext], CheckResult]] = {}

def qtask(id: Optional[str] = None, requires: Optional[List[str]] = None, cache: bool = True):
    """Register a task (produces data/artifacts)."""
    requires = requires or []
    def deco(fn):
        rid = id or fn.__name__
        @functools.wraps(fn)
        def wrapper(ctx: FContext):
            path = None
            if cache:
                key = _cache_key(rid, fn, ctx.params)
                path = pathlib.Path(ctx.workdir) / "cache" / f"{rid}-{key}.json"
                if path.exists():
                    return _load_cached(path)
            out = fn(ctx)
            ctx.put(rid, out)
            if cache and path is not None:
                path.parent.mkdir(parents=True, exist_ok=True)
                try:
                    path.write_text(json.dumps(_jsonify(out), default=str))
                except Exception:
                    pass
            return out
        wrapper.__qmeta__ = {"id": rid, "requires": requires, "type": "task", "cache": cache}
        _TASKS[rid] = wrapper
        return wrapper
    return deco

def qcheck(id: Optional[str] = None, requires: Optional[List[str]] = None, severity="error"):
    """Register a check (assertions with metrics)."""
    requires = requires or []
    def deco(fn):
        rid = id or fn.__name__
        @functools.wraps(fn)
        def wrapper(ctx: FContext) -> CheckResult:
            try:
                res: CheckResult = fn(ctx)
                res.id = res.id or rid
                res.severity = res.severity or severity
                res.finished_at = time.time()
                return res
            except Exception as e:
                return CheckResult(id=rid, status="error", severity=severity, error=str(e), finished_at=time.time())
        wrapper.__qmeta__ = {"id": rid, "requires": requires, "type": "check", "severity": severity}
        _CHECKS[rid] = wrapper
        return wrapper
    return deco

def _cache_key(name, fn, params):
    src = inspect.getsource(fn)
    blob = json.dumps({"name": name, "src": src, "params": params}, sort_keys=True).encode()
    return hashlib.sha256(blob).hexdigest()[:12]

def _jsonify(x):
    try:
        import pandas as pd
        if isinstance(x, pd.DataFrame):
            return {"__df__": True, "data": x.to_dict(orient="records")}
    except Exception:
        pass
    return x

def _dejsonify(x):
    if isinstance(x, dict) and x.get("__df__"):
        import pandas as pd
        return pd.DataFrame(x.get("data", []))
    return x

def _load_cached(path: pathlib.Path):
    return _dejsonify(json.loads(path.read_text()))

# ---------- Run store ----------
class RunStore:
    def persist_run(self, run_id: str, results: Dict[str, Any]) -> None:
        raise NotImplementedError

class JsonRunStore(RunStore):
    def __init__(self, workdir: str):
        self.dir = pathlib.Path(workdir) / "runs"
        self.dir.mkdir(parents=True, exist_ok=True)
    def persist_run(self, run_id: str, results: Dict[str, Any]) -> None:
        out = {k:(v[1].__dict__ if v[0]=="check" else v[1]) for k,v in results.items()}
        (self.dir / f"{run_id}.json").write_text(json.dumps(out, default=str, indent=2))

# ---------- Runner ----------
class Runner:
    def __init__(self, env: Dict[str, Any], params: Dict[str, Any],
                 workdir: str = ".formiq", max_workers: int = 8,
                 store: Optional[RunStore] = None):
        self.env, self.params, self.workdir, self.max_workers = env, params, workdir, max_workers
        self.store = store or JsonRunStore(workdir)
        pathlib.Path(self.workdir).mkdir(parents=True, exist_ok=True)

    def run(self, targets: Iterable[str], parallel: bool = False):
        graph = {**_TASKS, **_CHECKS}
        order = self._topo(targets, graph)
        ctx = FContext(env=self.env, params=self.params, artifacts={}, workdir=self.workdir)
        results: Dict[str, Any] = {}

        if not parallel:
            for name in order:
                fn = graph[name]
                kind = fn.__qmeta__["type"]
                results[name] = ("task", fn(ctx)) if kind=="task" else ("check", fn(ctx))
        else:
            pending = set(order)
            done = set()
            while pending:
                ready = [n for n in pending if all(d in done for d in graph[n].__qmeta__["requires"])]
                if not ready:
                    raise RuntimeError("Deadlocked DAG (cyclic dependencies?)")
                with ThreadPoolExecutor(max_workers=self.max_workers) as pool:
                    futs = {pool.submit(graph[n], ctx): n for n in ready}
                    for fut in as_completed(futs):
                        n = futs[fut]; fn = graph[n]; kind = fn.__qmeta__["type"]
                        val = fut.result()
                        results[n] = ("task", val) if kind=="task" else ("check", val)
                        done.add(n); pending.remove(n)

        run_id = f"{int(time.time())}"
        if self.store:
            self.store.persist_run(run_id, results)
        return results

    def _topo(self, targets, graph):
        seen, out = set(), []
        def visit(n):
            if n in seen: return
            for dep in graph[n].__qmeta__["requires"]:
                visit(dep)
            seen.add(n); out.append(n)
        for t in targets:
            if t not in graph: raise KeyError(f"Unknown node/target: {t}")
            visit(t)
        return out

def list_nodes():
    return {"tasks": list(_TASKS.keys()), "checks": list(_CHECKS.keys())}
