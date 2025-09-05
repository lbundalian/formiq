import argparse
from types import SimpleNamespace
import yaml
import pytest

from formiq import cli

def test_cmd_run_expands_group_targets(tmp_path, monkeypatch):
    """When args.targets is a list with a group name, CLI should expand to group's nodes."""
    cfg = {
        "project": "test",
        "profile": "dev",
        "envs": {},
        "params": {},
        "targets": {"daily": ["one", "two"]},
        "modules": []
    }
    cfg_path = tmp_path / "formiq.yml"
    cfg_path.write_text(yaml.safe_dump(cfg), encoding="utf-8")

    class FakeRunner:
        last = None
        def __init__(self, env, params, workdir, max_workers, store=None):
            FakeRunner.last = self
            self.env = env
            self.params = params
            self.workdir = workdir
            self.max_workers = max_workers
        def run(self, targets, parallel=False):
            # record what was passed and return a minimal success result
            self.called_targets = list(targets)
            check_obj = SimpleNamespace(id="two", status="pass", severity="info",
                                       metrics={}, samples=[], description=None, error=None)
            return {"one": ("task", {"value": 1}), "two": ("check", check_obj)}

    monkeypatch.setattr(cli, "Runner", FakeRunner)

    args = argparse.Namespace(targets=["daily"], config=str(cfg_path),
                              workdir=str(tmp_path), reporter="json", parallel=False)

    with pytest.raises(SystemExit) as exc:
        cli.cmd_run(args)

    assert exc.value.code == 0
    assert FakeRunner.last is not None
    assert FakeRunner.last.called_targets == ["one", "two"]

