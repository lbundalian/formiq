# tests/test_core.py
from formiq.core import qtask, qcheck, Runner, CheckResult

def test_runner_executes_simple_dag(tmp_path):
    @qtask(id="produce")
    def produce(ctx): return 42

    @qcheck(id="check_answer", requires=["produce"])
    def check_answer(ctx):
        val = ctx.get("produce")
        return CheckResult(id="check_answer", status="pass" if val==42 else "fail")

    r = Runner(env={}, params={}, workdir=str(tmp_path))
    res = r.run(["produce","check_answer"])
    assert res["produce"][1] == 42
    assert res["check_answer"][1].status == "pass"
