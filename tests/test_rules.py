import pandas as pd
from formiq.core import qtask, qcheck, Runner, CheckResult

def test_pandas_flow(tmp_path):
    @qtask(id="df")
    def df(ctx): return pd.DataFrame({"id":[1,2,3], "x":[10,20,30]})
    @qcheck(id="qc", requires=["df"])
    def qc(ctx):
        d = ctx.get("df")
        return CheckResult(id="qc", status="pass" if len(d)==3 else "fail")
    r = Runner(env={}, params={}, workdir=str(tmp_path))
    out = r.run(["df","qc"])
    assert out["qc"][1].status == "pass"
