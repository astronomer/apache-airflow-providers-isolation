import sys
import textwrap
from datetime import datetime
from io import StringIO
from pathlib import Path
from shutil import copyfile

import pytest
import sh
from click.testing import CliRunner

from isolationctl import SH_KWARGS
from isolationctl.__main__ import init, deploy
from tests.conftest import manual_tests


@pytest.fixture(scope="function")
def astro_dev_start():
    yield
    sh.astro.dev.stop(**SH_KWARGS)


# noinspection SpellCheckingInspection
@manual_tests
def test_e2e(
    default_environments,
    default_environment,
    astro,
    project_root,
    astro_dev_start,
    dotenv,
):
    # FOLLOWING THE QUICKSTART INSTRUCTIONS IN README.MD:
    # 1. pip install apache-airflow-providers-isolation[cli]
    # 2. isolationctl init --example --local --local-registry --astro --git --dependency
    # noinspection PyTypeChecker
    CliRunner().invoke(
        init, "--yes --example --local --local-registry --astro --git --dependency"
    )

    # Just to clean up the output a bit
    Path(".env").open("a").write(
        textwrap.dedent(
            """
            OPENLINEAGE_DISABLED=true
            AIRFLOW__LOGGING__LOGGING_LEVEL=INFO
            """
        )
    )

    # 3. echo "\npandas==1.4.2" >> environments/example/requirements.txt
    (default_environment / "requirements.txt").open("a").write("\npandas==1.4.2\n")

    # 4. pushd environments/example/; astro registry provider add http; popd
    sh.astro.registry.provider.add("http", _cwd=str(default_environment))
    # 5. isolationctl deploy --local-registry
    # noinspection PyTypeChecker
    CliRunner().invoke(deploy, "--yes --local-registry")

    # 6. wget https://raw.githubusercontent.com/
    # astronomer/apache-airflow-providers-isolation/main/isolation /example_dags/isolation_provider_example_dag.py
    example_dag_source = (
        project_root
        / "isolation"
        / "example_dags"
        / "isolation_provider_example_dag.py"
    )
    assert (
        example_dag_source.exists()
    ), "We have an example dag where we expect it, named as we expect it"
    example_dag_dest = Path("dags/isolation_provider_example_dag.py")
    copyfile(example_dag_source, example_dag_dest)

    # 7. astro registry provider add kubernetes
    sh.astro.registry.provider.add("kubernetes", **SH_KWARGS)

    # 8. astro dev start
    try:
        sh.astro.dev.start("--no-browser", **SH_KWARGS)
    except sh.ErrorReturnCode_1 as e:
        sh.astro.dev.logs(**SH_KWARGS)
        raise e

    # 9. astro run isolation_provider_example_dag
    s = StringIO()
    # noinspection PyUnresolvedReferences
    try:
        sh.astro.run(
            "isolation_provider_example_dag",
            _out=s,
            _err_to_out=True,
            _tee=True,
            _tty_size=(24, 200),
        )
    except sh.ErrorReturnCode_1 as e:
        print("===================")
        sys.stdout.write(s.getvalue())
        print("===================")
        raise e

    actual = s.getvalue()
    parent_pandas_running = (
        """Running \x1b[1;33mparent_pandas\x1b[0m\x1b[33m...\x1b[0m"""
    )
    parent_pandas_output = """op_classpath=airflow.operators.python.PythonOperator\r\nPandas Version: 2.0.3 \r\nAnd printing other stuff for fun: \r\narg='Hello!', ds='2023-08-25', params={'hi': 'hello'}\r\n\x1b[1;32mSUCCESS"""
    assert parent_pandas_running in actual
    assert parent_pandas_output in actual

    isolated_environment_pandas = """Running \x1b[1;33misolated_environment_pandas"""
    isolated_environment_pandas_output = f"""
Pandas Version: 1.4.2 \r
And printing other stuff for fun: \r
"arg='Hello!', ds='{datetime.now().date()}', params={{'hi': 'hello'}}\r
\x1b[1;32mSUCCESS\x1b[0m ✅"""
    assert isolated_environment_pandas in actual
    assert isolated_environment_pandas_output in actual

    dag_complete = "Completed running the DAG isolation_provider_example_dag"
    assert dag_complete in actual
