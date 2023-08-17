<p align="center">
  <a href="https://www.airflow.apache.org">
    <img src="https://raw.githubusercontent.com/astronomer/telescope/main/astro.png" alt="Astronomer Telescope Logo" />
  </a>
</p>
<h1 align="center">
   Isolated Operators
</h1>
  <h3 align="center">
  Runtime Operator Isolation in Airflow. Created with ❤️ by the CSE Team @ Astronomer.
</h3>

<br/>

## IsolatedOperator
The purpose of this repo is to demonstrate a new interface and operator called the `IsolatedOperator`. This operator intends to address a common and industry-wide complaint around Airflow, and it's inability to have an isolated runtime environment

### What it is
- A Wrapper on top of `KubernetesPodOperator`, `DockerOperator`, or `ExternalPythonOperator` to run a `NormalOperator` without any need to know or understand how those first three operators work (beyond what is absolutely required to run)
  - These three operators will run, _effectively_, `python -c "from airflow.operators.bash import BashOperator; BashOperator(..., bash_command='echo hi')"`
  - We anticipate that it will be able to run any existing Airflow Operator, as it would normally be run, with a few known and obvious exceptions
- A build script, which can (maybe) someday be built into a CLI or the `astro` CLI
  - The build script builds any amount of "mask"/"child" images on top of a normal "parent" Docker image (the normal Astro Airflow `Dockerfile`)
  - These "mask"/"child" images will be simple to assume by an end user, based on an easy pattern
  - These "mask"/"child" images can be pushed to an owned Registry that Astro can access
- At runtime, Airflow will utilize one of the above three operators (currently just the KubernetesPodOperator)
- Some amount of initial state (e.g. `{{ds}}`/Airflow Context, Connections, Variables) will be provided to the Isolated environment
- Many things work out-of-the-box just due to the inheritance on existing operators (such as KPO) - e.g. logging

### What it isn't
- Anything that you can and should just do normally in Airflow, you should do normally in Airflow - this won't make an operator that would run _normally_ any easier to run
- Anything that requires talking _back_ to Airflow at runtime will not be supported - e.g. `Variable.set(...)`
- Anything that requires querying Airflow state at runtime e.g. `@provide_session` or `TaskInstance.query(...)`
- `ExternalTaskSensor` or `TriggerDagRunOperator`, likely, would never work or make sense with this
- It's possible other less-traditional parts of Airflow may not be supported, just due to development effort - e.g. `@task` annotations or the new (as of this writing) setup and teardowns
- It is possible that things like `on_failure_callback`'s or lineage data may not work - depending on how exactly they are invoked - but if these things work with a KPO, then they should work with this

### What remains to do
- [x] ~~Airflow Context Injection - e.g `{{ ds }}` and `{{ ti }}`~~
- [x] ~~PythonOperator serialization (likely Pickle?) - e.g. `PythonOperator(python_callable=foo_fn, ...)`~~
- [x] ~~Ensure XCOMs work in all situations~~
- [ ] Ensure that things like templated files work (e.g. `sql="file.sql"`)


# Technical Requirements - Host Airflow
- Due to the Isolated requirement, only Airflow 2.3 and above is supported - the intent is still to match Airflow versions between host and target

# Technical Requirements - Target Isolated Environment
- Airflow 2.2 doesn't have `test_mode` for executing tasks, so currently only Airflow 2.3 and above is supported
-
