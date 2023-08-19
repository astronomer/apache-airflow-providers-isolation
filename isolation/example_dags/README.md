# Example DAG
To run the example DAG

# Setup
- Local Registry
- Local Kubernetes
- Kubernetes Connection
```shell

```

## `requirements.txt`
```text
apache-airflow-providers-http
```

## `.env`
```dotenv
AIRFLOW_CONN_HTTP='http://api.github.com/https'
AIRFLOW__ISOLATED_POD_OPERATOR__KUBERNETES_CONN_ID="KUBERNETES"
AIRFLOW__ISOLATED_POD_OPERATOR__IMAGE_PULL_POLICY="Never"
```
