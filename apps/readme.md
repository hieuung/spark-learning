# Pyspark job

Directory tree

```
├── common_utils
│   ├── cache.py
│   ├── database.py
│   ├── exception.py
│   ├── __init__.py
│   └── yaml.py
├── database.yaml
├── etl_config.py
├── extractor.py
├── __init__.py
├── iris_inference.py
├── iris_model_selection.py
├── iris.py
├── models
│   ├── __init__.py
│   ├── job.py
├── rdd.py
├── readme.md
├── resources
├── table_config
│   └── hieuut.yaml
└── transform_load.py
```

## iris_inference.py

- Spark job that inference trained Iris classification model saved in `resources`.

- Submint job to spark cluster:
```sh
spark-submit ./apps/iris_inference.py
```

## iris_model_selection.py

- Spark job that training/model selecting Iris classification model and save into `resources`.

- Submint job to spark cluster:
```sh
spark-submit ./apps/iris_model_selection.py
```
## extractor.py

- Extract data from source database and load into Delta lake raw-zone and move to staging-zone.

### - Initiate source/log database

```sh
cd ../
kubectl apply -f ./database/k8s/ n spark
```

- Get your database external-ip
```sh
kubectl get svc -n spark
```

- Populate data with `./database/k8s/populate_database.sql`

### - Submint job to spark cluster:
```sh
spark-submit --packages io.delta:delta-spark_2.12:3.1.0 ./apps/extractor.py
```

## transform_load.py
- Transform_load interface. Using data from staging-zone transfrom and load into data warehouse.
```sh
spark-submit --packages io.delta:delta-spark_2.12:3.1.0 ./apps/transform_load.py
```