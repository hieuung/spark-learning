# Spark

## Local installation
- Download lastest [spark](https://www.apache.org/dyn/closer.lua/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz)
- Extract file
``` sh
export SPARK_TAR=spark-3.5.0-bin-hadoop3.tgz
export SPARK_FOLDER=spark-3.5.0-bin-hadoop3
tar xvf $SPARK_TAR
sudo mv $SPARK_FOLDER /opt/spark
```
- Add to path

```sh
nano ~/.bashrc
```

- Add below line to file

```
export SPARK_HOME=/opt/spark
export PATH=$PATH:$SPARK_HOME/bin
```

```sh
source ~/.bashrc
```

- Confirm installation
```
pyspark
```

- Sumbmit job
```sh
spark-submit ./apps/rdd.py
```