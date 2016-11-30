# Analytics Solution

## Requirements

- python 2.7
- pyspark
- jupyter (I use Anaconda)
- nose

## Additional Setup

1. Define SPARK_HOME
2. Define PYTHON_PATH
 - $SPARK_HOME/python/lib/py4j-0.10.3-src.zip (version depends on pyspark install)
 - $SPARK_HOME/python/bin

### Integrate Jupyter with pyspark

1. Add the following to <SPARK_HOME>/bin/pyspark
	export PYSPARK_DRIVER_PYTHON="jupyter"
	export PYSPARK_DRIVER_PYTHON_OPTS="notebook"
	exec "${SPARK_HOME}"/bin/spark-submit pyspark-shell-main --name "PySparkShell" "$@"
2. Run pyspark
3. Go to http://localhost:8888/

## Build

1. Run 
   python setup.py sdist
2. Test
   nosetests
3. Install
   pip install dist/

## Execute solution

1. Run pyspark
	pyspark
2. Open notebook (http://localhost:8888/) from analytics directory
3. Open Web Log Sessions.ipynb
4. Select Cell -> Run All
5. Results will appear in notebook


