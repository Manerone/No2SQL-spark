from pyspark import SparkContext, SQLContext


class No2SQL:
    def __init__(self, headers, values, sparkContext=None):
        self.headers = headers
        self.values = values
        self.context = self._build_spark_context(sparkContext)
        self.sql_context = SQLContext(self.context)
        self.data_frame = self.sql_context.createDataFrame(values, headers)

    def _build_spark_context(self, context):
        if context is None:
            return SparkContext()
        else:
            return context
