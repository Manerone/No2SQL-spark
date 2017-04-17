from pyspark import SparkContext, SQLContext


class No2SQL:
    """Transforms a csv like to a relational schema"""
    def __init__(self, headers, values, spark_context=None):
        """Initialize a new No2SQL object.

        Arguments:
        headers -- name of each attribute
        values -- list of instances
        """
        self.headers = headers
        self.values = values
        self.context = self.build_spark_context(spark_context)
        self.sql_context = SQLContext(self.context)
        self.data_frame = self.sql_context.createDataFrame(values, headers)

    def build_spark_context(self, context):
        """Creates a new SparkContext if not given."""
        if context is None:
            return SparkContext()
        else:
            return context

    def start(self):
        pass