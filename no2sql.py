from pyspark import SparkContext, SQLContext
import sys
sys.path.append('./no2sql')
from dependency_checker import DependencyChecker


class No2SQL:
    """Transforms a csv like to a relational schema.
    This class is an implementation using Apache Spark of the paper:
    Michael DiScala and Daniel J. Abadi. 2016.\
    Automatic Generation of Normalized Relational Schemas from Nested Key-Value Data
    """

    def __init__(self, headers, values, spark_context=None, soft_alfa=0.99):
        """Initialize a new No2SQL object.

        Arguments:
        headers -- name of each attribute
        values -- list of instances
        """
        self.headers = headers
        self.values = values
        self.soft_alfa = soft_alfa
        self.context = self._build_spark_context(spark_context)
        self.sql_context = SQLContext(self.context)
        self.data_frame = self.sql_context.createDataFrame(values, headers)

    def _build_spark_context(self, context):
        """Creates a new SparkContext if not given."""
        if context is None:
            return SparkContext()
        else:
            return context

    def start(self):
        sfd = self._find_soft_func_dependencies

    def _possible_soft_func_dependencies(self):
        sfds = []
        for i in xrange(len(self.headers)):
            for j in xrange(i+1, len(self.headers)):
                sfds.append((self.headers[i], self.headers[j]))
        return self.context.parallelize(sfds)

    def _find_soft_func_dependencies(self):
        self._possible_soft_func_dependencies.map(
            lambda x: self._is_soft_func_dependency(x[0], x[1])
        )

    def _is_soft_func_dependency(self, A, B):
        if DependencyChecker(self.data_frame, A, B, self.soft_alfa).check()
            return (A,B)
