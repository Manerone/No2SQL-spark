from dependency_checker import DependencyChecker


class No2SQL:
    """Transforms a csv like to a relational schema.
    This class is an implementation using Apache Spark of the paper:
    Michael DiScala and Daniel J. Abadi. 2016.\
    Automatic Generation of Normalized Relational Schemas\
    from Nested Key-Value Data
    """

    def __init__(self, data_frame, spark_context, soft_alfa=0.99):
        """Initialize a new No2SQL object.

        Arguments:
        headers -- name of each attribute
        values -- list of instances
        """
        self.soft_alfa = soft_alfa
        self.context = spark_context
        self.context.addPyFile('dependency_checker.py')
        self.data_frame = data_frame
        self.columns = self._split_by_columns(self.data_frame)

    def start(self):
        sfd = self._find_soft_func_dependencies()
        sfd = sfd.filter(lambda x: x is not None)

    def _split_by_columns(self, df):
        dict = {}
        for col in df.columns:
            dict[col] = df.select(col).rdd.map(lambda x: x[0]).collect()
        return dict

    def _possible_soft_func_dependencies(self):
        sfds = []
        headers = self.data_frame.schema.names
        for i in xrange(len(headers)):
            for j in xrange(i+1, len(headers)):
                sfds.append((headers[i], headers[j]))
        return self.context.parallelize(sfds)

    def _find_soft_func_dependencies(self):
        return self._possible_soft_func_dependencies().map(
            lambda x: self._is_soft_func_dependency(x[0], x[1])
        )

    def _is_soft_func_dependency(self, A, B):
        sfdChecker = DependencyChecker(
            self.columns[A], self.columns[B], self.soft_alfa
        )
        if sfdChecker.check():
            return (A, B)
        return None
