class DependencyChecker(object):
	"""Given a dataFrame with two columns A and B,
	checks if A -> B is a Soft Functional Dependency based on given alfa param.

	Implementation based on: Michael DiScala and Daniel J. Abadi. 2016.\
    Automatic Generation of Normalized Relational Schemas from Nested Key-Value Data

	Arguments:
	 data_frame -- Spark dataframe with the data
	 A -- Name of the left-hand side column in the SFD
	 B -- Name of the right-hand side column in the SFD
	 soft_alfa -- 'Softness' of the functional dependencies (default 0.99)
	"""
	def __init__(self, data_frame, A, B, soft_alfa=0.99):
		self.A = A
		self.B = B
		self.soft_alfa = soft_alfa
		self.data_frame = data_frame

	def check(self):
		"""Return true if A -> B is a Soft Functional Dependency."""
		return self.strength(self.A, self.B) > self.soft_alfa and
			   self.density(self.A) >= self.density(self.B)

	def strength(self, A, B):
		unique = self.unique_count(A)
		unique_pairs = self.unique_pairs_count(A,B)
		if unique_pairs == 0:
			return 0
		return unique / float(unique_pairs)

	def density(self, column):
		non_null = self.data_frame.filter('column is not null').count()
		total = self.data_frame.select(column).count()
		return non_null/float(total)

	def unique_count(self, column_name):
		return self.data_frame.select(column_name).distinct().count()

	def unique_pairs_count(self, A, B):
		return self.data_frame.select(A, B).distinct().count()
