class JSONReader:

    # JSONReader constructor
    # Params:
    #   +json+ - Array to be flattened
    def __init__(self, json):
        self.json = json

    # Method responsible for flattening the dictionaries
    # Observations:
    # * Keys who doesn't exist in one dictionary will receive null
    # Return:
    #   A dictionary in the current format:
    #   {
    #       'root': RDD ou DataFrame ou Dataset,
    #       <attr1-one-to-many>: {
    #           'root': RDD ou DataFrame ou Dataset,
    #           <attr1-one-to-many>: {...}
    #       },
    #       <attr2-one-to-many>: {...},
    #       ...
    #       <attrN-one-to-many>: {...},
    #   }
    def flatten(self, parent_key='', sep='_'):
        # items = []
        # for key, value in self.array_of_dictionaries.items():
        #     pass
        # return items
        pass
