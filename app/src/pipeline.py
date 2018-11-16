"""
A simple Pipeline structure
"""

class Filter(object):
    """ A wrapper object for filters """
    def __init__(self, afilter):
        self.afilter = afilter

    def __call__(self, record):
        """ Call user specified filter """
        return self.afilter(record)

class Mapper(object):
    """ A wrapper object for mappers """
    def __init__(self, mapper):
        self.mapper = mapper

    def __call__(self, record):
        """ Call user specified filter """
        return self.mapper(record)

class Handlers(object):
    """ A wrapper object for handlers """
    def __init__(self, handlers, filters_mappers):
        self.handlers = handlers
        self.filters_mappers = filters_mappers

    def __call__(self, record):
        """ Call user specified filter """
        if self.filters_mappers:
            pipeline_apply(record, self.filters_mappers)
        for handler in self.handlers:
            handler(record)


class Pipeline(object):
    """
    Create a simple processing pipeline with support for filtering and mapping.
    The begining of the pipeline is a Source which generates a stream of records.
    The end of pipeline can be one or more handlers.
    Handlers consume the final version of the modified record set.
    """
    def __init__(self, source):
        self.source = source
        self.elements = []

    def run(self):
        """ Run the pipline by opening source and streaming records the elements of pipeline """
        for record in self.source:
            pipeline_apply(record, self.elements)
 
    def add_filter(self, afilter):
        """ Add a filter element """
        self.elements.append(Filter(afilter))

    def add_mapper(self, mapper):
        """ Add a mapper element """
        self.elements.append(Mapper(mapper))

    def add_handler_group(self, handlers, filters_mappers=None):
        """ Add a handler group element """
        self.elements.append(Handlers(handlers, [] if not filters_mappers else filters_mappers))

def pipeline_apply(record, filters_mappers):
    """ Apply filters and mappers on a record """
    for element in filters_mappers:
        filtered = False
        if isinstance(element, Filter):
            filtered = element(record)
        if not filtered:
            if isinstance(element, Mapper):
                record = element(record)
            if isinstance(element, Handlers):
                element(record)

def test():
    """ Sample test """
    pipeline = Pipeline(range(1, 10))
    pipeline.add_filter(lambda x: x%2 == 0)
    pipeline.add_mapper(lambda x: (x, x*x))
    dictionary = {}
    def add_to_map(record):
        """ add to a map """
        dictionary[record[0]] = record[1]
    pipeline.add_handler_group([add_to_map])
    pipeline.run()
    print("Final", dictionary)

# test()