
from pipeline import Pipeline
from globe_services import lookup, City
import gzip

def init_services(file_name):
    """ Initialize all services """
    id_service = lookup("IdLookup")
    names_service = lookup("SearchService")
    index_service = lookup("GeoIndex")
    print("Parsing filename", file_name)
    with gzip.open(file_name, mode="rb") as cities:
        pipeline = Pipeline(cities)
        pipeline.add_mapper(lambda line: City(line))
        pipeline.add_handler_group(
            [lambda record: id_service.add(record),
             lambda record: index_service.add(record),
             lambda record: names_service.add(record)])
        pipeline.run()

def test():
    """ Sample test """
    import time
    start = time.clock()
    init_services("cities1000.txt.gz")
    print("Init done", time.clock() - start)

    search_service = lookup("SearchService")
    index_service = lookup("GeoIndex")

    start = time.clock()
    for item in search_service.search("San Carlos"):
        print(type(item.line))
        print(item.line)
        print(item)
        # for n in index_service.nearest(item.geonameid):
        #     print(u"{0} {1} {2} {3}".format(n[0].name, n[0].ad1, n[0].ad2, n[0].country))
        # print("US Only")
        # for n in index_service.nearest(item.geonameid, 7, "US"):
        #     print("  ", str(n[0]))
    print("Search done", time.clock() - start)
