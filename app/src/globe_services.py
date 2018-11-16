
"""
Collection of IdLookup, NameSearch and GeoIndex services
"""

import sys
import codecs
from collections import defaultdict
from pyproj import Proj
import kdtree

SERVICE_LOOKUP_TABLE = {}
PROJECT_XY = Proj(proj='latlong', datum='WGS84')

def lookup(name):
    """ Service lookup """
    if name not in SERVICE_LOOKUP_TABLE:
        this = sys.modules[__name__]
        klass = getattr(this, name)
        SERVICE_LOOKUP_TABLE[name] = klass()
    return SERVICE_LOOKUP_TABLE[name]

class City(object):
    """ City record, with fewer attributes """
    def __init__(self, line):
        self.line = line
        parsed = line.split("\t")
        self.geonameid = int(parsed[0])
        self.name = parsed[1]
        self.altnames = parsed[3]
        self.lat = float(parsed[4])
        self.lon = float(parsed[5])
        self.population = int(parsed[-5])
        self.country = parsed[8]
        self.ad1 = parsed[9] if not parsed[9].isdigit() else ""
        self.ad2 = parsed[10] if not parsed[10].isdigit() else ""

    def __str__(self):
        return u"City {0} {1}({2}, {3} {4}) @{5},{6}".format(self.geonameid, self.name, self.country, self.ad1, self.ad2, self.lat, self.lon)

    def __repr__(self):
        return self.__str__()




class IdLookup(object):
    """ Service to lookup City record by Id """
    def __init__(self):
        self.index = {}

    def add(self, record):
        """ Index a new record """
        self.index[record.geonameid] = record

    def get(self, geonameid):
        """ Find a City record by geonameid """
        return self.index[geonameid]




class SearchService(object):
    """ Simple geo name services uses trie """
    def __init__(self):
        self.index = defaultdict(set)
        self.id_service = lookup("IdLookup")

    def _update(self, name, geonameid):
        if name:
            name = name.lower().split(" ")
            one = name[0]
            self.index[(one)].add(geonameid)
            for i in range(1, len(name)):
                two = name[i]
                self.index[(two)].add(geonameid)
                self.index[(one, two)].add(geonameid)
                one = two
    
    def _search(self, name):
        results = []
        if name:
            name = name.lower().split(" ")
            collector = []

            match = self.index[tuple(name)]
            if match:
                collector.append(match)

            one = name[0]
            for i in range(1, len(name)):
                two = name[i]
                match = self.index[(one, two)]
                if match:
                    collector.append(match)
                one = two

            for i in range(len(name)):
                match = self.index[(name[i])]
                if match:
                    collector.append(match)
            results = set.intersection(*collector)
        return results

    def add(self, record):
        """ Index a new record """
        # hard coding field selection and analyzer
        self._update(record.name, record.geonameid)
        self._update(record.name + " " + record.country, record.geonameid)
        if record.ad2:
            self._update(record.name + " " + record.ad2 + " " + record.country, record.geonameid)
            if record.ad1:
                self._update(record.name + " " + record.ad2 + " " + record.ad1 + " " + record.country, record.geonameid)
        for name in record.altnames.split(","):
            self._update(name, record.geonameid)

    def search(self, name):
        """ Find a City record by name or alternate name """
        name = ''.join(c for c in name if c.isalnum() or c.isspace())
        return [self.id_service.get(geonameid) for geonameid in self._search(name)]




class CityKD(object):
    """ City Node inside KD Tree """
    def __init__(self, coordniates, geonameid):
        self.coordniates = coordniates
        self.geonameid = geonameid

    def __len__(self):
        return len(self.coordniates)

    def __getitem__(self, i):
        return self.coordniates[i]

    def __str__(self):
        return u"CityKD {0} @({1}, {2}".format(self.geonameid, self.coordniates[0], self.coordniates[1])

    def __repr__(self):
        return self.__str__()

class GeoIndex(object):
    """ Geo index service using lang, lat """
    def __init__(self):
        self.index = {}
        self.search_service = lookup("SearchService")
        self.id_service = lookup("IdLookup")
        self.kdindex = kdtree.create(dimensions=2)

    def add(self, record):
        """ Index a new record """
        self.kdindex.add(CityKD(PROJECT_XY(record.lon, record.lat), record.geonameid))

    def nearest(self, geonameid, k=7):
        """ Find the nearest Service """
        record = self.id_service.get(geonameid)
        # country = int(codecs.encode(country, 'hex'), 16)
        results = []
        for node, distance in self.kdindex.search_knn(PROJECT_XY(record.lon, record.lat), k):
            results.append((self.id_service.get(node.data.geonameid), distance))
        return results
