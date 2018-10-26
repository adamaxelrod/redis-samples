""" ************************************************************************************************************************** """
""" File: data_structures_sample.py"""
"""
    Description: Demo of a few of the Redis data structures and capabilities.
                Takes a JSON list of locations and stores the cities in a set and
                also stores JSON object into a Redis hash. A geospatial set is
                added to store the lat/long coordinates for each location for comparison
                purposes. Redis piplelines are used to show multiple transactions in a grouped
                sequence.
"""
""" Author: Adam Axelrod """
""" Revision: October 2018 """
""" ************************************************************************************************************************** """
# --- Sampling Run of the Redis Commands ---
# 1540525496.503549 [0 [::1]:64554] "PING"
# 1540525496.504008 [0 [::1]:64554] "MULTI"
# 1540525496.504025 [0 [::1]:64554] "HMSET" "locations:ArloHQ" "city" "San Jose" "name" "Arlo Office" "longitude" "-121.925433" "state" "California" "keyName" "locations:ArloHQ" "latitude" "37.394848"
# 1540525496.504044 [0 [::1]:64554] "SADD" "locations" "San Jose"
# 1540525496.504052 [0 [::1]:64554] "GEOADD" "geoLocations" "-121.925433" "37.394848" "locations:ArloHQ"
# 1540525496.504072 [0 [::1]:64554] "EXEC"
# 1540525496.504387 [0 [::1]:64554] "MULTI"
# 1540525496.504401 [0 [::1]:64554] "HMSET" "locations:EmpireStateBuilding" "city" "New York" "name" "Empire State Building" "longitude" "-73.985675" "state" "New York" "keyName" "locations:EmpireStateBuilding" "latitude" "40.748757"
# 1540525496.504460 [0 [::1]:64554] "SADD" "locations" "New York"
# 1540525496.504465 [0 [::1]:64554] "GEOADD" "geoLocations" "-73.985675" "40.748757" "locations:EmpireStateBuilding"
# 1540525496.504481 [0 [::1]:64554] "EXEC"
# 1540525496.504738 [0 [::1]:64554] "MULTI"
# 1540525496.504749 [0 [::1]:64554] "HMSET" "locations:GrandCanyon" "city" "Grand Canyon Village" "name" "Grand Canyon" "longitude" "-112.100177" "state" "Arizona" "keyName" "locations:GrandCanyon" "latitude" "36.126062"
# 1540525496.504768 [0 [::1]:64554] "SADD" "locations" "Grand Canyon Village"
# 1540525496.504773 [0 [::1]:64554] "GEOADD" "geoLocations" "-112.100177" "36.126062" "locations:GrandCanyon"
# 1540525496.504787 [0 [::1]:64554] "EXEC"
# 1540525496.505066 [0 [::1]:64554] "MULTI"
# 1540525496.505077 [0 [::1]:64554] "HMSET" "locations:USCapitol" "city" "Washington" "name" "US Capitol Building" "longitude" "-77.009104" "state" "DC" "keyName" "locations:USCapitol" "latitude" "38.890173"
# 1540525496.505094 [0 [::1]:64554] "SADD" "locations" "Washington"
# 1540525496.505099 [0 [::1]:64554] "GEOADD" "geoLocations" "-77.009104" "38.890173" "locations:USCapitol"
# 1540525496.505113 [0 [::1]:64554] "EXEC"
# 1540525496.505373 [0 [::1]:64554] "MULTI"
# 1540525496.505384 [0 [::1]:64554] "HMSET" "locations:PearlHarbor" "city" "Honolulu" "name" "Pearl Harbor" "longitude" "-157.975957" "state" "HI" "keyName" "locations:PearlHarbor" "latitude" "21.355813"
# 1540525496.505401 [0 [::1]:64554] "SADD" "locations" "Honolulu"
# 1540525496.505406 [0 [::1]:64554] "GEOADD" "geoLocations" "-157.975957" "21.355813" "locations:PearlHarbor"
# 1540525496.505420 [0 [::1]:64554] "EXEC"
# 1540525496.505587 [0 [::1]:64554] "HGETALL" "locations:ArloHQ"
# 1540525496.505796 [0 [::1]:64554] "SMEMBERS" "locations"
# 1540525496.505987 [0 [::1]:64554] "ZREVRANGE" "geoLocations" "0" "5" "WITHSCORES"
# 1540525496.506193 [0 [::1]:64554] "HGETALL" "locations:ArloHQ"
# 1540525496.506425 [0 [::1]:64554] "HSET" "locations:ArloHQ" "city" "NEW-SanJose"
# 1540525496.506526 [0 [::1]:64554] "HGETALL" "locations:ArloHQ"
# 1540525496.506748 [0 [::1]:64554] "HSET" "locations:ArloHQ" "newAttr" "newValue"
# 1540525496.506855 [0 [::1]:64554] "HGETALL" "locations:ArloHQ"
# 1540525496.507114 [0 [::1]:64554] "HDEL" "locations:ArloHQ" "newAttr"
# 1540525496.507214 [0 [::1]:64554] "HGETALL" "locations:ArloHQ"
# 1540525496.507407 [0 [::1]:64554] "SMEMBERS" "locations"
# 1540525496.507548 [0 [::1]:64554] "SADD" "locations" "NEW-CITY"
# 1540525496.507639 [0 [::1]:64554] "SMEMBERS" "locations"
# 1540525496.507825 [0 [::1]:64554] "SREM" "locations" "NEW-CITY"
# 1540525496.507909 [0 [::1]:64554] "SMEMBERS" "locations"
# 1540525496.508065 [0 [::1]:64554] "SMOVE" "locations" "newSet" "San Jose"
# 1540525496.508151 [0 [::1]:64554] "SMEMBERS" "locations"
# 1540525496.508281 [0 [::1]:64554] "SMEMBERS" "newSet"
# 1540525496.508415 [0 [::1]:64554] "SUNION" "locations" "newSet"
# 1540525496.508745 [0 [::1]:64554] "SMEMBERS" "locations"
# 1540525496.508886 [0 [::1]:64554] "ZADD" "sortedLocations" "1" "New York"
# 1540525496.509005 [0 [::1]:64554] "ZADD" "sortedLocations" "1" "Washington"
# 1540525496.509129 [0 [::1]:64554] "ZADD" "sortedLocations" "1" "Honolulu"
# 1540525496.509226 [0 [::1]:64554] "ZADD" "sortedLocations" "1" "Grand Canyon Village"
# 1540525496.509352 [0 [::1]:64554] "ZCARD" "sortedLocations"
# 1540525496.509497 [0 [::1]:64554] "ZRANGEBYSCORE" "sortedLocations" "0" "5" "WITHSCORES"
# 1540525496.509701 [0 [::1]:64554] "ZCARD" "sortedLocations"
# 1540525496.509868 [0 [::1]:64554] "HGET" "locations:ArloHQ" "longitude"
# 1540525496.509974 [0 [::1]:64554] "HGET" "locations:ArloHQ" "latitude"
# 1540525496.510105 [0 [::1]:64554] "GEORADIUS" "geoLocations" "-121.925433" "37.394848" "500" "km"
# 1540525496.510250 [0 [::1]:64554] "GEORADIUSBYMEMBER" "geoLocations" "locations:ArloHQ" "1500" "km"
# 1540525496.510438 [0 [::1]:64554] "GEORADIUSBYMEMBER" "geoLocations" "locations:ArloHQ" "5500" "km"
# 1540525496.510639 [0 [::1]:64554] "GEODIST" "geoLocations" "locations:ArloHQ" "locations:USCapitol" "km"
# 1540525496.510765 [0 [::1]:64554] "GEODIST" "geoLocations" "locations:ArloHQ" "locations:USCapitol" "mi"
# 1540525496.510922 [0 [::1]:64554] "MULTI"
# 1540525496.510929 [0 [::1]:64554] "DEL" "geoLocations"
# 1540525496.510934 [0 [::1]:64554] "DEL" "locations"
# 1540525496.510939 [0 [::1]:64554] "EXEC"
# 1540525496.511085 [0 [::1]:64554] "SCAN" "0" "MATCH" "locations:*"
# 1540525496.511248 [0 [::1]:64554] "DEL" "locations:ArloHQ"
# 1540525496.511390 [0 [::1]:64554] "DEL" "locations:USCapitol"
# 1540525496.511489 [0 [::1]:64554] "DEL" "locations:GrandCanyon"
# 1540525496.511613 [0 [::1]:64554] "DEL" "locations:EmpireStateBuilding"
# 1540525496.511733 [0 [::1]:64554] "DEL" "locations:PearlHarbor"
# ---

from redis import StrictRedis
import os
import sys
import util
import time
import json

from util.custom_utils import *

""" Global Variables and Settings """
redis = None
redis_pipeline = None
currStep = 1
testHashKey = "locations:ArloHQ"
testHashKey2 = "locations:USCapitol"
testSetKey = "locations"
testSortedSetKey = "sortedLocations"
testCity = "San Jose"
geoLocationsKey = "geoLocations"
radius = 500


def init():
    custom_print_separator()

    try:
        """Basic connection to Redis"""
        global redis
        redis = StrictRedis(host=os.environ.get("REDIS_HOST", "localhost"),
                            port=os.environ.get("REDIS_PORT", 6379),
                            db=0)
        if redis.ping():
            print("Connected to Redis")
    except:
        print("Cannot connect to redis")
        sys.exit(1)


def run_basic_tests():
    """Iterate through some basic functionality on different data structures"""
    load_data()
    show_hash_functions()
    show_set_functions()
    show_geo_functions()


def load_data(file="sample-data/locations.json"):
    custom_print_separator()

    try:
        """Load data from flat file"""
        f = open(file)
        locations = json.load(f)

        """Store location entries in a Redis hash and store list of unique locations in Set"""
        """Combining redis operations together into a pipeline (multi/exec flow)"""
        for i in range(len(locations)):
            v = locations[i]
            redis_pipeline.hmset(v['keyName'], v)
            print_results("Storing hash for: " + str(v['keyName']))

            print_results("Storing set entry for: " + str(v['city']))
            redis_pipeline.sadd(testSetKey, v['city'])

            print_results("Storing geo entry for: " + str(v['keyName']))
            redis_pipeline.geoadd(geoLocationsKey, v['longitude'], v['latitude'], v['keyName'])

            """ Execute the pipeline set of steps """
            redis_pipeline.execute()

        print_results("Retrieval of the first entity of the hash: " + str(redis.hgetall(locations[0]['keyName'])))
        print_results("List of all members in set: " + str(redis.smembers(testSetKey)))
        print_results("List of all members of geolocations: " + str(redis.zrange(geoLocationsKey, 0, len(locations), True, True)))
    except:
        print("Error reading file or storing data")


def show_hash_functions():
    custom_print_separator()

    """ Retrieve all values for a particular hash """
    print_results("Get values for a specific hash with key - " + testHashKey + ": " + str(redis.hgetall(testHashKey)))

    """ locations:ArloHQ -> city = "NEW-SanJose """
    redis.hset(testHashKey, "city", "NEW-SanJose")
    print_results("Set new value for existing attribute in hash with key - " + testHashKey + ": " + str(redis.hgetall(testHashKey)))

    """ locations:ArloHQ -> newAttr = newvalue """
    redis.hset(testHashKey, "newAttr", "newValue")
    print_results("Add new value to an existing hash with key - " + testHashKey + ": " + str(redis.hgetall(testHashKey)))

    """ Remove locations:ArloHQ -> newAttr """
    redis.hdel(testHashKey, "newAttr")
    print_results("Remove value from existing hash with key - " + testHashKey + ": " + str(redis.hgetall(testHashKey)))


def show_set_functions():
    custom_print_separator()

    print_results("Get values for a Set with key - " + testSetKey + ": " + str(redis.smembers(testSetKey)))

    """ locations add "NEW-CITY" """
    redis.sadd(testSetKey, "NEW-CITY")
    print_results("Add new value to an existing Set with key - " + testSetKey + ": " + str(redis.smembers(testSetKey)))

    """ Remove locations:ArloHQ -> newAttr """
    redis.srem(testSetKey, "NEW-CITY")
    print_results("Remove value from existing hash with key - " + testSetKey + ": " + str(redis.smembers(testSetKey)))

    """ Moving San Jose from one set to another """
    redis.smove(testSetKey, "newSet", testCity)
    print_results("Moving values from set to another: " + str(redis.smembers(testSetKey)) + ", NEW SET: " + str(redis.smembers("newSet")))

    """ Now, take the union of the old set and the new set where the entry was moved to """
    print_results("Union of two sets: " + str(redis.sunion(testSetKey, "newSet")))

    """ Convert the Set to a SortedSet """
    for i in redis.smembers(testSetKey):
        redis.zadd(testSortedSetKey, 1, i)

    print_results("Sorted Set results: " + str(redis.zrangebyscore(testSortedSetKey, 0, redis.zcard(testSortedSetKey), None, None, True)))
    print_results("Sorted Set cardinality: " + str(redis.zcard(testSortedSetKey)))


def show_geo_functions():
    custom_print_separator()

    """ Get latitude/longitude from the test hash key and then use those values for georadius """
        longitude = redis.hget(testHashKey, "longitude")
    latitude = redis.hget(testHashKey, "latitude")
    print_results("Entries that are within " + str(radius) + " km from " + testHashKey + ": " +
                  str(redis.georadius(geoLocationsKey, longitude, latitude, radius, "km")))

    """ Use of georadiusbymember to use an existing key as a search value """
    print_results("Entries that are within " + str(radius + 1000) + " km from " + testHashKey + ": " +
                  str(redis.georadiusbymember(geoLocationsKey, testHashKey, radius + 1000, "km")))

    print_results("Entries that are within " + str(radius + 5000) + " km from " + testHashKey + ": " +
                  str(redis.georadiusbymember(geoLocationsKey, testHashKey, radius + 5000, "km")))

    """ Distance between 2 keys """
    print_results("Distance between 2 locations: " + testHashKey + " and " + testHashKey2 + " is: " +
                 str(redis.geodist(geoLocationsKey, testHashKey, testHashKey2, "km")) + " km")

    print_results("Distance between 2 locations: " + testHashKey + " and " + testHashKey2 + " is: " +
                 str(redis.geodist(geoLocationsKey, testHashKey, testHashKey2, "mi")) + " mi")


def teardown():
    custom_print_separator()

    """ Delete geolocations set """
    redis_pipeline.delete(geoLocationsKey)

    """ Delete locations set """
    redis_pipeline.delete(testSetKey)

    """ Complete the pipeline """
    redis_pipeline.execute()

    """ Retrieve all the hash keys first matching a particular pattern, then iterate and delete each """
    for i in redis.scan_iter("locations:*"):
        print_results("Deleting Key: " + str(i))
        redis.delete(i)

    print_results("Deleting all entries")


def print_results(msg):
    global currStep
    custom_print(currStep, msg)
    currStep = currStep + 1


def main(argv):
    init()

    global redis
    global redis_pipeline
    redis_pipeline = redis.pipeline()

    run_basic_tests()
    teardown()


if __name__ == "__main__":
    main(sys.argv[1:])
