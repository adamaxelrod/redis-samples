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
#""" Sampling Run of the Redis Commands
# 1540299537.538282 [0 [::1]:55511] "PING"
# 1540299537.540649 [0 [::1]:55511] "MULTI"
# 1540299537.540669 [0 [::1]:55511] "HMSET" "locations:ArloHQ" "city" "San Jose" "name" "Arlo Office" "longitude" "-121.925433" "state" "California" "keyName" "locations:ArloHQ" "latitude" "37.394848"
# 1540299537.540961 [0 [::1]:55511] "SADD" "locations" "San Jose"
# 1540299537.540972 [0 [::1]:55511] "GEOADD" "geoLocations" "-121.925433" "37.394848" "locations:ArloHQ"
# 1540299537.541676 [0 [::1]:55511] "EXEC"
# 1540299537.542142 [0 [::1]:55511] "MULTI"
# 1540299537.542159 [0 [::1]:55511] "HMSET" "locations:EmpireStateBuilding" "city" "New York" "name" "Empire State Building" "longitude" "-73.985675" "state" "New York" "keyName" "locations:EmpireStateBuilding" "latitude" "40.748757"
# 1540299537.542195 [0 [::1]:55511] "SADD" "locations" "New York"
# 1540299537.542202 [0 [::1]:55511] "GEOADD" "geoLocations" "-73.985675" "40.748757" "locations:EmpireStateBuilding"
# 1540299537.542346 [0 [::1]:55511] "EXEC"
# 1540299537.542624 [0 [::1]:55511] "MULTI"
# 1540299537.542636 [0 [::1]:55511] "HMSET" "locations:GrandCanyon" "city" "Grand Canyon Village" "name" "Grand Canyon" "longitude" "-112.100177" "state" "Arizona" "keyName" "locations:GrandCanyon" "latitude" "36.126062"
# 1540299537.542655 [0 [::1]:55511] "SADD" "locations" "Grand Canyon Village"
# 1540299537.542660 [0 [::1]:55511] "GEOADD" "geoLocations" "-112.100177" "36.126062" "locations:GrandCanyon"
# 1540299537.542674 [0 [::1]:55511] "EXEC"
# 1540299537.542940 [0 [::1]:55511] "MULTI"
# 1540299537.542955 [0 [::1]:55511] "HMSET" "locations:USCapitol" "city" "Washington" "name" "US Capitol Building" "longitude" "77.009104" "state" "DC" "keyName" "locations:USCapitol" "latitude" "38.890173"
# 1540299537.542983 [0 [::1]:55511] "SADD" "locations" "Washington"
# 1540299537.542987 [0 [::1]:55511] "GEOADD" "geoLocations" "77.009104" "38.890173" "locations:USCapitol"
# 1540299537.543001 [0 [::1]:55511] "EXEC"
# 1540299537.543304 [0 [::1]:55511] "MULTI"
# 1540299537.543326 [0 [::1]:55511] "HMSET" "locations:PearlHarbor" "city" "Honolulu" "name" "Pearl Harbor" "longitude" "-157.975957" "state" "HI" "keyName" "locations:PearlHarbor" "latitude" "21.355813"
# 1540299537.543355 [0 [::1]:55511] "SADD" "locations" "Honolulu"
# 1540299537.543362 [0 [::1]:55511] "GEOADD" "geoLocations" "-157.975957" "21.355813" "locations:PearlHarbor"
# 1540299537.543385 [0 [::1]:55511] "EXEC"
# 1540299537.543524 [0 [::1]:55511] "HGETALL" "locations:ArloHQ"
# 1540299537.543755 [0 [::1]:55511] "SMEMBERS" "locations"
# 1540299537.543972 [0 [::1]:55511] "ZREVRANGE" "geoLocations" "0" "5" "WITHSCORES"
# 1540299537.544193 [0 [::1]:55511] "HGETALL" "locations:ArloHQ"
# 1540299537.544357 [0 [::1]:55511] "HSET" "locations:ArloHQ" "city" "NEW-SanJose"
# 1540299537.544474 [0 [::1]:55511] "HGETALL" "locations:ArloHQ"
# 1540299537.544684 [0 [::1]:55511] "HSET" "locations:ArloHQ" "newAttr" "newValue"
# 1540299537.544783 [0 [::1]:55511] "HGETALL" "locations:ArloHQ"
# 1540299537.544947 [0 [::1]:55511] "HDEL" "locations:ArloHQ" "newAttr"
# 1540299537.545036 [0 [::1]:55511] "HGETALL" "locations:ArloHQ"
# 1540299537.545249 [0 [::1]:55511] "SMEMBERS" "locations"
# 1540299537.545406 [0 [::1]:55511] "SADD" "locations" "NEW-CITY"
# 1540299537.545500 [0 [::1]:55511] "SMEMBERS" "locations"
# 1540299537.545662 [0 [::1]:55511] "SREM" "locations" "newAttr"
# 1540299537.545754 [0 [::1]:55511] "SMEMBERS" "locations"
# 1540299537.545916 [0 [::1]:55511] "SMOVE" "locations" "newSet" "San Jose"
# 1540299537.546008 [0 [::1]:55511] "SMEMBERS" "locations"
# 1540299537.546141 [0 [::1]:55511] "SMEMBERS" "newSet"
# 1540299537.546266 [0 [::1]:55511] "SUNION" "locations" "newSet"
# 1540299537.546419 [0 [::1]:55511] "SMEMBERS" "locations"
# 1540299537.546548 [0 [::1]:55511] "ZADD" "sortedLocations" "1" "NEW-CITY"
# 1540299537.546653 [0 [::1]:55511] "ZADD" "sortedLocations" "1" "New York"
# 1540299537.546768 [0 [::1]:55511] "ZADD" "sortedLocations" "1" "Washington"
# 1540299537.546903 [0 [::1]:55511] "ZADD" "sortedLocations" "1" "Honolulu"
# 1540299537.547066 [0 [::1]:55511] "ZADD" "sortedLocations" "1" "Grand Canyon Village"
# 1540299537.547180 [0 [::1]:55511] "ZCARD" "sortedLocations"
# 1540299537.547318 [0 [::1]:55511] "ZRANGEBYSCORE" "sortedLocations" "0" "5" "WITHSCORES"
# 1540299537.547530 [0 [::1]:55511] "ZCARD" "sortedLocations"
# 1540299537.547906 [0 [::1]:55511] "HGET" "locations:ArloHQ" "longitude"
# 1540299537.548045 [0 [::1]:55511] "HGET" "locations:ArloHQ" "latitude"
# 1540299537.548171 [0 [::1]:55511] "GEORADIUS" "geoLocations" "-121.925433" "37.394848" "500" "km"
# 1540299537.548502 [0 [::1]:55511] "GEORADIUSBYMEMBER" "geoLocations" "locations:ArloHQ" "1500" "km"
# 1540299537.548693 [0 [::1]:55511] "GEORADIUSBYMEMBER" "geoLocations" "locations:ArloHQ" "15500" "km"
# 1540299537.548880 [0 [::1]:55511] "GEODIST" "geoLocations" "locations:ArloHQ" "locations:USCapitol" "km"
# 1540299537.549569 [0 [::1]:55511] "MULTI"
# 1540299537.549585 [0 [::1]:55511] "DEL" "geoLocations"
# 1540299537.549593 [0 [::1]:55511] "DEL" "locations"
# 1540299537.549603 [0 [::1]:55511] "EXEC"
# 1540299537.550010 [0 [::1]:55511] "SCAN" "0" "MATCH" "locations:*"
# 1540299537.550269 [0 [::1]:55511] "DEL" "locations:USCapitol"
# 1540299537.550438 [0 [::1]:55511] "DEL" "locations:GrandCanyon"
# 1540299537.550576 [0 [::1]:55511] "DEL" "locations:PearlHarbor"
# 1540299537.550680 [0 [::1]:55511] "SCAN" "11" "MATCH" "locations:*"
# 1540299537.550826 [0 [::1]:55511] "DEL" "locations:EmpireStateBuilding"
# 1540299537.550921 [0 [::1]:55511] "DEL" "locations:ArloHQ
#"""

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
    """Iterate through some basic functionality"""
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
    redis.srem(testSetKey, "newAttr")
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

    print_results("Entries that are within " + str(radius + 15000) + " km from " + testHashKey + ": " +
                  str(redis.georadiusbymember(geoLocationsKey, testHashKey, radius + 15000, "km")))

    """ Distance between 2 keys """
    print_results("Distance between 2 locations: " + testHashKey + " and " + testHashKey2 + " is: " +
                 str(redis.geodist(geoLocationsKey, testHashKey, testHashKey2, "km")) + " km")


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
