""" ***************** """
"""hash_example.py"""
"""Description: Simple intro to Redis script that shows basic manipulation of hash"""
""" ***************** """
#""" Sampling Run of the Redis Commands
# 1540165307.521900 [0 [::1]:61712] "MULTI"
# 1540165307.521927 [0 [::1]:61712] "HMSET" "locations:ArloHQ" "city" "San Jose" "name" "Arlo Office" "longitude" "-121.925433" "state" "California" "keyName" "locations:ArloHQ" "latitude" "37.394848"
# 1540165307.521953 [0 [::1]:61712] "SADD" "locations" "San Jose"
# 1540165307.521960 [0 [::1]:61712] "GEOADD" "geoLocations" "-121.925433" "37.394848" "locations:ArloHQ"
# 1540165307.521982 [0 [::1]:61712] "EXEC"
# 1540165307.522392 [0 [::1]:61712] "MULTI"
# 1540165307.522412 [0 [::1]:61712] "HMSET" "locations:EmpireStateBuilding" "city" "New York" "name" "Empire State Building" "longitude" "-73.985675" "state" "New York" "keyName" "locations:EmpireStateBuilding" "latitude" "40.748757"
# 1540165307.522439 [0 [::1]:61712] "SADD" "locations" "New York"
# 1540165307.522445 [0 [::1]:61712] "GEOADD" "geoLocations" "-73.985675" "40.748757" "locations:EmpireStateBuilding"
# 1540165307.522469 [0 [::1]:61712] "EXEC"
# 1540165307.522814 [0 [::1]:61712] "MULTI"
# 1540165307.522830 [0 [::1]:61712] "HMSET" "locations:GrandCanyon" "city" "Grand Canyon Village" "name" "Grand Canyon" "longitude" "-112.100177" "state" "Arizona" "keyName" "locations:GrandCanyon" "latitude" "36.126062"
# 1540165307.522868 [0 [::1]:61712] "SADD" "locations" "Grand Canyon Village"
# 1540165307.522875 [0 [::1]:61712] "GEOADD" "geoLocations" "-112.100177" "36.126062" "locations:GrandCanyon"
# 1540165307.522894 [0 [::1]:61712] "EXEC"
# 1540165307.523275 [0 [::1]:61712] "MULTI"
# 1540165307.523289 [0 [::1]:61712] "HMSET" "locations:USCapitol" "city" "Washington" "name" "US Capitol Building" "longitude" "77.009104" "state" "DC" "keyName" "locations:USCapitol" "latitude" "38.890173"
# 1540165307.523312 [0 [::1]:61712] "SADD" "locations" "Washington"
# 1540165307.523318 [0 [::1]:61712] "GEOADD" "geoLocations" "77.009104" "38.890173" "locations:USCapitol"
# 1540165307.523336 [0 [::1]:61712] "EXEC"
# 1540165307.523529 [0 [::1]:61712] "HGETALL" "locations:ArloHQ"
# 1540165307.523772 [0 [::1]:61712] "SMEMBERS" "locations"
# 1540165307.523980 [0 [::1]:61712] "ZREVRANGE" "geoLocations" "0" "4" "WITHSCORES"
# 1540165307.524260 [0 [::1]:61712] "HGETALL" "locations:ArloHQ"
# 1540165307.524564 [0 [::1]:61712] "HSET" "locations:ArloHQ" "city" "NEW-SanJose"
# 1540165307.524773 [0 [::1]:61712] "HGETALL" "locations:ArloHQ"
# 1540165307.525245 [0 [::1]:61712] "HSET" "locations:ArloHQ" "newAttr" "newValue"
# 1540165307.525409 [0 [::1]:61712] "HGETALL" "locations:ArloHQ"
# 1540165307.525704 [0 [::1]:61712] "HDEL" "locations:ArloHQ" "newAttr"
# 1540165307.525834 [0 [::1]:61712] "HGETALL" "locations:ArloHQ"
# 1540165307.526062 [0 [::1]:61712] "SMEMBERS" "locations"
# 1540165307.526255 [0 [::1]:61712] "SADD" "locations" "NEW-CITY"
# 1540165307.526390 [0 [::1]:61712] "SMEMBERS" "locations"
# 1540165307.526587 [0 [::1]:61712] "SREM" "locations" "newAttr"
# 1540165307.526712 [0 [::1]:61712] "SMEMBERS" "locations"
# 1540165307.526904 [0 [::1]:61712] "SMOVE" "locations" "newSet" "San Jose"
# 1540165307.527068 [0 [::1]:61712] "SMEMBERS" "locations"
# 1540165307.527235 [0 [::1]:61712] "SMEMBERS" "newSet"
# 1540165307.527400 [0 [::1]:61712] "SUNION" "locations" "newSet"
# 1540165307.527629 [0 [::1]:61712] "MULTI"
# 1540165307.527639 [0 [::1]:61712] "DEL" "geoLocations"
# 1540165307.527644 [0 [::1]:61712] "DEL" "locations"
# 1540165307.527651 [0 [::1]:61712] "EXEC"
# 1540165307.527827 [0 [::1]:61712] "SCAN" "0" "MATCH" "locations:*"
# 1540165307.528013 [0 [::1]:61712] "DEL" "locations:USCapitol"
# 1540165307.528143 [0 [::1]:61712] "DEL" "locations:ArloHQ"
# 1540165307.528274 [0 [::1]:61712] "DEL" "locations:EmpireStateBuilding"
# 1540165307.528398 [0 [::1]:61712] "SCAN" "15" "MATCH" "locations:*"
# 1540165307.528550 [0 [::1]:61712] "DEL" "locations:GrandCanyon"
#"""

from redis import StrictRedis
import os
import util
import time
import json

from util.custom_utils import *

redis = None
redis_pipeline = None
currStep = 1
testHashKey = "locations:ArloHQ"
testSetKey = "locations"
testCity = "San Jose"

def init():
    """Basic connection to Redis"""
    global redis
    redis = StrictRedis(host=os.environ.get("REDIS_HOST", "localhost"),
                        port=os.environ.get("REDIS_PORT", 6379),
                        db=0)


def run_basic_tests():
    """Iterate through some basic functionality"""
    load_data()
    show_hash_functions()
    show_set_functions()


def load_data(file="sample-data/locations.json"):
    """Load data from flat file"""
    f = open(file)
    locations = json.load(f)

    """Store location entries in a Redis hash and store list of unique locations in Set"""
    """Combining redis operations together into a pipeline (multi/exec flow)"""
    for i in range(len(locations)):
        v = locations[i]
        redis_pipeline.hmset(v['keyName'], v)
        print_results("Storing hash for: " + str(v['keyName']))

        redis_pipeline.sadd("locations", v['city'])
        print_results("Storing set entry for: " + str(v['city']))

        redis_pipeline.geoadd("geoLocations", v['longitude'], v['latitude'], v['keyName'])
        print_results("Storing geo entry for: " + str(v['keyName']))
        redis_pipeline.execute()

    print_results("Retrieval of the first entity of the hash: " + str(redis.hgetall(locations[0]['keyName'])))
    print_results("List of all members in set: " + str(redis.smembers("locations")))
    print_results("List of all members of geolocations: " + str(redis.zrange("geoLocations", 0, len(locations), True, True)))


def show_hash_functions():
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


def teardown():
    """ Delete geolocations set """
    redis_pipeline.delete("geoLocations")

    """ Delete locations set """
    redis_pipeline.delete("locations")

    """ Complete the pipeline """
    redis_pipeline.execute()

    """ Retrieve all the hash keys first matching a particular pattern, then iterate and delete each """
    for i in redis.scan_iter("locations:*"):
        print_results("Key: " + str(i))
        redis.delete(i)

    print_results("Deleting all entries")


def print_results(msg):
    global currStep
    custom_print(currStep, msg)
    currStep = currStep + 1


def main():
    init()
    global redis
    global redis_pipeline
    redis_pipeline = redis.pipeline()
    run_basic_tests()
    teardown()


if __name__ == "__main__":
    main()
