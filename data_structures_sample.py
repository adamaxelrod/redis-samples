"""hash_example.py"""
"""Description: Simple intro to Redis script that shows basic manipulation of hash"""
from redis import StrictRedis
import os
import util
import time
import json

from util.custom_utils import *

redis = None
redis_pipeline = None

def init():
    """Basic connection to Redis"""
    global redis
    redis = StrictRedis(host=os.environ.get("REDIS_HOST", "localhost"),
                        port=os.environ.get("REDIS_PORT", 6379),
                        db=0)

    global currStep
    """Global variable for clean printing purposes"""
    currStep = 1


def run_basic_tests():
    """Iterate through some basic functionality"""
    load_data()


def load_data(file="sample-data/locations.json"):
    """Load data from flat file"""
    f = open(file)
    locations = json.load(f)

    """Store location entries in a Redis hash and store list of unique locations in Set"""
    for i in range(len(locations)):
        v = locations[i]
        redis_pipeline.hmset(v['keyName'], v)
        print_results("Storing hash for: " + str(v['keyName']))

        redis_pipeline.sadd("locations", v['keyName'])
        print_results("Storing set entry for: " + str(v['keyName']))

        redis_pipeline.geoadd("geoLocations", v['longitude'], v['latitude'], v['keyName'])
        print_results("Storing geo entry for: " + str(v['keyName']))
        redis_pipeline.execute()

    print_results("Retrieval of the first entity of the hash: " + str(redis.hgetall(locations[0]['keyName'])))
    print_results("List of all members in set: " + str(redis.smembers("locations")))
    print_results("List of all members of geolocations: " + str(redis.zrange("geoLocations", 0, len(locations), True, True)))


def teardown():
    redis_pipeline.delete("geoLocations")
    redis_pipeline.delete("locations")
    redis_pipeline.execute()
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
