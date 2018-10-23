""" ************************************************************************************************************************** """
""" File: status_example.py"""
"""
    Description: Sample Redis use case that stores a list of IoT-like devices in a hash with real-time status
                 along with pub/sub implementation for notifying subscribers when status of a device changes.
"""
""" Author: Adam Axelrod """
""" Revision: October 2018 """
""" ************************************************************************************************************************** """

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
numEntries = 1000000
iterations = 1


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


def create_status_object_hash(i):
    """ Create a Redis hash object for this key """
    hashKey = "device:DEVID-" + str(i);
    hashAttrDict = {'status': 'online', 'power': 'on', 'batteryLevel': '15'}
    redis.hmset(hashKey, hashAttrDict);
    print_results("Creating hash for: " + hashKey);


def generate_data():
    """ Iterate through n elements """
    global n
    for i in range(1, numEntries):
        create_status_object_hash(i);


def simulate_updates():
    for i in range(1, iterations):
        generate_data()


def teardown():
    try:
        custom_print_separator()
        global redis_pipeline
        print_results("Deleting all entries")

        """ Retrieve all the hash keys first matching a particular pattern, then iterate and delete each """
        for i in redis.scan_iter("locations:*"):
            print_results("Deleting Key: " + str(i))
            redis_pipeline.delete(i)

        redis_pipeline.execute()
    except:
        print("Error in teardown while removing entries")
        sys.exit(1)


def print_results(msg):
    global currStep
    custom_print(currStep, msg)
    currStep = currStep + 1


def main():
    init()
    global redis
    global redis_pipeline
    redis_pipeline = redis.pipeline()

    generate_data()
    # start_pubsub()
    simulate_updates()
    teardown()


if __name__ == "__main__":
    main()
