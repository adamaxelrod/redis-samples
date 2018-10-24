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
from dateutil.parser import *

import os
import sys
import util
import time
import json
import datetime
import dateutil

from util.custom_utils import *

""" Global Variables and Settings """
redis = None
redis_pipeline = None
currStep = 1
numEntries = 1000000
iterations = 5

hashPrefix = "deviceHash:"
setPrefix = "locations:"
deviceIdPrefix = "DEV-"


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
    except Exception as e:
        print("Exception while trying to connect to Redis: " + str(e))
        sys.exit(1)


def upsert_status_object_hash(deviceId, evenOddChannel):
    try:
        """ Create a Redis hash object for this key """
        global hashPrefix
        hashKey = hashPrefix + deviceId
        hashAttrDict = {'status': 'online', 'power': 'on', 'batteryLevel': '15'}
        redis.hmset(hashKey, hashAttrDict)

        """ Store list of channels to notify in a separate Redis set """
        global setPrefix
        setKey = setPrefix + deviceId
        redis.sadd(setKey, 'channel-' + deviceId)
        redis.sadd(setKey, 'channel-' + str(evenOddChannel))

        print_results("Creating hash and set for: " + deviceId)
    except Exception as e:
        print("Exception while trying to update data in Redis: " + str(e))
        sys.exit(1)


def publish_status_updates(deviceId):
    try:
        """ Publish an update message to any listening subscribers """
        """ Even-numbered ids go to channel-0 and odd-numbered ids go to channel-1 """
        global setPrefix
        setKey = setPrefix + deviceId
        channelSet = redis.smembers(setKey)

        for channel in channelSet:
            print_results("Publishing to channel: " + channel)
            redis.publish(channel, "Update for key: " + setKey)
    except Exception as e:
        print("Exception while trying to publish to Redis: " + str(e))
        sys.exit(1)


def generate_data():
    """ Iterate through n elements """
    global n
    starttime = datetime.datetime.now()

    for i in range(1, numEntries):
        global deviceIdPrefix
        deviceId = deviceIdPrefix + str(i)
        upsert_status_object_hash(deviceId, i % 2)
        publish_status_updates(deviceId)

    endtime = datetime.datetime.now()
    print("Start, End Times: "  + str(starttime) + ", " + str(endtime))
    print(str(numEntries) + " records: " + str((endtime - starttime).total_seconds()) + " msec\n\n")


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
    except Exception as e:
        print("Exception while trying to teardown: " + str(e))
        sys.exit(1)


def print_results(msg):
    global currStep
    custom_print(currStep, msg)
    currStep = currStep + 1


def main(argv):
    init()
    global redis
    global redis_pipeline
    redis_pipeline = redis.pipeline()

    global numEntries
    numEntries = int(sys.argv[1])
    generate_data()

    simulate_updates()
    teardown()


if __name__ == "__main__":
    main(sys.argv[1:])
