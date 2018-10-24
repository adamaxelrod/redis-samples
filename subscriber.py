""" ************************************************************************************************************************** """
""" File: subscriber.py"""
"""
    Description: Sample Redis subscriber from pub/sub
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
import threading

from util.custom_utils import *

""" Global Variables and Settings """
redis = None
redis_pipeline = None
currStep = 1
numEntries = 1000000
iterations = 1
channel = "channel-1"

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


def callback():
    sub = redis.pubsub()
    sub.subscribe(channel)
    while True:
        for msg in sub.listen():
            print("[" + channel + "]: " + str(msg))


def print_results(msg):
    global currStep
    custom_print(currStep, msg)
    currStep = currStep + 1


def main(argv):
    init()
    global redis
    global channel
    channel = sys.argv[1]
    t = threading.Thread(target=callback)
    t.setDaemon(True)
    t.start()

    while True:
        time.sleep(30)


if __name__ == "__main__":
    main(sys.argv[1:])
