"""basic_set_get.py"""
"""Description: Simple intro to Redis script that shows basic manipulation of string key"""
from redis import StrictRedis
import util
from util.custom_utils import *
import os
import time

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
    init()
    run_basic_set()
    show_ttl()
    show_append()
    show_delete()
    show_basicOps()


def run_basic_set():
    """Sample set of a key"""
    redis.set("sampleKey", "sampleValue")
    redis.set("sampleIntKey", "123")

    """Print result of GET"""
    print_results("Key with string value: " + redis.get("sampleKey"))
    print_results("Key with integral value: " + redis.get("sampleIntKey"))
    print_results("Incrementing integral value by 5: " + str(redis.incrby("sampleIntKey", 5)))
    print_results("Key exists?: " + str(redis.exists("sampleKey")))
    print_results("Debug information about sampleKey: " + str(redis.debug_object("sampleKey")))
    print_results("Debug information about sampleIntKey: " + str(redis.debug_object("sampleIntKey")))


def show_ttl():
    """Managing TTLs"""
    print_results("Current TTL on sampleKey: " + str(redis.ttl("sampleKey")))

    redis.setex("sampleKey", 5, redis.get("sampleKey"))
    print_results("Setting TTL on sampleKey for: " + str(redis.ttl("sampleKey")) + " seconds")
    print_results("Sleeping for TTL: " + str(redis.ttl("sampleKey")))

    time.sleep(redis.ttl("sampleKey") + 1)

    print_results(str(redis.get("sampleKey")))

def show_append():
    """Basic appending of a string"""
    redis.append("sampleKey", "AppendedValue")
    print_results("Updated Key: " + redis.get("sampleKey"))


def show_delete():
    """Delete keys"""
    redis.delete("sampleKey")
    redis.delete("sampleKeyInt")


def show_basicOps():
    """Basic operations on keyset"""
    print_results("Total number of keys: " + str(redis.dbsize()))
    print_results("Redis server info: " + str(redis.info()))


def print_results(msg):
    global currStep
    custom_print(currStep, msg)
    currStep = currStep + 1


run_basic_tests()
