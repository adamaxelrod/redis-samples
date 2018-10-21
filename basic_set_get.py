""" ***************** """
"""basic_set_get.py"""
"""Description: Simple intro to Redis script that shows basic manipulation of string key"""
""" ***************** """
#""" Sample Run of the Redis Commands
# 1540160740.290552 [0 [::1]:60283] "SET" "sampleKey" "sampleValue"
# 1540160740.290727 [0 [::1]:60283] "SET" "sampleIntKey" "123"
# 1540160740.290893 [0 [::1]:60283] "GET" "sampleKey"
# 1540160740.291074 [0 [::1]:60283] "GET" "sampleIntKey"
# 1540160740.291233 [0 [::1]:60283] "INCRBY" "sampleIntKey" "5"
# 1540160740.291399 [0 [::1]:60283] "EXISTS" "sampleKey"
# 1540160740.291904 [0 [::1]:60283] "TTL" "sampleKey"
# 1540160740.292040 [0 [::1]:60283] "GET" "sampleKey"
# 1540160740.292175 [0 [::1]:60283] "SETEX" "sampleKey" "5" "sampleValue"
# 1540160740.292296 [0 [::1]:60283] "TTL" "sampleKey"
# 1540160740.292426 [0 [::1]:60283] "TTL" "sampleKey"
# 1540160740.292556 [0 [::1]:60283] "TTL" "sampleKey"
# 1540160746.293662 [0 [::1]:60283] "GET" "sampleKey"
# 1540160746.293979 [0 [::1]:60283] "APPEND" "sampleKey" "AppendedValue"
# 1540160746.294175 [0 [::1]:60283] "GET" "sampleKey"
# 1540160746.294371 [0 [::1]:60283] "DEL" "sampleKey"
# 1540160746.294556 [0 [::1]:60283] "DEL" "sampleKeyInt"
# 1540160746.294698 [0 [::1]:60283] "DBSIZE"
# 1540160746.294835 [0 [::1]:60283] "INFO"
# 1540160746.295587 [0 [::1]:60283] "DEL" "sampleKey"
# 1540160746.295717 [0 [::1]:60283] "DEL" "sampleIntKey"
#"""

from redis import StrictRedis
import util
from util.custom_utils import *
import os
import time

currStep = 1

def init():
    """Basic connection to Redis"""
    global redis
    redis = StrictRedis(host=os.environ.get("REDIS_HOST", "localhost"),
                        port=os.environ.get("REDIS_PORT", 6379),
                        db=0)


def run_basic_tests():
    """Iterate through some basic functionality"""
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


def teardown():
    """Remove all keys that were created"""
    redis.delete("sampleKey")
    redis.delete("sampleIntKey")


def print_results(msg):
    global currStep
    custom_print(currStep, msg)
    currStep = currStep + 1


def main():
    """Main method"""
    init()
    run_basic_tests()
    teardown()


if __name__ == "__main__":
    main()
