""" ************************************************************************************************************************** """
""" File: custered_get.py"""
"""
    Description: Sample when connecting to a clustered environment
"""
""" Author: Adam Axelrod """
""" Revision: October 2018 """
""" ************************************************************************************************************************** """

import sys
from rediscluster import StrictRedisCluster


def fetch_test_redis():
    """ Connect to a Redis cluster """
	startup_nodes = [{'host': 'hmscloud-dyn-cache.5evooa.clustercfg.euw1.cache.amazonaws.com', 'port': '6379'}]
	rc = StrictRedisCluster(startup_nodes=startup_nodes, decode_responses=True, skip_full_coverage_check=True)
	keys = {}

	key = '1020_1540442262000_918359a632f442e18242b0db9b0586b1_DEV-336-62964'
	print('Key: ' + key)
	print("Value: " + str(rc.get(key)))


def main(argv):
   fetch_test_redis()

if __name__ == "__main__":
   main()
