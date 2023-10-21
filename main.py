#!/usr/bin/python
import argparse
import importlib
import time
import os
import sys
import logging
# pylint:disable=E0401
try:
    import pyspark
except:
    import findspark
    findspark.init()
    import pyspark

from pyspark.sql import SparkSession

from run.runner import runner_1, runner_2, runner_3, runner_4, runner_5


if __name__ == '__main__':

    spark = SparkSession.builder.master("local[1]").appName(
        "Dynamic Planner App").getOrCreate()

    runner_1(spark)
    runner_2(spark)
    runner_3(spark)
    runner_4(spark)
    runner_5(spark)
