#!/usr/bin/env python3.7

#
# Copyright (c) 2019, James C. McPherson.  All rights reserved
#

#
# This file is a demonstration of using Apache Spark with Python
# to provide analysis of CSV-delimited data from a PV solar inverter.
# See https://www.pvoutput.org/intraday.jsp?id=21105&sid=18984,
# https://github.com/jmcp/jfy-monitor and
# https://www.jmcpdotcom.com/blog/posts/2018-04-03-monitoring-my-inverter/
#
# There are two file formats involved in the entire collection. The first
# is the output from https://github.com/jcroucher/solarmonj, and has
# the following schema:
#
# Timestamp:         seconds-since-epoch
# Temperature:       float (degrees C)
# energyNow:         float (Watts)
# energyToday:       float (Watt-hours)
# powerGenerated:    float (Hertz)
# voltageDC:         float (Volts)
# current:           float (Amps)
# energyTotal:       float (Watt-hours)
# voltageAC:         float (Volts)
#
# Due to bugs in solarmonj when combined with occasionally marginal
# hardware, some rows in the first version are invalid
#    1370752022,1.4013e-45,-0.27184,0,-0.27184,1.4013e-45,1.3703e-40,1.36638e-40,6.43869e-41
#
# The second schema is from jfy-monitor, and has this schema:
#
# Timestamp:        ISO8601-like ("yyyy-MM-dd'T'HH:mm:ss")
# Temperature:      float (degrees C)
# PowerGenerated:   float (Watts)
# VoltageDC:        float (Volts)
# Current:          float (Amps)
# EnergyGenerated:  float (Watts)
# VoltageAC:        float (Volts)
#
# The first schema is in effect for records starting on 2013-06-04
# and ending on 2018-03-26.
# The second schema takes effect with the logfiles starting on
# 2018-03-27. There were some records from 2018-03-27/8 which have
# different fields, because I was updating jfyMonitor in production
# and breaking things. We drop those records.
#

# We load up *all* the data files, and then from within a venv which
# has pyspark installed, we run
#
# $ SPARK_LOCAL_IP=0.0.0.0 pyspark /path/to/this/file
#
# to generate several reports:
# - for each year, which month had the day with the max and min energy outputs
# - for each month, what was the average energy generated
# - for each month, what was the total energy generated


import csv
import datetime
import json
import getopt
import glob

import org.apache.log4j.Logger
import org.apache.log4j.Level

import os
import sys


from pyspark.sql.functions import date_format
from pyspark import SparkContext
from pyspark.sql import SparkSession

# Basic Spark session configuration
sc = SparkContext("local", "PV Inverter Analysis")
spark = SparkSession(sc)

# We don't need most of this output
Logger.getLogger("org").setLevel(Level.ERROR)
Logger.getLogger("akka").setLevel(Level.ERROR)

allYears = range(2013, 2020)
allFrames = {}
allMonths = ["01", "02", "03", "04", "05", "06", "07", "08",
             "09", "10", "11", "12"]
rdds = {}


def importCSV(fname, isOld):
    output = []
    if isOld:
        multiplier = 100.0
    else:
        multiplier = 1.0

    # print("processing {fname}".format(fname=fname))
    csvreader = csv.reader(open(fname).readlines())
    for row in csvreader:
        try:
            if isOld:
                (tstamp, temp, _enow, _etoday, powergen, vdc,
                 current, energen, vac) = row
            else:
                (tstamp, temp, powergen, vdc, current, energen, vac) = row
        except ValueError as ve:
            # print("failed at {row} of {fname}".format(row=row, fname=fname))
            continue

        if "e" in temp:
            # invalid line, skip it
            continue

        if isOld:
            isostamp = datetime.datetime.fromtimestamp(int(tstamp))
        else:
            isostamp = datetime.datetime.fromisoformat(tstamp)

        output.append({
            "timestamp": isostamp,
            "Temperature": float(temp),
            "PowerGenerated": float(powergen),
            "VoltageDC": float(vdc),
            "Current": float(current),
            "EnergyGenerated": float(energen) * multiplier,
            "VoltageAC": float(vac)})
    return output


def generateFiles(topdir, year, month):
    """Construct per-year dicts of lists of files"""
    allfiles = {}
    kkey = ""
    if year and month:
        pattern = "{year}/{month}/*".format(year=year, month=month)
        kkey = "{year}{month}".format(year=year, month=month)
    elsif year:
        pattern = "{year}/*/*".format(year=year)
        kkey = year
    elsif month:
        pattern = "*/{month}/*".format(month=month)
        kkey = month

    if pattern:
        allfiles[kkey] = glob.glob(os.path.join(topdir, pattern))
    else:
        for yy in allYears:
            allfiles[yy] = glob.glob(os.path.join(topdir,
                                                  "{yy}/*/*".format(yy=yy)))
    return allfiles


if __name__ == "__main__":

    qmonth = None
    qyear = None

    # parse the command line options
    if len(sys.argv) > 1:
        try:
            o, a = getopt.getopt(sys.argv[1:], "m:y:")
        except getopt.GetoptError as err:
            print("Invalid options supplied: ", err)
            sys.exit(1)
        opts = dict(o)
        if "-m" in opts:
            qmonth = opts["-m"]
        if "-y" in opts:
            qyear = opts["-y"]

    if qyear:
       allYears = [qyear]

    # We're only going to search for data underneath $PWD
    allFiles = generateFiles("data", qyear, qmonth)

    print("Importing data files")

    for k in allFiles:
        rddyear = []
        for fn in allFiles[k]:
            if fn.endswith(".csv"):
                rddyear.extend(importCSV(fn, True))
            else:
                rddyear.extend(importCSV(fn, False))
        rdds[k] = rddyear

    print("All data files imported")

    for year in allYears:
        rdd = sc.parallelize(rdds[year])
        allFrames[year] = rdd.toDF()
        newFrame = "new" + str(year)
        # Extend the schema for our convenience
        allFrames[newFrame] = allFrames[year].withColumn(
            "DateOnly", date_format('timestamp', "yyyyMMdd")
        ).withColumn("TimeOnly", date_format('timestamp', "HHmmss"))
        allFrames[newFrame].createOrReplaceTempView("view{year}".format(
        year=year))

    print("Data transformed into RDDS")


# Now we generate some reports
# - for each year, which month had the day with the max and min energy outputs
# - for each month, what was the average energy generated
# - for each month, what was the total energy generated


# I'm doing to this with for loops over the dataframe, because that seems
# to be a more efficient way of answering these specific questions.
# With more experience using Spark over time I might ask the questions again
# in a more Spark-like fashion. We still need to get the data out somehow,
# however.

reports = {}

ymdquery = "SELECT DISTINCT DateOnly from view{view} WHERE DateOnly "
ymdquery += "LIKE '{yyyymm}%' ORDER BY DateOnly ASC"
        
for year in allYears:

    print("Analysing {year}".format(year=year))

    yearEnergy = {}
    yearEnergy["yearly generation"] = 0.0
    view = "view" + str(year)
    frame = allFrames["new" + str(year)]

    for mon in allMonths:
        yyyymm = str(year) + mon
        _dates = spark.sql(ymdquery.format(view=year, yyyymm=yyyymm)).collect()
        days = [n.asDict()["DateOnly"] for n in _dates]
        _monthMax = frame.filter(
            frame.DateOnly.isin(days)).agg(
                {"EnergyGenerated": "max"}).collect()[0]
        monthMax = _monthMax.asDict()["max(EnergyGenerated)"]
        yearEnergy[yyyymm + "-max"] = monthMax
        # Obtaining the *average* and the minimum output on any day
        # is a little more difficult since we have to loop through
        # grabbing only the last value for each day - the field in
        # the CSV is the ongoing day total, not an instantaneous value.
        avgval = 0.0
        minval = monthMax
        minDay = ""
        maxDay = ""
        endOfMonth = 1
        for day in days:
            _val = frame.filter(frame.DateOnly == day).agg(
                {"EnergyGenerated": "max"}).collect()[0]
            val = _val.asDict()["max(EnergyGenerated)"]
            maxDay = day
            if val < minval:
                minval = val
                minDay = day
            avgval += minval
            endOfMonth += 1
        yearEnergy[yyyymm + "-min"] = minval
        yearEnergy[yyyymm + "-avg"] = avgval / endOfMonth
        yearEnergy["yearly generation"] += avgval

        # When did these record values (min, max) occur during
        # the month?
        yearEnergy[yyyymm + "-record-min"] = minDay
        yearEnergy[yyyymm + "-record-max"] = maxDay

    reports[view] = yearEnergy


print(json.dumps(reports, indent=4))    
