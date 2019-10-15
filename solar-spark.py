#!/usr/bin/env python3.7

#
# Copyright (c) 2019, James C. McPherson.  All rights reserved
#

# This code and data is made available to you under the MIT LICENSE:
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
#
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
# $ spark-submit /path/to/this/file (args)
#
# to generate several reports:
# - for each year, which month had the day with the max and min energy outputs
# - for each month, what was the average energy generated
# - for each month, what was the total energy generated


import csv
# import json
import getopt
import glob

import os
import sys

from datetime import datetime

from pyspark.sql.functions import date_format
from pyspark import SparkContext
from pyspark.sql import SparkSession

# Basic Spark session configuration
sc = SparkContext("local", "PV Inverter Analysis")
spark = SparkSession(sc)

# We don't need most of this output
log4j = sc._jvm.org.apache.log4j
log4j.LogManager.getRootLogger().setLevel(log4j.Level.ERROR)


allYears = range(2013, 2020)
allFrames = {}
allMonths = range(1, 13)
mnames = ["", "January", "February", "March", "April", "May",
          "June", "July", "August", "September", "October",
          "November", "December"]
rdds = {}


def now():
    """ Returns an ISO8601-formatted (without microseconds) timestamp"""
    return datetime.now().strftime("%Y-%M-%dT%H:%m:%S")

def importCSV(fname, isOld):
    output = []
    if isOld:
        multiplier = 1000.0
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
        except ValueError as _ve:
            # print("failed at {row} of {fname}".format(row=row, fname=fname))
            continue

        if "e" in temp:
            # invalid line, skip it
            continue

        if isOld:
            isostamp = datetime.fromtimestamp(int(tstamp))
        else:
            isostamp = datetime.fromisoformat(tstamp)

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
    patterns = []
    months = []
    # Since some of our data dirs have months as bare numbers and
    # others have a prepended 0, let's match them correctly.
    if month:
        if month < 10:
            months = [month, "0" + str(month)]
        else:
            months = [month]
    if year and month:
        patterns = ["{year}/{monthp}/**".format(year=year, monthp=monthp)
                    for monthp in months]
        kkey = year
    elif year:
        patterns = ["{year}/*/**".format(year=year)]
        kkey = year

    if patterns:
        globs = []
        for pat in patterns:
            globs.extend(glob.glob(os.path.join(topdir, pat)))
        allfiles[kkey] = globs
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
            # We'll take a bare number, not a 0-prepended form
            qmonth = int(opts["-m"])
        if "-y" in opts:
            qyear = opts["-y"]
        else:
            print("A year argument must be supplied")
            sys.exit(1)

    if qyear:
        allYears = [qyear]
    if qmonth:
        allMonths = [qmonth]

    # We're only going to search for data underneath $PWD
    allFiles = generateFiles("data", qyear, qmonth)
    # Sanity check - did we get any files to process?
    for k in allFiles.keys():
        if len(allFiles[k]) == 0:
            print("No files to import for year {qyear} month {qmonth}".format(
                qyear=qyear, qmonth=qmonth))
            sys.exit(0)

    print(now(), "Importing data files")

    for k in allFiles:
        rddyear = []
        for fn in allFiles[k]:
            if fn.endswith(".csv"):
                rddyear.extend(importCSV(fn, True))
            else:
                rddyear.extend(importCSV(fn, False))
        rdds[k] = rddyear

    print(now(), "All data files imported")
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

    print(now(), "Data transformed from RDDs into DataFrames")

    # Now we generate some reports
    # - for each year, which month had the day with max and min energy output
    # - for each month, what was the average energy generated
    # - for each month, what was the total energy generated

    # I'm doing to this with for loops over the dataframe, because that seems
    # to be a more efficient way of answering these specific questions.
    # With more experience using Spark over time I might ask the question
    # again in a more Spark-like fashion. We still need to get the data out,
    # somehow.

    reports = {}

    ymdquery = "SELECT DISTINCT DateOnly from {view} WHERE DateOnly "
    ymdquery += "LIKE '{yyyymm}%' ORDER BY DateOnly ASC"

    for year in allYears:

        print(now(), "Analysing {year}".format(year=year))

        yearEnergy = {}
        yearEnergy["yearly generation"] = 0.0
        view = "view" + str(year)
        frame = allFrames["new" + str(year)]

        for mon in allMonths:

            print(now(), "\t {monthname}".format(monthname=mnames[mon]))

            if mon < 10:
                yyyymm = str(year) + "0" + str(mon)
            else:
                yyyymm = str(year) + str(mon)

            _dates = spark.sql(ymdquery.format(
                view=view, yyyymm=yyyymm)).collect()
            days = [n.asDict()["DateOnly"] for n in _dates]

            _monthMax = frame.filter(
                frame.DateOnly.isin(days)).agg(
                    {"EnergyGenerated": "max"}).collect()[0]
            monthMax = _monthMax.asDict()["max(EnergyGenerated)"]
            monthGen = {}
            monthGen["max"] = monthMax

            # Obtaining the *average* and the minimum output on any day
            # is a little more difficult since we have to loop through
            # grabbing only the last value for each day - the field in
            # the CSV is the ongoing day total, not an instantaneous value.
            monthTot = 0.0
            minval = monthMax
            minDay = ""
            maxDay = ""
            endOfMonth = 0

            if len(days) == 0:
                # Handle the case where we're analysing a year but don't have
                # full-year data available.
                continue
            for day in days:
                _val = frame.filter(frame.DateOnly == day).agg(
                    {"EnergyGenerated": "max"}).collect()[0]
                val = _val.asDict()["max(EnergyGenerated)"]
                maxDay = day
                if val < minval:
                    minval = val
                    minDay = day
                monthTot += val
                endOfMonth += 1

            monthGen["min"] = minval
            monthGen["avg"] = monthTot / endOfMonth
            monthGen["total"] = monthTot
            yearEnergy["yearly generation"] += monthTot

            # When did these record values (min, max) occur during
            # the month?
            monthGen["record-min"] = minDay
            monthGen["record-max"] = maxDay
            yearEnergy[mnames[mon]] = monthGen

        reports[view] = yearEnergy

    print(now(), "All data analysed")

    for yview in reports:
        data = reports[yview]
        # print(json.dumps(data, indent=4))
        year = yview[4:]
        print(now(), "{year} total generation: {total:.2f} KW/h".format(
            year=year, total=data["yearly generation"]))
        for m in allMonths:
            mname = mnames[m]
            mview = data[mname]
            print(now(), "\t{mon} total:               {total:.2f} KW/h".format(
                mon=mname, total=mview["total"]))

            if mview["total"] == 0.00:
                print(now(), "\t----------------")
                continue

            print(now(), "\tRecord dates for {mon}:    "
                  "Max on {maxday} ({maxgen:.2f} KW/h), "
                  "Min on {minday} ({mingen:.2f} KW/h)".format(
                      mon=mname,
                      maxday=mview["record-max"],
                      maxgen=mview["max"],
                      minday=mview["record-min"],
                      mingen=mview["min"]))
            print(now(), "\tAverage daily generation  {dayavg:.2f} KW/h".format(
                dayavg=mview["avg"]))
            print(now(), "----------------")
