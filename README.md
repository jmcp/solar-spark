About this repo
---------------


This repo is a demonstration of using Apache Spark with Python to provide
analysis of CSV-delimited data from a PV solar inverter. See 
[pvoutput.org][https://www.pvoutput.org/intraday.jsp?id=21105&sid=18984],
[jfy-monitor][https://github.com/jmcp/jfy-monitor] and
[monitoring my inverter][https://www.jmcpdotcom.com/blog/posts/2018-04-03-monitoring-my-inverter/]


There are two file formats involved in the entire collection. The first is the
output from [solarmonj][https://github.com/jcroucher/solarmonj], and has the
following schema: 

Timestamp:         seconds-since-epoch
Temperature:       float (degrees C)
energyNow:         float (Watts)
energyToday:       float (Watt-hours)
powerGenerated:    float (Hertz)
voltageDC:         float (Volts)
current:           float (Amps)
energyTotal:       float (Watt-hours)
voltageAC:         float (Volts)

Due to bugs in solarmonj when combined with occasionally marginal hardware,
some rows in the first version are invalid:

   1370752022,1.4013e-45,-0.27184,0,-0.27184,1.4013e-45,1.3703e-40,1.36638e-40,6.43869e-41

Those records are dropped prior to creating RDDs.

The second schema is from [jfy-monitor], and has this schema:

Timestamp:        ISO8601-like ("yyyy-MM-dd'T'HH:mm:ss")
Temperature:      float (degrees C)
PowerGenerated:   float (Watts)
VoltageDC:        float (Volts)
Current:          float (Amps)
EnergyGenerated:  float (Watts)
VoltageAC:        float (Volts)

The first schema is in effect for records starting on 2013-06-04
and ending on 2018-03-26.

The second schema takes effect with the logfiles starting on
2018-03-27. There were some records from 2018-03-27/8 which have
different fields, because I was updating jfyMonitor in production
and breaking things. We drop those records.


We load up *all* the data files, and then from within a venv which
has pyspark installed, we run

$ spark-submit /path/to/this/file (args)

to generate several reports:
- for each year, which month had the day with the max and min energy outputs
- for each month, what was the average energy generated
- for each month, what was the total energy generated

