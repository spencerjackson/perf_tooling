# Create some analysis functions

import copy
import pandas
import warnings
import requests
import scipy
from scipy import stats
import subprocess
import numpy as np
import numpy
from matplotlib import pyplot as plt
import json
import seaborn
from collections import namedtuple

MetricData = namedtuple("MetricData", ["fixed_data", "diff_data", "raw_data"])

def make_differential_frame(df, dx):
    b = pandas.DataFrame()
    b[dx] = df[dx]
    b["d(t_pure)"] = df["d(t_pure)"]
    b["d(t_total)"] = df["d(t_total)"]
    b["d(t_overhead)"] = df["d(t_overhead)"]

    b["total_latency"] = b["d(t_total)"] / b[dx]
    b["total_latency(ms)"] = b["total_latency"] / 1000000
    b["overhead_latency"] = b["d(t_overhead)"] / b[dx]
    b["overhead_latency(ms)"] = b["overhead_latency"] / 1000000
    b["pure_latency"] = b["d(t_pure)"] / b[dx]
    b["pure_latency(ms)"] = b["pure_latency"] / 1000000
    b["ts"] = df["ts"]
    b["actor_id"] = df["actor_id"]

    b["total_ops"] = df["d(ops)"].cumsum()
    b["duration"] = (b["ts"] - b["ts"].iloc[0]).astype(int) / 1000000000
    b["throughput"] = b["total_ops"] / b["duration"]

    b["mean_pure_latency"] = df["d(t_pure)"].cumsum() / b["total_ops"]
    b["mean_pure_latency(ms)"] = b["mean_pure_latency"] / 1000000
    b["median_pure_latency"] = b["pure_latency"].expanding().median()
    b["median_pure_latency(ms)"] = b["median_pure_latency"] / 1000000

    # b["throughput"] = ops / (b["ts"][len(b)-1] - b["ts"][0]).total_seconds()

    # Have a single row for every sample/increment
    b = b.loc[b.index.repeat(b[dx])]

    return b

def get_raw_data(jsonFile):
    data = {
        "id": [],
        "counters.n": [],
        "counters.ops": [],
        "counters.size": [],
        "counters.errors": [],
        "timers.dur": [],
        "timers.total": [],
        "gauges.state": [],
        "gauges.workers": [],
        "gauges.failed": [],
        "ts":[]
    }
    with open(jsonFile) as f:
        for line in f:
            loaded = json.loads(line)
            data["id"].append(loaded["id"])
            data["counters.n"].append(loaded["counters"]["n"])
            data["counters.ops"].append(loaded["counters"]["ops"])
            data["counters.size"].append(loaded["counters"]["size"])
            data["counters.errors"].append(loaded["counters"]["errors"])
            data["timers.dur"].append(loaded["timers"]["dur"])
            data["timers.total"].append(loaded["timers"]["total"])
            data["gauges.state"].append(loaded["gauges"]["state"])
            data["gauges.workers"].append(loaded["gauges"]["workers"])
            data["gauges.failed"].append(loaded["gauges"]["failed"])
            data["ts"].append(loaded["ts"])
    return pandas.DataFrame(data)

def get_data(jsonFile):
    raw_data = get_raw_data(jsonFile)
    new_row = pandas.DataFrame({
        "id": 0,
        "counters.n": 0,
        "counters.ops": 0,
        "counters.size": 0,
        "counters.errors": 0,
        "timers.dur": 0,
        "timers.total": 0,
        "gauges.state": 0,
        "gauges.workers": 0,
        "gauges.failed": 0,
    }, index=[-1])
    intermediate = pandas.concat([new_row, raw_data])
    intermediate

    fixed_data = pandas.DataFrame()
    fixed_data["actor_id"] = intermediate["id"]
    fixed_data["d(n)"] = intermediate["counters.n"].diff()
    fixed_data["d(ops)"] = intermediate["counters.ops"].diff()
    fixed_data["d(size)"] = intermediate["counters.size"].diff()
    fixed_data["d(err)"] = intermediate["counters.errors"].diff()
    fixed_data["d(t_pure)"] = intermediate["timers.dur"].diff()
    fixed_data["d(t_total)"] = intermediate["timers.total"].diff()
    fixed_data["d(t_overhead)"] = fixed_data["d(t_total)"] - fixed_data["d(t_pure)"]

    fixed_data["ts"] = pandas.to_datetime(intermediate["ts"], unit="ms")

    fixed_data = fixed_data.loc[0:]
    b = make_differential_frame(fixed_data, "d(ops)")
    return MetricData(fixed_data, b, intermediate.loc[0:])

def get_summary_statistics(b, fixed_data, raw_data):
    quantiles = stats.mstats.mquantiles(b.loc[:,"pure_latency"].values, prob=[0.5,0.8,0.9,0.95,0.99], alphap=1/3, betap=1/3)
    averages = b.mean(numeric_only=True)
    maximum = b.max()
    minimum = b.min()
    duration = (b["ts"][len(b)-1] - b["ts"][0]).total_seconds()
    ops = fixed_data["d(ops)"].sum()
    size = fixed_data["d(size)"].sum()
    docs = fixed_data["d(n)"].sum()
    errs = fixed_data["d(err)"].sum()
    overhead = fixed_data["d(t_overhead)"].sum()
    return {
        'AverageLatency': averages["pure_latency"],
        'AverageSize': size / ops,
        'OperationThroughput': ops / duration,
        'DocumentThroughput': docs / duration,
        'SizeThroughput': size / duration,
        'ErrorRate': errs / duration,
        'Latency50thPercentile': quantiles[0],
        'Latency80thPercentile': quantiles[1],
        'Latency90thPercentile': quantiles[2],
        'Latency95thPercentile': quantiles[3],
        'Latency99thPercentile': quantiles[4],
        'WorkersMin': raw_data["gauges.workers"].min(),
        'WorkersMax': raw_data["gauges.workers"].max(),
        'LatencyMax': maximum["pure_latency"],
        'LatencyMin': minimum["pure_latency"],
        'DurationTotal': duration * 1e9,
        'ErrorsTotal': errs,
        'OperationsTotal': ops,
        'DocumentsTotal': docs,
        'SizeTotal': size,
        'OverheadTotal': overhead
    }

def check_are_close(expected, calculated):
    e_arr = []
    c_arr = []
    for k in expected.keys():
        e_arr.append(expected[k])
        c_arr.append(calculated[k])
    return np.allclose(e_arr, c_arr)

def make_latency_plot(df, interval, measure, transition=None, include_outliers=True):
    grouped_latencies = df.groupby(pandas.Grouper(key="ts", freq=interval))
    c = pandas.DataFrame()
    c["median"] = grouped_latencies[measure].median()
    c["25th"] = grouped_latencies[measure].quantile(0.25)
    c["75th"] = grouped_latencies[measure].quantile(0.75)
    c["IQR"] = c["75th"] - c["25th"]
    c["maximum"] = c["75th"] + 1.5 * c["IQR"]
    c["minimum"] = c["25th"] - 1.5 * c["IQR"]
    plt.figure(figsize=(20, 20))
    plt.plot(c["median"], color="yellow")
    plt.plot(c["maximum"], color="black", alpha=0.5)
    plt.plot(c["minimum"], color="black", alpha=0.5)
    plt.fill_between(c.index, c["minimum"], c["25th"], color="blue", alpha=0.2);
    plt.fill_between(c.index, c["25th"], c["median"], color="red", alpha=0.2);
    plt.fill_between(c.index, c["median"], c["75th"], color="red", alpha=0.2);
    plt.fill_between(c.index, c["75th"], c["maximum"], color="blue", alpha=0.2);
    if include_outliers:
        c["max"] = grouped_latencies[measure].max()
        c["min"] = grouped_latencies[measure].min()
        plt.plot(c["max"], color="green", alpha=0.5)
        plt.plot(c["min"], color="green", alpha=0.5)
    if transition:
        plt.axvline(transition, color="red", linestyle="--")

def linear_polyfit(df, x, y):
    return np.polyfit(df[x], df[y], 1)

def log_polyfit(df, x, y):
    return np.polyfit(np.log2(df[x]), df[y], 1)

def plot_latency_stats(df, xaxis, title=None, regr=None, ax=None, start=None, end=None):
    ylabel="pure_latency(ms)"
    calc_stats = pandas.DataFrame()
    calc_stats[xaxis] = df[xaxis]
    calc_stats["exponential weighted moving avg alpha=0.02"] = df[ylabel].ewm(alpha=0.02).mean()
    calc_stats["simple moving avg k=1024"] = df[ylabel].rolling(1024).mean()
    calc_stats["cumulative mean(ms)"] = df["mean_pure_latency(ms)"]
    calc_stats["cumulative median(ms)"] = df["median_pure_latency(ms)"]
    if regr is "log":
        fit = np.polyfit(np.log2(df[xaxis]), df[ylabel], 1)
        polyfunc=str(fit[0]) + " log2(x) + " + str(fit[1])
        calc_stats["least sq poly y=" + polyfunc] = np.log2(df[xaxis]) * fit[0] + fit[1]
        print(polyfunc)
    elif regr is "line":
        fit = np.polyfit(df[xaxis], df[ylabel], 1)
        polyfunc=str(fit[0]) + " x + " + str(fit[1])
        calc_stats["least sq poly y=" + polyfunc] = df[xaxis] * fit[0] + fit[1]
        print(polyfunc)
    return calc_stats[start:end].plot(ax=ax, x=xaxis, figsize=(20,20), ylabel="milliseconds", title=title)

