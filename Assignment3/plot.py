"""
file name:plot.py
file author: Kai-kai Lin
Date: 2022.05.31
Description:
Plot the same job in different number of cores.
X: cores
y: time-consuming
"""
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
with open("output/timings.txt") as r:
    y = []
    x = []
    for line in r:
        time = float(line.split()[-1])
        x_axis = int(line.split()[0])
        y.append(time)
        x.append(x_axis)
y = np.array(y)
x = np.array(x)
plt.plot(x,y)
plt.xlabel("core(s)")
plt.ylabel("time")
plt.savefig("output/timings.png")
