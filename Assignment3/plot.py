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
plt.savefig("output/timings.png")
