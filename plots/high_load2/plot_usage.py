import matplotlib.pyplot as plt
import pandas as pd
import numpy as np


# Define the CSV file name
csv1 = "compute2.csv"
csv2 = "compute3.csv"

# Read the CSV file
df1 = pd.read_csv(csv1)
df2 = pd.read_csv(csv2)

def transform_dataframe(df, start_time, smoothing):
    newdf = df.copy()
    newdf["timestamp"] = (pd.to_datetime(newdf["Timestamp"]) - pd.to_datetime(start_time)).dt.total_seconds() - 80
    newdf["util"] = newdf["CPU_Utilization(%)"].rolling(smoothing).mean()
    # conv = np.ones(smoothing)*0.9/(smoothing-1)
    # conv[-1] = 0.1
    # newdf["util"] = np.convolve(newdf["CPU_Utilization(%)"], conv, mode='same')
    newdf = newdf.drop(columns=["Timestamp"])
    newdf = newdf.drop(columns=["CPU_Utilization(%)"])
    return newdf

# Convert the Timestamp column to datetime format
minTS = min(df1["Timestamp"].min(), df2["Timestamp"].min())
smoothing = 30

df1 = transform_dataframe(df1, minTS, smoothing)
df2 = transform_dataframe(df2, minTS, smoothing)

df1.to_csv("corepi2_transformed.csv", index=False)
df2.to_csv("corepi3_transformed.csv", index=False)


# Plot the data
plt.figure(figsize=(10, 5))
plt.xlim(0, 95)
plt.plot(df1["timestamp"], df1["util"], label="Node 1", color="b")
plt.plot(df2["timestamp"], df2["util"], label="Node 2", color="g")


plt.xlabel("Time (in seconds)")
plt.ylabel("CPU Utilization (%)")
plt.title("CPU Utilization Over Time upon Node Failure")
plt.legend()
# plt.xticks(rotation=45)
plt.grid()
plt.show()
