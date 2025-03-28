import matplotlib.pyplot as plt
import pandas as pd

# Define the CSV file name
csv1 = "corepi2.csv"
csv2 = "corepi3.csv"

# Read the CSV file
df1 = pd.read_csv(csv1)
df2 = pd.read_csv(csv2)

# Convert the Timestamp column to datetime format
df1["Timestamp"] = pd.to_datetime(df1["Timestamp"])
df2["Timestamp"] = pd.to_datetime(df2["Timestamp"])

smoothing=10
df1["smoothed_util"] = df1["CPU_Utilization(%)"].rolling(smoothing).sum() / smoothing
df2["smoothed_util"] = df2["CPU_Utilization(%)"].rolling(smoothing).sum() / smoothing


# Plot the data
plt.figure(figsize=(10, 5))
plt.plot(df1["Timestamp"], df1["smoothed_util"], label="Node 1", color="b")
plt.plot(df2["Timestamp"], df2["smoothed_util"], label="Node 2", color="g")

plt.xlabel("Time")
plt.ylabel("CPU Utilization (%)")
plt.title("CPU Utilization Over Time")
plt.legend()
plt.xticks(rotation=45)
plt.grid()
plt.show()
