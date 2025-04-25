import psutil
import csv
import time
from datetime import datetime

# Define the CSV file name
csv_filename = "cpu_usage_log.csv"

# Write the header to the CSV file
with open(csv_filename, mode="w", newline="") as file:
    writer = csv.writer(file)
    writer.writerow(["Timestamp", "CPU_Utilization(%)"])

# Continuous logging loop
try:
    while True:
        # Get the current timestamp with millisecond accuracy
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        
        # Get the average CPU utilization
        cpu_utilization = psutil.cpu_percent(interval=0.1)
        
        # Write the data to the CSV file
        with open(csv_filename, mode="a", newline="") as file:
            writer = csv.writer(file)
            writer.writerow([timestamp, cpu_utilization])
        
        print(f"{timestamp}, {cpu_utilization}%")
        
        # Wait for 100 milliseconds before the next measurement
        time.sleep(0.1)

except KeyboardInterrupt:
    print("Logging stopped.")

