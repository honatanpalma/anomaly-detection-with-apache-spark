# Network Traffic Anomaly Detection with PySpark 

This repository contains a basic PySpark exercise focused on anomaly detection in network traffic logs. The code demonstrates essential steps in the process, from data loading and preprocessing to feature engineering and simple outlier detection techniques. 

## Sample Network Traffic Data

The following table represents a snapshot of network traffic logs, capturing key attributes of network events.

| timestamp           | source_ip      | destination_ip   | port | protocol | bytes_transferred |
|--------------------|----------------|------------------|------|----------|-------------------|
| 2023-08-01 10:00:00 | 192.168.1.10   | 10.0.0.1         | 80   | TCP      | 512               |
| 2023-08-01 10:00:05 | 192.168.1.20   | 8.8.8.8          | 53   | UDP      | 64                |
| 2023-08-01 10:00:10 | 192.168.1.30   | 172.217.16.142   | 443  | TCP      | 1024              |
| 2023-08-01 10:00:15 | 192.168.1.10   | 10.0.0.1         | 80   | TCP      | 2048              |
| 2023-08-01 10:00:20 | 192.168.1.40   | 192.168.1.10    | 22   | TCP      | 32                |
| 2023-08-01 10:00:25 | 192.168.1.20   | 8.8.4.4          | 53   | UDP      | 128               |
| 2023-08-01 10:00:30 | 192.168.1.50   | 142.250.185.110  | 443  | TCP      | 4096              |
| 2023-08-01 10:00:35 | 192.168.1.10   | 10.0.0.1         | 80   | TCP      | 512               |
| 2023-08-01 10:00:40 | 192.168.1.30   | 172.217.16.142   | 443  | TCP      | 2048              |
| 2023-08-01 10:00:45 | 192.168.1.20   | 8.8.8.8          | 53   | UDP      | 64                |

* **timestamp:** The date and time of the network event.
* **source_ip:** The IP address originating the connection.
* **destination_ip:** The IP address receiving the connection.
* **port:** The destination port number.
* **protocol:** The network protocol used (e.g., TCP, UDP).
* **bytes_transferred:** The number of bytes transferred during the event.

## Key Features:

* **Data Loading:** Loads network traffic data from a CSV file using a defined schema.
* **Basic Preprocessing:** Handles null values, removes duplicates, and filters out irrelevant traffic (e.g., ICMP).
* **Feature Engineering:** Calculates relevant features like traffic volume per source IP, total bytes transferred per protocol, and traffic patterns over time.
* **Anomaly Detection:** 
    * Identifies potential DDoS attacks by detecting high traffic volume from single sources.
    * Detects outliers in `bytes_transferred` using statistical methods (3 standard deviations from the mean).

## How to Run:

1. **Prerequisites:**
   * Ensure you have a PySpark environment set up. 
   * Replace `"network_traffic.csv"` with the actual path to your network traffic log file. 
   * Adjust the `threshold` values in the anomaly detection sections based on your specific network environment and expected traffic patterns.

2. **Execute the Code:** Run the PySpark script in your preferred environment (e.g., Databricks, Jupyter Notebook, command line).

## Potential Enhancements:

* **More Advanced Anomaly Detection:** Implement additional techniques like clustering, machine learning models, or time series analysis to detect more sophisticated anomalies.
* **Real-time Processing:** Explore PySpark Streaming to analyze network traffic in real-time.
* **Visualization:** Integrate data visualization libraries to create insightful plots and graphs of network traffic patterns and anomalies.
* **Error Handling and Robustness:** Add error handling mechanisms to gracefully handle potential issues during data loading or processing.
* **Configuration Management:** Externalize configuration parameters (e.g., file paths, thresholds) to improve flexibility and maintainability.

## Disclaimer:

This code is intended for educational and illustrative purposes. It provides a basic foundation for network traffic anomaly detection using PySpark. For production environments, further refinement and customization would be necessary to address specific security requirements and adapt to real-world network traffic complexities.

**Feel free to contribute to this project by suggesting improvements, adding new features, or exploring more advanced anomaly detection techniques!**
