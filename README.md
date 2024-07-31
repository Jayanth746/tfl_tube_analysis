# tfl_tube_analysis

This notebook accomplishes the following:

Obtain the latest Tube line reports from the TFL Open API: https://api.tfl.gov.uk/Line/Mode/tube/Status
Insert the latest results into a Databricks SQL table with columns: current_timestamp, line, status, and disruption_reason.


Approaches Used:

Approach 1:
Fetch the latest tube status whenever the notebook is run.
Retrieve the response from the public TFL API using the Python requests library.
For each line in the response JSON array, create a list with only the required columns (current_timestamp, line, status, disruption_reason).
Transform the data and use a custom schema to create a DataFrame.
Write the DataFrame to a SQL table tfl_tube_status.

Approach 2:
Fetch the latest tube status whenever the notebook is run, retaining the history of statuses from all runs.
Retrieve the response from the public TFL API using the Python requests library.
Create a custom schema to map the response JSON object.
Create a DataFrame and select only the required columns (current_timestamp, line, status, disruption_reason).
Append the data into a history table to maintain a record of all statuses.
Load the data into the latest status table to capture the most recent statuses.
This approach supports further analysis of the tube lines and ensures scalability through the use of a well-defined schema.
