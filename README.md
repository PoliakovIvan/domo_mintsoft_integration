<H1> Domo Mintsoft Integration </H1>

<h3>Overview</h3>
Domo Mintsoft Integration is an automated solution designed to seamlessly integrate data from the Mintsoft warehouse management system into the Domo analytics platform. This integration collects, processes, and syncs order and warehouse data, updating it in real-time within Domo for accurate business insights.

<h3>Features</h3>

- Automated Data Collection: Retrieves order information from Mintsoft via API.
- Data Processing & Filtering: Handles unique processing of each order, ensuring accurate and complete records.
- PostgreSQL Integration: Stores and updates data in a PostgreSQL database.
- Domo API Sync: Automatically uploads the processed data to Domo for analytics.
- Flexible Scheduling: Daily and weekly update schedules can be customized.

<h3>Technologies Used</h3>

- Python: Core language for data processing and API communication.
- Pandas: Data manipulation and filtering.
- PostgreSQL: For persistent data storage.
- Mintsoft API: Source of order, warehouse, client, and status information.
- Domo API: For pushing data to Domo's analytics platform.
- Schedule: Automates the timing of data updates.
