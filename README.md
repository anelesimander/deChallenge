## Data Cleaning

1. Verify that all necessary fields are filled before inserting data into the 'events' table.
2. Identify non-registered users with a different event_type using the following query directly on the table:

    ```sql
    SELECT * FROM events WHERE user_id NOT IN (SELECT user_id FROM events WHERE event_type = 'registration');
    ```

    Upon running the query, you might encounter an output similar to:
    ```
    47747|transaction|1274572644|89f807ec-683b-11ee-aca7-8699b86be788|||||0.99|EUR
    ```

3. Detect duplicate logs using the query below:

    ```sql
    SELECT event_id, COUNT(event_id) FROM events GROUP BY event_type, event_timestamp, user_id HAVING COUNT(*) > 1;
    ```

4. Check if user logs, when sorted by timestamp, follow the pattern: login, logout, ..., login, logout.

    If the pattern is disrupted, for instance:
    ```
    login, logout, login, logout, login, login, logout, logout
    ```

    Begin by examining the list from the start. Group elements in pairs and if the first element in a pair is not 'login' and the second isn't 'logout', remove the first element. Proceed by grouping the second element in the pair with the rest of the list.

## Python Script Overview

### Table Creation and Data Cleaning

1. **Table Creation:** Created a table named 'events' containing all fields from a JSON line provided in the dataset.

2. **Data Cleaning:**
   - **Removed Non-Registered Users:** Identified and removed entries associated with non-registered users.
   - **Removed Duplicate Entries:** Eliminated duplicate logs from the 'events' table.
   - **Chronological Pattern Check:** Ensured logs follow a chronological pattern; removed entries that deviate from this pattern.

### Enhanced Data Retrieval

3. **Table Creation for Easier Data Retrieval:**
   - Created three additional tables:
     - **'transactions' Table:** Contains transaction-related data.
     - **'users' Table:** Holds user-specific information.
     - **'session' Table:** Contains session-related data.

These additional tables aim to facilitate and optimize data retrieval within the API script.

