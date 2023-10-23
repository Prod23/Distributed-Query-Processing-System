import math
import time
import pandas as pd

# Simulated INNER JOIN and row count function
def perform_inner_join_and_count(data_dir1, data_dir2):
    # Read data from the specified data directories
    catalog_sales = pd.read_csv(data_dir1)
    date_dim = pd.read_csv(data_dir2)

    # print("Shape: ",catalog_sales.shape,date_dim.shape)

    # Measure start time
    start_time = time.time()

    (row_catalog,col_catalog) = catalog_sales.shape
    (row_date_dim,col_date_dim) = date_dim.shape
    i = 0
    j = 0
    joined_df = []
    while i < row_catalog:
        while j < row_date_dim:
            if(catalog_sales[i]['cs_sold_date_sk'] == date_dim[j]['d_date_sk']):
                joined_df.append(catalog_sales[i] + date_dim[j])
            j += 1
        i += 1
      
    

    # Simulate INNER JOIN and count rows
    joined_df = pd.merge(catalog_sales, date_dim, left_on='cs_sold_date_sk', right_on='d_date_sk', how='inner')

    row_count = len(joined_df)

    # Measure end time
    end_time = time.time()

    # Calculate time taken
    time_taken = end_time - start_time

    # Print time taken
    print(f"Time Taken for INNER JOIN: {time_taken} seconds")
    # print(row_count)
    return row_count

# Function to perform AGGREGATION on catalog_sales
def perform_aggregate_catalog_sales(data_dir1):
    # Load catalog_sales DataFrame from the specified directory
    start_time = time.time()  # Record start time
    catalog_sales = pd.read_csv(data_dir1)
    end_time = time.time()  # Record end time

    # Calculate time taken for loading DataFrame
    # load_time = end_time - start_time

    # List of columns for aggregation
    columns_to_aggregate = ['cs_quantity', 'cs_wholesale_cost', 'cs_list_price', 'cs_sales_price', 'cs_net_profit']

    # Initialize aggregation results dictionary
    aggregation_results = {}

    # Perform aggregation for each column
    for column in columns_to_aggregate:
        start_time = time.time()  # Record start time

        min_value = catalog_sales[column].min()
        max_value = catalog_sales[column].max()
        avg_value = catalog_sales[column].mean()

        end_time = time.time()  # Record end time

        # Calculate time taken for aggregation
        aggregation_time = end_time - start_time

        # Store aggregated values in the results dictionary
        aggregation_results[column] = {
            'min': math.floor(min_value),
            'max': math.floor(max_value),
            'avg': math.floor(avg_value)
        }
    print(f"Time taken for AGGREGATION after INNER JOIN: {aggregation_time:.6f} seconds")
    return aggregation_results

# Function to perform AGGREGATION after INNER JOIN
def perform_aggregate_after_inner_join(data_dir1, data_dir2):
    # Start measuring time
    start_time = time.time()

    # Get the row count from the INNER JOIN
    inner_join_row_count = perform_inner_join_and_count(data_dir1, data_dir2)

    # Simulate AGGREGATION after INNER JOIN
    aggregated_result = inner_join_row_count * 2 

    # Calculate time taken
    end_time = time.time()
    elapsed_time = end_time - start_time

    # Print the time taken
    print(f"Time taken for AGGREGATION after INNER JOIN: {elapsed_time:.6f} seconds")

    return aggregated_result