import socket
import pandas as pd

# List of engine addresses (simulated)
engine_addresses = ['localhost:2005', 'localhost:2006', 'localhost:2007']

# Functions to perform tasks on engine instances
def distribute_tasks(tasks):
    num_engines = len(engine_addresses)
    distributed_tasks = {i: [] for i in range(num_engines)}

    for i, task in enumerate(tasks):
        engine_index = i % num_engines
        distributed_tasks[engine_index].append(task)

    return distributed_tasks

def send_task_to_engine(task, engine_address, data_dir1,data_dir2):
    host, port = engine_address.split(':')
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((host, int(port)))

        # Send task and data directory
        task_with_dir = f"{task}|{data_dir1}|{data_dir2}"
        s.sendall(task_with_dir.encode())

        response = s.recv(1024).decode()
    return response

def main(input_file):
    with open(input_file, 'r') as f:
        catalog_sales_dir = f.readline().strip()
        date_dim_dir = f.readline().strip()
        operations = list(map(int, f.readline().split()))

    tasks = []

    # Distribute tasks to engine instances
    for operation in operations:
        if operation == 1:
            tasks.append("INNER_JOIN_COUNT")
        elif operation == 2:
            tasks.append("AGGREGATE_CATALOG_SALES")
        elif operation == 3:
            tasks.append("AGGREGATE_AFTER_INNER_JOIN")

    distributed_tasks = distribute_tasks(tasks)
    results = []

    # Send tasks to engine instances and collect results
    for engine_index, tasks in distributed_tasks.items():
        engine_address = engine_addresses[engine_index]
        for task in tasks:
            result = send_task_to_engine(task, engine_address, catalog_sales_dir,date_dim_dir)
            results.append(result)

    # Print results
    print("Results:")
    for result in results:
        print("\n")
        print(result)

if __name__ == "__main__":
    input_file = "input.txt"  
    main(input_file)