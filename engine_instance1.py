import socket
import common_engine
import pandas as pd

def main(port):
    host = 'localhost'

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((host, port))
        s.listen()

        print(f"Engine instance listening on {host}:{port}")

        while True:
            conn, addr = s.accept()

            with conn:
                print(f"Connected by {addr}")
                task_with_dir = conn.recv(1024).decode()
                task, data_dir1, data_dir2 = task_with_dir.split('|')

                # print(task)
                # catalog_sales = pd.read_csv(data_dir1)
                # date_dim = pd.read_csv(data_dir2)
                
                # print("Shape: ",catalog_sales.shape,date_dim.shape,date_dim)

                if task == "INNER_JOIN_COUNT":
                    result = common_engine.perform_inner_join_and_count(data_dir1, data_dir2)
                    conn.sendall(str(result).encode())
                elif task == "AGGREGATE_CATALOG_SALES":
                    result = common_engine.perform_aggregate_catalog_sales(data_dir1)
                    conn.sendall(str(result).encode())
                elif task == "AGGREGATE_AFTER_INNER_JOIN":
                    result = common_engine.perform_aggregate_after_inner_join(data_dir1,data_dir2)
                    conn.sendall(str(result).encode())
                else:
                    result = "Invalid task"
                    conn.sendall(str(result).encode())

if __name__ == "__main__":
    port = 2005  # Change the port number for each engine instance
    main(port)
