import subprocess
import time
import os
import signal

# Run the shell command to start driver.py (assuming it starts a server)
shell_command = "python driver.py input.txt"
server_process = subprocess.Popen(shell_command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

# Wait for some time to ensure the server has started (you might need to adjust this)
time.sleep(5)  # Adjust the sleep time as needed

# Get the output and error streams
stdout, stderr = server_process.communicate()

# Print the output and error
print("Server Output:")
print(stdout.decode())
# print("Server Error:")
# print(stderr.decode())

# Terminate the server process
server_process.terminate()

# Wait for the process to fully terminate
server_process.wait()

# Check if the process is still alive and force terminate if needed
if server_process.poll() is None:
    server_process.kill()

# Ensure engine instances are terminated (assuming you're using port numbers like 2005, 2006, 2007)
for port in [2005, 2006, 2007]:
    try:
        pid = os.system(f"lsof -t -i:{port}")
        if pid:
            os.kill(pid, signal.SIGTERM)
    except Exception as e:
        print(f"Error terminating engine on port {port}: {e}")


