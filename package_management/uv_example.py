
import os
import subprocess

# This script demonstrates how to use uv to create a virtual environment and install packages.

# 1. Create a virtual environment
print("Creating a new virtual environment...")
subprocess.run(["uv", "venv"])

# 2. Activate the virtual environment
# Note: Activating an environment in a script is tricky.
# This command will not have a persistent effect on the shell running the script.
# You would typically run 'source .venv/bin/activate' in your terminal.
print("To activate the environment, run: source .venv/bin/activate")

# 3. Install packages
print("Installing requests...")
subprocess.run(["uv", "pip", "install", "requests"])

# 4. Deactivate the virtual environment
# You would typically run 'deactivate' in your terminal.
print("To deactivate the environment, run: deactivate")

print("uv example script finished.")
