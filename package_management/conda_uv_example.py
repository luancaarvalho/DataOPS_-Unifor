
import os
import subprocess

# This script demonstrates how to use conda to create an environment and uv to install packages into it.

# 1. Create a new conda environment
print("Creating a new conda environment named 'myenv'...")
os.system("conda create --name myenv python=3.9 -y")

# 2. Activate the environment
print("To activate the environment, run: conda activate myenv")

# 3. Install uv into the conda environment
print("Installing uv into the 'myenv' environment...")
os.system("conda run -n myenv pip install uv")

# 4. Use uv to install packages
print("Using uv to install requests...")
os.system("conda run -n myenv uv pip install requests")

print("conda + uv example script finished.")
