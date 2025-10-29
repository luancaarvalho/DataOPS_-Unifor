
import os

# This script demonstrates how to use conda to create a new environment and install packages.

# 1. Create a new environment named 'myenv' with Python 3.9
print("Creating a new conda environment named 'myenv' with Python 3.9...")
os.system("conda create --name myenv python=3.9 -y")

# 2. Activate the environment
# Note: Activating an environment in a script is tricky. 
# This command will not have a persistent effect on the shell running the script.
# You would typically run 'conda activate myenv' in your terminal.
print("To activate the environment, run: conda activate myenv")

# 3. Install packages into the environment
print("Installing pandas into the 'myenv' environment...")
os.system("conda install --name myenv pandas -y")

# 4. Deactivate the environment
# You would typically run 'conda deactivate' in your terminal.
print("To deactivate the environment, run: conda deactivate")

print("Miniconda example script finished.")
