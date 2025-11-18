
import os
import subprocess

# This script demonstrates how to use pixi to manage a project.

# 1. Create a new directory for the project
project_dir = "my_pixi_project"
if not os.path.exists(project_dir):
    os.makedirs(project_dir)

# 2. Initialize a new pixi project
print(f"Initializing a new pixi project in {project_dir}...")
subprocess.run(["pixi", "init"], cwd=project_dir)

# 3. Add dependencies
print("Adding dependencies...")
subprocess.run(["pixi", "add", "python=3.9"], cwd=project_dir)
subprocess.run(["pixi", "add", "pandas"], cwd=project_dir)
subprocess.run(["pixi", "add", "requests", "--pypi"], cwd=project_dir)

# 4. Install dependencies
print("Installing dependencies...")
subprocess.run(["pixi", "install"], cwd=project_dir)

# 5. Run a command in the project's environment
print("Running a command in the project's environment...")
subprocess.run(["pixi", "run", "python", "-c", "import pandas; print(pandas.__version__)"], cwd=project_dir)

print("pixi example script finished.")
