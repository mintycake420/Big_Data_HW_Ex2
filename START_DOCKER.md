# How to Start Docker and Run Tasks

## Issue: Docker Desktop Not Running

You have Docker installed but it's not running. Here's how to fix it:

### Step 1: Start Docker Desktop

**Windows:**
1. Press `Windows Key`
2. Type "Docker Desktop"
3. Click on "Docker Desktop" to start it
4. Wait for Docker to fully start (whale icon in system tray will stop animating)
5. You should see "Docker Desktop is running" in the system tray

**Or use command line:**
```bash
# Start Docker Desktop
start "" "C:\Program Files\Docker\Docker\Docker Desktop.exe"
```

### Step 2: Verify Docker is Running

Open a new terminal and run:
```bash
docker ps
```

You should see a table (even if empty) instead of an error.

### Step 3: Run the Tasks

Once Docker is running:

**Option A - Run all tasks automatically:**
```bash
cd "c:\Users\yotam\Documents\GitHub\Big_Data_HW_Ex2"
docker-run.bat
```

**Option B - Run tasks manually:**
```bash
cd "c:\Users\yotam\Documents\GitHub\Big_Data_HW_Ex2"

# Build and start container
docker-compose up -d

# Wait a moment for container to start, then run a task
docker-compose exec spark-homework mvn exec:java -Dexec.mainClass="Task1_DataCleaning"
```

### Step 4: Check Output

After tasks complete:
```bash
# List output directories
ls -la output/

# Check for files in a specific directory
ls -la output/cleaned_imdb_data/

# View a CSV file (if using Git Bash or WSL)
cat output/cleaned_imdb_data/*.csv | head -20
```

## Quick Test

To test if Docker is working:
```bash
# This should show Docker version
docker --version

# This should show running containers (or empty table)
docker ps

# This should download and run a test container
docker run hello-world
```

## Alternative: Run Without Docker

If Docker won't start, you can run directly with Maven (but may have output issues on Windows):

```bash
# Set Hadoop home
export HADOOP_HOME=/c/hadoop

# Run a task
cd "c:\Users\yotam\Documents\GitHub\Big_Data_HW_Ex2"
mvn clean compile
mvn exec:java -Dexec.mainClass="Task1_DataCleaning"
```

The tasks will run and show results in the console even if CSV writing fails.

## Troubleshooting

### Docker Desktop won't start
- Check if WSL 2 is enabled (Windows feature)
- Try restarting your computer
- Reinstall Docker Desktop

### Still can't connect to Docker
```bash
# Check Docker service status (PowerShell as Admin)
Get-Service *docker*

# Or restart Docker Desktop manually
```

### Want to see real-time progress
```bash
# Run in foreground to see all output
docker-compose up
```
