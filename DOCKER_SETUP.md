# Docker Setup Guide for Spark IMDB Analysis

This guide explains how to run the Spark homework tasks using Docker, which provides a clean Linux environment and avoids Windows-specific Hadoop compatibility issues.

## Prerequisites

- **Docker Desktop** (Windows/Mac) or **Docker Engine** (Linux)
- **Docker Compose** (usually included with Docker Desktop)

### Install Docker

**Windows/Mac:**
- Download and install [Docker Desktop](https://www.docker.com/products/docker-desktop)
- Ensure Docker Desktop is running (check system tray/menu bar)

**Linux:**
```bash
# Ubuntu/Debian
sudo apt-get update
sudo apt-get install docker.io docker-compose

# Start Docker service
sudo systemctl start docker
sudo systemctl enable docker
```

## Quick Start

### Option 1: Run All Tasks Automatically

**Windows:**
```bash
docker-run.bat
```

**Linux/Mac:**
```bash
chmod +x docker-run.sh
./docker-run.sh
```

This will:
1. Build the Docker image with all dependencies
2. Start the container
3. Run all 8 tasks sequentially
4. Save output files to `./output/` directory
5. Display completion status

### Option 2: Interactive Mode

Start the container and run tasks manually:

```bash
# Build and start container
docker-compose up -d

# Enter the container
docker-compose exec spark-homework bash

# Inside the container, run individual tasks
mvn exec:java -Dexec.mainClass="Task1_DataCleaning"
mvn exec:java -Dexec.mainClass="Task2_TopRatedByGenre"
# ... etc

# Exit container
exit

# Stop container
docker-compose down
```

## Running Individual Tasks

```bash
# Make sure container is running
docker-compose up -d

# Run specific task
docker-compose exec spark-homework mvn exec:java -Dexec.mainClass="Task1_DataCleaning"

# Or run with output suppressed
docker-compose exec spark-homework mvn exec:java -Dexec.mainClass="Task1_DataCleaning" -q
```

## File Structure

```
Big_Data_HW_Ex2/
├── Dockerfile              # Docker image definition
├── docker-compose.yml      # Docker service configuration
├── docker-run.sh          # Linux/Mac runner script
├── docker-run.bat         # Windows runner script
├── src/                   # Source code (mounted into container)
├── data/                  # Dataset (mounted into container)
└── output/                # Results (mounted from container)
```

## How It Works

1. **Docker Image**: Based on Maven 3.9 with Java 11 (Eclipse Temurin)
2. **Volume Mounts**:
   - Source code is mounted for live editing
   - Data directory is mounted read-only
   - Output directory is mounted to persist results
   - Maven cache is persisted for faster rebuilds

3. **Environment**:
   - Linux environment (no Windows Hadoop issues)
   - Java 11 (optimal Spark compatibility)
   - 4GB heap memory allocated
   - All dependencies pre-downloaded

## Output Files

All output files are saved to the `./output/` directory on your host machine:

```bash
# View output structure
ls -la output/

# Example output directories:
output/
├── cleaned_imdb_data/           # Task 1 output
├── top_rated_by_genre/          # Task 2 output
├── actor_collaborations/        # Task 3 output
├── actor_collaboration_stats/   # Task 3 output
├── hidden_gems/                 # Task 4 output
├── word_frequency/              # Task 5 output
├── genre_diversity/             # Task 6 output
├── certification_distribution/  # Task 7 output
├── tv_vs_movies_stats/         # Task 8 output
└── tv_vs_movies_trends/        # Task 8 output
```

Each directory contains CSV or JSON files with the analysis results.

## Viewing Results

```bash
# View CSV files
cat output/cleaned_imdb_data/*.csv | head -20

# View JSON files
cat output/actor_collaborations/*.json | head -20

# Count output files
find output/ -type f | wc -l
```

## Development Workflow

### Editing Code

1. Edit Java files in `src/main/java/` on your host machine
2. No need to rebuild Docker image - files are mounted
3. Just recompile inside the container:

```bash
docker-compose exec spark-homework mvn clean compile
docker-compose exec spark-homework mvn exec:java -Dexec.mainClass="TaskX_Name"
```

### Rebuilding

Only rebuild the Docker image if you change:
- `pom.xml` (dependencies)
- `Dockerfile`
- Need a clean environment

```bash
# Rebuild image
docker-compose build --no-cache

# Or rebuild and start
docker-compose up -d --build
```

## Troubleshooting

### Docker not running
```bash
# Check Docker status
docker ps

# If error, start Docker Desktop (Windows/Mac)
# Or start Docker service (Linux)
sudo systemctl start docker
```

### Permission denied
```bash
# Linux - add user to docker group
sudo usermod -aG docker $USER
# Log out and log back in
```

### Port already in use
```bash
# Stop and remove all containers
docker-compose down

# If still issues, remove container
docker rm -f spark-imdb-analysis
```

### Out of disk space
```bash
# Clean up Docker
docker system prune -a
docker volume prune
```

### Container won't start
```bash
# View logs
docker-compose logs

# Rebuild from scratch
docker-compose down
docker-compose build --no-cache
docker-compose up -d
```

## Performance

- **First build**: ~5-10 minutes (downloads all dependencies)
- **Subsequent builds**: ~30 seconds (uses cache)
- **Task execution**: ~10-15 seconds per task
- **Total runtime**: ~2-3 minutes for all 8 tasks

## Cleanup

```bash
# Stop and remove containers
docker-compose down

# Remove volumes (Maven cache)
docker-compose down -v

# Remove Docker image
docker rmi big_data_hw_ex2_spark-homework

# Complete cleanup (removes all output)
docker-compose down -v
rm -rf output/*
```

## Advantages of Docker Approach

✅ **Cross-platform**: Works identically on Windows, Mac, and Linux
✅ **No Hadoop setup**: Native Linux environment, no winutils needed
✅ **Reproducible**: Exact same environment every time
✅ **Isolated**: Doesn't affect your system
✅ **Clean**: Easy to reset and start fresh
✅ **Production-like**: Similar to how Spark runs in real clusters

## Alternative: Docker with GUI

If you prefer a graphical interface:

```bash
# Run with interactive shell
docker-compose run --rm spark-homework bash

# Or use VS Code with Docker extension
# Install "Remote - Containers" extension
# Right-click docker-compose.yml -> "Compose Up"
# Then attach VS Code to running container
```

## Submitting Results

For homework submission:

```bash
# Run all tasks in Docker
./docker-run.sh  # or docker-run.bat on Windows

# Verify output
ls -la output/

# Include these in submission:
# - src/main/java/*.java (source code)
# - output/ directory (results)
# - README.md (documentation)
# - CLAUDE.md (implementation notes)
# - Dockerfile + docker-compose.yml (reproducibility)
```

## Summary

Docker provides the cleanest way to run these Spark tasks:
- No Windows compatibility issues
- Actual CSV/JSON output files generated
- Reproducible environment
- Professional development practice

Just run `docker-run.sh` (or `.bat`) and you're done! ✅
