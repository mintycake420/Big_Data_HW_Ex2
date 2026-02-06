# Quick Start Guide

## Option 1: Docker (Recommended) 🐳

**Best for: Everyone, especially Windows users**

### Prerequisites
- Install [Docker Desktop](https://www.docker.com/products/docker-desktop)

### Run Everything
```bash
# Windows
docker-run.bat

# Linux/Mac
chmod +x docker-run.sh
./docker-run.sh
```

**That's it!** All 8 tasks will run and output files will be in `./output/`

---

## Option 2: Direct Maven

**Best for: Linux/Mac users with Java 11 already installed**

### Prerequisites
- Java 11+
- Maven 3.6+
- (Windows only) Hadoop binaries in `C:\hadoop\bin\`

### Run Tasks
```bash
# Windows - set environment first
export HADOOP_HOME=/c/hadoop

# Build
mvn clean compile

# Run individual tasks
mvn exec:java -Dexec.mainClass="Task1_DataCleaning"
mvn exec:java -Dexec.mainClass="Task2_TopRatedByGenre"
# ... etc
```

---

## What Gets Produced

### Output Files (in `./output/` directory)
- `cleaned_imdb_data/` - Cleaned dataset (8,784 entries)
- `top_rated_by_genre/` - Top 10 movies per genre
- `actor_collaborations/` - Actor collaboration network
- `hidden_gems/` - High-rated, low-vote movies
- `word_frequency/` - Most common words in titles
- `genre_diversity/` - Rating variability by genre
- `certification_distribution/` - Analysis by rating (PG, R, etc.)
- `tv_vs_movies_stats/` - TV shows vs movies comparison

### Console Output
Each task also prints detailed statistics and analysis results.

---

## Viewing Results

```bash
# List all output directories
ls -la output/

# View a CSV file
cat output/cleaned_imdb_data/*.csv | head -20

# View a JSON file
cat output/actor_collaborations/*.json | head -10

# Count total output files
find output/ -type f | wc -l
```

---

## Troubleshooting

### Docker approach not working?
```bash
# Check Docker is running
docker ps

# If not, start Docker Desktop
# Then run docker-run script again
```

### Maven approach not working?
- **Windows**: Use Docker instead (recommended)
- **Linux/Mac**: Check Java version with `java -version` (need 11+)
- **All**: See [README.md](README.md) troubleshooting section

---

## Task Descriptions

1. **Data Cleaning** - Preprocesses and cleans 9,957 IMDB entries
2. **Top Rated by Genre** - Finds top 10 movies in each genre using Window functions
3. **Actor Collaboration** - Analyzes which actors work together most (uses UDFs)
4. **Hidden Gems** - Finds highly-rated but lesser-known titles
5. **Word Frequency** - Most common words in movie titles (excludes stop words)
6. **Genre Diversity** - Measures rating consistency across genres
7. **Certification Analysis** - Compares PG, R, TV-MA ratings
8. **TV vs Movies** - Comprehensive comparison of TV shows and movies

---

## Next Steps

1. **Run the tasks**: Use Docker approach for easiest setup
2. **Check output**: Explore `./output/` directory
3. **Read results**: Review console output and CSV/JSON files
4. **Understand code**: Check Java source files in `src/main/java/`
5. **Read docs**: See [CLAUDE.md](CLAUDE.md) for implementation details

---

## Time Required

- **Docker first run**: 5-10 minutes (downloading dependencies)
- **Docker subsequent runs**: 2-3 minutes (cached)
- **All 8 tasks execution**: ~2-3 minutes total

---

## Questions?

- **Docker setup**: See [DOCKER_SETUP.md](DOCKER_SETUP.md)
- **Project details**: See [README.md](README.md)
- **Implementation notes**: See [CLAUDE.md](CLAUDE.md)
