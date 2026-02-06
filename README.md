# Big Data Homework 2 - Spark Analytics on IMDB Dataset

## Project Overview

This project implements various data analytics tasks using Apache Spark on the IMDB dataset. The assignment includes 8 different tasks that demonstrate data cleaning, aggregation, transformation, and analysis capabilities of Spark.

## Requirements

- Java 11 or higher
- Apache Maven 3.6+
- Apache Spark 3.5.8 (included as Maven dependency)

## Project Structure

```
Big_Data_HW_Ex2/
├── data/
│   └── IMBD.csv                    # Input dataset
├── src/main/java/
│   ├── Task1_DataCleaning.java              # Data cleaning and preprocessing
│   ├── Task2_TopRatedByGenre.java           # Top rated movies by genre
│   ├── Task3_ActorCollaboration.java        # Actor collaboration network
│   ├── Task4_HighRatedHiddenGems.java       # High-rated but low-vote movies
│   ├── Task5_WordFrequency.java             # Word frequency in titles
│   ├── Task6_GenreDiversity.java            # Genre rating variability
│   ├── Task7_CertificationDistribution.java # Certification analysis
│   └── Task8_TVvsMovies.java                # TV shows vs movies comparison
├── output/                         # Generated output directory
├── pom.xml                        # Maven project configuration
└── README.md                      # This file
```

## Tasks Description

### Task 1: Data Cleaning and Preprocessing
- **Objective**: Prepare the dataset for analysis
- **Steps**: Handle missing values, convert votes to numeric, extract year values
- **Output**: `output/cleaned_imdb_data/`

### Task 2: Top Rated Movies by Genre
- **Objective**: Identify top 10 movies for each genre
- **Steps**: Split genres, group by genre, rank by rating
- **Output**: `output/top_rated_by_genre/`

### Task 3: Actor Collaboration Network
- **Objective**: Analyze actor collaborations
- **Steps**: Parse actors, generate pairwise combinations, count collaborations
- **Output**: `output/actor_collaborations/`, `output/actor_collaboration_stats/`

### Task 4: High-Rated Hidden Gems
- **Objective**: Find high-rated movies with low votes
- **Steps**: Filter (rating > 8.0, votes < 10,000), sort results
- **Output**: `output/hidden_gems/`

### Task 5: Word Frequency in Movie Titles
- **Objective**: Find most frequent words in titles
- **Steps**: Word count, exclude stop words, identify top 20
- **Output**: `output/word_frequency/`

### Task 6: Genre Diversity in Ratings
- **Objective**: Measure rating variability across genres
- **Steps**: Group by genre, calculate standard deviation
- **Output**: `output/genre_diversity/`

### Task 7: Certification Rating Distribution
- **Objective**: Analyze movies by certification rating
- **Steps**: Group by certification, calculate statistics
- **Output**: `output/certification_distribution/`

### Task 8: Comparing TV Shows and Movies
- **Objective**: Compare TV shows and movies
- **Steps**: Separate by year format, calculate statistics, analyze trends
- **Output**: `output/tv_vs_movies_stats/`, `output/tv_vs_movies_trends/`

## How to Run

### 🐳 Recommended: Docker (Easiest - Works on All Platforms)

**For the best experience with actual output files:**

```bash
# Windows
docker-run.bat

# Linux/Mac
./docker-run.sh
```

This runs everything in a Linux container and generates all output files. See **[DOCKER_SETUP.md](DOCKER_SETUP.md)** for complete guide.

### Alternative: Direct Maven (May have Windows compatibility issues)

#### Building the Project

```bash
mvn clean compile
```

#### Running Individual Tasks

You can run each task separately using Maven:

```bash
# Task 1: Data Cleaning
mvn exec:java -Dexec.mainClass="Task1_DataCleaning"

# Task 2: Top Rated by Genre
mvn exec:java -Dexec.mainClass="Task2_TopRatedByGenre"

# Task 3: Actor Collaboration
mvn exec:java -Dexec.mainClass="Task3_ActorCollaboration"

# Task 4: High-Rated Hidden Gems
mvn exec:java -Dexec.mainClass="Task4_HighRatedHiddenGems"

# Task 5: Word Frequency
mvn exec:java -Dexec.mainClass="Task5_WordFrequency"

# Task 6: Genre Diversity
mvn exec:java -Dexec.mainClass="Task6_GenreDiversity"

# Task 7: Certification Distribution
mvn exec:java -Dexec.mainClass="Task7_CertificationDistribution"

# Task 8: TV Shows vs Movies
mvn exec:java -Dexec.mainClass="Task8_TVvsMovies"
```

### Running All Tasks

**Windows:**
```bash
run_all_tasks.bat
```

**Linux/Mac:**
```bash
chmod +x run_all_tasks.sh
./run_all_tasks.sh
```

## Output

All tasks generate output in the `output/` directory. The outputs include:
- CSV files with analysis results
- JSON files for complex data structures
- Console output with statistics and insights

## Dataset Information

The IMDB dataset (`data/IMBD.csv`) contains the following columns:
- **title**: Movie/TV show title
- **year**: Release year or year range
- **certificate**: Age certification (PG-13, R, TV-MA, etc.)
- **duration**: Runtime in minutes
- **genre**: Comma-separated genres
- **rating**: Average user rating (1-10)
- **description**: Plot synopsis
- **stars**: List of main actors
- **votes**: Total number of votes

## Key Spark Concepts Demonstrated

1. **Data Loading**: Reading CSV files with proper schema inference
2. **Transformations**: map, filter, flatMap, explode, split
3. **Aggregations**: groupBy, count, avg, sum, stddev
4. **Window Functions**: row_number, partitionBy, orderBy
5. **UDFs**: User-defined functions for custom logic
6. **Data Cleaning**: Handling missing values, type conversion
7. **Complex Operations**: Joins, pivots, collect_list
8. **Optimization**: Lazy evaluation, DAG optimization

## Notes

- The project uses Spark's DataFrame API for optimal performance
- All tasks run in local mode (`local[*]`)
- Log level is set to WARN to reduce console noise
- Output directories are overwritten on each run

## Troubleshooting

### Out of Memory Error
If you encounter memory issues, increase heap size:
```bash
export MAVEN_OPTS="-Xmx4g"
```

### Permission Issues on Output Directory
Delete the output directory before running:
```bash
rm -rf output/
```

## Author

Haifa University - Big Data Course
Homework 2: Spark Analytics
