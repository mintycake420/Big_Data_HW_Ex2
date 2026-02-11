import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

/**
 * Task 8+10: Comparing TV Shows and Movies
 *
 * Objective: Separate the dataset into movies and TV shows based on the year column
 *            and analyze their differences.
 *
 * Steps:
 * 1. Identify entries in the year column that indicate TV shows (entries with a "-")
 * 2. Compute average ratings and total votes for movies and TV shows
 * 3. Analyze trends in popularity (total votes) over time for both categories
 */
public class Task8_Task10 {

    public static void main(String[] args) {
        // Create Spark session
        SparkSession spark = SparkSession.builder()
                .appName("Task8: Comparing TV Shows and Movies")
                .master("local[*]")
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

        System.out.println("=== Task 8: Comparing TV Shows and Movies ===\n");

        // Load the IMDB dataset
        String dataPath = "data/IMBD.csv";
        Dataset<Row> df = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .option("escape", "\"")
                .csv(dataPath);

        // Clean the data
        Dataset<Row> cleanedDf = df
                .filter(col("title").isNotNull().and(col("title").notEqual("")))
                .filter(col("rating").isNotNull())  // rating is double, not string
                .filter(col("votes").isNotNull().and(col("votes").notEqual("")))
                .filter(col("year").isNotNull().and(col("year").notEqual("")))
                .withColumn("rating_numeric", col("rating").cast("double"))
                .withColumn("votes_numeric", regexp_replace(col("votes"), ",", "").cast("long"));

        System.out.println("Total entries analyzed: " + cleanedDf.count());

        // Step 1: Identify TV shows vs Movies based on year column
        System.out.println("\n--- Step 1: Separating TV Shows and Movies ---");

        // TV shows have "–" (en dash U+2013) or regular dash in year like "(2018– )" or "(2015–2022)"
        // Movies have a single year like "(2022)"
        // Check for both en-dash (–) and regular dash (-)
        Dataset<Row> categorized = cleanedDf
                .withColumn("is_tv_show",
                        col("year").contains("–").or(col("year").contains("—"))
                        .or(col("year").rlike(".*\\d{4}\\s*[-–—]\\s*\\d{0,4}.*"))
                        .or(col("year").rlike(".*\\d{4}\\s*[-–—]\\s*\\).*")))
                .withColumn("content_type",
                        when(col("is_tv_show"), "TV Show").otherwise("Movie"))
                // Extract start year
                .withColumn("year_clean", regexp_replace(col("year"), "[(),–\\s]", ""))
                .withColumn("start_year", regexp_extract(col("year"), "\\((\\d{4})", 1).cast("int"));

        System.out.println("\nSample of categorized data:");
        categorized.select("title", "year", "content_type", "start_year").show(20, false);

        // Count TV shows vs Movies
        System.out.println("\n=== TV Shows vs Movies Distribution ===");
        categorized
                .groupBy("content_type")
                .count()
                .orderBy(col("count").desc())
                .show(false);

        // Step 2: Compute average ratings and total votes for both categories
        System.out.println("\n--- Step 2: Computing Statistics ---");

        Dataset<Row> stats = categorized
                .groupBy("content_type")
                .agg(
                        count("*").alias("count"),
                        avg("rating_numeric").alias("avg_rating"),
                        stddev("rating_numeric").alias("stddev_rating"),
                        avg("votes_numeric").alias("avg_votes"),
                        sum("votes_numeric").alias("total_votes"),
                        min("rating_numeric").alias("min_rating"),
                        max("rating_numeric").alias("max_rating")
                );

        System.out.println("\n=== Overall Statistics: TV Shows vs Movies ===");
        stats.show(false);

        // Detailed comparison
        System.out.println("\n=== Detailed Comparison ===");

        Dataset<Row> tvShows = categorized.filter(col("content_type").equalTo("TV Show"));
        Dataset<Row> movies = categorized.filter(col("content_type").equalTo("Movie"));

        long tvCount = tvShows.count();
        long movieCount = movies.count();

        System.out.println("\nTV Shows: " + tvCount);
        System.out.println("Movies: " + movieCount);

        System.out.println("\n--- TV Shows Statistics ---");
        tvShows.select("rating_numeric", "votes_numeric").describe().show();

        System.out.println("\n--- Movies Statistics ---");
        movies.select("rating_numeric", "votes_numeric").describe().show();

        // Step 3: Analyze trends in popularity over time
        System.out.println("\n--- Step 3: Analyzing Trends Over Time ---");

        // Filter valid years
        Dataset<Row> withValidYear = categorized
                .filter(col("start_year").isNotNull())
                .filter(col("start_year").between(1990, 2023));  // Focus on recent decades

        // Yearly trends
        Dataset<Row> yearlyTrends = withValidYear
                .groupBy("start_year", "content_type")
                .agg(
                        count("*").alias("count"),
                        avg("rating_numeric").alias("avg_rating"),
                        avg("votes_numeric").alias("avg_votes"),
                        sum("votes_numeric").alias("total_votes")
                )
                .orderBy("start_year", "content_type");

        System.out.println("\n=== Yearly Trends (1990-2023) ===");
        yearlyTrends.show(100, false);

        // Pivot to compare side by side
        System.out.println("\n=== Popularity Trends by Year ===");
        Dataset<Row> yearComparison = withValidYear
                .groupBy("start_year")
                .pivot("content_type")
                .agg(
                        count("*").alias("count"),
                        avg("rating_numeric").alias("avg_rating"),
                        sum("votes_numeric").alias("total_votes")
                )
                .orderBy("start_year");

        yearComparison.show(50, false);

        // Trends in recent years (2015+)
        System.out.println("\n=== Recent Trends (2015-2023) ===");
        Dataset<Row> recentTrends = withValidYear
                .filter(col("start_year").geq(2015))
                .groupBy("start_year", "content_type")
                .agg(
                        count("*").alias("count"),
                        avg("rating_numeric").alias("avg_rating"),
                        avg("votes_numeric").alias("avg_votes")
                )
                .orderBy("start_year", "content_type");

        recentTrends.show(50, false);

        // Top rated TV shows vs Movies
        System.out.println("\n=== Top 10 Highest Rated TV Shows ===");
        tvShows
                .select("title", "year", "rating_numeric", "votes_numeric", "genre")
                .orderBy(col("rating_numeric").desc(), col("votes_numeric").desc())
                .show(10, false);

        System.out.println("\n=== Top 10 Highest Rated Movies ===");
        movies
                .select("title", "year", "rating_numeric", "votes_numeric", "genre")
                .orderBy(col("rating_numeric").desc(), col("votes_numeric").desc())
                .show(10, false);

        // Most popular (by votes)
        System.out.println("\n=== Top 10 Most Popular TV Shows (by votes) ===");
        tvShows
                .select("title", "year", "rating_numeric", "votes_numeric", "genre")
                .orderBy(col("votes_numeric").desc())
                .show(10, false);

        System.out.println("\n=== Top 10 Most Popular Movies (by votes) ===");
        movies
                .select("title", "year", "rating_numeric", "votes_numeric", "genre")
                .orderBy(col("votes_numeric").desc())
                .show(10, false);

        // Genre comparison
        System.out.println("\n=== Genre Distribution: TV Shows vs Movies ===");
        withValidYear
                .withColumn("genre_clean", regexp_replace(col("genre"), "\"", ""))
                .withColumn("genre_individual", explode(split(col("genre_clean"), ",\\s*")))
                .groupBy("content_type", "genre_individual")
                .count()
                .orderBy(col("content_type"), col("count").desc())
                .show(50, false);

        // Save results
        String outputPath1 = "output/tv_vs_movies_stats";
        System.out.println("\nSaving TV vs Movies statistics to: " + outputPath1);
        stats.write()
                .mode("overwrite")
                .option("header", "true")
                .csv(outputPath1);

        String outputPath2 = "output/tv_vs_movies_trends";
        System.out.println("Saving yearly trends to: " + outputPath2);
        yearlyTrends.write()
                .mode("overwrite")
                .option("header", "true")
                .csv(outputPath2);

        System.out.println("\n=== Task 8 Completed Successfully ===");

        spark.stop();
    }
}
