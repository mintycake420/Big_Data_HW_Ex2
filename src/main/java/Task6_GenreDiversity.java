import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

/**
 * Task 6: Genre Diversity in Ratings
 *
 * Objective: Measure the variability of ratings across genres.
 *
 * Steps:
 * 1. Group movies by genre
 * 2. Calculate the standard deviation of ratings within each genre
 * 3. Identify the genres with the highest and lowest variability
 */
public class Task6_GenreDiversity {

    public static void main(String[] args) {
        // Create Spark session
        SparkSession spark = SparkSession.builder()
                .appName("Task6: Genre Diversity in Ratings")
                .master("local[*]")
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

        System.out.println("=== Task 6: Genre Diversity in Ratings ===\n");

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
                .filter(col("genre").isNotNull().and(col("genre").notEqual("")))
                .withColumn("rating_numeric", col("rating").cast("double"));

        System.out.println("Total entries with valid genre and rating: " + cleanedDf.count());

        // Step 1: Split genres and explode
        System.out.println("\n--- Step 1: Grouping Movies by Genre ---");

        Dataset<Row> genreExploded = cleanedDf
                .withColumn("genre_clean", regexp_replace(col("genre"), "\"", ""))
                .withColumn("genre_individual", explode(split(col("genre_clean"), ",\\s*")))
                .select(
                        col("title"),
                        col("genre_individual").alias("genre"),
                        col("rating_numeric").alias("rating")
                );

        System.out.println("\nSample of genre-split data:");
        genreExploded.show(10, false);

        // Step 2: Calculate statistics including standard deviation for each genre
        System.out.println("\n--- Step 2: Calculating Rating Statistics by Genre ---");

        Dataset<Row> genreStats = genreExploded
                .groupBy("genre")
                .agg(
                        count("*").alias("count"),
                        avg("rating").alias("avg_rating"),
                        stddev("rating").alias("stddev_rating"),
                        min("rating").alias("min_rating"),
                        max("rating").alias("max_rating"),
                        expr("max(rating) - min(rating)").alias("rating_range")
                )
                .filter(col("count").geq(5))  // Only genres with at least 5 titles
                .orderBy(col("stddev_rating").desc());

        System.out.println("\n=== Genre Rating Statistics (All Genres) ===");
        genreStats.show(100, false);

        // Step 3: Identify genres with highest and lowest variability
        System.out.println("\n--- Step 3: Identifying Extreme Variability Genres ---");

        // Collect the results to find min and max stddev
        Row maxStddevRow = genreStats.orderBy(col("stddev_rating").desc()).first();
        Row minStddevRow = genreStats.orderBy(col("stddev_rating").asc()).first();

        System.out.println("\n=== Genre with HIGHEST Rating Variability ===");
        System.out.println("Genre: " + maxStddevRow.getString(0));
        System.out.println("Count: " + maxStddevRow.getLong(1));
        System.out.println("Average Rating: " + String.format("%.2f", maxStddevRow.getDouble(2)));
        System.out.println("Standard Deviation: " + String.format("%.4f", maxStddevRow.getDouble(3)));
        System.out.println("Min Rating: " + maxStddevRow.getDouble(4));
        System.out.println("Max Rating: " + maxStddevRow.getDouble(5));
        System.out.println("Rating Range: " + String.format("%.2f", maxStddevRow.getDouble(6)));

        System.out.println("\n=== Genre with LOWEST Rating Variability ===");
        System.out.println("Genre: " + minStddevRow.getString(0));
        System.out.println("Count: " + minStddevRow.getLong(1));
        System.out.println("Average Rating: " + String.format("%.2f", minStddevRow.getDouble(2)));
        System.out.println("Standard Deviation: " + String.format("%.4f", minStddevRow.getDouble(3)));
        System.out.println("Min Rating: " + minStddevRow.getDouble(4));
        System.out.println("Max Rating: " + minStddevRow.getDouble(5));
        System.out.println("Rating Range: " + String.format("%.2f", minStddevRow.getDouble(6)));

        // Top 10 most variable genres
        System.out.println("\n=== Top 10 Genres with HIGHEST Variability ===");
        genreStats.orderBy(col("stddev_rating").desc()).show(10, false);

        // Top 10 least variable genres
        System.out.println("\n=== Top 10 Genres with LOWEST Variability ===");
        genreStats.orderBy(col("stddev_rating").asc()).show(10, false);

        // Additional analysis: Genres with high average rating and low variability
        System.out.println("\n=== High Quality Consistent Genres (Avg Rating > 8.0, Low Variability) ===");
        genreStats
                .filter(col("avg_rating").gt(8.0))
                .orderBy(col("stddev_rating").asc())
                .show(20, false);

        // Genres with wide range
        System.out.println("\n=== Genres with Widest Rating Range ===");
        genreStats
                .orderBy(col("rating_range").desc())
                .show(20, false);

        // Correlation analysis: count vs variability
        System.out.println("\n=== Genre Statistics Summary ===");
        genreStats.select("stddev_rating").describe().show();

        // Show distribution by count bins
        System.out.println("\n=== Variability by Genre Popularity ===");
        genreStats
                .withColumn("popularity_tier",
                        when(col("count").geq(100), "High (100+)")
                        .when(col("count").geq(50), "Medium (50-99)")
                        .when(col("count").geq(20), "Low (20-49)")
                        .otherwise("Very Low (5-19)"))
                .groupBy("popularity_tier")
                .agg(
                        count("*").alias("num_genres"),
                        avg("stddev_rating").alias("avg_stddev"),
                        avg("avg_rating").alias("avg_of_avg_rating")
                )
                .orderBy(col("avg_stddev").desc())
                .show(false);

        // Save results
        String outputPath = "output/genre_diversity";
        System.out.println("\nSaving genre diversity analysis to: " + outputPath);
        genreStats
                .write()
                .mode("overwrite")
                .option("header", "true")
                .csv(outputPath);

        System.out.println("\n=== Task 6 Completed Successfully ===");

        spark.stop();
    }
}
