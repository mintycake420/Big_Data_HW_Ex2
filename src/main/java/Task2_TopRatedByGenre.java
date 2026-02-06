import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import static org.apache.spark.sql.functions.*;

/**
 * Task 2: Top Rated Movies by Genre
 *
 * Objective: Identify the top 10 movies for each genre based on ratings.
 *
 * Steps:
 * 1. Split the genre column into individual genres
 * 2. Group movies by genre and sort them by ratings
 * 3. Extract the top 10 movies for each genre
 */
public class Task2_TopRatedByGenre {

    public static void main(String[] args) {
        // Create Spark session
        SparkSession spark = SparkSession.builder()
                .appName("Task2: Top Rated Movies by Genre")
                .master("local[*]")
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

        System.out.println("=== Task 2: Top Rated Movies by Genre ===\n");

        // Load the IMDB dataset
        String dataPath = "data/IMBD.csv";
        Dataset<Row> df = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .option("escape", "\"")
                .csv(dataPath);

        // Clean the data first
        Dataset<Row> cleanedDf = df
                .filter(col("title").isNotNull().and(col("title").notEqual("")))
                .filter(col("rating").isNotNull())  // rating is double, not string
                .filter(col("genre").isNotNull().and(col("genre").notEqual("")))
                .withColumn("rating_numeric", col("rating").cast("double"));

        System.out.println("Total entries with valid genre and rating: " + cleanedDf.count());

        // Step 1: Split the genre column into individual genres
        System.out.println("\n--- Step 1: Splitting Genres ---");

        // Remove quotes and split by comma
        Dataset<Row> genreSplit = cleanedDf
                .withColumn("genre_clean", regexp_replace(col("genre"), "\"", ""))
                .withColumn("genre_individual", explode(split(col("genre_clean"), ",\\s*")));

        System.out.println("\nSample of genre splitting:");
        genreSplit.select("title", "genre", "genre_individual", "rating_numeric")
                .show(10, false);

        // Step 2 & 3: Group movies by genre, rank them, and get top 10
        System.out.println("\n--- Step 2 & 3: Ranking Movies by Genre ---");

        // Create window specification partitioned by genre and ordered by rating descending
        WindowSpec windowSpec = Window.partitionBy("genre_individual")
                .orderBy(col("rating_numeric").desc(), col("title"));

        // Add rank to each movie within its genre
        Dataset<Row> rankedByGenre = genreSplit
                .withColumn("rank", row_number().over(windowSpec))
                .filter(col("rank").leq(10));

        // Get unique genres
        System.out.println("\nUnique genres found:");
        Dataset<Row> uniqueGenres = rankedByGenre.select("genre_individual")
                .distinct()
                .orderBy("genre_individual");
        uniqueGenres.show(100, false);

        long genreCount = uniqueGenres.count();
        System.out.println("\nTotal unique genres: " + genreCount);

        // Display top 10 movies for each genre
        System.out.println("\n=== Top 10 Movies by Genre ===\n");

        // Get list of genres to iterate
        java.util.List<Row> genres = uniqueGenres.collectAsList();

        for (Row genreRow : genres) {
            String genre = genreRow.getString(0);
            System.out.println("\n========================================");
            System.out.println("Genre: " + genre);
            System.out.println("========================================");

            rankedByGenre
                    .filter(col("genre_individual").equalTo(genre))
                    .select(
                            col("rank"),
                            col("title"),
                            col("rating_numeric").alias("rating"),
                            col("year")
                    )
                    .orderBy("rank")
                    .show(10, false);
        }

        // Summary statistics
        System.out.println("\n=== Summary Statistics ===");

        // Average rating by genre
        System.out.println("\nAverage Rating by Genre:");
        genreSplit.groupBy("genre_individual")
                .agg(
                        avg("rating_numeric").alias("avg_rating"),
                        count("*").alias("total_titles"),
                        max("rating_numeric").alias("max_rating"),
                        min("rating_numeric").alias("min_rating")
                )
                .orderBy(col("avg_rating").desc())
                .show(50, false);

        // Save results
        String outputPath = "output/top_rated_by_genre";
        System.out.println("\nSaving top rated movies by genre to: " + outputPath);
        rankedByGenre.select(
                        col("genre_individual").alias("genre"),
                        col("rank"),
                        col("title"),
                        col("rating_numeric").alias("rating"),
                        col("year"),
                        col("votes")
                )
                .orderBy("genre_individual", "rank")
                .write()
                .mode("overwrite")
                .option("header", "true")
                .csv(outputPath);

        System.out.println("\n=== Task 2 Completed Successfully ===");

        spark.stop();
    }
}
