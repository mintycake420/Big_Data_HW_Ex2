import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

/**
 * Task 4: High-Rated Hidden Gems
 *
 * Objective: Identify movies with high ratings but relatively low votes.
 *
 * Steps:
 * 1. Filter movies with ratings greater than 8.0 and votes fewer than 10,000
 * 2. Sort the filtered results by rating and vote count
 */
public class Task4_HighRatedHiddenGems {

    public static void main(String[] args) {
        // Create Spark session
        SparkSession spark = SparkSession.builder()
                .appName("Task4: High-Rated Hidden Gems")
                .master("local[*]")
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

        System.out.println("=== Task 4: High-Rated Hidden Gems ===\n");

        // Load the IMDB dataset
        String dataPath = "data/IMBD.csv";
        Dataset<Row> df = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .option("escape", "\"")
                .csv(dataPath);

        // Clean the data: convert votes to numeric and rating to double
        Dataset<Row> cleanedDf = df
                .filter(col("title").isNotNull().and(col("title").notEqual("")))
                .filter(col("rating").isNotNull())  // rating is double, not string
                .filter(col("votes").isNotNull().and(col("votes").notEqual("")))
                .withColumn("rating_numeric", col("rating").cast("double"))
                .withColumn("votes_numeric", regexp_replace(col("votes"), ",", "").cast("long"));

        System.out.println("Total entries with valid rating and votes: " + cleanedDf.count());

        // Step 1: Filter movies with rating > 8.0 and votes < 10,000
        System.out.println("\n--- Step 1: Filtering Hidden Gems ---");
        System.out.println("Criteria: Rating > 8.0 AND Votes < 10,000\n");

        Dataset<Row> hiddenGems = cleanedDf
                .filter(col("rating_numeric").gt(8.0))
                .filter(col("votes_numeric").lt(10000));

        long hiddenGemsCount = hiddenGems.count();
        System.out.println("Found " + hiddenGemsCount + " hidden gems\n");

        // Step 2: Sort the filtered results by rating (desc) and vote count (asc)
        System.out.println("--- Step 2: Sorting Results ---");

        Dataset<Row> sortedHiddenGems = hiddenGems
                .select(
                        col("title"),
                        col("year"),
                        col("rating_numeric").alias("rating"),
                        col("votes_numeric").alias("votes"),
                        col("genre"),
                        col("certificate"),
                        col("description")
                )
                .orderBy(col("rating").desc(), col("votes").asc());

        System.out.println("\n=== Top 50 High-Rated Hidden Gems ===");
        System.out.println("(Sorted by Rating DESC, then Votes ASC)\n");
        sortedHiddenGems.show(50, false);

        // Additional analysis
        System.out.println("\n=== Hidden Gems Statistics ===");

        System.out.println("\nRating distribution:");
        sortedHiddenGems.select("rating").describe().show();

        System.out.println("\nVotes distribution:");
        sortedHiddenGems.select("votes").describe().show();

        // Group by certificate
        System.out.println("\nHidden gems by certificate:");
        sortedHiddenGems
                .groupBy("certificate")
                .agg(
                        count("*").alias("count"),
                        avg("rating").alias("avg_rating"),
                        avg("votes").alias("avg_votes")
                )
                .orderBy(col("count").desc())
                .show(20, false);

        // Extract genres
        System.out.println("\nHidden gems by genre (top genres):");
        sortedHiddenGems
                .withColumn("genre_clean", regexp_replace(col("genre"), "\"", ""))
                .withColumn("genre_individual", explode(split(col("genre_clean"), ",\\s*")))
                .groupBy("genre_individual")
                .agg(
                        count("*").alias("count"),
                        avg("rating").alias("avg_rating"),
                        min("votes").alias("min_votes"),
                        max("votes").alias("max_votes")
                )
                .orderBy(col("count").desc())
                .show(20, false);

        // Extremely high-rated with very few votes
        System.out.println("\n=== Ultra Hidden Gems (Rating > 9.0, Votes < 1000) ===");
        Dataset<Row> ultraHidden = cleanedDf
                .filter(col("rating_numeric").gt(9.0))
                .filter(col("votes_numeric").lt(1000))
                .select(
                        col("title"),
                        col("year"),
                        col("rating_numeric").alias("rating"),
                        col("votes_numeric").alias("votes"),
                        col("genre"),
                        col("description")
                )
                .orderBy(col("rating").desc(), col("votes").asc());

        long ultraCount = ultraHidden.count();
        System.out.println("\nFound " + ultraCount + " ultra hidden gems\n");
        ultraHidden.show(20, false);

        // Save results
        String outputPath = "output/hidden_gems";
        System.out.println("\nSaving hidden gems to: " + outputPath);
        sortedHiddenGems
                .write()
                .mode("overwrite")
                .option("header", "true")
                .csv(outputPath);

        System.out.println("\n=== Task 4 Completed Successfully ===");

        spark.stop();
    }
}
