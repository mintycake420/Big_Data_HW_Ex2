import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

/**
 * Task 7: Certification Rating Distribution
 *
 * Objective: Analyze the distribution of movies across different certification ratings.
 *            Identify which certification has the highest average rating.
 *
 * Steps:
 * 1. Group movies by certification
 * 2. Count the number of movies in each certification category
 * 3. Calculate the average rating for each certification type
 * 4. Identify the certification with the highest average rating
 */
public class Task7_CertificationDistribution {

    public static void main(String[] args) {
        // Create Spark session
        SparkSession spark = SparkSession.builder()
                .appName("Task7: Certification Rating Distribution")
                .master("local[*]")
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

        System.out.println("=== Task 7: Certification Rating Distribution ===\n");

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
                .withColumn("rating_numeric", col("rating").cast("double"))
                .withColumn("votes_numeric", regexp_replace(col("votes"), ",", "").cast("long"))
                .withColumn("certificate_clean",
                        when(col("certificate").isNull().or(col("certificate").equalTo("")), "Not Rated")
                        .otherwise(col("certificate")));

        System.out.println("Total entries analyzed: " + cleanedDf.count());

        // Step 1 & 2: Group by certification and count
        System.out.println("\n--- Step 1 & 2: Grouping and Counting by Certification ---");

        Dataset<Row> certificationCounts = cleanedDf
                .groupBy("certificate_clean")
                .count()
                .orderBy(col("count").desc());

        System.out.println("\n=== Movie Count by Certification ===");
        certificationCounts.show(50, false);

        long totalCertifications = certificationCounts.count();
        System.out.println("\nTotal unique certification types: " + totalCertifications);

        // Step 3: Calculate average rating for each certification
        System.out.println("\n--- Step 3: Calculating Average Rating by Certification ---");

        Dataset<Row> certificationStats = cleanedDf
                .groupBy("certificate_clean")
                .agg(
                        count("*").alias("count"),
                        avg("rating_numeric").alias("avg_rating"),
                        stddev("rating_numeric").alias("stddev_rating"),
                        min("rating_numeric").alias("min_rating"),
                        max("rating_numeric").alias("max_rating"),
                        avg("votes_numeric").alias("avg_votes"),
                        sum("votes_numeric").alias("total_votes")
                )
                .orderBy(col("avg_rating").desc());

        System.out.println("\n=== Certification Statistics (Sorted by Average Rating) ===");
        certificationStats.show(50, false);

        // Step 4: Identify certification with highest average rating
        System.out.println("\n--- Step 4: Identifying Highest Average Rating Certification ---");

        Row bestCertification = certificationStats.first();

        System.out.println("\n=== Certification with HIGHEST Average Rating ===");
        System.out.println("Certification: " + bestCertification.getString(0));
        System.out.println("Count: " + bestCertification.getLong(1));
        System.out.println("Average Rating: " + String.format("%.4f", bestCertification.getDouble(2)));
        Object stddevObj = bestCertification.get(3);
        if (stddevObj != null) {
            System.out.println("Std Dev Rating: " + String.format("%.4f", (Double)stddevObj));
        } else {
            System.out.println("Std Dev Rating: N/A (single entry)");
        }
        System.out.println("Min Rating: " + bestCertification.getDouble(4));
        System.out.println("Max Rating: " + bestCertification.getDouble(5));
        System.out.println("Average Votes: " + String.format("%.0f", bestCertification.getDouble(6)));

        // Additional analysis
        System.out.println("\n=== Additional Analysis ===");

        // Most common certifications
        System.out.println("\n--- Top 10 Most Common Certifications ---");
        certificationStats
                .orderBy(col("count").desc())
                .show(10, false);

        // Most popular certifications (by total votes)
        System.out.println("\n--- Top 10 Most Popular Certifications (by total votes) ---");
        certificationStats
                .orderBy(col("total_votes").desc())
                .show(10, false);

        // High-rated certifications with significant sample size (>10 titles)
        System.out.println("\n--- High-Rated Certifications (Count > 10) ---");
        certificationStats
                .filter(col("count").gt(10))
                .orderBy(col("avg_rating").desc())
                .show(20, false);

        // Distribution of movies by certification type
        System.out.println("\n=== Certification Type Distribution ===");

        // Categorize certifications
        Dataset<Row> certificationCategories = cleanedDf
                .withColumn("cert_category",
                        when(col("certificate_clean").isin("G", "PG", "PG-13"), "Family Friendly")
                        .when(col("certificate_clean").isin("R", "NC-17"), "Adult")
                        .when(col("certificate_clean").like("TV-%"), "TV Rating")
                        .when(col("certificate_clean").equalTo("Not Rated"), "Not Rated")
                        .otherwise("Other"))
                .groupBy("cert_category")
                .agg(
                        count("*").alias("count"),
                        avg("rating_numeric").alias("avg_rating")
                )
                .orderBy(col("count").desc());

        certificationCategories.show(false);

        // Compare TV vs Movie certifications
        System.out.println("\n--- TV Certifications vs Movie Certifications ---");
        cleanedDf
                .withColumn("is_tv_cert", col("certificate_clean").like("TV-%"))
                .groupBy("is_tv_cert")
                .agg(
                        count("*").alias("count"),
                        avg("rating_numeric").alias("avg_rating"),
                        avg("votes_numeric").alias("avg_votes")
                )
                .show(false);

        // Save results
        String outputPath = "output/certification_distribution";
        System.out.println("\nSaving certification distribution to: " + outputPath);
        certificationStats
                .write()
                .mode("overwrite")
                .option("header", "true")
                .csv(outputPath);

        System.out.println("\n=== Task 7 Completed Successfully ===");

        spark.stop();
    }
}
