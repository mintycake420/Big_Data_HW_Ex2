import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

/**
 * Task 9: Certification Rating Distribution
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
public class Task9_CertificationRatingDistribution {

    public static void main(String[] args) {
        // Create Spark session
        SparkSession spark = SparkSession.builder()
                .appName("Task9: Certification Rating Distribution")
                .master("local[*]")
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

        System.out.println("=== Task 9: Certification Rating Distribution ===\n");

        // Load the IMDB dataset
        String dataPath = "data/IMBD.csv";
        Dataset<Row> df = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .option("escape", "\"")
                .csv(dataPath);

        // Clean the data - handle missing certifications and ratings
        Dataset<Row> cleanedDf = df
                .filter(col("title").isNotNull().and(col("title").notEqual("")))
                .filter(col("rating").isNotNull())  // rating is double, not string
                .withColumn("rating_numeric", col("rating").cast("double"))
                .withColumn("certificate_clean",
                        when(col("certificate").isNull().or(col("certificate").equalTo("")), "Not Rated")
                        .otherwise(col("certificate")));

        System.out.println("Total entries analyzed: " + cleanedDf.count());

        // Step 1 & 2: Group by certification and count
        System.out.println("\n--- Step 1 & 2: Grouping by Certification and Counting ---\n");

        Dataset<Row> certificationCounts = cleanedDf
                .groupBy("certificate_clean")
                .count()
                .orderBy(col("count").desc());

        System.out.println("=== Movie Count by Certification ===");
        certificationCounts.show(50, false);

        long totalCertifications = certificationCounts.count();
        System.out.println("Total unique certification types: " + totalCertifications);

        // Step 3: Calculate average rating for each certification
        System.out.println("\n--- Step 3: Calculating Average Rating by Certification ---\n");

        Dataset<Row> certificationAvgRating = cleanedDf
                .groupBy("certificate_clean")
                .agg(
                        count("*").alias("count"),
                        avg("rating_numeric").alias("avg_rating")
                )
                .orderBy(col("avg_rating").desc());

        System.out.println("=== Average Rating by Certification (Sorted Highest to Lowest) ===");
        certificationAvgRating.show(50, false);

        // Step 4: Identify certification with highest average rating
        System.out.println("\n--- Step 4: Identifying Certification with Highest Average Rating ---\n");

        Row highestRatedCert = certificationAvgRating.first();

        System.out.println("=== HIGHEST AVERAGE RATING CERTIFICATION ===");
        System.out.println("Certification: " + highestRatedCert.getString(0));
        System.out.println("Number of Movies: " + highestRatedCert.getLong(1));
        System.out.println("Average Rating: " + String.format("%.4f", highestRatedCert.getDouble(2)));

        // Additional insights
        System.out.println("\n=== Additional Insights ===\n");

        // Filter certifications with significant sample size (>= 10 movies)
        System.out.println("--- Highest Rated Certifications (with at least 10 movies) ---");
        Dataset<Row> significantCerts = certificationAvgRating
                .filter(col("count").geq(10))
                .orderBy(col("avg_rating").desc());

        significantCerts.show(10, false);

        Row highestSignificantCert = significantCerts.first();
        System.out.println("\nMost reliable highest-rated certification (10+ movies):");
        System.out.println("Certification: " + highestSignificantCert.getString(0));
        System.out.println("Number of Movies: " + highestSignificantCert.getLong(1));
        System.out.println("Average Rating: " + String.format("%.4f", highestSignificantCert.getDouble(2)));

        // Most common certifications
        System.out.println("\n--- Top 10 Most Common Certifications ---");
        certificationAvgRating
                .orderBy(col("count").desc())
                .show(10, false);

        // Save results
        String outputPath = "output/certification_rating_distribution";
        System.out.println("\nSaving certification rating distribution to: " + outputPath);
        certificationAvgRating
                .write()
                .mode("overwrite")
                .option("header", "true")
                .csv(outputPath);

        System.out.println("\n=== Task 9 Completed Successfully ===");

        spark.stop();
    }
}
