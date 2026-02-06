import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

/**
 * Task 1: Data Cleaning and Preprocessing
 *
 * Objective: Prepare the dataset for analysis by handling missing values and ensuring proper formatting.
 *
 * Steps:
 * 1. Identify and handle missing values by filtering or filling them appropriately
 * 2. Convert the votes column to numeric by removing commas
 * 3. Extract numeric values from the year column for consistency
 */
public class Task1_DataCleaning {

    public static void main(String[] args) {
        // Create Spark session
        SparkSession spark = SparkSession.builder()
                .appName("Task1: Data Cleaning and Preprocessing")
                .master("local[*]")
                .getOrCreate();

        // Set log level to reduce console output
        spark.sparkContext().setLogLevel("WARN");

        System.out.println("=== Task 1: Data Cleaning and Preprocessing ===\n");

        // Load the IMDB dataset
        String dataPath = "data/IMBD.csv";
        Dataset<Row> df = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .option("escape", "\"")
                .csv(dataPath);

        System.out.println("Original dataset schema:");
        df.printSchema();

        System.out.println("\nOriginal dataset count: " + df.count());
        System.out.println("\nSample of original data:");
        df.show(5, false);

        // Step 1: Identify and handle missing values
        System.out.println("\n--- Step 1: Handling Missing Values ---");

        // Count missing values per column
        System.out.println("\nMissing values count per column:");
        for (String col : df.columns()) {
            long nullCount = df.filter(df.col(col).isNull().or(df.col(col).equalTo(""))).count();
            System.out.println(col + ": " + nullCount);
        }

        // Filter out rows where critical columns (title, rating, votes) are missing or empty
        Dataset<Row> cleanedDf = df
                .filter(col("title").isNotNull().and(col("title").notEqual("")))
                .filter(col("rating").isNotNull())
                .filter(col("votes").isNotNull().and(col("votes").notEqual("")));

        // Fill missing certificate values with "Not Rated"
        cleanedDf = cleanedDf.withColumn("certificate",
                when(col("certificate").isNull().or(col("certificate").equalTo("")), "Not Rated")
                .otherwise(col("certificate")));

        System.out.println("\nAfter filtering missing critical values: " + cleanedDf.count());

        // Step 2: Convert votes column to numeric by removing commas
        System.out.println("\n--- Step 2: Converting Votes to Numeric ---");

        cleanedDf = cleanedDf.withColumn("votes_numeric",
                regexp_replace(col("votes"), ",", "").cast("long"));

        System.out.println("\nSample of votes conversion:");
        cleanedDf.select("title", "votes", "votes_numeric").show(5, false);

        // Step 3: Extract numeric values from year column
        System.out.println("\n--- Step 3: Extracting Year Values ---");

        // Extract start year from patterns like "(2018– )" or "(2022)"
        // Also create a flag for TV shows vs movies
        // Note: Data uses EN DASH (–) character U+2013, not regular hyphen
        cleanedDf = cleanedDf
                .withColumn("year_clean", regexp_replace(col("year"), "[(),–—\\-\\s]", ""))
                .withColumn("start_year",
                        regexp_extract(col("year"), "\\((\\d{4})", 1).cast("int"))
                .withColumn("is_tv_show",
                        col("year").rlike(".*\\d{4}\\s*[–—\\-].*"));

        System.out.println("\nSample of year extraction:");
        cleanedDf.select("title", "year", "start_year", "is_tv_show").show(10, false);

        // Step 4: Convert rating to double for proper numeric operations
        cleanedDf = cleanedDf.withColumn("rating_numeric", col("rating").cast("double"));

        // Final cleaned dataset summary
        System.out.println("\n=== Cleaned Dataset Summary ===");
        System.out.println("Final row count: " + cleanedDf.count());
        System.out.println("\nCleaned dataset schema:");
        cleanedDf.printSchema();

        // Show statistics
        System.out.println("\nRating statistics:");
        cleanedDf.select("rating_numeric").describe().show();

        System.out.println("\nVotes statistics:");
        cleanedDf.select("votes_numeric").describe().show();

        System.out.println("\nYear statistics:");
        cleanedDf.select("start_year").describe().show();

        // Distribution of TV shows vs movies
        System.out.println("\nTV Shows vs Movies:");
        cleanedDf.groupBy("is_tv_show")
                .count()
                .withColumnRenamed("count", "total")
                .orderBy(col("is_tv_show").desc())
                .show();

        // Save cleaned dataset for use in other tasks
        String outputPath = "output/cleaned_imdb_data";
        System.out.println("\nSaving cleaned dataset to: " + outputPath);
        cleanedDf.write()
                .mode("overwrite")
                .option("header", "true")
                .csv(outputPath);

        System.out.println("\n=== Task 1 Completed Successfully ===");

        // Stop Spark session
        spark.stop();
    }
}
