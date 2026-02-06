import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import static org.apache.spark.sql.functions.*;

/**
 * Task 5: Word Frequency in Movie Titles
 *
 * Objective: Find the most frequently used words in movie titles, excluding stop words.
 *
 * Steps:
 * 1. Perform a word count on the title column
 * 2. Exclude common stop words using a predefined list
 * 3. Identify the top 20 most frequent words
 */
public class Task5_WordFrequency {

    // Common English stop words
    private static final Set<String> STOP_WORDS = new HashSet<>(Arrays.asList(
            "a", "an", "and", "are", "as", "at", "be", "by", "for", "from",
            "has", "he", "in", "is", "it", "its", "of", "on", "that", "the",
            "to", "was", "will", "with", "the", "this", "but", "they", "have",
            "had", "what", "when", "where", "who", "which", "why", "how",
            "i", "you", "we", "or", "not", "if", "can", "so", "than", "too",
            "very", "just", "there", "their", "about", "out", "up", "then",
            "them", "these", "so", "some", "her", "would", "make", "like",
            "him", "into", "time", "has", "look", "two", "more", "go", "see",
            "number", "no", "way", "could", "people", "my", "than", "first",
            "been", "call", "who", "oil", "its", "now", "find", "long", "down",
            "day", "did", "get", "come", "made", "may", "part", "ii", "iii", "iv"
    ));

    public static void main(String[] args) {
        // Create Spark session
        SparkSession spark = SparkSession.builder()
                .appName("Task5: Word Frequency in Movie Titles")
                .master("local[*]")
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

        System.out.println("=== Task 5: Word Frequency in Movie Titles ===\n");

        // Load the IMDB dataset
        String dataPath = "data/IMBD.csv";
        Dataset<Row> df = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .option("escape", "\"")
                .csv(dataPath);

        // Clean the data
        Dataset<Row> cleanedDf = df
                .filter(col("title").isNotNull().and(col("title").notEqual("")));

        System.out.println("Total titles to analyze: " + cleanedDf.count());

        // Step 1: Perform a word count on the title column
        System.out.println("\n--- Step 1: Extracting Words from Titles ---");

        // Split titles into words, convert to lowercase, and remove punctuation
        Dataset<Row> words = cleanedDf
                .select(col("title"))
                .withColumn("title_lower", lower(col("title")))
                // Remove special characters and keep only letters, numbers, and spaces
                .withColumn("title_clean", regexp_replace(col("title_lower"), "[^a-z0-9\\s]", " "))
                // Split into words
                .withColumn("word", explode(split(col("title_clean"), "\\s+")))
                // Trim whitespace
                .withColumn("word_trimmed", trim(col("word")))
                // Filter out empty strings
                .filter(col("word_trimmed").notEqual(""))
                .select(col("word_trimmed").alias("word"));

        System.out.println("\nSample words extracted:");
        words.show(20, false);

        // Count all words (before filtering stop words)
        System.out.println("\n--- Word Frequency (Before Stop Word Filtering) ---");
        Dataset<Row> allWordCounts = words
                .groupBy("word")
                .count()
                .orderBy(col("count").desc());

        System.out.println("\nTop 30 most frequent words (including stop words):");
        allWordCounts.show(30, false);

        // Step 2 & 3: Exclude stop words and get top 20
        System.out.println("\n--- Step 2 & 3: Filtering Stop Words and Finding Top 20 ---");

        // Convert stop words set to Spark array for filtering
        String[] stopWordsArray = STOP_WORDS.toArray(new String[0]);

        Dataset<Row> filteredWords = words
                .filter(not(col("word").isin((Object[]) stopWordsArray)))
                // Also filter very short words (1-2 characters) which are often not meaningful
                .filter(length(col("word")).gt(2));

        // Count filtered words
        Dataset<Row> wordCounts = filteredWords
                .groupBy("word")
                .count()
                .orderBy(col("count").desc())
                .limit(20);

        System.out.println("\n=== Top 20 Most Frequent Words (Excluding Stop Words) ===\n");
        wordCounts.show(20, false);

        // Additional analysis
        System.out.println("\n=== Additional Word Analysis ===");

        // Total unique words
        long totalUniqueWords = allWordCounts.count();
        long uniqueAfterFiltering = filteredWords.select("word").distinct().count();
        long stopWordsRemoved = totalUniqueWords - uniqueAfterFiltering;

        System.out.println("\nTotal unique words: " + totalUniqueWords);
        System.out.println("Unique words after filtering: " + uniqueAfterFiltering);
        System.out.println("Stop words removed: " + stopWordsRemoved);

        // Word length distribution
        System.out.println("\n--- Word Length Distribution (Top Words) ---");
        wordCounts
                .withColumn("word_length", length(col("word")))
                .select("word", "count", "word_length")
                .show(20, false);

        // Top 50 for broader analysis
        System.out.println("\n=== Top 50 Most Frequent Words ===");
        Dataset<Row> top50 = filteredWords
                .groupBy("word")
                .count()
                .orderBy(col("count").desc())
                .limit(50);

        top50.show(50, false);

        // Save results
        String outputPath = "output/word_frequency";
        System.out.println("\nSaving word frequency analysis to: " + outputPath);
        top50
                .write()
                .mode("overwrite")
                .option("header", "true")
                .csv(outputPath);

        System.out.println("\n=== Task 5 Completed Successfully ===");

        spark.stop();
    }
}
