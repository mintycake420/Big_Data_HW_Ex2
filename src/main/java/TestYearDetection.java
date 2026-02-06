import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

public class TestYearDetection {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("Test Year Detection")
                .master("local[*]")
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

        Dataset<Row> df = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .option("escape", "\"")
                .csv("data/IMBD.csv");

        System.out.println("=== Testing Year Detection ===\n");

        // Show first 10 years
        df.select("title", "year").show(10, false);

        // Try different detection methods
        Dataset<Row> test1 = df.withColumn("has_dash", col("year").contains("–"));
        Dataset<Row> test2 = df.withColumn("has_hyphen", col("year").contains("-"));
        Dataset<Row> test3 = df.withColumn("has_space_paren", col("year").rlike(".*\\d+\\s+\\)"));

        System.out.println("\n=== Method 1: contains('–') ===");
        test1.select("year", "has_dash").show(10, false);
        System.out.println("Count with en-dash: " + test1.filter(col("has_dash")).count());

        System.out.println("\n=== Method 2: contains('-') ===");
        test2.select("year", "has_hyphen").show(10, false);
        System.out.println("Count with hyphen: " + test2.filter(col("has_hyphen")).count());

        System.out.println("\n=== Method 3: Space before paren ===");
        test3.select("year", "has_space_paren").show(10, false);
        System.out.println("Count with space-paren pattern: " + test3.filter(col("has_space_paren")).count());

        spark.stop();
    }
}
