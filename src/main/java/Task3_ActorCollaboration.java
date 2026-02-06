import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import scala.collection.mutable.WrappedArray;
import java.util.ArrayList;
import java.util.List;
import static org.apache.spark.sql.functions.*;

/**
 * Task 3: Actor Collaboration Network
 *
 * Objective: Analyze actor collaborations by creating a co-occurrence network.
 *
 * Steps:
 * 1. Parse the stars column to extract individual actor names
 * 2. Generate pairwise combinations of actors for each movie
 * 3. Count the number of collaborations for each pair
 */
public class Task3_ActorCollaboration {

    public static void main(String[] args) {
        // Create Spark session
        SparkSession spark = SparkSession.builder()
                .appName("Task3: Actor Collaboration Network")
                .master("local[*]")
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

        System.out.println("=== Task 3: Actor Collaboration Network ===\n");

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
                .filter(col("stars").isNotNull().and(col("stars").notEqual("")));

        System.out.println("Total entries with valid stars: " + cleanedDf.count());

        // Step 1: Parse the stars column to extract individual actor names
        System.out.println("\n--- Step 1: Extracting Actor Names ---");

        // The stars column is in format: "['Name1, ', 'Name2, ', ...]"
        // We need to clean it and extract individual names
        Dataset<Row> actorsDf = cleanedDf
                .withColumn("stars_clean",
                        regexp_replace(col("stars"), "[\\[\\]'\"]", ""))
                .withColumn("actor_list",
                        split(col("stars_clean"), ",\\s*"))
                .select(col("title"), col("actor_list"));

        System.out.println("\nSample of extracted actors:");
        actorsDf.show(5, false);

        // Step 2: Generate pairwise combinations of actors for each movie
        System.out.println("\n--- Step 2: Generating Actor Pairs ---");

        // Register UDF to create actor pairs
        UDF1<WrappedArray<String>, List<String>> createActorPairs = (WrappedArray<String> actors) -> {
            List<String> pairs = new ArrayList<>();
            List<String> cleanActors = new ArrayList<>();

            // Convert Scala array to Java list and filter
            for (int i = 0; i < actors.size(); i++) {
                String actor = actors.apply(i).trim();
                if (!actor.isEmpty() &&
                        !actor.equals("|") &&
                        !actor.equals("Stars:") &&
                        actor.length() > 2) {
                    cleanActors.add(actor);
                }
            }

            // Create all unique pairs
            for (int i = 0; i < cleanActors.size(); i++) {
                for (int j = i + 1; j < cleanActors.size(); j++) {
                    String actor1 = cleanActors.get(i);
                    String actor2 = cleanActors.get(j);

                    // Sort the pair alphabetically to avoid duplicates (A-B and B-A)
                    if (actor1.compareTo(actor2) < 0) {
                        pairs.add(actor1 + " <-> " + actor2);
                    } else {
                        pairs.add(actor2 + " <-> " + actor1);
                    }
                }
            }

            return pairs;
        };

        spark.udf().register("create_actor_pairs", createActorPairs,
                DataTypes.createArrayType(DataTypes.StringType));

        Dataset<Row> actorPairs = actorsDf
                .withColumn("actor_pairs", callUDF("create_actor_pairs", col("actor_list")))
                .withColumn("pair", explode(col("actor_pairs")))
                .select(col("title"), col("pair"));

        System.out.println("\nSample of actor pairs:");
        actorPairs.show(10, false);

        // Step 3: Count the number of collaborations for each pair
        System.out.println("\n--- Step 3: Counting Collaborations ---");

        Dataset<Row> collaborationCounts = actorPairs
                .groupBy("pair")
                .agg(
                        count("*").alias("collaboration_count"),
                        collect_list("title").alias("movies")
                )
                .orderBy(col("collaboration_count").desc());

        System.out.println("\n=== Top 50 Most Frequent Actor Collaborations ===");
        collaborationCounts.show(50, false);

        // Statistics
        System.out.println("\n=== Collaboration Statistics ===");

        long totalPairs = collaborationCounts.count();
        System.out.println("Total unique actor pairs: " + totalPairs);

        System.out.println("\nCollaboration count distribution:");
        collaborationCounts.select("collaboration_count")
                .describe()
                .show();

        // Actors with most collaborations
        System.out.println("\n--- Actors with Most Collaborations ---");

        // Split pairs and count per actor
        Dataset<Row> actorCollabCount = actorPairs
                .withColumn("pair_split", split(col("pair"), " <-> "))
                .withColumn("actor", explode(col("pair_split")))
                .groupBy("actor")
                .agg(
                        countDistinct("pair").alias("unique_collaborations"),
                        countDistinct("title").alias("total_movies")
                )
                .orderBy(col("unique_collaborations").desc());

        System.out.println("\nTop 30 actors by number of unique collaborations:");
        actorCollabCount.show(30, false);

        // Actors who collaborated most frequently (same pair)
        System.out.println("\n--- Most Frequent Collaborations (Same Pair) ---");
        Dataset<Row> frequentPairs = collaborationCounts
                .filter(col("collaboration_count").geq(3))
                .orderBy(col("collaboration_count").desc());

        System.out.println("\nActor pairs who worked together 3+ times:");
        frequentPairs.select("pair", "collaboration_count", "movies").show(20, false);

        // Save results
        String outputPath1 = "output/actor_collaborations";
        System.out.println("\nSaving collaboration network to: " + outputPath1);
        collaborationCounts
                .write()
                .mode("overwrite")
                .option("header", "true")
                .json(outputPath1);

        String outputPath2 = "output/actor_collaboration_stats";
        System.out.println("Saving actor statistics to: " + outputPath2);
        actorCollabCount
                .write()
                .mode("overwrite")
                .option("header", "true")
                .csv(outputPath2);

        System.out.println("\n=== Task 3 Completed Successfully ===");

        spark.stop();
    }
}
