package retail;

import org.apache.spark.sql.SparkSession;

import java.nio.file.Path;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.not;
import static org.apache.spark.sql.functions.sum;

/** Smoke-test: грузит CSV, печатает размер и топ-5 товаров. */
public class Main {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("retail-smoke")
                .master("local[*]")
                .config("spark.ui.enabled", "false")
                .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");

        String path = args.length > 0 ? args[0] : "/data/sample.csv";
        RetailDataset dataset = new RetailDatasetImpl(spark);
        dataset.load(Path.of(path));

        long total = dataset.rows().count();
        System.out.println("=== valid rows: " + total + " ===");

        System.out.println("=== top-5 products ===");
        dataset.rows()
                .filter(not(col("invoiceNo").startsWith("C")))
                .groupBy("stockCode", "description")
                .agg(sum("quantity").alias("total"))
                .orderBy(col("total").desc())
                .limit(5)
                .show(false);

        spark.stop();
    }
}
