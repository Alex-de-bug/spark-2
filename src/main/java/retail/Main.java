package retail;

import org.apache.spark.sql.SparkSession;

import java.nio.file.Path;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.collect_set;
import static org.apache.spark.sql.functions.size;
import static org.apache.spark.sql.functions.sum;

/** Прогон запросов из задания: топ-5 товаров и статистика по клиентам. */
public class Main {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("retail-report")
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
                .groupBy("stockCode", "description")
                .agg(sum("quantity").alias("total"))
                .orderBy(col("total").desc())
                .limit(5)
                .show(false);

        System.out.println("=== customer stats (top-10 by totalSpent) ===");
        dataset.rows()
                .withColumn("lineTotal", col("quantity").multiply(col("unitPrice")))
                .groupBy("customerId")
                .agg(
                        size(collect_set(col("invoiceNo"))).alias("orders"),
                        sum("lineTotal").alias("totalSpent"))
                .withColumn("avgOrderValue", col("totalSpent").divide(col("orders")))
                .orderBy(col("totalSpent").desc())
                .limit(10)
                .show(false);

        spark.stop();
    }
}
