package retail;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.nio.file.Path;

import static org.apache.spark.sql.functions.col;

/** Прогон запросов из задания через RetailAnalytics + выгрузка в CSV. */
public class Main {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("retail-report")
                .master("local[*]")
                .config("spark.ui.enabled", "false")
                .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");

        String input  = args.length > 0 ? args[0] : "/data/data.csv";
        String outDir = args.length > 1 ? args[1] : "/data/out";

        RetailDataset dataset = new RetailDatasetImpl(spark);
        dataset.load(Path.of(input));

        RetailAnalytics analytics = new RetailAnalyticsImpl();

        System.out.println("=== valid rows: " + dataset.rows().count() + " ===");

        System.out.println("=== top-5 products ===");
        Dataset<Row> top5 = analytics.getTopProducts(dataset, 5);
        top5.show(false);

        System.out.println("=== customer stats (top-10 by totalSpent) ===");
        Dataset<Row> stats = analytics.getCustomerStats(dataset);
        stats.orderBy(col("totalSpent").desc()).show(10, false);

        analytics.exportToCsv(top5.coalesce(1), outDir + "/top5");
        analytics.exportToCsv(stats.coalesce(1), outDir + "/customer-stats");
        System.out.println("=== saved to " + outDir + "/{top5, customer-stats} ===");

        
        spark.stop();
    }
}
