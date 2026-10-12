package retail;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.collect_set;
import static org.apache.spark.sql.functions.size;
import static org.apache.spark.sql.functions.sum;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Примеры запросов из задания — то, что будет писать AN поверх RetailDataset.
 * Каждый пример одновременно демонстрирует запрос и проверяет его корректность
 * на известных тестовых данных.
 */
class RetailQueryExamplesTest {

    private static SparkSession spark;
    private RetailDatasetImpl dataset;
    @TempDir Path tempDir;

    @BeforeAll
    static void startSpark() {
        spark = SparkSession.builder().master("local[1]")
                .appName("retail-query-examples")
                .config("spark.ui.enabled", "false")
                .config("spark.driver.host", "127.0.0.1")
                .config("spark.driver.bindAddress", "127.0.0.1")
                .config("spark.sql.shuffle.partitions", "1")
                .config("spark.sql.session.timeZone", "UTC")
                .getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");
    }

    @AfterAll
    static void stopSpark() {
        if (spark != null) spark.stop();
    }

    @AfterEach
    void unpersist() {
        if (dataset != null) dataset.rows().unpersist();
    }

    @Test
    void top5MostPopularProductsByQuantity() throws IOException {
        /*
         * Два заказа 10+20 TEA, один 100 SUGAR, один 80 BREAD, один 30 MILK, 15 COFFEE,
         * ещё 40 SUGAR и 50 TEA. Ожидаемый итог:
         *   SUGAR 140, TEA 80, BREAD 80, MILK 30, COFFEE 15
         */
        dataset = loadFixture(
                "536365,10001,TEA,10,12/1/2010 8:26,2.50,17850,UK",
                "536366,10001,TEA,20,12/1/2010 9:00,2.50,17851,UK",
                "536368,10001,TEA,50,12/1/2010 11:00,2.50,17852,UK",
                "536367,10003,SUGAR,100,12/1/2010 10:00,1.00,17853,UK",
                "536373,10003,SUGAR,40,12/1/2010 16:00,1.00,17857,UK",
                "536371,10005,BREAD,80,12/1/2010 14:00,2.00,17855,UK",
                "536369,10004,MILK,30,12/1/2010 12:00,1.50,17854,UK",
                "536370,10002,COFFEE,15,12/1/2010 13:00,3.00,17856,UK"
        );

        List<Row> top5 = dataset.rows()
                .groupBy("stockCode", "description")
                .agg(sum("quantity").alias("total"))
                .orderBy(col("total").desc())
                .limit(5)
                .collectAsList();

        assertEquals(5, top5.size());
        assertEquals("10003", top5.get(0).getAs("stockCode"));
        assertEquals(140L, (long) top5.get(0).getAs("total"));
        assertEquals(80L, (long) top5.get(1).getAs("total")); // TEA или BREAD
        assertEquals(80L, (long) top5.get(2).getAs("total"));
        assertEquals(30L, (long) top5.get(3).getAs("total"));
        assertEquals("10002", top5.get(4).getAs("stockCode"));
        assertEquals(15L, (long) top5.get(4).getAs("total"));
    }

    @Test
    void customerStatsOrdersTotalAvgCheck() throws IOException {
        /*
         * Клиент 17850:
         *   заказ 536365: 10*2.50 + 5*3.00 = 25.00 + 15.00 = 40.00
         *   заказ 536366: 20*2.50 = 50.00
         *   total = 90.00, orders = 2, avg = 45.00
         *
         * Клиент 17851:
         *   заказ 536367: 100*1.00 = 100.00
         *   total = 100.00, orders = 1, avg = 100.00
         */
        dataset = loadFixture(
                "536365,10001,TEA,10,12/1/2010 8:26,2.50,17850,UK",
                "536365,10002,COFFEE,5,12/1/2010 8:26,3.00,17850,UK",
                "536366,10001,TEA,20,12/1/2010 9:00,2.50,17850,UK",
                "536367,10003,SUGAR,100,12/1/2010 10:00,1.00,17851,UK"
        );

        Dataset<Row> stats = dataset.rows()
                .withColumn("lineTotal", col("quantity").multiply(col("unitPrice")))
                .groupBy("customerId")
                .agg(
                        size(collect_set(col("invoiceNo"))).alias("orders"),
                        sum("lineTotal").alias("totalSpent"))
                .withColumn("avgOrderValue", col("totalSpent").divide(col("orders")))
                .orderBy("customerId");

        List<Row> result = stats.collectAsList();
        assertEquals(2, result.size());

        Row c17850 = result.get(0);
        assertEquals("17850", c17850.getAs("customerId"));
        assertEquals(2, (int) c17850.getAs("orders"));
        assertEquals(new BigDecimal("90.00"), c17850.<BigDecimal>getAs("totalSpent").setScale(2, RoundingMode.HALF_UP));
        assertEquals(new BigDecimal("45.00"), c17850.<BigDecimal>getAs("avgOrderValue").setScale(2, RoundingMode.HALF_UP));

        Row c17851 = result.get(1);
        assertEquals("17851", c17851.getAs("customerId"));
        assertEquals(1, (int) c17851.getAs("orders"));
        assertEquals(new BigDecimal("100.00"), c17851.<BigDecimal>getAs("totalSpent").setScale(2, RoundingMode.HALF_UP));
        assertEquals(new BigDecimal("100.00"), c17851.<BigDecimal>getAs("avgOrderValue").setScale(2, RoundingMode.HALF_UP));
    }

    private RetailDatasetImpl loadFixture(String... rows) throws IOException {
        Path csv = tempDir.resolve("fixture-" + System.nanoTime() + ".csv");
        Files.writeString(csv,
                "InvoiceNo,StockCode,Description,Quantity,InvoiceDate,UnitPrice,CustomerID,Country\n"
                        + String.join("\n", rows) + "\n");
        RetailDatasetImpl ds = new RetailDatasetImpl(spark);
        ds.load(csv);
        return ds;
    }
}
