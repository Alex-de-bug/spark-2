
package retail;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.*;

public class RetailAnalyticsImpl implements RetailAnalytics {

    @Override
    public Dataset<Row> getTopProducts(
            RetailDataset dataset,
            int limit) {

        return dataset.rows()
                .filter(not(col("invoiceNo").startsWith("C")))
                .filter(col("quantity").gt(0))
                .groupBy("stockCode")
                .agg(
                        sum("quantity").alias("totalQuantity")
                )
                .orderBy(
                        col("totalQuantity").desc(),
                        col("stockCode").asc()
                )
                .limit(limit);
    }

    @Override
    public Dataset<Row> getCustomerStats(
            RetailDataset dataset) {

        // Суммарная стоимость каждого заказа
        Dataset<Row> orders = dataset.rows()
                .filter(col("customerId").isNotNull())
                .filter(not(col("invoiceNo").startsWith("C")))
                .filter(col("quantity").gt(0))
                .filter(col("unitPrice").gt(0))
                .groupBy("customerId", "invoiceNo")
                .agg(
                        sum(
                                col("quantity")
                                        .multiply(col("unitPrice"))
                        ).alias("totalPrice")
                );

        // Итоговая статистика по каждому клиенту
        return orders
                .groupBy("customerId")
                .agg(
                        count("invoiceNo").alias("totalOrders"),
                        sum("totalPrice").alias("totalSpent"),
                        avg("totalPrice").alias("averageCheck")
                )
                .orderBy("customerId");
    }

    @Override
    public void exportToCsv(
            Dataset<Row> result,
            String path) {

        result.write()
                .mode("overwrite")
                .option("header", "true")
                .csv(path);
    }
}
