package retail;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Запросы по заданию поверх RetailDataset. Реализует AN.
 */
public interface RetailAnalytics {

    /** Топ-N товаров по сумме проданных единиц. */
    Dataset<Row> getTopProducts(RetailDataset dataset, int limit);

    /** По каждому клиенту: число заказов, суммарная трата, средний чек. */
    Dataset<Row> getCustomerStats(RetailDataset dataset);

    /** Выгрузить результат запроса в CSV. */
    void exportToCsv(Dataset<Row> result, String path);
}
