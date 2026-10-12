package retail;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.nio.file.Path;

/**
 * Контракт между DE, QA и AN для датасета Online Retail (UCI).
 * DE реализует load + rows; QA тестирует валидатор и схему; AN собирает запросы через Spark API.
 *
 * Схема, которую возвращает rows():
 *   InvoiceNo    string
 *   StockCode    string
 *   Description  string
 *   Quantity     int
 *   InvoiceDate  timestamp
 *   UnitPrice    decimal(10,2)
 *   CustomerID   string   (nullable)
 *   Country      string
 */
public interface RetailDataset {

    /** Загрузка и валидация CSV. Невалидные строки отбрасываются по правилам, согласованным с QA. */
    void load(Path csv);

    /** Все валидные строки датасета в виде Spark Dataset. */
    Dataset<Row> rows();
}
