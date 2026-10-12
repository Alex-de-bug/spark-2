package retail;

import org.apache.spark.sql.Dataset;

import java.nio.file.Path;

/**
 * Контракт между DE, QA и AN для датасета Online Retail (UCI).
 * DE реализует load + rows (внутри — Encoders.bean(RetailRow.class)).
 * QA тестирует валидатор и то, что типы полей совпадают с RetailRow.
 * AN собирает запросы через Spark API — типы и имена колонок видны в IDE.
 */
public interface RetailDataset {

    /** Загрузка и валидация CSV. Невалидные строки отбрасываются по правилам, согласованным с QA. */
    void load(Path csv);

    /** Все валидные строки датасета, типизированные как RetailRow. */
    Dataset<RetailRow> rows();
}
