package retail;

import org.apache.spark.sql.Dataset;

import java.nio.file.Path;

/**
 * Контракт между DE, QA и AN для датасета Online Retail (UCI).
 *
 * Жизненный цикл:
 *   1. load(csv)  — Spark job читает CSV, валидирует строки по правилам QA,
 *                   отбрасывает невалидные, кэширует результат в состоянии объекта.
 *   2. rows()     — возвращает тот самый закэшированный Dataset. Spark job здесь
 *                   уже не запускается, повторные вызовы дешёвые.
 *
 * DE реализует load + rows (внутри — Encoders.bean(RetailRow.class), .cache()).
 * QA тестирует валидатор и то, что типы полей совпадают с RetailRow.
 * AN собирает запросы через Spark API — типы и имена колонок видны в IDE.
 */
public interface RetailDataset {

    /**
     * Прочитать CSV, провалидировать, отбросить невалидные строки, сохранить в объекте.
     * Повторный вызов с другим файлом заменяет содержимое.
     */
    void load(Path csv);

    /**
     * Валидные строки, загруженные последним вызовом load().
     * Бросает IllegalStateException, если load() не вызывался.
     */
    Dataset<RetailRow> rows();
}
