package retail;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;

import java.nio.file.Path;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.length;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.trim;

/**
 * Реализация RetailDataset.
 *
 * load() запускает Spark job: читает CSV со строгой схемой, валидирует колонки
 * под паспорт UCI Online Retail, удаляет дубликаты, материализует и кэширует.
 * rows() возвращает тот же закэшированный Dataset без повторного чтения файла.
 *
 * Правила валидации:
 *   invoiceNo    not null, начинается с цифры (отбрасывает C* отмены и A* корректировки)
 *   stockCode    строго 5 цифр ^[0-9]{5}$ (паспорт UCI; отбрасывает POST/DOT/M/AMAZONFEE/варианты с буквенным суффиксом)
 *   description  not null, непустой после trim, не "?"
 *   quantity     not null, > 0
 *   invoiceDate  not null, в диапазоне [2010-12-01, 2011-12-09 23:59:59] (паспорт UCI)
 *   unitPrice    not null, > 0
 *   customerId   строго 5 цифр ^[0-9]{5}$ (null и опечатки отбрасываются)
 *   country      not null, непустой
 *
 * Дедупликация после фильтрации:
 *   1. полные дубликаты строк — dropDuplicates()
 *   2. дубликаты по (invoiceNo, stockCode) — одна позиция на пару заказ×товар
 */
public class RetailDatasetImpl implements RetailDataset {

    private static final StructType SCHEMA = new StructType()
            .add("invoiceNo",   DataTypes.StringType,               true)
            .add("stockCode",   DataTypes.StringType,               true)
            .add("description", DataTypes.StringType,               true)
            .add("quantity",    DataTypes.IntegerType,              true)
            .add("invoiceDate", DataTypes.TimestampType,            true)
            .add("unitPrice",   DataTypes.createDecimalType(10, 2), true)
            .add("customerId",  DataTypes.StringType,               true)
            .add("country",     DataTypes.StringType,               true);

    private static final String DATE_MIN = "2010-12-01 00:00:00";
    private static final String DATE_MAX = "2011-12-09 23:59:59";

    private final SparkSession spark;
    private Dataset<RetailRow> cached;

    public RetailDatasetImpl(SparkSession spark) {
        this.spark = spark;
    }

    @Override
    public void load(Path csv) {
        Dataset<Row> raw = spark.read()
                .option("header", "true")
                .option("timestampFormat", "M/d/yyyy H:mm")
                .option("mode", "DROPMALFORMED")
                .schema(SCHEMA)
                .csv(csv.toString());

        Dataset<Row> filtered = raw
                .filter(col("invoiceNo").isNotNull().and(col("invoiceNo").rlike("^[0-9].*")))
                .filter(col("stockCode").rlike("^[0-9]{5}$"))
                .filter(col("description").isNotNull()
                        .and(length(trim(col("description"))).gt(0))
                        .and(trim(col("description")).notEqual("?")))
                .filter(col("quantity").isNotNull().and(col("quantity").gt(0)))
                .filter(col("invoiceDate").isNotNull()
                        .and(col("invoiceDate").between(lit(DATE_MIN).cast(DataTypes.TimestampType),
                                                        lit(DATE_MAX).cast(DataTypes.TimestampType))))
                .filter(col("unitPrice").isNotNull().and(col("unitPrice").gt(0)))
                .filter(col("customerId").rlike("^[0-9]{5}$"))
                .filter(col("country").isNotNull().and(length(trim(col("country"))).gt(0)));

        Dataset<RetailRow> valid = filtered
                .dropDuplicates()
                .dropDuplicates("invoiceNo", "stockCode")
                .as(Encoders.bean(RetailRow.class));

        if (this.cached != null) {
            this.cached.unpersist();
        }
        valid.persist(StorageLevel.MEMORY_AND_DISK());
        valid.count(); // прогоняем Spark job здесь, чтобы невалидное отсеялось в load, а не отложенно в rows()
        this.cached = valid;
    }

    @Override
    public Dataset<RetailRow> rows() {
        if (cached == null) {
            throw new IllegalStateException("load() must be called before rows()");
        }
        return cached;
    }
}
