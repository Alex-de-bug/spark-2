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

/**
 * Реализация RetailDataset.
 *
 * load() запускает Spark job: читает CSV со строгой схемой, парсит дату/цену,
 * отбрасывает невалидные строки, материализует результат (.count()) и кэширует.
 * rows() возвращает тот же закэшированный Dataset без повторного чтения файла.
 *
 * Правила валидации (минимум; QA расширит):
 *   - invoiceNo, stockCode, country не null и не пустые
 *   - quantity, invoiceDate не null (DROPMALFORMED отбросит нечисла и невалидные даты)
 *   - unitPrice не null и >= 0
 *   - customerId может быть null (пропускается в агрегатах уже на стороне AN)
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

        Dataset<RetailRow> valid = raw
                .filter(col("invoiceNo").isNotNull().and(length(col("invoiceNo")).gt(0)))
                .filter(col("stockCode").isNotNull().and(length(col("stockCode")).gt(0)))
                .filter(col("quantity").isNotNull())
                .filter(col("invoiceDate").isNotNull())
                .filter(col("unitPrice").isNotNull().and(col("unitPrice").geq(0)))
                .filter(col("country").isNotNull().and(length(col("country")).gt(0)))
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
