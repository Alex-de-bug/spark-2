package retail;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Timestamp;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class RetailDatasetImplTest {
    private static SparkSession spark;
    private RetailDatasetImpl dataset;
    @TempDir Path tempDir;

    @BeforeAll
    static void startSpark() {
        spark = SparkSession.builder().master("local[1]")
                .appName("RetailDatasetImplTest")
                .config("spark.ui.enabled", "false")
                .config("spark.driver.host", "127.0.0.1")
                .config("spark.driver.bindAddress", "127.0.0.1")
                .config("spark.sql.shuffle.partitions", "1")
                .config("spark.sql.session.timeZone", "UTC")
                .getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");
    }

    @AfterEach
    void releaseDataset() {
        if (dataset != null) dataset.rows().unpersist();
    }

    @AfterAll
    static void stopSpark() {
        if (spark != null) spark.stop();
    }

    @ParameterizedTest
    @CsvSource({"0, ''", "1, ''", "7, ''", "3, ''", "3, abc",
            "3, 2147483648", "3, -2147483649", "4, ''", "4, invalid-date",
            "4, 2/30/2010 8:26", "5, -0.01", "5, ''", "5, abc"})
    void rejectsInvalidRequiredField(int field, String value) throws IOException {
        String[] invalid = validFields();
        invalid[field] = value;
        List<RetailRow> rows = load(String.join(",", invalid), String.join(",", validFields()));
        assertEquals(1, rows.size());
        assertEquals("536365", rows.get(0).getInvoiceNo());
    }

    @ParameterizedTest
    @CsvSource({"0, 0", "-1, 0.75"})
    void acceptsZeroAndNegativeQuantityAndNonnegativePrice(int quantity, String price)
            throws IOException {
        String[] fields = validFields();
        fields[3] = Integer.toString(quantity);
        fields[5] = price;
        List<RetailRow> rows = load(String.join(",", fields));
        assertEquals(1, rows.size());
        assertEquals(quantity, rows.get(0).getQuantity());
        assertEquals(0, new BigDecimal(price).compareTo(rows.get(0).getUnitPrice()));
    }

    @ParameterizedTest
    @CsvSource({"true, false", "false, true", "true, true"})
    void acceptsMissingDescriptionAndCustomerId(boolean missingDescription,
                                               boolean missingCustomerId) throws IOException {
        String[] fields = validFields();
        if (missingDescription) fields[2] = "";
        if (missingCustomerId) fields[6] = "";
        List<RetailRow> rows = load(String.join(",", fields));
        assertEquals(1, rows.size());
        assertEquals(missingDescription ? null : fields[2], rows.get(0).getDescription());
        assertEquals(missingCustomerId ? null : fields[6], rows.get(0).getCustomerId());
    }

    @Test
    void preservesCancellationInvoice() throws IOException {
        String[] fields = validFields();
        fields[0] = "C536365";
        fields[3] = "-6";
        List<RetailRow> rows = load(String.join(",", fields));
        assertEquals(1, rows.size());
        assertEquals("C536365", rows.get(0).getInvoiceNo());
    }

    @Test
    void preservesAllEightValuesAndSchemaTypes() throws IOException {
        List<RetailRow> rows = load(String.join(",", validFields()));
        assertEquals(1, rows.size());
        RetailRow row = rows.get(0);
        assertAll(
                () -> assertEquals("536365", row.getInvoiceNo()),
                () -> assertEquals("85123A", row.getStockCode()),
                () -> assertEquals("WHITE HANGING HEART", row.getDescription()),
                () -> assertEquals(6, row.getQuantity()),
                () -> assertEquals(Timestamp.from(java.time.Instant.parse("2010-12-01T08:26:00Z")),
                        row.getInvoiceDate()),
                () -> assertEquals(new BigDecimal("2.55"), row.getUnitPrice()),
                () -> assertEquals("17850", row.getCustomerId()),
                () -> assertEquals("United Kingdom", row.getCountry()));

        var schema = dataset.rows().schema();
        assertEquals(8, schema.fields().length);
        for (String name : List.of("invoiceNo", "stockCode", "description", "customerId", "country")) {
            assertEquals(DataTypes.StringType, schema.apply(name).dataType(), name);
        }
        assertEquals(DataTypes.IntegerType, schema.apply("quantity").dataType());
        assertEquals(DataTypes.TimestampType, schema.apply("invoiceDate").dataType());
        assertEquals(DataTypes.createDecimalType(10, 2), schema.apply("unitPrice").dataType());
    }

    private List<RetailRow> load(String... lines) throws IOException {
        Path csv = tempDir.resolve("retail.csv");
        Files.writeString(csv,
                "invoiceNo,stockCode,description,quantity,invoiceDate,unitPrice,customerId,country\n"
                        + String.join("\n", lines) + "\n");
        dataset = new RetailDatasetImpl(spark);
        dataset.load(csv);
        return dataset.rows().collectAsList();
    }

    private static String[] validFields() {
        return new String[]{"536365", "85123A", "WHITE HANGING HEART", "6",
                "12/1/2010 8:26", "2.55", "17850", "United Kingdom"};
    }
}
