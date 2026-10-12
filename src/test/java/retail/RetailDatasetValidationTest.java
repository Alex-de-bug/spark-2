package retail;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Проверки правил валидации в RetailDatasetImpl.load — по одной колонке за раз. */
class RetailDatasetValidationTest {

    private static final String HEADER =
            "InvoiceNo,StockCode,Description,Quantity,InvoiceDate,UnitPrice,CustomerID,Country";
    private static final String VALID =
            "536365,12345,TEA,6,12/1/2010 8:26,2.55,17850,United Kingdom";

    private static SparkSession spark;
    private RetailDatasetImpl dataset;
    @TempDir Path tempDir;

    @BeforeAll
    static void startSpark() {
        spark = SparkSession.builder().master("local[1]")
                .appName("retail-validation-tests")
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
    void validRowPasses() throws IOException {
        assertEquals(1, load(VALID).size());
    }

    // invoiceNo
    @ParameterizedTest
    @ValueSource(strings = {"C536365", "A563185", "ABC123", ""})
    void invoiceNoNotStartingWithDigitIsDropped(String bad) throws IOException {
        assertEquals(0, loadWithField(0, bad).size());
    }

    // stockCode — строго 5 цифр
    @ParameterizedTest
    @ValueSource(strings = {"85123A", "1234", "123456", "POST", "DOT", "AMAZONFEE", "15056bl", "gift_0001_20", ""})
    void stockCodeNotFiveDigitsIsDropped(String bad) throws IOException {
        assertEquals(0, loadWithField(1, bad).size());
    }

    @Test
    void stockCodeExactlyFiveDigitsPasses() throws IOException {
        assertEquals(1, loadWithField(1, "12345").size());
    }

    // description
    @ParameterizedTest
    @ValueSource(strings = {"", " ", "?", "   "})
    void descriptionEmptyOrQuestionMarkIsDropped(String bad) throws IOException {
        assertEquals(0, loadWithField(2, bad).size());
    }

    // quantity > 0
    @ParameterizedTest
    @ValueSource(strings = {"0", "-1", "-100"})
    void quantityZeroOrNegativeIsDropped(String bad) throws IOException {
        assertEquals(0, loadWithField(3, bad).size());
    }

    // invoiceDate диапазон
    @ParameterizedTest
    @ValueSource(strings = {"11/30/2010 23:59", "12/10/2011 0:00", "1/1/2009 0:00", "1/1/2012 0:00"})
    void invoiceDateOutsideRangeIsDropped(String bad) throws IOException {
        assertEquals(0, loadWithField(4, bad).size());
    }

    @ParameterizedTest
    @ValueSource(strings = {"12/1/2010 0:00", "12/9/2011 23:59", "6/15/2011 12:00"})
    void invoiceDateInsideRangePasses(String ok) throws IOException {
        assertEquals(1, loadWithField(4, ok).size());
    }

    // unitPrice > 0
    @ParameterizedTest
    @ValueSource(strings = {"0", "0.00", "-0.01", "-10.50"})
    void unitPriceZeroOrNegativeIsDropped(String bad) throws IOException {
        assertEquals(0, loadWithField(5, bad).size());
    }

    // customerId — строго 5 цифр, null запрещён
    @ParameterizedTest
    @ValueSource(strings = {"", "1234", "123456", "12345A", "ABCDE", "abc"})
    void customerIdNotFiveDigitsIsDropped(String bad) throws IOException {
        assertEquals(0, loadWithField(6, bad).size());
    }

    @Test
    void customerIdExactlyFiveDigitsPasses() throws IOException {
        assertEquals(1, loadWithField(6, "99999").size());
    }

    // country
    @ParameterizedTest
    @ValueSource(strings = {"", "   "})
    void countryEmptyIsDropped(String bad) throws IOException {
        assertEquals(0, loadWithField(7, bad).size());
    }

    // дедупликация
    @Test
    void fullDuplicateRowsCollapseToOne() throws IOException {
        List<RetailRow> rows = load(VALID, VALID, VALID);
        assertEquals(1, rows.size());
    }

    @Test
    void duplicateInvoiceAndStockCodeCollapseToOne() throws IOException {
        String a = "536365,12345,TEA,6,12/1/2010 8:26,2.55,17850,United Kingdom";
        String b = "536365,12345,TEA,99,12/1/2010 8:26,2.55,17850,United Kingdom"; // другой quantity
        List<RetailRow> rows = load(a, b);
        assertEquals(1, rows.size());
    }

    // malformed → DROPMALFORMED
    @ParameterizedTest
    @CsvSource({
            "3, abc",           // quantity nan
            "3, 2147483648",    // quantity > Int.MAX
            "5, nope"           // nan цена
    })
    void malformedRequiredFieldIsDropped(int fieldIdx, String bad) throws IOException {
        assertEquals(0, loadWithField(fieldIdx, bad).size());
    }

    @Test
    void rowsThrowsBeforeLoad() {
        RetailDatasetImpl empty = new RetailDatasetImpl(spark);
        assertTrue(assertThrowsIllegalState(() -> empty.rows()));
    }

    private boolean assertThrowsIllegalState(Runnable r) {
        try { r.run(); return false; }
        catch (IllegalStateException e) { return true; }
    }

    private List<RetailRow> loadWithField(int idx, String value) throws IOException {
        String[] f = VALID.split(",", -1);
        f[idx] = value;
        return load(String.join(",", f));
    }

    private List<RetailRow> load(String... rows) throws IOException {
        Path csv = tempDir.resolve("retail-" + System.nanoTime() + ".csv");
        Files.writeString(csv, HEADER + "\n" + String.join("\n", rows) + "\n");
        dataset = new RetailDatasetImpl(spark);
        dataset.load(csv);
        return dataset.rows().collectAsList();
    }
}
