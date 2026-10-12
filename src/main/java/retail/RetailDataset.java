package retail;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.stream.Stream;

/**
 * Контракт между DE, QA и AN для датасета Online Retail (UCI).
 * DE реализует load + rows; QA тестирует валидатор и форму Row; AN собирает запросы через Stream API.
 */
public interface RetailDataset {

    /** Загрузка и валидация CSV. Невалидные строки отбрасываются по правилам, согласованным с QA. */
    void load(Path csv);

    /** Все валидные строки датасета. Каждый вызов возвращает новый Stream. */
    Stream<Row> rows();

    record Row(
            String invoiceNo,
            String stockCode,
            String description,
            int quantity,
            LocalDateTime invoiceDate,
            BigDecimal unitPrice,
            String customerId,
            String country
    ) {
        public BigDecimal lineTotal() {
            return unitPrice.multiply(BigDecimal.valueOf(quantity));
        }

        public boolean isCancellation() {
            return invoiceNo != null && invoiceNo.startsWith("C");
        }
    }
}
