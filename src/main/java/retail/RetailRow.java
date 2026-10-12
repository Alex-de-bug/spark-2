package retail;

import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.Timestamp;

/** Одна валидная строка Online Retail. JavaBean для Spark Encoders.bean(). */
public class RetailRow implements Serializable {

    private String invoiceNo;
    private String stockCode;
    private String description;
    private int quantity;
    private Timestamp invoiceDate;
    private BigDecimal unitPrice;
    private String customerId; // nullable
    private String country;

    public RetailRow() {}

    public String getInvoiceNo() { return invoiceNo; }
    public void setInvoiceNo(String v) { this.invoiceNo = v; }

    public String getStockCode() { return stockCode; }
    public void setStockCode(String v) { this.stockCode = v; }

    public String getDescription() { return description; }
    public void setDescription(String v) { this.description = v; }

    public int getQuantity() { return quantity; }
    public void setQuantity(int v) { this.quantity = v; }

    public Timestamp getInvoiceDate() { return invoiceDate; }
    public void setInvoiceDate(Timestamp v) { this.invoiceDate = v; }

    public BigDecimal getUnitPrice() { return unitPrice; }
    public void setUnitPrice(BigDecimal v) { this.unitPrice = v; }

    public String getCustomerId() { return customerId; }
    public void setCustomerId(String v) { this.customerId = v; }

    public String getCountry() { return country; }
    public void setCountry(String v) { this.country = v; }
}
