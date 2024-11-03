package com.ververica.flink.training.provided;

import com.ververica.flink.training.common.ShoppingCartRecord;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Objects;

/*
 * A version of ShoppingCartRecord that only has the fields we use.
 */
public class ECommerceRecord {
    private static final String COUNTRY_FOR_END_RECORD = "end_country";

    // Fields we sort on.
    private String country;
    private String paymentMethod;

    // Other fields that we print
    private String transactionId;
    private long transactionTime;
    private String customerId;
    private String shippingAddress;
    private double shippingCost;
    private String couponCode;

    public ECommerceRecord() { }

    public ECommerceRecord(ShoppingCartRecord base) {
        setCountry(base.getCountry());
        setPaymentMethod(base.getPaymentMethod());
        setTransactionId(base.getTransactionId());
        setTransactionTime(base.getTransactionTime());
        setCustomerId(base.getCustomerId());
        setShippingAddress(base.getShippingAddress());
        setShippingCost(base.getShippingCost());
        setCouponCode(base.getCouponCode());
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public String getPaymentMethod() {
        return paymentMethod;
    }

    public void setPaymentMethod(String paymentMethod) {
        this.paymentMethod = paymentMethod;
    }

    public String getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
    }

    public long getTransactionTime() {
        return transactionTime;
    }

    public void setTransactionTime(long transactionTime) {
        this.transactionTime = transactionTime;
    }

    public String getCustomerId() {
        return customerId;
    }

    public void setCustomerId(String customerId) {
        this.customerId = customerId;
    }

    public String getShippingAddress() {
        return shippingAddress;
    }

    public void setShippingAddress(String shippingAddress) {
        this.shippingAddress = shippingAddress;
    }

    public double getShippingCost() {
        return shippingCost;
    }

    public void setShippingCost(double shippingCost) {
        this.shippingCost = shippingCost;
    }

    public String getCouponCode() {
        return couponCode;
    }

    public void setCouponCode(String couponCode) {
        this.couponCode = couponCode;
    }

    public boolean isEndRecord() {
        return country.equals(COUNTRY_FOR_END_RECORD);
    }

    /**
     * @return A special ECommerceRecord that we can identify as
     * an "end" record.
     */
    public static ECommerceRecord makeEndRecord() {
        ECommerceRecord result = new ECommerceRecord();
        result.setCountry(COUNTRY_FOR_END_RECORD);
        return result;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ECommerceRecord that = (ECommerceRecord) o;

        if (transactionTime != that.transactionTime) return false;
        if (Double.compare(that.shippingCost, shippingCost) != 0) return false;
        if (!Objects.equals(country, that.country)) return false;
        if (!Objects.equals(paymentMethod, that.paymentMethod))
            return false;
        if (!Objects.equals(transactionId, that.transactionId))
            return false;
        if (!Objects.equals(customerId, that.customerId)) return false;
        if (!Objects.equals(shippingAddress, that.shippingAddress))
            return false;
        return Objects.equals(couponCode, that.couponCode);
    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        result = country != null ? country.hashCode() : 0;
        result = 31 * result + (paymentMethod != null ? paymentMethod.hashCode() : 0);
        result = 31 * result + (transactionId != null ? transactionId.hashCode() : 0);
        result = 31 * result + (int) (transactionTime ^ (transactionTime >>> 32));
        result = 31 * result + (customerId != null ? customerId.hashCode() : 0);
        result = 31 * result + (shippingAddress != null ? shippingAddress.hashCode() : 0);
        temp = Double.doubleToLongBits(shippingCost);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        result = 31 * result + (couponCode != null ? couponCode.hashCode() : 0);
        return result;
    }

    public void write(DataOutputStream dos) throws IOException {
        boolean isEndRecord = isEndRecord();
        dos.writeBoolean(isEndRecord);
        if (!isEndRecord) {
            dos.writeUTF(country);
            dos.writeUTF(paymentMethod);
            dos.writeUTF(transactionId);
            dos.writeLong(transactionTime);
            dos.writeUTF(customerId);
            dos.writeUTF(shippingAddress);
            dos.writeDouble(shippingCost);
            dos.writeUTF(couponCode);
        }
    }

    public void read(DataInputStream in) throws IOException {
        if (!in.readBoolean()) {
            country = in.readUTF();
            paymentMethod = in.readUTF();
            transactionId = in.readUTF();
            transactionTime = in.readLong();
            customerId = in.readUTF();
            shippingAddress = in.readUTF();
            shippingCost = in.readDouble();
            couponCode = in.readUTF();
        } else {
            country = COUNTRY_FOR_END_RECORD;
        }
    }

    public void read(DataInput in) throws IOException {
        if (!in.readBoolean()) {
            country = in.readUTF();
            paymentMethod = in.readUTF();
            transactionId = in.readUTF();
            transactionTime = in.readLong();
            customerId = in.readUTF();
            shippingAddress = in.readUTF();
            shippingCost = in.readDouble();
            couponCode = in.readUTF();
        } else {
            country = COUNTRY_FOR_END_RECORD;
        }
    }
}
