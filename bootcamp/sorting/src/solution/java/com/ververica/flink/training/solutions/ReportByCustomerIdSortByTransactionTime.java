package com.ververica.flink.training.solutions;

import com.ververica.flink.training.provided.ECommerceRecord;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Objects;

public class ReportByCustomerIdSortByTransactionTime implements ReportBy {

    @Override
    public ReportByRecord getSortableRecord(ECommerceRecord value) {
        if (value == null) {
            return new ReportByCustomerIdSortByTransactionTimeRecord();
        } else {
            return new ReportByCustomerIdSortByTransactionTimeRecord(value.getCustomerId(), value.getTransactionTime());
        }
    }

    @Override
    public int compare(ECommerceRecord o1, ECommerceRecord o2) {
        int result = o1.getCountry().compareTo(o2.getCountry());
        if (result == 0) {
            result = Double.compare(o1.getShippingCost(), o2.getShippingCost());
        }

        return result;
    }

    public static class ReportByCustomerIdSortByTransactionTimeRecord extends ReportByRecord {

        private String customerId;
        private long transactionTime;

        public ReportByCustomerIdSortByTransactionTimeRecord() {}

        public ReportByCustomerIdSortByTransactionTimeRecord(String customerId, long transactionTime) {
            this.customerId = customerId;
            this.transactionTime = transactionTime;
        }

        public String getCustomerId() {
            return customerId;
        }

        public void setCustomerId(String customerId) {
            this.customerId = customerId;
        }

        public long getTransactionTime() {
            return transactionTime;
        }

        public void setTransactionTime(long transactionTime) {
            this.transactionTime = transactionTime;
        }

        @Override
        public int compareTo(ReportByRecord o) {
            ReportByCustomerIdSortByTransactionTimeRecord other = (ReportByCustomerIdSortByTransactionTimeRecord)o;
            if (customerId == null) {
                if (other.customerId == null) {
                    return 0;
                } else {
                    return 1;
                }
            }

            int result = customerId.compareTo(other.customerId);
            if (result == 0) {
                result = Long.compare(transactionTime, other.transactionTime);
            }

            if (result == 0) {
                result = Long.compare(getOffset(), o.getOffset());
            }

            return result;
        }

        @Override
        public int compare(ReportByRecord o1, ReportByRecord o2) {
            return o1.compareTo(o2);
        }

        @Override
        public void read(DataInputStream in) throws IOException {
            super.read(in);
            customerId = in.readUTF();
            transactionTime = in.readLong();
        }

        @Override
        public void write(DataOutputStream out) throws IOException {
            super.write(out);
            out.writeUTF(customerId);
            out.writeLong(transactionTime);
        }

        @Override
        public int estimateSerializedBytes() {
            return ReportByRecord.getSerializedSize()
                    + Long.BYTES
                    + customerId.length() // assumes ascii names
                    + 2;    // For length of customerId.
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            ReportByCustomerIdSortByTransactionTimeRecord that = (ReportByCustomerIdSortByTransactionTimeRecord) o;

            if (transactionTime != that.transactionTime) return false;
            return Objects.equals(customerId, that.customerId);
        }

        @Override
        public int hashCode() {
            int result = customerId != null ? customerId.hashCode() : 0;
            result = 31 * result + (int) (transactionTime ^ (transactionTime >>> 32));
            return result;
        }
    }
}
