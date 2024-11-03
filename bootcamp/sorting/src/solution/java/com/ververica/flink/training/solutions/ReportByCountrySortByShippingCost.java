package com.ververica.flink.training.solutions;

import com.ververica.flink.training.provided.ECommerceRecord;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Objects;

public class ReportByCountrySortByShippingCost implements ReportBy {

    @Override
    public ReportByRecord getSortableRecord(ECommerceRecord value) {
        if (value == null) {
            return new ReportByCountrySortByShippingCostRecord();
        } else {
            return new ReportByCountrySortByShippingCostRecord(value.getCountry(), (float) value.getShippingCost());
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

    public static class ReportByCountrySortByShippingCostRecord extends ReportByRecord {

        private String country;
        private float shippingCost;

        public ReportByCountrySortByShippingCostRecord() {}

        public ReportByCountrySortByShippingCostRecord(String country, float shippingCost) {
            this.country = country;
            this.shippingCost = shippingCost;
        }

        public String getCountry() {
            return country;
        }

        public void setCountry(String country) {
            this.country = country;
        }

        public float getShippingCost() {
            return shippingCost;
        }

        public void setShippingCost(float shippingCost) {
            this.shippingCost = shippingCost;
        }

        @Override
        public int compareTo(ReportByRecord o) {
            ReportByCountrySortByShippingCostRecord other = (ReportByCountrySortByShippingCostRecord)o;
            if (country == null) {
                if (other.country == null) {
                    return 0;
                } else {
                    return 1;
                }
            }

            int result = country.compareTo(other.country);
            if (result == 0) {
                result = Float.compare(shippingCost, other.shippingCost);
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
            country = in.readUTF();
            shippingCost = in.readFloat();
        }

        @Override
        public void write(DataOutputStream out) throws IOException {
            super.write(out);
            out.writeUTF(country);
            out.writeFloat(shippingCost);
        }

        @Override
        public int estimateSerializedBytes() {
            return ReportByRecord.getSerializedSize()
                    + Float.BYTES
                    + country.length() // assumes ascii names
                    + 2;    // For length of country name.
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            ReportByCountrySortByShippingCostRecord that = (ReportByCountrySortByShippingCostRecord) o;

            if (Float.compare(that.shippingCost, shippingCost) != 0) return false;
            return Objects.equals(country, that.country);
        }

        @Override
        public int hashCode() {
            int result = country != null ? country.hashCode() : 0;
            result = 31 * result + (shippingCost != +0.0f ? Float.floatToIntBits(shippingCost) : 0);
            return result;
        }
    }
}
