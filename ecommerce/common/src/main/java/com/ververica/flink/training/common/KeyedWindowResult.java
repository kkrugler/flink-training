package com.ververica.flink.training.common;

public class KeyedWindowResult {

    private String key;
    private long time;
    private long result;

    public KeyedWindowResult() {}

    public KeyedWindowResult(String key, long time, long result) {
        this.key = key;
        this.time = time;
        this.result = result;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    public long getResult() {
        return result;
    }

    public void setResult(long result) {
        this.result = result;
    }

    @Override
    public String toString() {
        return "KeyedWindowResult{" +
                "key='" + key + '\'' +
                ", time=" + time +
                ", result=" + result +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        KeyedWindowResult that = (KeyedWindowResult) o;

        if (time != that.time) return false;
        if (result != that.result) return false;
        return key.equals(that.key);
    }

    @Override
    public int hashCode() {
        int result1 = key.hashCode();
        result1 = 31 * result1 + (int) (time ^ (time >>> 32));
        result1 = 31 * result1 + (int) (result ^ (result >>> 32));
        return result1;
    }
}
