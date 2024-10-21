package com.ververica.flink.training.solutions;

import com.ververica.flink.training.common.CartItem;
import com.ververica.flink.training.common.CurrencyConversionRecord;
import com.ververica.flink.training.provided.AddUSDollarEquivalentFunction;
import com.ververica.flink.training.provided.ShoppingCartWithExchangeRate;
import com.ververica.flink.training.provided.TrimmedShoppingCart;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;

import static org.apache.flink.table.api.DataTypes.*;
import static org.apache.flink.table.api.Expressions.*;

/**
 * Use the exchangeRateStream to add the exchangeRate field to TrimmedShoppingCart
 * records, using the Table API
 */
public class BootcampTablesSolutionWorkflow {

    private StreamExecutionEnvironment env;
    private DataStream<TrimmedShoppingCart> shoppingCartStream;
    private DataStream<CurrencyConversionRecord> exchangeRateStream;
    private Sink<TrimmedShoppingCart> resultsSink;

    public BootcampTablesSolutionWorkflow(StreamExecutionEnvironment env) {
        this.env = env;
    }

    public BootcampTablesSolutionWorkflow setCartStream(DataStream<TrimmedShoppingCart> shoppingCartStream) {
        this.shoppingCartStream = shoppingCartStream;
        return this;
    }

    public BootcampTablesSolutionWorkflow setExchangeRateStream(DataStream<CurrencyConversionRecord> exchangeRateStream) {
        this.exchangeRateStream = exchangeRateStream;
        return this;
    }

    public BootcampTablesSolutionWorkflow setResultsSink(Sink<TrimmedShoppingCart> resultsSink) {
        this.resultsSink = resultsSink;
        return this;
    }

    public void build() {
        Preconditions.checkNotNull(shoppingCartStream, "shoppingCartStream must be set");
        Preconditions.checkNotNull(exchangeRateStream, "exchangeRateStream must be set");
        Preconditions.checkNotNull(resultsSink, "resultsSink must be set");

        // Set the "session timezone" to be UTC, so FROM_UNIXTIME generates
        // results in UTC.
        Configuration config = new Configuration();
        config.setString("table.local-time-zone", "UTC");

        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inBatchMode()
                .withConfiguration(config)
                .build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        Schema shoppingCartSchema = Schema.newBuilder()
                .column("transactionId", DataTypes.STRING())
                .column("country", DataTypes.STRING())
                .column("transactionCompleted", DataTypes.BOOLEAN())
                .column("transactionTime", DataTypes.BIGINT())
                // .column("items", ARRAY(itemsRecordType))
                .column("items", DataTypes.ARRAY(DataTypes.of(CartItem.class)))
//                .columnByExpression("event_time", "TO_TIMESTAMP_LTZ(transactionTime, 3)")
//                .watermark("event_time", "event_time - INTERVAL '1' MINUTE")
                .build();
        Table cartTable = tEnv.fromDataStream(shoppingCartStream, shoppingCartSchema);
        tEnv.createTemporaryView("cartTable", cartTable);

        StreamTableEnvironment exchangeRateTableEnv = StreamTableEnvironment.create(env, settings);
        Schema exchangeRateSchema = Schema.newBuilder()
                .column("exchangeRateCountry", DataTypes.STRING())
                .column("exchangeRateTime", DataTypes.BIGINT())
                .columnByExpression("exchange_rate_time", "TO_TIMESTAMP_LTZ(exchangeRateTime, 3)")
                .watermark("exchange_rate_time","exchange_rate_time - INTERVAL '1' MINUTE")
                .column("exchangeRate", DataTypes.DOUBLE())
                .build();
        Table exchangeRateTable = tEnv.fromDataStream(exchangeRateStream, exchangeRateSchema);
        tEnv.createTemporaryView("exchangeRateTable", exchangeRateTable);

        Table resultTable = cartTable
                .join(exchangeRateTable)
                .where(
                        and(
                                $("country").isEqual($("exchangeRateCountry")),
                                $("transactionTime").between(
                                        $("exchangeRateTime"),
                                        $("exchangeRateTime").plus(lit(60000))
                                )
                        )
                )
                .select($("country"), $("transactionId"),
                        $("transactionTime"), $("transactionCompleted"),
                        $("items"), $("exchangeRate"));

        // Convert back to DataStream, connect to sink
//        DataStream<TrimmedShoppingCart> enrichedStream = tEnv.toDataStream(resultTable, ShoppingCartWithExchangeRate.class)
//                .map(new AddUSDollarEquivalentFunction());

        DataStream<TrimmedShoppingCart> enrichedStream = tEnv.toDataStream(cartTable, TrimmedShoppingCart.class);

        enrichedStream.sinkTo(resultsSink);
    }

}
