package com.ververica.flink.training.solutions;

import com.ververica.flink.training.common.CartItem;
import com.ververica.flink.training.common.CurrencyConversionRecord;
import com.ververica.flink.training.common.ShoppingCartRecord;
import com.ververica.flink.training.provided.ShoppingCartWithExchangeRate;
import com.ververica.flink.training.provided.TrimmedShoppingCart;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.*;

public class BootcampTablesSolutionWorkflow {

    private StreamExecutionEnvironment env;
    private DataStream<TrimmedShoppingCart> shoppingCartStream;
    private DataStream<CurrencyConversionRecord> exchangeRateStream;
    private Sink<TrimmedShoppingCart> resultsSink;

    public BootcampTablesSolutionWorkflow(StreamExecutionEnvironment env) {
        this.env =env;
    }

    public void build() {
        // Set the "session timezone" to be UTC, so FROM_UNIXTIME generates
        // results in UTC.
        Configuration config = new Configuration();
        config.setString("table.local-time-zone", "UTC");

        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .withConfiguration(config)
                .build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        Schema shoppingCartSchema = Schema.newBuilder()
                .column("transactionId", DataTypes.STRING())
                .column("country", DataTypes.STRING())
                .column("transactionCompleted", DataTypes.BOOLEAN())
                .column("transactionTime", DataTypes.BIGINT())
                .columnByExpression("transactionDT", "FROM_UNIXTIME(transactionTime / 1000)")
                .column("items", DataTypes.ARRAY(DataTypes.of(CartItem.class)))
                .build();
        Table cartTable = tEnv.fromDataStream(shoppingCartStream, shoppingCartSchema);
        tEnv.createTemporaryView("cartTable", cartTable);

        StreamTableEnvironment exchangeRateTableEnv = StreamTableEnvironment.create(env, settings);
        Schema exchangeRateSchema = Schema.newBuilder()
                .column("country", DataTypes.STRING())
                .column("timestamp", DataTypes.BIGINT())
                .columnByExpression("timestampDT", "FROM_UNIXTIME(timestamp / 1000)")
                .column("conversionRate", DataTypes.DOUBLE())
                .build();
        Table currencyTable = tEnv.fromDataStream(exchangeRateStream, exchangeRateSchema);
        tEnv.createTemporaryView("currencyTable", currencyTable);

        Table resultTable = cartTable
                .join(currencyTable)
                .where(
                        and(
                                $("cartTable.country").isEqual($("currencyTable.country")),
                                $("cartTable.timestamp").between(
                                        $("currencyTable.transactionDT"),
                                        $("currencyTable.timestampDT").plus(lit(1).minutes())
                                )
                        )
                )
                .select($("cartTable.*"), $("currencyTable.conversionRate"));

        // Convert back to DataStream, connect to sink
        DataStream<TrimmedShoppingCart> enrichedStream = tEnv.toDataStream(resultTable, ShoppingCartWithExchangeRate.class)
                .map(new AddUSDollarEquivalentFunction());

        enrichedStream.sinkTo(resultsSink);
    }

    private static class AddUSDollarEquivalentFunction implements MapFunction<ShoppingCartWithExchangeRate, TrimmedShoppingCart> {

        @Override
        public TrimmedShoppingCart map(ShoppingCartWithExchangeRate in) throws Exception {
            TrimmedShoppingCart result = new TrimmedShoppingCart(in);

            for (CartItem item : result.getItems()) {
                item.setUsDollarEquivalent(item.getPrice() * in.getExchangeRate());
            }

            return result;
        }
    }
}
