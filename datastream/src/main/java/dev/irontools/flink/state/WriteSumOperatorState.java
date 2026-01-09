package dev.irontools.flink.state;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.state.api.OperatorIdentifier;
import org.apache.flink.state.api.OperatorTransformation;
import org.apache.flink.state.api.SavepointWriter;
import org.apache.flink.state.api.StateBootstrapTransformation;
import org.apache.flink.state.api.functions.KeyedStateBootstrapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;
import java.util.List;

/**
 * Writing state for StreamGroupedReduceOperator
 */
public class WriteSumOperatorState {

    public static class CategoryAmount {
        public String category;
        public Double amount;

        public CategoryAmount() {}

        public CategoryAmount(String category, Double amount) {
            this.category = category;
            this.amount = amount;
        }

        @Override
        public String toString() {
            return "CategoryAmount{category='" + category + "', amount=" + amount + "}";
        }
    }

    public static class SumStateBootstrapFunction
            extends KeyedStateBootstrapFunction<String, CategoryAmount> {

        private ValueState<Tuple2<String, Double>> state;

        @Override
        public void open(OpenContext openContext) throws Exception {
            ValueStateDescriptor<Tuple2<String, Double>> stateDescriptor =
                new ValueStateDescriptor<>(
                    "_op_state",
                    TypeInformation.of(new TypeHint<Tuple2<String, Double>>() {})
                );
            state = getRuntimeContext().getState(stateDescriptor);
        }

        @Override
        public void processElement(CategoryAmount value, Context ctx) throws Exception {
            state.update(Tuple2.of(value.category, value.amount));
        }
    }

    // Needs JVM args: --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED
    public static void main(String[] args) throws Exception {
        String savepointPath = "/tmp/flink-savepoints/generated-savepoint";
        String operatorUid = "category-amount-sum";
        int maxParallelism = 128;

        Configuration conf = new Configuration();
        conf.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.BATCH);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        List<CategoryAmount> categories = Arrays.asList(
            new CategoryAmount("Sports", 77777.77),
            new CategoryAmount("Books", 77777.77),
            new CategoryAmount("Home & Garden", 77777.77),
            new CategoryAmount("Electronics", 77777.77),
            new CategoryAmount("Clothing", 77777.77)
        );

        DataStream<CategoryAmount> categoryStream = env.fromCollection(categories);

        StateBootstrapTransformation<CategoryAmount> transformation = OperatorTransformation
            .bootstrapWith(categoryStream)
            .keyBy(ca -> ca.category)
            .transform(new SumStateBootstrapFunction());

        SavepointWriter
            .newSavepoint(env, new HashMapStateBackend(), maxParallelism)
            .withOperator(OperatorIdentifier.forUid(operatorUid), transformation)
            .write(savepointPath);

        env.execute("Write Sum Operator State");
    }
}
