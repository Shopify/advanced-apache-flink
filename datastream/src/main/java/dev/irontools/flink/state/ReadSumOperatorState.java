package dev.irontools.flink.state;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.state.api.OperatorIdentifier;
import org.apache.flink.state.api.SavepointReader;
import org.apache.flink.state.api.functions.KeyedStateReaderFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * Reading state from StreamGroupedReduceOperator
 */
public class ReadSumOperatorState {

    public static class CategorySumState {
        public String category;
        public Double sum;

        public CategorySumState() {}

        public CategorySumState(String category, Double sum) {
            this.category = category;
            this.sum = sum;
        }

        @Override
        public String toString() {
            return "CategorySumState{category='" + category + "', sum=" + sum + "}";
        }
    }

    public static class SumStateReaderFunction
            extends KeyedStateReaderFunction<String, CategorySumState> {

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
        public void readKey(
                String key,
                Context ctx,
                Collector<CategorySumState> out) throws Exception {
            Tuple2<String, Double> value = state.value();
            if (value != null) {
                out.collect(new CategorySumState(value.f0, value.f1));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        String savepointPath = "/tmp/flink-savepoints/generated-savepoint";
        String operatorUid = "category-amount-sum";

        Configuration conf = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        SavepointReader savepoint = SavepointReader.read(
            env,
            savepointPath,
            new HashMapStateBackend()
        );

        savepoint.readKeyedState(
                OperatorIdentifier.forUid(operatorUid),
                new SumStateReaderFunction()
            )
            .print();

        env.execute("Read Sum Operator State");
    }
}
