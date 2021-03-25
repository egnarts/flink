package org.apache.flink.streaming.examples.state;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.state.api.BootstrapTransformation;
import org.apache.flink.state.api.OperatorTransformation;
import org.apache.flink.state.api.Savepoint;
import org.apache.flink.state.api.functions.KeyedStateBootstrapFunction;

import org.apache.commons.compress.utils.Lists;

import java.util.Date;
import java.util.List;

/** */
public class StateWriteApi {
    /** */
    public static class KeyedState {
        public int key;

        public int value;

        public List<Long> times;
    }

    /** */
    public static class AccountBootstrapper
            extends KeyedStateBootstrapFunction<Integer, KeyedState> {
        ValueState<Integer> state;

        ListState<Long> updateTimes;

        @Override
        public void open(Configuration parameters) {
            ValueStateDescriptor<Integer> stateDescriptor =
                    new ValueStateDescriptor<>("state", Types.INT);
            state = getRuntimeContext().getState(stateDescriptor);

            ListStateDescriptor<Long> updateDescriptor =
                    new ListStateDescriptor<>("times", Types.LONG);
            updateTimes = getRuntimeContext().getListState(updateDescriptor);
        }

        @Override
        public void processElement(KeyedState value, Context ctx) throws Exception {
            state.update(value.value);
            updateTimes.update(value.times);
        }
    }

    public static void main(String[] args) {
        ExecutionEnvironment bEnv = ExecutionEnvironment.getExecutionEnvironment();
        List<KeyedState> states = Lists.newArrayList();
        KeyedState state = new KeyedState();
        state.key = 0;
        state.value = 30;
        state.times = Lists.newArrayList();
        state.times.add(new Date().getTime());

        states.add(state);
        DataSet<KeyedState> dataSet = bEnv.fromCollection(states);

        BootstrapTransformation<KeyedState> transformation =
                OperatorTransformation.bootstrapWith(dataSet)
                        .keyBy(acc -> acc.key)
                        .transform(new AccountBootstrapper());

        Savepoint.create(new MemoryStateBackend(), 128)
                .withOperator("StatefulFunctionWithTime", transformation)
                .write(
                        "hdfs://ip-172-31-36-202.ap-northeast-2.compute.internal:9000/flink/savepoints/");

        System.out.println("Write");
    }
}
