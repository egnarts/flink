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
import org.apache.flink.state.api.ExistingSavepoint;
import org.apache.flink.state.api.Savepoint;
import org.apache.flink.state.api.functions.KeyedStateReaderFunction;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/** */
public class StateApi {
    /** */
    public static class KeyedState {
        public int key;

        public int value;

        public List<Long> times;
    }

    /** */
    public static class ReaderFunction extends KeyedStateReaderFunction<Integer, KeyedState> {

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
        public void readKey(Integer key, Context ctx, Collector<KeyedState> out) throws Exception {

            KeyedState data = new KeyedState();
            data.key = key;
            data.value = state.value();
            data.times =
                    StreamSupport.stream(updateTimes.get().spliterator(), false)
                            .collect(Collectors.toList());

            out.collect(data);
        }
    }

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment bEnv = ExecutionEnvironment.getExecutionEnvironment();
        ExistingSavepoint savepoint =
                Savepoint.load(
                        bEnv,
                        "hdfs://ip-172-31-36-202.ap-northeast-2.compute.internal:9000/flink/checkpoints/90d867e06ddb695dfb920e7041381092/chk-19/_metadata",
                        new MemoryStateBackend());
        DataSet<KeyedState> keyedStateDataSet =
                savepoint.readKeyedState("75f236f423d43ae0113f80f0a7e051a1", new ReaderFunction());

        List<KeyedState> keyedStates = keyedStateDataSet.collect();
        for (KeyedState ks : keyedStates) {
            System.out.println(ks.key + " -> " + ks.value);
        }
        //        savepoint.
        //        ExecutionEnvironment bEnv = ExecutionEnvironment.getExecutionEnvironment();
        //
        //        DataSet<Account> accountDataSet = bEnv.fromCollection(accounts);
        //
        //        Savepoint
        //                .create(new MemoryStateBackend(), maxParallelism)
        //                .withOperator("uid1", transformation1)
        //                .withOperator("uid2", transformation2)
        //                .write(savepointPath);
    }
}
