package org.apache.flink.streaming.examples.state;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.java.utils.MultipleParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

/** */
public class StateJob {
    /** */
    public static final class Generator implements SourceFunction<Integer>, CheckpointedFunction {

        private static final long serialVersionUID = -2819385275681175792L;

        private final int numKeys;
        private final int idlenessMs;
        private final int recordsToEmit;

        private volatile int numRecordsEmitted = 0;
        private volatile boolean canceled = false;

        private ListState<Integer> state = null;

        Generator(final int numKeys, final int idlenessMs, final int durationSeconds) {
            this.numKeys = numKeys;
            this.idlenessMs = idlenessMs;

            this.recordsToEmit = ((durationSeconds * 1000) / idlenessMs) * numKeys;
        }

        @Override
        public void run(final SourceContext<Integer> ctx) throws Exception {
            while (numRecordsEmitted < recordsToEmit) {
                synchronized (ctx.getCheckpointLock()) {
                    for (int i = 0; i < numKeys; i++) {
                        ctx.collect(i);
                        numRecordsEmitted++;
                    }
                }
                Thread.sleep(idlenessMs);
            }

            while (!canceled) {
                Thread.sleep(50);
            }
        }

        @Override
        public void cancel() {
            canceled = true;
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            state =
                    context.getOperatorStateStore()
                            .getListState(
                                    new ListStateDescriptor<Integer>(
                                            "state", IntSerializer.INSTANCE));

            for (Integer i : state.get()) {
                numRecordsEmitted += i;
            }
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            state.clear();
            state.add(numRecordsEmitted);
        }
    }

    /** */
    public static class StatefulFunctionWithTime
            extends KeyedProcessFunction<Integer, Integer, Integer> {

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
        public void processElement(Integer value, Context ctx, Collector<Integer> out)
                throws Exception {
            Integer stateValue = state.value();
            if (stateValue == null) {
                stateValue = 0;
            }

            state.update(stateValue + 1);
            updateTimes.add(System.currentTimeMillis());
            out.collect(state.value());
        }
    }

    public static void main(String[] args) throws Exception {
        // Checking input parameters
        final MultipleParameterTool params = MultipleParameterTool.fromArgs(args);

        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);
        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

        env.getCheckpointConfig()
                .enableExternalizedCheckpoints(
                        CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        env.setStateBackend(
                new FsStateBackend(
                        "hdfs://ip-172-31-36-202.ap-northeast-2.compute.internal:9000/flink/checkpoints/state-test",
                        false));

        // get input data
        DataStream<Integer> source = env.addSource(new Generator(1, 1000, 60));
        //        DataStream<Integer> text = env.fromElements(1, 2, 3);

        DataStream<String> counts =
                source.keyBy(value -> value)
                        .process(new StatefulFunctionWithTime())
                        .uid("StatefulFunctionWithTime")
                        .map(value -> "value -> " + value);

        // emit result
        if (params.has("output")) {
            counts.writeAsText(params.get("output"));
        } else {
            counts.print();
        }
        // execute program
        env.execute("Streaming StatefulJob");
    }
}
