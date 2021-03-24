package org.apache.flink.streaming.examples.state;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.utils.MultipleParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 *
 */
public class StateApi {
    /**
     *
     */
    public static class StatefulFunctionWithTime extends KeyedProcessFunction<Integer, Integer, Void> {

        ValueState<Integer> state;

        ListState<Long> updateTimes;

        @Override
        public void open(Configuration parameters) {
            ValueStateDescriptor<Integer> stateDescriptor = new ValueStateDescriptor<>("state", Types.INT);
            state = getRuntimeContext().getState(stateDescriptor);

            ListStateDescriptor<Long> updateDescriptor = new ListStateDescriptor<>("times", Types.LONG);
            updateTimes = getRuntimeContext().getListState(updateDescriptor);
        }

        @Override
        public void processElement(Integer value, Context ctx, Collector<Void> out) throws Exception {
            state.update(value + 1);
            updateTimes.add(System.currentTimeMillis());
        }
    }

    public static void main(String[] args) throws Exception {
        // Checking input parameters
        final MultipleParameterTool params = MultipleParameterTool.fromArgs(args);

        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        // get input data
        DataStream<Integer> text = env.fromElements(1, 2, 3);

        DataStream<Void> counts = text.keyBy(value -> value).process(new StatefulFunctionWithTime());

        // emit result
        if (params.has("output")) {
            counts.writeAsText(params.get("output"));
        } else {
            System.out.println("Printing result to stdout. Use --output to specify output path.");
            counts.print();
        }
        // execute program
        env.execute("Streaming WordCount");
    }
}
