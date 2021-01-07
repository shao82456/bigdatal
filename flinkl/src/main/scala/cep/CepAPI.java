package cep;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Author: shaoff
 * Date: 2020/7/9 19:33
 * Package: cep
 * Description:
 */
public class CepAPI {

    public static void main(String[] args) throws Exception {
        testSimplePatternWithSingleState();
    }

    public static void testSimplePatternWithSingleState() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Tuple2<Integer, Integer>> input = env.fromElements(
                new Tuple2<>(0, 0),
                new Tuple2<>(0, 4),
                new Tuple2<>(0, 1),
                new Tuple2<>(0, 2),
                new Tuple2<>(0, 3),
                new Tuple2<>(0, 5),
                new Tuple2<>(0, 4),
                new Tuple2<>(0, 1),
                new Tuple2<>(0, 3));
        Pattern<Tuple2<Integer, Integer>, ?> pattern =
                Pattern.<Tuple2<Integer, Integer>>begin("start").where(
                        new SimpleCondition<Tuple2<Integer, Integer>>() {
                            @Override
                            public boolean filter(Tuple2<Integer, Integer> rec) throws Exception {
                                return rec.f1 == 4;
                            }
                        }).next("next").where(
                        new SimpleCondition<Tuple2<Integer, Integer>>() {
                            @Override
                            public boolean filter(Tuple2<Integer, Integer> rec) throws Exception {
                                return rec.f1 == 1;
                            }
                        }).followedBy("end").where(
                        new SimpleCondition<Tuple2<Integer, Integer>>() {
                            @Override
                            public boolean filter(Tuple2<Integer, Integer> rec) throws Exception {
                                return rec.f1 == 3;
                            }
                        });

        PatternStream<Tuple2<Integer, Integer>> pStream = CEP.pattern(input, pattern);
        DataStream<Tuple2<Integer, Integer>> result = pStream.select(new PatternSelectFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>() {
            @Override
            public Tuple2<Integer, Integer> select(Map<String, List<Tuple2<Integer, Integer>>> pattern) throws Exception {
                System.out.println(pattern);
                List<Tuple2<Integer, Integer>> res = pattern.get("start");
//                System.out.println(res);
                return res.get(0);
            }
        });
        List<Tuple2<Integer, Integer>> resultList = new ArrayList<>();
        DataStreamUtils.collect(result).forEachRemaining(resultList::add);
        assertEquals(Arrays.asList(new Tuple2<>(0, 1)), resultList);
    }

    private static void assertEquals(List<Tuple2<Integer, Integer>> asList, List<Tuple2<Integer, Integer>> resultList) {

        if (!asList.equals(resultList)) {
            throw new RuntimeException("assert failed");
        }
    }
}
