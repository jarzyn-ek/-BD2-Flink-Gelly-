import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.operators.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.asm.degree.annotate.directed.VertexInDegree;
import org.apache.flink.graph.asm.degree.annotate.directed.VertexOutDegree;
import org.apache.flink.types.LongValue;

import java.util.List;


public class GellyJava {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple2<String, String>> vertexTuples = env
                .readCsvFile("src/resources/cycle-share-dataset/station.csv")
                .ignoreFirstLine()
                .fieldDelimiter(",")
                .includeFields(true, true, false, false, false, false, false, false, false)
                .types(String.class, String.class);


        MapOperator<Tuple3<String, String, String>, Tuple3<String, String, String>> allEdges = env
                .readCsvFile("src/resources/cycle-share-dataset/trip.csv")
                .ignoreFirstLine()
                .fieldDelimiter(",")
                .includeFields(false, false, false, true, false, false, false, true, true, false, false, false)
                .types(String.class, String.class, String.class)
                .map(
                        new MapFunction<Tuple3<String, String, String>, Tuple3<String, String, String>>() {
                            @Override
                            public Tuple3<String, String, String> map(Tuple3<String, String, String> tupleIn) throws Exception {
                                return new Tuple3<String, String, String>(tupleIn.f1, tupleIn.f2, tupleIn.f0);
                            }
                });

        Graph fullGraph = Graph.fromTupleDataSet(vertexTuples, allEdges, env);

        List<Tuple4<String, String, String, Integer>> edgeTuples = allEdges
                .map(new MapFunction<Tuple3<String, String, String>, Tuple4<String, String, String, Integer>>() {
                    @Override
                    public Tuple4<String, String, String, Integer> map(Tuple3<String, String, String> tupleIn) throws Exception {
                        return new Tuple4<String, String, String, Integer>(tupleIn.f0, tupleIn.f1, tupleIn.f2, new Integer(1));
                    }
                })
                .groupBy(2)
                .aggregate(Aggregations.SUM,3)
                .maxBy(3)
                .collect();

        String bikeId = edgeTuples.get(0).f2;

        for (Tuple4<String, String, String,Integer> edge : edgeTuples) {
            System.out.println("Rower z największą ilością przejazdów: ");
            System.out.println(edge.f0 + " " + edge.f1 + " " + edge.f2 + " " + edge.f3);
        }

        Graph frequentlyUsedBikeGraph = fullGraph.subgraph(
                new FilterFunction<Vertex>() {
                    @Override
                    public boolean filter(Vertex vertex) throws Exception {
                        return true;
                    }
                },
                new FilterFunction<Edge>() {
                    @Override
                    public boolean filter(Edge edge) throws Exception {
                        return edge.f2.equals(bikeId);
                    }
                }
        );


        DataSet<Vertex<String,LongValue>> inDegree = (DataSet<Vertex<String, LongValue>>) frequentlyUsedBikeGraph.run(new VertexInDegree().setIncludeZeroDegreeVertices(true));
        DataSet<Vertex<String,LongValue>> outDegree = (DataSet<Vertex<String, LongValue>>) frequentlyUsedBikeGraph.run(new VertexOutDegree().setIncludeZeroDegreeVertices(true));


        System.out.println("Most frequently used as a start station: " + GellyJava.getMaxMessage(inDegree));
        System.out.println("Most frequently used as an end station: " + GellyJava.getMaxMessage(outDegree));

    }

    public static String getMaxMessage(DataSet<Vertex<String,LongValue>> degreeDataSet) throws Exception {
        LongValue max = new LongValue(0);
        String stationName = "";
        for (Vertex<String, LongValue> v : degreeDataSet.collect()) {
            if (v.f1.compareTo(max) > 0) {
                max = v.f1;
                stationName = v.f0;
            }
        }

        return new String("name: " + stationName + " number of vertices: " + max);
    }


}
