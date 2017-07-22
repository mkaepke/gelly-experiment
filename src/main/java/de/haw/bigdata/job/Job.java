package de.haw.bigdata.job;

import de.haw.bigdata.pageRank.PageRank;
import org.apache.commons.math3.random.JDKRandomGenerator;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.generator.RMatGraph;
import org.apache.flink.graph.generator.random.JDKRandomGeneratorFactory;
import org.apache.flink.graph.generator.random.RandomGenerableFactory;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.NullValue;

/**
 * Created by marc on 10.07.17.
 */
public class Job {

  public static void main(String[] args) throws Exception {

    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    RandomGenerableFactory<JDKRandomGenerator> rnd = new JDKRandomGeneratorFactory();

    int vertexCount = 1 << 10L;
    int edgeCount = 4 * vertexCount;

    Graph<LongValue, NullValue, NullValue> graph = new RMatGraph<>(env, rnd, vertexCount, edgeCount)
        .generate();

    try {

    } catch (Exception e) {
      e.printStackTrace();
    }

    DataSet<Tuple2<Double, Double>> vertexTuple = env
        .readCsvFile("/Users/marc/Downloads/vertices2.csv")
        .ignoreComments("%")
        .fieldDelimiter(", ")
        .types(Double.class, Double.class);

    DataSet<Tuple3<Double, Double, Double>> edgeTuple = env
        .readCsvFile("/Users/marc/Downloads/edges2.csv")
        .ignoreComments("%")
        .fieldDelimiter(",")
        .types(Double.class, Double.class, Double.class);

    Graph<Double, Double, Double> small = Graph.fromTupleDataSet(vertexTuple, edgeTuple, env);

    small.getTriplets().printToErr();

//    Graph<Double, Double, Double> pagerank = small.run(new PageRank<>(0.5, 10));
    Graph<Double, Double, Double> pagerank = small.run(new PageRank<>(0.5, 4, 10));

    pagerank.getTriplets().printToErr();
  }
}
