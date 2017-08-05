package de.haw.bigdata.pageRank;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.EdgeJoinFunction;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.pregel.ComputeFunction;
import org.apache.flink.graph.pregel.MessageCombiner;
import org.apache.flink.graph.pregel.MessageIterator;
import org.apache.flink.types.LongValue;

/**
 * This is an implementation of a simple PageRankVC algorithm, using a vertex-centric iteration.
 * The user can define the damping factor and the maximum number of iterations.
 * If the number of vertices of the input graph is known, it should be provided as a parameter
 * to speed up computation. Otherwise, the algorithm will first execute a job to count the vertices.
 *
 * The implementation assumes that each page has at least one incoming and one outgoing link.
 *
 *
 * Created by marc on 10.07.17.
 * follow: https://github.com/eBay/Flink/blob/master/
 *         flink-libraries/flink-gelly/src/main/java/org/apache/flink/graph/library/PageRank.java
 */
public class PageRankVC<K> implements GraphAlgorithm<K, Double, Double, DataSet<Vertex<K, Double>>> {

  private double beta;
  private int maxIterations;
  private long numberOfVertices;

  /**
   * Creates an instance of the PageRankVC algorithm.
   * If the number of vertices of the input graph is known,
   * use the {@link PageRankVC(double, long, int)} constructor instead.
   *
   * The implementation assumes that each page has at least one incoming and one outgoing link.
   *
   * @param beta the damping factor
   * @param maxIterations the maximum number of iterations
   */
  public PageRankVC(double beta, int maxIterations) {
    this.beta = beta;
    this.maxIterations = maxIterations;
    this.numberOfVertices = 0;
  }

  /**
   * Creates an instance of the PageRankVC algorithm.
   *
   * @param beta the damping factor
   * @param numVertices the number of vertices in the input
   * @param maxIterations the maximum number of iterations
   */
  public PageRankVC(double beta, long numVertices, int maxIterations) {
    this.beta = beta;
    this.maxIterations = maxIterations;
    this.numberOfVertices = numVertices;
  }

  @Override
  public DataSet<Vertex<K, Double>> run(Graph<K, Double, Double> network) throws Exception {

    if(numberOfVertices == 0) {
      numberOfVertices = network.numberOfVertices();
    }

    DataSet<Tuple2<K, LongValue>> vertexOutDegrees = network.outDegrees();

    Graph<K, Double, Double> networkWithWeights = network.joinWithEdgesOnSource(vertexOutDegrees, new InitWeights());

    return networkWithWeights.runVertexCentricIteration(new VertexRankCompute<>(beta,
        numberOfVertices), new RankCombiner<>(), maxIterations).getVertices();
  }

  public static final class VertexRankCompute<K> extends ComputeFunction<K, Double, Double, Double>{

    private static final long serialVersionUID = -589634963507396544L;
    private final double beta;
    private final long numVertices;

    VertexRankCompute(double beta, long numberOfVertices) {
      this.beta = beta;
      this.numVertices = numberOfVertices;
    }

    /*
    Function that updates the rank of a vertex by summing up the partial
    ranks from all incoming messages and then applying the dampening formula.
     */
    @Override
    public void compute(Vertex<K, Double> vertex, MessageIterator<Double> inMessages) throws Exception {

      double rankSum = 0d;
      for (double msg : inMessages) {
        rankSum += msg;
      }

      // apply the dampening factor / random jump
      double newRank = (beta * rankSum) + (1 - beta) / numVertices;
      setNewVertexValue(newRank);

      /*
       * Distributes the rank of a vertex among all target vertices according to
       * the transition probability, which is associated with an edge as the edge
       * value.
       */
      if(getSuperstepNumber() == 1) {
        // initialize vertex ranks
        vertex.setValue(1.0 / numVertices);
      }

      for(Edge<K, Double> edge : getEdges()) {
        sendMessageTo(edge.getTarget(), vertex.getValue() * edge.getValue());
      }
    }
  }

  public static final class RankCombiner<K> extends MessageCombiner<K, Double> {

    private static final long serialVersionUID = -261828209153517058L;

    @Override
    public void combineMessages(MessageIterator<Double> inMessages) throws Exception {

      double rankSum = 0d;
      for(double msg : inMessages) {
        rankSum += msg;
      }

      sendCombinedMessage(rankSum);
    }
  }

  private static final class InitWeights implements EdgeJoinFunction<Double, LongValue> {

    private static final long serialVersionUID = 4132680333364960335L;

    public Double edgeJoin(Double edgeValue, LongValue inputValue) {
      return edgeValue / (double) inputValue.getValue();
    }
  }
}
