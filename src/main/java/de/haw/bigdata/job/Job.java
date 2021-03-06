package de.haw.bigdata.job;

import de.haw.bigdata.pageRank.PageRankGSA;
import de.haw.bigdata.pageRank.PageRankSG;
import de.haw.bigdata.pageRank.PageRankVC;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphCsvReader;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.asm.translate.TranslateFunction;
import org.apache.flink.graph.library.link_analysis.PageRank;
import org.apache.flink.graph.library.link_analysis.PageRank.Result;
import org.apache.flink.types.NullValue;

/**
 * Created by marc on 10.07.17.
 */
public class Job {

  private static DataSet<Vertex<Double, Double>> result = null;
  private static DataSet<Result<Double>> resultPR = null;

  public static void main(String[] args) throws Exception {

    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    ParameterTool params = ParameterTool.fromArgs(args);
    env.setParallelism(params.getInt("p", 3));

    String path = params.getRequired("input");
    String algo = params.getRequired("algo");
    Double factor = params.getDouble("factor", 0.85);
    Integer maxIterations = params.getInt("iter", 10);
    String delimiterEdges;

    switch (params.get("del", "default")) {
      case "tab" : delimiterEdges = "\t"; break;
      case "space" : delimiterEdges = " "; break;
      case "comma" : delimiterEdges = ","; break;
      case "comma-space" : delimiterEdges = ", "; break;

      default : delimiterEdges = ";";
    }

    Graph<Double, Double, Double> graph = new GraphCsvReader(path, env)
        .ignoreCommentsEdges("#")
        .fieldDelimiterEdges(delimiterEdges)
        .keyType(Double.class)
        .translateVertexValues(new ValueMapper())
        .translateEdgeValues(new ValueMapper());

    Long start = System.currentTimeMillis();

    switch (algo) {
      /* vertex centric */
      case "vc" : result = graph.run(new PageRankVC<>(factor, maxIterations)); break;
      /* scatter gather */
      case "sg" : result = graph.run(new PageRankSG<>(factor, maxIterations)); break;
      /* gather sum apply */
      case "gsa" : result = graph.run(new PageRankGSA<>(factor, maxIterations)); break;
      /* without graph model -> bulk iteration */
      case "bi" : resultPR = graph.run(new PageRank<>(factor, maxIterations)); break;

      default : throw new IllegalArgumentException("invalid algo. Choose: vc, sg, gsa or bi");
    }

    env.fromElements(Tuple2.of(System.currentTimeMillis() - start, algo)).printToErr();

    if(result != null) {
      result.first(1).printToErr();
    } else {
      resultPR.first(1).printToErr();
    }
  }


  private static final class ValueMapper implements TranslateFunction<NullValue, Double> {

    private static final long serialVersionUID = -5668422539280882691L;

    @Override
    public Double translate(NullValue value, Double reuse) throws Exception {
      return 1d;
    }
  }
}
