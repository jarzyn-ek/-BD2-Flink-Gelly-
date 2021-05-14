import org.apache.flink.api.java.ExecutionEnvironment
import org.apache.flink.graph.{Edge, Graph, Vertex}
object GellyExample extends App {

  // 1) Tworzymy środowisko przetwarzania danych
   val env = ExecutionEnvironment.getExecutionEnvironment

  // 2) Tworzymy węzły oraz krawędzie, z których zbudujemy graf
   val vertexDS = env.fromElements(
     new Vertex(0L, ("Alice", 42L)),
     new Vertex(1L, ("Bob", 23L)),
     new Vertex(2L, ("Eve", 84L))
   )
  val edgeDS = env.fromElements(
    new Edge(0L, 1L, "23/01/1987"),
    new Edge(1L, 2L, "12/12/2009")
  )

  // 3) Tworzymy graf z węzłów i krawędzi
  val graph = Graph.fromDataSet(vertexDS, edgeDS, env)

  // 4) Dokonajmy w nim drobnej modyfikacji
  val updatedGraph = graph.addEdges {
    val edges : java.util.List[Edge[Long,String]] =
      new java.util.ArrayList[Edge[Long,String]]()
    edges.add(new Edge(0L, 2L, "27/04/2021"))
    edges
  }
  // 6) Uzyskajmy z grafu kogoś kto zna conajmniej dwie osoby
  val someone = updatedGraph.outDegrees.filter(_.f1.getValue >= 2).collect().get(0)
  println(someone.f0)
}