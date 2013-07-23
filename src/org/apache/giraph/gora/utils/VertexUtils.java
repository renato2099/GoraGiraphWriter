/**
 * 
 */
package org.apache.giraph.gora.utils;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import org.apache.avro.util.Utf8;
import org.apache.giraph.gora.generated.Edge;
import org.apache.giraph.gora.generated.Vertex;

/**
 * @author renatomarroquin
 *
 */
public class VertexUtils {
  public static Map<String, Vertex> generateGraph(int pVerticesCount){
    // Adding a new user
    Map<String, Vertex> genGraph = new HashMap<String, Vertex>();
    int iCntt = 0;
    while (iCntt < pVerticesCount) {
      Vertex vrtx = createVertex(genGraph);
      genGraph.put(vrtx.getVertexId().toString(), vrtx);
      iCntt ++;
    }
    return genGraph;
  }

  private static Vertex createVertex(Map<String, Vertex> pGraph){
      Vertex vrtx = new Vertex();
      vrtx.setVertexId(new Utf8(UUID.randomUUID().toString()));
      Random randomGenerator = new Random();
      // add at most number of vertices created
      int numEdges = pGraph.size()>0?randomGenerator.nextInt(pGraph.size()):0;
      for (int iCnt = 0; iCnt < numEdges; iCnt ++){
        // select a specific vertex
        Edge tmpEdg = getValidEdge(vrtx,pGraph);
        vrtx.putToEdges(tmpEdg.getVertexId(), new Utf8(String.valueOf(tmpEdg.getEdgeValue())));
      }
      System.out.println(vrtx.toString());/**/
      return vrtx;
  }

  /**
   * Gets a validated edge to be attached to a specific vertex.
   * @param pVertex
   * @param pGraph
   * @return
   */
  private static Edge getValidEdge(Vertex pVertex, Map<String, Vertex> pGraph){
    Edge edg = new Edge();
    Random randomGenerator = new Random();
    Vertex vrtxEnd = null;
    do{
      vrtxEnd = ((Vertex)pGraph.values().toArray()[(randomGenerator.nextInt(pGraph.size()-1))]);
    }while (!validateEdge(pVertex, vrtxEnd));
    Utf8 sinkVrtx = vrtxEnd.getVertexId();
    edg.setVertexId(new Utf8(sinkVrtx.toString()));
    edg.setEdgeValue(randomGenerator.nextFloat());
    return edg;
  }

  /**
   * Validates if an edge is a valid Edge.
   * @param pVertexBegin
   * @param pVertexEnd
   * @return
   */
  private static boolean validateEdge(Vertex pVertexBegin, Vertex pVertexEnd){
    Iterator<Utf8> it = pVertexBegin.getEdges().keySet().iterator();
    while (it.hasNext()){
      Utf8 tmpEdg = it.next();
      if (tmpEdg.equals(pVertexEnd.getVertexId()))
        return false;
    }
    return true;
  }

}
