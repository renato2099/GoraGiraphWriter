/**
 * 
 */
package org.apache.giraph.gora.utils;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;

import org.apache.avro.util.Utf8;
import org.apache.giraph.io.gora.generated.Edge;
import org.apache.giraph.io.gora.generated.GVertex;

/**
 * @author renatomarroquin
 *
 */
public class VertexUtils {
  public static Map<String, GVertex> generateGraph(int pVerticesCount){
    // Adding a new user
    Map<String, GVertex> genGraph = new HashMap<String, GVertex>();
    int iCntt = 0;
    while (iCntt < pVerticesCount) {
      GVertex vrtx = createVertex(genGraph, pVerticesCount);
      genGraph.put(vrtx.getVertexId().toString(), vrtx);
      iCntt ++;
      System.out.println("GVertex number = " + iCntt);
    }
    return genGraph;
  }

  private static String getValidVertexID(Map<String, GVertex> pGraph, int pMaxVertices){
    int high = 10, low = 0;
    String vertexId = String.valueOf(low);
    Random randomGenerator = new Random();
    if (pGraph.size() != 0) {
      while (pGraph.containsKey(vertexId)) {
        vertexId = String.valueOf(randomGenerator.nextInt(high-low)+low);
      }
    }
    else {
      vertexId = "1";
    }
    System.out.println("VertexID escogido: " + vertexId);
    return vertexId;
  }

  private static GVertex createVertex(Map<String, GVertex> pGraph, int pVerticesCount){
    Random randomGenerator = new Random();
    GVertex vrtx = new GVertex();
    vrtx.setVertexId(new Utf8(getValidVertexID(pGraph, pVerticesCount)));
    // add at most number of vertices created
    int maxEdges = (int) (pGraph.size());//*0.10);
    int numEdges = maxEdges>0?randomGenerator.nextInt(maxEdges):0;
    for (int iCnt = 0; iCnt < numEdges; iCnt ++){
      // select a specific vertex
      Edge tmpEdg = getValidEdge(vrtx,pGraph);
      vrtx.putToEdges(tmpEdg.getVertexId(), new Utf8(String.valueOf(tmpEdg.getEdgeValue())));
    }
    /*System.out.println(vrtx.toString());*/
    return vrtx;
  }

  /**
   * Gets a validated edge to be attached to a specific vertex.
   * @param pVertex
   * @param pGraph
   * @return
   */
  private static Edge getValidEdge(GVertex pVertex, Map<String, GVertex> pGraph){
    Edge edg = new Edge();
    Random randomGenerator = new Random();
    GVertex vrtxEnd = null;
    do{
      vrtxEnd = ((GVertex)pGraph.values().toArray()[(randomGenerator.nextInt(pGraph.size()-1))]);
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
  private static boolean validateEdge(GVertex pVertexBegin, GVertex pVertexEnd){
    Iterator<Utf8> it = pVertexBegin.getEdges().keySet().iterator();
    while (it.hasNext()){
      Utf8 tmpEdg = it.next();
      if (tmpEdg.equals(pVertexEnd.getVertexId()))
        return false;
    }
    return true;
  }

}
