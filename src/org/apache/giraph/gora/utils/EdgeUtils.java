/**
 * 
 */
package org.apache.giraph.gora.utils;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.avro.util.Utf8;
import org.apache.giraph.io.gora.generated.GEdge;

/**
 * @author renatomarroquin
 *
 */
public class EdgeUtils {
  private static int DEFAULT_MAX_EDGE_COUNT = 10;

  public static Map<String, GEdge> generateGraph(int pEdgeCount){
    // Adding a new user
    Map<String, GEdge> genGraph = new HashMap<String, GEdge>();
    int iCntt = 0;
    while (iCntt < pEdgeCount) {
      GEdge edge = createEdge(genGraph, pEdgeCount);
      genGraph.put(edge.getEdgeId().toString(), edge);
      iCntt ++;
      System.out.println("GEdge number = " + iCntt);
    }
    return genGraph;
  }
  
    private static String getValidEdgeID(Map<String, GEdge> pGraph, int pMaxEdge){
    int high = DEFAULT_MAX_EDGE_COUNT, low = 0;
    String edgeId = String.valueOf(low);
    Random randomGenerator = new Random();
    if (pGraph.size() != 0) {
      while (pGraph.containsKey(edgeId)) {
        edgeId = String.valueOf(randomGenerator.nextInt(high-low)+low);
      }
    }
    else {
      edgeId = "1";
    }
    System.out.println("EdgeID escogido: " + edgeId);
    return edgeId;
  }

  private static GEdge createEdge(Map<String, GEdge> pGraph, int pEdgeCount){
    Random randomGenerator = new Random();
    GEdge edge = new GEdge();
    edge.setEdgeId(new Utf8(getValidEdgeID(pGraph, pEdgeCount)));
    int vertex = randomGenerator.nextInt(pEdgeCount);
    edge.setVertexInId(new Utf8(String.valueOf(vertex)));
    vertex = randomGenerator.nextInt(pEdgeCount);
    edge.setVertexOutId(new Utf8(String.valueOf(vertex)));
    /*System.out.println(vrtx.toString());*/
    return edge;
  }
}