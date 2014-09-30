/**
 *Licensed to the Apache Software Foundation (ASF) under one
 *or more contributor license agreements.  See the NOTICE file
 *distributed with this work for additional information
 *regarding copyright ownership.  The ASF licenses this file
 *to you under the Apache License, Version 2.0 (the"
 *License"); you may not use this file except in compliance
 *with the License.  You may obtain a copy of the License at
 *
  * http://www.apache.org/licenses/LICENSE-2.0
 * 
 *Unless required by applicable law or agreed to in writing, software
 *distributed under the License is distributed on an "AS IS" BASIS,
 *WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *See the License for the specific language governing permissions and
 *limitations under the License.
 */
package org.apache.giraph.gora.utils;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;

import org.apache.avro.util.Utf8;
import org.apache.giraph.io.gora.generated.GEdge;
import org.apache.giraph.io.gora.generated.GVertex;

/**
 * @author renatomarroquin
 *
 */
public class VertexUtils {
  public static Map<String, GVertex> generateGraph(int pVerticesCount){
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

  public static Map<String, GVertex> createSimpleGraph() {
    Map<String, GVertex> genGraph = new HashMap<String, GVertex>();
    /* Vertex 0 */
    Utf8 id = new Utf8("0");
    GVertex vrtx = new GVertex();
    GVertex.newBuilder().setVertexValue(0).build();
    vrtx.setVertexId(id);
     
    Map<CharSequence, CharSequence> edges = new HashMap<CharSequence, CharSequence>();
    edges.put(new Utf8("1"), new Utf8("1"));
    edges.put(new Utf8("3"), new Utf8("3"));
    vrtx.setEdges(edges);
    genGraph.put(id.toString(), vrtx);

    /* Vertex 1 */
    vrtx = new GVertex();
    id = new Utf8("1");
    vrtx.setVertexId(id);
    edges = new HashMap<CharSequence, CharSequence>();
    edges.put(new Utf8("0"), new Utf8("1"));
    edges.put(new Utf8("2"), new Utf8("2"));
    edges.put(new Utf8("3"), new Utf8("1"));
    vrtx.setEdges(edges);
    genGraph.put(id.toString(), vrtx);

    /* Vertex 2 */
    vrtx = new GVertex();
    id = new Utf8("2");
    vrtx.setVertexId(id);
    edges = new HashMap<CharSequence, CharSequence>();
    edges.put(new Utf8("1"), new Utf8("2"));
    edges.put(new Utf8("4"), new Utf8("4"));
    vrtx.setEdges(edges);
    genGraph.put(id.toString(), vrtx);

    /* Vertex 3 */
    vrtx = new GVertex();
    id = new Utf8("3");
    vrtx.setVertexId(id);
    edges = new HashMap<CharSequence, CharSequence>();
    edges.put(new Utf8("0"), new Utf8("3"));
    edges.put(new Utf8("1"), new Utf8("1"));
    edges.put(new Utf8("4"), new Utf8("4"));
    vrtx.setEdges(edges);
    genGraph.put(id.toString(), vrtx);

    /* Vertex 4 */
    vrtx = new GVertex();
    id = new Utf8("4");
    vrtx.setVertexId(id);
    edges = new HashMap<CharSequence, CharSequence>();
    edges.put(new Utf8("3"), new Utf8("4"));
    edges.put(new Utf8("2"), new Utf8("4"));
    vrtx.setEdges(edges);
    genGraph.put(id.toString(), vrtx);
    
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
      GEdge tmpEdg = getValidEdge(vrtx,pGraph);
      vrtx.getEdges().put(tmpEdg.getVertexInId(), String.valueOf(tmpEdg.getEdgeWeight()));
    }
    /**/System.out.println(vrtx.toString());
    return vrtx;
  }

  /**
   * Gets a validated edge to be attached to a specific vertex.
   * @param pVertex
   * @param pGraph
   * @return
   */
  private static GEdge getValidEdge(GVertex pVertex, Map<String, GVertex> pGraph){
    GEdge edg = new GEdge();
    Random randomGenerator = new Random();
    GVertex vrtxEnd = null;
    do{
      vrtxEnd = ((GVertex)pGraph.values().toArray()[(randomGenerator.nextInt(pGraph.size()-1))]);
    }while (!validateEdge(pVertex, vrtxEnd));
    Utf8 sinkVrtx = new Utf8(vrtxEnd.getVertexId().toString());
    edg.setVertexInId(new Utf8(sinkVrtx.toString()));
    edg.setEdgeWeight(randomGenerator.nextFloat());
    return edg;
  }

  /**
   * Validates if an edge is a valid Edge.
   * @param pVertexBegin
   * @param pVertexEnd
   * @return
   */
  private static boolean validateEdge(GVertex pVertexBegin, GVertex pVertexEnd){
    Iterator<CharSequence> it = pVertexBegin.getEdges().keySet().iterator();
    while (it.hasNext()){
      Utf8 tmpEdg = new Utf8(it.next().toString());
      if (tmpEdg.equals(pVertexEnd.getVertexId()))
        return false;
    }
    return true;
  }

}
