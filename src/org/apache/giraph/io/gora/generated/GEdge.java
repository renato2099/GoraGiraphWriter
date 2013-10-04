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

package org.apache.giraph.io.gora.generated;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.HashMap;
import org.apache.avro.Protocol;
import org.apache.avro.Schema;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Protocol;
import org.apache.avro.util.Utf8;
import org.apache.avro.ipc.AvroRemoteException;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.specific.FixedSize;
import org.apache.avro.specific.SpecificExceptionBase;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.avro.specific.SpecificRecord;
import org.apache.avro.specific.SpecificFixed;
import org.apache.gora.persistency.StateManager;
import org.apache.gora.persistency.impl.PersistentBase;
import org.apache.gora.persistency.impl.StateManagerImpl;
import org.apache.gora.persistency.StatefulHashMap;
import org.apache.gora.persistency.ListGenericArray;

@SuppressWarnings("all")
public class GEdge extends PersistentBase {
  public static final Schema _SCHEMA = Schema.parse("{\"type\":\"record\",\"name\":\"GEdge\",\"namespace\":\"org.apache.giraph.gora.generated\",\"fields\":[{\"name\":\"edgeId\",\"type\":\"string\"},{\"name\":\"edgeWeight\",\"type\":\"float\"},{\"name\":\"vertexInId\",\"type\":\"string\"},{\"name\":\"vertexOutId\",\"type\":\"string\"},{\"name\":\"label\",\"type\":\"string\"}]}");
  public static enum Field {
    EDGE_ID(0,"edgeId"),
    EDGE_WEIGHT(1,"edgeWeight"),
    VERTEX_IN_ID(2,"vertexInId"),
    VERTEX_OUT_ID(3,"vertexOutId"),
    LABEL(4,"label"),
    ;
    private int index;
    private String name;
    Field(int index, String name) {this.index=index;this.name=name;}
    public int getIndex() {return index;}
    public String getName() {return name;}
    public String toString() {return name;}
  };
  public static final String[] _ALL_FIELDS = {"edgeId","edgeWeight","vertexInId","vertexOutId","label",};
  static {
    PersistentBase.registerFields(GEdge.class, _ALL_FIELDS);
  }
  private Utf8 edgeId;
  private float edgeWeight;
  private Utf8 vertexInId;
  private Utf8 vertexOutId;
  private Utf8 label;
  public GEdge() {
    this(new StateManagerImpl());
  }
  public GEdge(StateManager stateManager) {
    super(stateManager);
  }
  public GEdge newInstance(StateManager stateManager) {
    return new GEdge(stateManager);
  }
  public Schema getSchema() { return _SCHEMA; }
  public Object get(int _field) {
    switch (_field) {
    case 0: return edgeId;
    case 1: return edgeWeight;
    case 2: return vertexInId;
    case 3: return vertexOutId;
    case 4: return label;
    default: throw new AvroRuntimeException("Bad index");
    }
  }
  @SuppressWarnings(value="unchecked")
  public void put(int _field, Object _value) {
    if(isFieldEqual(_field, _value)) return;
    getStateManager().setDirty(this, _field);
    switch (_field) {
    case 0:edgeId = (Utf8)_value; break;
    case 1:edgeWeight = (Float)_value; break;
    case 2:vertexInId = (Utf8)_value; break;
    case 3:vertexOutId = (Utf8)_value; break;
    case 4:label = (Utf8)_value; break;
    default: throw new AvroRuntimeException("Bad index");
    }
  }
  public Utf8 getEdgeId() {
    return (Utf8) get(0);
  }
  public void setEdgeId(Utf8 value) {
    put(0, value);
  }
  public float getEdgeWeight() {
    return (Float) get(1);
  }
  public void setEdgeWeight(float value) {
    put(1, value);
  }
  public Utf8 getVertexInId() {
    return (Utf8) get(2);
  }
  public void setVertexInId(Utf8 value) {
    put(2, value);
  }
  public Utf8 getVertexOutId() {
    return (Utf8) get(3);
  }
  public void setVertexOutId(Utf8 value) {
    put(3, value);
  }
  public Utf8 getLabel() {
    return (Utf8) get(4);
  }
  public void setLabel(Utf8 value) {
    put(4, value);
  }
}
