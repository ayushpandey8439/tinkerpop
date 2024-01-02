package org.apache.tinkerpop.gremlin.tinkergraph;

import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerVertex;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;

public class TinkerGraphLabellingTest {
    @Test
    public void LinearGraphlabellingTest() {
        TinkerGraph graph = TinkerGraph.open(); //1
        Vertex v1 = graph.addVertex(T.id, 1, T.label, "person", "name", "marko", "age", 29); //2
        Vertex v2 = graph.addVertex(T.id, 2, T.label, "person", "name", "vadas", "age", 27); //3
        Vertex v3 = graph.addVertex(T.id, 3, T.label, "software", "name", "lop", "lang", "java"); //4
        Vertex v4 = graph.addVertex(T.id, 4, T.label, "person", "name", "josh", "age", 32); //5
        Vertex v5 = graph.addVertex(T.id, 5, T.label, "software", "name", "ripple", "lang", "java"); //6
        v1.addEdge("knows", v2, T.id, 7, "weight", 0.5f); //7
        v2.addEdge("knows", v3, T.id, 8, "weight", 1.0f); //8
        v3.addEdge("created", v4, T.id, 9, "weight", 0.4f); //9
        v4.addEdge("created", v5, T.id, 10, "weight", 1.0f); //10

        graph.labelGraph();
        assertEquals(new ArrayList<>(Arrays.asList(1)), ((TinkerVertex) v1).getPathLabel());
        assertEquals(new ArrayList<>(Arrays.asList(1, 2)), ((TinkerVertex) v2).getPathLabel());
        assertEquals(new ArrayList<>(Arrays.asList(1, 2, 3)), ((TinkerVertex) v3).getPathLabel());
        assertEquals(new ArrayList<>(Arrays.asList(1, 2, 3, 4)), ((TinkerVertex) v4).getPathLabel());
        assertEquals(new ArrayList<>(Arrays.asList(1, 2, 3, 4, 5)), ((TinkerVertex) v5).getPathLabel());
    }

    @Test
    public void OneCycleGraphlabellingTest() {
        TinkerGraph graph = TinkerGraph.open(); //1
        Vertex v1 = graph.addVertex(T.id, 1, T.label, "person", "name", "marko", "age", 29); //2
        Vertex v2 = graph.addVertex(T.id, 2, T.label, "person", "name", "vadas", "age", 27); //3
        Vertex v3 = graph.addVertex(T.id, 3, T.label, "software", "name", "lop", "lang", "java"); //4
        Vertex v4 = graph.addVertex(T.id, 4, T.label, "person", "name", "josh", "age", 32); //5
        Vertex v5 = graph.addVertex(T.id, 5, T.label, "software", "name", "ripple", "lang", "java"); //6
        v1.addEdge("knows", v2, T.id, 7, "weight", 0.5f); //7
        v2.addEdge("knows", v3, T.id, 8, "weight", 1.0f); //8
        v3.addEdge("created", v4, T.id, 9, "weight", 0.4f); //9
        v4.addEdge("created", v5, T.id, 10, "weight", 1.0f); //10
        v5.addEdge("created", v2, T.id, 11, "weight", 0.4f); //11

        graph.labelGraph();
        assertEquals(new ArrayList<>(Arrays.asList(1)), ((TinkerVertex) v1).getPathLabel());
        assertEquals(new ArrayList<>(Arrays.asList(1, 2)), ((TinkerVertex) v2).getPathLabel());
        assertEquals(new ArrayList<>(Arrays.asList(1, 2, 3)), ((TinkerVertex) v3).getPathLabel());
        assertEquals(new ArrayList<>(Arrays.asList(1, 2, 3, 4)), ((TinkerVertex) v4).getPathLabel());
        assertEquals(new ArrayList<>(Arrays.asList(1, 2, 3, 4, 5)), ((TinkerVertex) v5).getPathLabel());
    }

    @Test
    public void MultipleRootGraphlabellingTest() {
        TinkerGraph graph = TinkerGraph.open(); //1
        Vertex v1 = graph.addVertex(T.id, 1, T.label, "person", "name", "marko", "age", 29); //2
        Vertex v2 = graph.addVertex(T.id, 2, T.label, "person", "name", "vadas", "age", 27); //3
        Vertex v3 = graph.addVertex(T.id, 3, T.label, "software", "name", "lop", "lang", "java"); //4
        Vertex v4 = graph.addVertex(T.id, 4, T.label, "person", "name", "josh", "age", 32); //5
        Vertex v5 = graph.addVertex(T.id, 5, T.label, "software", "name", "ripple", "lang", "java"); //6
        Vertex v6 = graph.addVertex(T.id, 6, T.label, "person", "name", "peter", "age", 35); //7

        v1.addEdge("knows", v2, T.id, 7, "weight", 0.5f); //7
        v2.addEdge("knows", v3, T.id, 8, "weight", 1.0f); //8
        v3.addEdge("created", v4, T.id, 9, "weight", 0.4f); //9
        v4.addEdge("created", v5, T.id, 10, "weight", 1.0f); //10
        v5.addEdge("created", v2, T.id, 11, "weight", 0.4f); //11
        v6.addEdge("created", v4, T.id, 12, "weight", 0.2f); //12

        graph.labelGraph();
        assertEquals(new ArrayList<>(Arrays.asList(1)), ((TinkerVertex) v1).getPathLabel());
        assertEquals(new ArrayList<>(Arrays.asList(2)), ((TinkerVertex) v2).getPathLabel());
        assertEquals(new ArrayList<>(Arrays.asList(2, 3)), ((TinkerVertex) v3).getPathLabel());
        assertEquals(new ArrayList<>(Arrays.asList(4)), ((TinkerVertex) v4).getPathLabel());
        assertEquals(new ArrayList<>(Arrays.asList(4, 5)), ((TinkerVertex) v5).getPathLabel());
        assertEquals(new ArrayList<>(Arrays.asList(6)), ((TinkerVertex) v6).getPathLabel());
    }

}
