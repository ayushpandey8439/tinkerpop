/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.tinkergraph.structure;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;
import org.apache.commons.configuration2.BaseConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.strategy.optimization.TinkerGraphCountStrategy;
import org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.strategy.optimization.TinkerGraphStepStrategy;
import org.apache.tinkerpop.gremlin.tinkergraph.services.TinkerServiceRegistry;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.apache.tinkerpop.shaded.jackson.core.JsonFactory;
import org.apache.tinkerpop.shaded.jackson.core.JsonParser;
import org.apache.tinkerpop.shaded.jackson.core.JsonToken;
import org.apache.tinkerpop.shaded.jackson.databind.JsonNode;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;
import org.javatuples.Pair;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * An in-memory (with optional persistence on calls to {@link #close()}), reference implementation of the property
 * graph interfaces provided by TinkerPop.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@Graph.OptIn(Graph.OptIn.SUITE_STRUCTURE_STANDARD)
@Graph.OptIn(Graph.OptIn.SUITE_STRUCTURE_INTEGRATE)
@Graph.OptIn(Graph.OptIn.SUITE_PROCESS_STANDARD)
@Graph.OptIn(Graph.OptIn.SUITE_PROCESS_COMPUTER)
@Graph.OptIn(Graph.OptIn.SUITE_PROCESS_LIMITED_STANDARD)
@Graph.OptIn(Graph.OptIn.SUITE_PROCESS_LIMITED_COMPUTER)
public class TinkerGraph extends AbstractTinkerGraph {

    static {
        TraversalStrategies.GlobalCache.registerStrategies(TinkerGraph.class, TraversalStrategies.GlobalCache.getStrategies(Graph.class).clone().addStrategies(
                TinkerGraphStepStrategy.instance(),
                TinkerGraphCountStrategy.instance()));
    }

    private static final Configuration EMPTY_CONFIGURATION = new BaseConfiguration() {{
        this.setProperty(Graph.GRAPH, TinkerGraph.class.getName());
    }};

    private final TinkerGraphFeatures features = new TinkerGraphFeatures();
    private boolean bulkloading = false;
    protected Map<Object, Vertex> vertices = new ConcurrentHashMap<>();
    protected Map<Object, Edge> edges = new ConcurrentHashMap<>();
    public Set<TinkerVertex> roots = new HashSet<>();
    public Set<TinkerVertex> sinks = new HashSet<>();

    /**
     * An empty private constructor that initializes {@link TinkerGraph}.
     */
    TinkerGraph(final Configuration configuration) {
        this.configuration = configuration;
        vertexIdManager = selectIdManager(configuration, GREMLIN_TINKERGRAPH_VERTEX_ID_MANAGER, Vertex.class);
        edgeIdManager = selectIdManager(configuration, GREMLIN_TINKERGRAPH_EDGE_ID_MANAGER, Edge.class);
        vertexPropertyIdManager = selectIdManager(configuration, GREMLIN_TINKERGRAPH_VERTEX_PROPERTY_ID_MANAGER, VertexProperty.class);
        defaultVertexPropertyCardinality = VertexProperty.Cardinality.valueOf(
                configuration.getString(GREMLIN_TINKERGRAPH_DEFAULT_VERTEX_PROPERTY_CARDINALITY, VertexProperty.Cardinality.single.name()));
        allowNullPropertyValues = configuration.getBoolean(GREMLIN_TINKERGRAPH_ALLOW_NULL_PROPERTY_VALUES, false);

        graphLocation = configuration.getString(GREMLIN_TINKERGRAPH_GRAPH_LOCATION, null);
        graphFormat = configuration.getString(GREMLIN_TINKERGRAPH_GRAPH_FORMAT, null);

        if ((graphLocation != null && null == graphFormat) || (null == graphLocation && graphFormat != null))
            throw new IllegalStateException(String.format("The %s and %s must both be specified if either is present",
                    GREMLIN_TINKERGRAPH_GRAPH_LOCATION, GREMLIN_TINKERGRAPH_GRAPH_FORMAT));

        if (graphLocation != null) loadGraph();

        serviceRegistry = new TinkerServiceRegistry(this);
        configuration.getList(String.class, GREMLIN_TINKERGRAPH_SERVICE, Collections.emptyList()).forEach(serviceClass ->
                serviceRegistry.registerService(instantiate(serviceClass)));
    }

    /**
     * Open a new {@link TinkerGraph} instance.
     * <p/>
     * <b>Reference Implementation Help:</b> If a {@link Graph} implementation does not require a {@code Configuration}
     * (or perhaps has a default configuration) it can choose to implement a zero argument
     * {@code open()} method. This is an optional constructor method for TinkerGraph. It is not enforced by the Gremlin
     * Test Suite.
     */
    public static TinkerGraph open() {
        return open(EMPTY_CONFIGURATION);
    }

    /**
     * Open a new {@code TinkerGraph} instance.
     * <p/>
     * <b>Reference Implementation Help:</b> This method is the one use by the {@link GraphFactory} to instantiate
     * {@link Graph} instances.  This method must be overridden for the Structure Test Suite to pass. Implementers have
     * latitude in terms of how exceptions are handled within this method.  Such exceptions will be considered
     * implementation specific by the test suite as all test generate graph instances by way of
     * {@link GraphFactory}. As such, the exceptions get generalized behind that facade and since
     * {@link GraphFactory} is the preferred method to opening graphs it will be consistent at that level.
     *
     * @param configuration the configuration for the instance
     * @return a newly opened {@link Graph}
     */
    public static TinkerGraph open(final Configuration configuration) {
        return new TinkerGraph(configuration);
    }

    ////////////// STRUCTURE API METHODS //////////////////

    public void loadGraphCSV(String Path){
        bulkloading = true;
        Set<Integer> vertices = new HashSet<>();
        Set<Pair<Integer, Integer>> edges = new HashSet<>();
        try (CSVReader reader = new CSVReader(new FileReader(Path))) {
            List<String[]> records = reader.readAll();
            for (String[] record : records) {
                Vertex v1, v2;
                if(!vertices.contains(Integer.parseInt(record[0]))){
                    v1 = this.addVertex(T.id, Integer.parseInt(record[0]), T.label, "vertex");
                    vertices.add(Integer.parseInt(record[0]));
                } else {
                    v1 = this.vertices(Integer.parseInt(record[0])).next();
                }
                if(!vertices.contains(Integer.parseInt(record[1]))){
                    v2 = this.addVertex(T.id, Integer.parseInt(record[1]), T.label, "vertex");
                    vertices.add(Integer.parseInt(record[1]));
                }
                else {
                    v2 = this.vertices(Integer.parseInt(record[1])).next();
                }
                if(vertices.contains(Integer.parseInt(record[0])) && vertices.contains(Integer.parseInt(record[1])) && !edges.contains(new Pair<>(Integer.parseInt(record[0]), Integer.parseInt(record[1])))){
                    v1.addEdge("edge", v2);
                    edges.add(new Pair<>(Integer.parseInt(record[0]), Integer.parseInt(record[1])));
                }
//                System.out.println(record[0] + " "+ record[1] );
            }
        } catch (IOException | CsvException e) {
            e.printStackTrace();
        }
        bulkloading = false;
    }

    public void loadGraphSON(String Path) throws IOException {
        bulkloading = true;
        Set<Integer> vertices = new HashSet<>();
        Set<Pair<Integer, Integer>> edges = new HashSet<>();
        JsonFactory jfactory = new JsonFactory();
        JsonParser jParser = jfactory.createParser(new File(Path));
        Stack<JsonToken> TokenStack = new Stack<>();
        boolean vertexMode = false;
        boolean edgeMode = false;
        do {
            JsonToken token = jParser.nextToken();
            if(token == JsonToken.START_OBJECT && ! vertexMode && ! edgeMode) {
                TokenStack.push(token);
            } else if (token == JsonToken.START_ARRAY) {
                String fieldname = jParser.getCurrentName();
                if("vertices".equals(fieldname)){
                    vertexMode = true;
                    edgeMode = false;
                }
                if("edges".equals(fieldname)){
                    vertexMode = false;
                    edgeMode = true;
                }
                TokenStack.push(token);
            } else if (token == JsonToken.START_OBJECT && vertexMode) {
                List<Object> vertexProps = new ArrayList<>();
                while(jParser.nextToken() != JsonToken.END_OBJECT) {
                    String fieldname = jParser.getCurrentName();
                    if ("_id".equals(fieldname)) {
                        jParser.nextToken();
                        vertexProps.add(T.id);
                        vertexProps.add(Integer.parseInt(jParser.getText()));
                    }
                    if ("_label".equals(fieldname)) {
                        jParser.nextToken();
                        vertexProps.add(T.label);
                        vertexProps.add(jParser.getText());
                    }
                    if (!"_id".equals(fieldname) && !"_label".equals(fieldname)) {
                        jParser.nextToken();
                        vertexProps.add(fieldname);
                        vertexProps.add(jParser.getText());
                    }
                }
                Vertex vertex = this.addVertex(vertexProps.toArray());
                vertices.add(Integer.parseInt(vertex.id().toString()));

            }
            else if (token == JsonToken.START_OBJECT && edgeMode) {
                List<Object> edgeProps = new ArrayList<>();
                String edgeLabel = "";
                TinkerVertex outV = null, inV = null;
                while(jParser.nextToken() != JsonToken.END_OBJECT) {
                    String fieldname = jParser.getCurrentName();
                    if ("_id".equals(fieldname)) {
                        jParser.nextToken();
                        edgeProps.add(T.id);
                        edgeProps.add(jParser.getText());
                    }
                    if ("_label".equals(fieldname)) {
                        jParser.nextToken();
                        edgeLabel = (jParser.getText());
                    }
                    if("_inV".equals(fieldname)){
                        jParser.nextToken();
                        inV = (TinkerVertex) this.vertices(Integer.parseInt(jParser.getText())).next();
                    }
                    if("_outV".equals(fieldname)){
                        jParser.nextToken();
                        outV = (TinkerVertex) this.vertices(Integer.parseInt(jParser.getText())).next();
                    }
                    if (!"_id".equals(fieldname) && !"_label".equals(fieldname) && !"_inV".equals(fieldname) && !"_outV".equals(fieldname)) {
                        jParser.nextToken();
                        edgeProps.add(fieldname);
                        edgeProps.add(jParser.getText());
                    }
                }
                if(inV != null && outV != null) {
                    this.addEdge(outV, inV, edgeLabel, edgeProps.toArray());
                } else {
                    throw new IllegalArgumentException("Edge cannot be added without both inV and outV");
                }
            }
            else if (token == JsonToken.END_OBJECT || token == JsonToken.END_ARRAY) {
                TokenStack.pop();
            }
        } while (!TokenStack.isEmpty());
        jParser.close();

        bulkloading = false;
    }

    @Override
    public Vertex addVertex(final Object... keyValues) {
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        Object idValue = vertexIdManager.convert(ElementHelper.getIdValue(keyValues).orElse(null));
        final String label = ElementHelper.getLabelValue(keyValues).orElse(Vertex.DEFAULT_LABEL);

        if (null != idValue) {
            if (this.vertices.containsKey(idValue))
                throw Exceptions.vertexWithIdAlreadyExists(idValue);
        } else {
            idValue = vertexIdManager.getNextId(this);
        }

        final TinkerVertex vertex = createTinkerVertex(idValue, label, this);
        ElementHelper.attachProperties(vertex, VertexProperty.Cardinality.list, keyValues);
        this.vertices.put(vertex.id(), vertex);
        this.roots.add(vertex);
        this.sinks.add(vertex);

        return vertex;
    }

    @Override
    public void removeVertex(final Object vertexId)
    {
        this.vertices.remove(vertexId);

    }

    @Override
    public Edge addEdge(final TinkerVertex outVertex, final TinkerVertex inVertex, final String label, final Object... keyValues) {
        ElementHelper.validateLabel(label);
        ElementHelper.legalPropertyKeyValueArray(keyValues);

        Object idValue = edgeIdManager.convert(ElementHelper.getIdValue(keyValues).orElse(null));

        final Edge edge;
        if (null != idValue) {
            if (edges.containsKey(idValue))
                throw Graph.Exceptions.edgeWithIdAlreadyExists(idValue);
        } else {
            idValue = edgeIdManager.getNextId(this);
        }

        edge = new TinkerEdge(idValue, outVertex, label, inVertex);
        ElementHelper.attachProperties(edge, keyValues);
        edges.put(edge.id(), edge);
        addOutEdge(outVertex, label, edge);
        addInEdge(inVertex, label, edge);
        inVertex.parents.add(outVertex);
        outVertex.children.add(inVertex);
        roots.remove(inVertex);
        sinks.remove(outVertex);
        if (!bulkloading) inVertex.recomputeLabel(new HashSet<>());
        return edge;
    }

    @Override
    public void removeEdge(final Object edgeId) {
        final Edge edge = edges.get(edgeId);
        // already removed?
        if (null == edge) return;

        final TinkerVertex outVertex = (TinkerVertex) edge.outVertex();
        final TinkerVertex inVertex = (TinkerVertex) edge.inVertex();

        if (null != outVertex && null != outVertex.outEdges) {
            final Set<Edge> edges = outVertex.outEdges.get(edge.label());
            if (null != edges){
                edges.removeIf(e -> e.id() == edgeId);
                outVertex.children.remove(inVertex);
            }
            if(edges.isEmpty()) sinks.add(outVertex);
        }
        if (null != inVertex && null != inVertex.inEdges) {
            final Set<Edge> edges = inVertex.inEdges.get(edge.label());
            if (null != edges) {
                edges.removeIf(e -> e.id() == edgeId);
                inVertex.parents.remove(outVertex);
            }
            if (edges.isEmpty()) roots.add(inVertex);

        }
        this.edges.remove(edgeId);
    }


    public void labelGraph(){
        if(roots.isEmpty() && sinks.isEmpty()) {
            // If every vertex has atleast one incoming and one outgoing edge, the graph is connected.
            // In this case, we can start from any vertex and label the graph.
            Random generator = new Random();
            Object[] values = vertices.keySet().toArray();
            Object randomVertex = values[generator.nextInt(values.length)];
            TinkerVertex v = (TinkerVertex) vertices.get(randomVertex);
            v.isLabelled = true;
            v.children.forEach(child -> child.recomputeLabel(new HashSet<>()));
        }
        else {
            for (TinkerVertex v : roots){
                v.isLabelled = true;
                v.recomputeLabel(new HashSet<>());
            }
        }

    }

    @Override
    public void clear() {
        super.clear();
        this.vertices.clear();
        this.edges.clear();
    }

    @Override
    public Transaction tx() {
        throw Exceptions.transactionsNotSupported();
    }

    @Override
    public int getVerticesCount() { return vertices.size(); }

    @Override
    public boolean hasVertex(Object id) { return vertices.containsKey(id); }

    @Override
    public int getEdgesCount() {  return edges.size(); }

    @Override
    public boolean hasEdge(Object id) { return edges.containsKey(id); }

    @Override
    public TinkerServiceRegistry getServiceRegistry() {
        return serviceRegistry;
    }

    @Override
    public Vertex vertex(final Object vertexId) {
        return vertices.get(vertexIdManager.convert(vertexId));
    }

    @Override
    public Iterator<Vertex> vertices(final Object... vertexIds) {
        return createElementIterator(Vertex.class, vertices, vertexIdManager, vertexIds);
    }

    @Override
    public Iterator<Edge> edges(final Object... edgeIds) {
        return createElementIterator(Edge.class, edges, edgeIdManager, edgeIds);
    }

    @Override
    public Edge edge(final Object edgeId) {
        return edges.get(edgeIdManager.convert(edgeId));
    }


    private <T extends Element> Iterator<T> createElementIterator(final Class<T> clazz, final Map<Object, T> elements,
                                                                  final IdManager idManager,
                                                                  final Object... ids) {
        final Iterator<T> iterator;
        if (0 == ids.length) {
            iterator = new TinkerGraphIterator<>(elements.values().iterator());
        } else {
            final List<Object> idList = Arrays.asList(ids);

            // TinkerGraph can take a Vertex/Edge or any object as an "id". If it is an Element then we just cast
            // to that type and pop off the identifier. there is no need to pass that through the IdManager since
            // the assumption is that if it's already an Element, its identifier must be valid to the Graph and to
            // its associated IdManager. All other objects are passed to the IdManager for conversion.
            return new TinkerGraphIterator<>(IteratorUtils.filter(IteratorUtils.map(idList, id -> {
                // ids cant be null so all of those filter out
                if (null == id) return null;
                final Object iid = clazz.isAssignableFrom(id.getClass()) ? clazz.cast(id).id() : idManager.convert(id);
                return elements.get(idManager.convert(iid));
            }).iterator(), Objects::nonNull));
        }
        return TinkerHelper.inComputerMode(this) ?
                (Iterator<T>) (clazz.equals(Vertex.class) ?
                        IteratorUtils.filter((Iterator<Vertex>) iterator, t -> this.graphComputerView.legalVertex(t)) :
                        IteratorUtils.filter((Iterator<Edge>) iterator, t -> this.graphComputerView.legalEdge(t.outVertex(), t))) :
                iterator;
    }

    @Override
    protected void addOutEdge(final TinkerVertex vertex, final String label, final Edge edge) {
        if (null == vertex.outEdges) vertex.outEdges = new HashMap<>();
        Set<Edge> edges = vertex.outEdges.get(label);
        if (null == edges) {
            edges = new HashSet<>();
            vertex.outEdges.put(label, edges);
        }
        edges.add(edge);
    }

    @Override
    protected void addInEdge(final TinkerVertex vertex, final String label, final Edge edge) {
        if (null == vertex.inEdges) vertex.inEdges = new HashMap<>();
        Set<Edge> edges = vertex.inEdges.get(label);
        if (null == edges) {
            edges = new HashSet<>();
            vertex.inEdges.put(label, edges);
        }
        edges.add(edge);
    }

    /**
     * Return TinkerGraph feature set.
     * <p/>
     * <b>Reference Implementation Help:</b> Implementers only need to implement features for which there are
     * negative or instance configured features.  By default, all {@link Graph.Features} return true.
     */
    @Override
    public Features features() {
        return features;
    }

    public class TinkerGraphFeatures implements Features {

        private final TinkerGraphGraphFeatures graphFeatures = new TinkerGraphGraphFeatures();
        private final TinkerGraphEdgeFeatures edgeFeatures = new TinkerGraphEdgeFeatures();
        private final TinkerGraphVertexFeatures vertexFeatures = new TinkerGraphVertexFeatures();

        private TinkerGraphFeatures() {
        }

        @Override
        public GraphFeatures graph() {
            return graphFeatures;
        }

        @Override
        public EdgeFeatures edge() {
            return edgeFeatures;
        }

        @Override
        public VertexFeatures vertex() {
            return vertexFeatures;
        }

        @Override
        public String toString() {
            return StringFactory.featureString(this);
        }

    }

    public class TinkerGraphGraphFeatures implements Features.GraphFeatures {

        private TinkerGraphGraphFeatures() {
        }

        @Override
        public boolean supportsConcurrentAccess() {
            return false;
        }

        @Override
        public boolean supportsTransactions() {
            return false;
        }

        @Override
        public boolean supportsThreadedTransactions() {
            return false;
        }

        @Override
        public boolean supportsServiceCall() {
            return true;
        }

    }


    ///////////// GRAPH SPECIFIC INDEXING METHODS ///////////////

    /**
     * Create an index for said element class ({@link Vertex} or {@link Edge}) and said property key.
     * Whenever an element has the specified key mutated, the index is updated.
     * When the index is created, all existing elements are indexed to ensure that they are captured by the index.
     *
     * @param key          the property key to index
     * @param elementClass the element class to index
     * @param <E>          The type of the element class
     */
    public <E extends Element> void createIndex(final String key, final Class<E> elementClass) {
        if (Vertex.class.isAssignableFrom(elementClass)) {
            if (null == this.vertexIndex) this.vertexIndex = new TinkerIndex<>(this, TinkerVertex.class);
            this.vertexIndex.createKeyIndex(key);
        } else if (Edge.class.isAssignableFrom(elementClass)) {
            if (null == this.edgeIndex) this.edgeIndex = new TinkerIndex<>(this, TinkerEdge.class);
            this.edgeIndex.createKeyIndex(key);
        } else {
            throw new IllegalArgumentException("Class is not indexable: " + elementClass);
        }
    }

    /**
     * Drop the index for the specified element class ({@link Vertex} or {@link Edge}) and key.
     *
     * @param key          the property key to stop indexing
     * @param elementClass the element class of the index to drop
     * @param <E>          The type of the element class
     */
    public <E extends Element> void dropIndex(final String key, final Class<E> elementClass) {
        if (Vertex.class.isAssignableFrom(elementClass)) {
            if (null != this.vertexIndex) this.vertexIndex.dropKeyIndex(key);
        } else if (Edge.class.isAssignableFrom(elementClass)) {
            if (null != this.edgeIndex) this.edgeIndex.dropKeyIndex(key);
        } else {
            throw new IllegalArgumentException("Class is not indexable: " + elementClass);
        }
    }
}
