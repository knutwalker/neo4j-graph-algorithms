/**
 * Copyright (c) 2017 "Neo4j, Inc." <http://neo4j.com>
 *
 * This file is part of Neo4j Graph Algorithms <http://github.com/neo4j-contrib/neo4j-graph-algorithms>.
 *
 * Neo4j Graph Algorithms is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.graphalgo.impl;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.graphalgo.TestProgressLogger;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.RelationshipWeights;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.huge.HugeGraphFactory;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.impl.infomap.MapEquation;
import org.neo4j.graphalgo.impl.infomap.MapEquationOpt;
import org.neo4j.graphalgo.impl.pagerank.PageRankAlgorithm;
import org.neo4j.graphalgo.impl.pagerank.PageRankResult;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

import java.util.Arrays;
import java.util.function.Supplier;
import java.util.stream.LongStream;

import static org.mockito.AdditionalMatchers.eq;

/**
 * Graph:
 *
 *        (b)        (e)
 *       /  \       /  \     (x)
 *     (a)--(c)---(d)--(f)
 *
 * @author mknblch
 */
public class MapEquationTest {

    @ClassRule
    public static ImpermanentDatabaseRule db = new ImpermanentDatabaseRule();

    private static Graph graph;
    private static PageRankResult pageRankResult;
    private static RelationshipWeights normalizedWeights;

    @BeforeClass
    public static void setupGraph() throws KernelException {

        final String cypher =
                "CREATE (a:Node {name:'a'})\n" +
                        "CREATE (b:Node {name:'b'})\n" +
                        "CREATE (c:Node {name:'c'})\n" +
                        "CREATE (d:Node {name:'d'})\n" +
                        "CREATE (e:Node {name:'e'})\n" +
                        "CREATE (f:Node {name:'f'})\n" +
                        "CREATE (x:Node {name:'x'})\n" +
                        "CREATE" +
                        " (b)-[:TYPE]->(a),\n" +
                        " (a)-[:TYPE]->(c),\n" +
                        " (c)-[:TYPE]->(a),\n" +

                        " (d)-[:TYPE]->(c),\n" +

                        " (d)-[:TYPE]->(e),\n" +
                        " (d)-[:TYPE]->(f),\n" +
                        " (e)-[:TYPE]->(f)";

        db.execute(cypher);

        graph = new GraphLoader(db)
                .withAnyRelationshipType()
                .withAnyLabel()
                .withoutNodeProperties()
                .asUndirected(true)
                .load(HugeGraphFactory.class);

        pageRankResult = PageRankAlgorithm.of(graph, 1. - MapEquationOpt.TAU, LongStream.empty())
                .compute(10)
                .result();

        normalizedWeights = (sourceNodeId, targetNodeId) -> {
            final int degree = graph.degree(sourceNodeId, Direction.OUTGOING);
            if (degree == 0) {
                return 0;
            }
            return 1. / degree;
        };
    }


    @Test
    public void testMove() throws Exception {

        final MapEquation algo = new MapEquation(graph, pageRankResult::score, normalizedWeights);

        info(algo.getCommunities(), algo.getMDL(), algo.getIndexCodeLength(), algo.getModuleCodeLength());
        algo.move(id("b"), id("a"));
        algo.move(id("c"), id("a"));

        info(algo.getCommunities(), algo.getMDL(), algo.getIndexCodeLength(), algo.getModuleCodeLength());
        algo.move(id("e"), id("d"));
        algo.move(id("f"), id("d"));
        info(algo.getCommunities(), algo.getMDL(), algo.getIndexCodeLength(), algo.getModuleCodeLength());

        algo.move(id("d"), id("a"));
        algo.move(id("e"), id("a"));
        algo.move(id("f"), id("a"));
        info(algo.getCommunities(), algo.getMDL(), algo.getIndexCodeLength(), algo.getModuleCodeLength());
    }

    @Test
    public void testClustering() throws Exception {

        final MapEquation algo = new MapEquation(graph, pageRankResult::score, normalizedWeights);
        info(algo.getCommunities(), algo.getMDL(), algo.getIndexCodeLength(), algo.getModuleCodeLength());

        algo.compute(10, true);
        info(algo.getCommunities(), algo.getMDL(), algo.getIndexCodeLength(), algo.getModuleCodeLength());
    }

    private void info(int[] communities, double mdl, double indexCodeLength, double moduleCodeLength) {
        System.out.printf("%s | mdl: %5.2f | icl: %5.2f | mcl: %5.2f%n",
                Arrays.toString(communities),
                mdl,
                indexCodeLength,
                moduleCodeLength);
    }

    private int id(String name) {
        final Node[] node = new Node[1];
        db.execute("MATCH (n:Node) WHERE n.name = '" + name + "' RETURN n").accept(row -> {
            node[0] = row.getNode("n");
            return false;
        });
        return graph.toMappedNodeId(node[0].getId());
    }
}
