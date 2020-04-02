package neo4j.impact;


import org.junit.*;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;


import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import static org.junit.Assert.*;

public class ImpactTest {
    private static final String SETUP_CUSTOM_RELATIONS = "CREATE " +
            "(a:Node{name:'A'}), " +
            "(b:Node{name:'B'}), " +
            "(c:Node{name:'C'}), " +
            "(a)-[:MY_NON_DEFAULT_IMPACT_RELATION]->(b), " +
            "(a)-[:WHATEVER]->(c) " ;
    private static final String SETUP_SIMPLE_ABCD = "CREATE " +
            "(a:Node{name:'A'}), " +
            "(b:Node{name:'B'}), " +
            "(c:Node{name:'C'}), " +
            "(d:Node{name:'D'}), " +
            "(a)-[:IMPACTS {name: 'AC'}]->(c), " +
            "(d)-[:IMPACTS {name: 'DC'}]->(c), " +
            "(b)-[:IMPACTS {name: 'BC'}]->(c) " ;
    private static final String SETUP_ABC_LOOP = "CREATE " +
            "(s:X{name:'START'}), " +
            "(a:X{name:'A'}), " +
            "(b:X{name:'B'}), " +
            "(c:X{name:'C'}), " +
            "(s)-[:IMPACTS {name: 'SA'}]->(a), " +
            "(a)-[:IMPACTS {name: 'AB'}]->(b), " +
            "(b)-[:IMPACTS {name: 'BC'}]->(c), " +
            "(c)-[:IMPACTS {name: 'CA'}]->(a) " ;
    private static final String SETUP_NETWORK = "CREATE " +
            "(loc:Location {name:'loc1'}), " +
            "(a:NE{name:'A'}), " +
            "(b:NE{name:'B'}), " +
            "(a1:Card{name:'A-C1'}), " +
            "(a1_1:Port{name:'A-C1-1'}), " +
            "(a1_2:Port{name:'A-C1-2'}), " +
            "(b1:Card{name:'B-C1'}), " +
            "(b1_1:Port{name:'B-C1-1'}), " +
            "(b1_2:Port{name:'B-C1-2'}), " +
            "(l:Link{name:'link A-B'}), " +
            "(l2:Link{name:'protection link A-B'}), " +
            "(c1:Circuit{name:'prev circ'}), " +
            "(c2:Circuit{name:'circ'}), " +
            "(c3:Circuit{name:'next circ'}), " +
            "(e2e:Circuit{name:'e2e'}), " +
            "(x1_1:Port{name:'X-1-1'}), " +
            "(loc)-[:IMPACTS]->(a), " +
            "(loc)-[:IMPACTS]->(b), " +
            "(a1)<-[:IMPACTS]-(a), " +
            "(b1)<-[:IMPACTS]-(b), " +
            "(b1_1)<-[:IMPACTS]-(b1), " +
            "(b1_2)<-[:IMPACTS]-(b1), " +
            "(a1_1)<-[:IMPACTS]-(a1), " +
            "(a1_2)<-[:IMPACTS]-(a1), " +
            "(l)<-[:IMPACTS]-(a1_1), " +
            "(l)<-[:IMPACTS]-(b1_1), " +
            "(l2)<-[:IMPACTS]-(x1_1), " +
            "(e2e)<-[:IMPACTS]-(c1), " +
            "(e2e)<-[:IMPACTS]-(c2), " +
            "(e2e)<-[:IMPACTS]-(c3), " +
            "(c2)<-[:IMPACTS {impact_group: '1', impact_propagation: 'PROTECTION'}]-(l2), " +
            "(c2)<-[:IMPACTS {impact_group: 1, impact_propagation: 'PROTECTION'}]-(l) ";
//    private static final String SETUP_GENERATE_ER = "call apoc.generate.er(100, 200, 'Node', 'IMPACTS')\n" +
//            "\tWITH ['MOST_SEVERE', 'DEGRADATION','RISK_PROPAGATION', 'LEAST_SEVERE', 'PROTECTION'] as rules\n" +
//            "\tMATCH ()-[r:IMPACTS]->() \n" +
//            "\tWITH r, rules[ toInteger(rand()*size(rules)) ] as rule, \n" +
//            "\tCASE \n" +
//            "\t\tWHEN rand() < 0.2 THEN [0] \n" +
//            "\t    WHEN rand() > 0.95 THEN [1] \n" +
//            "\t    ELSE [] \n" +
//            "\tEND as group\n" +
//            "\tset r.impact_propagation = rule, r.impact_group = group[0]\n" +
//            "\tRETURN r, rule, group\n";

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule()
            .withSetting(GraphDatabaseSettings.log_queries, GraphDatabaseSettings.LogQueryLevel.VERBOSE);


    @Before
    public void setUp() throws Exception {
        registerProcedure(db, Impact.class);
       // registerProcedure(db, apoc.generate.Generate.class);
    }

    @After
    public void tearDown() {
        db.shutdown();
    }


    //test for each propagation rule
    @Test
    public void testImpact_customRelations() throws Exception {
        System.out.println("testImpact_customRelations...");
        db.executeTransactionally(SETUP_CUSTOM_RELATIONS);
        // custom relation, either way
        testResult(db, "MATCH (a:Node) WHERE a.name='A' " +
                        "CALL neo4j.impact.compute(a, 'MY_NON_DEFAULT_IMPACT_RELATION') yield node, state " +
                        "RETURN  node, state" , null,
                r -> assertImpactResult_nodeState(r, "B", "FAILED") );
        //default : no impact
        testResult(db, "MATCH (a:Node) WHERE a.name='A' " +
                        "CALL neo4j.impact.compute(a) yield node, state " +
                        "RETURN  node, state" , null,
                r -> assertImpactResult_resultCount(r, 1) );
        // custom relation, wrong way
        testResult(db, "MATCH (a:Node) WHERE a.name='A' " +
                        "CALL neo4j.impact.compute(a, '<MY_NON_DEFAULT_IMPACT_RELATION') yield node, state " +
                        "RETURN  node, state" , null,
                r -> assertImpactResult_resultCount(r, 1) );
        // custom relation, right way
        testResult(db, "MATCH (a:Node) WHERE a.name='A' " +
                        "CALL neo4j.impact.compute(a, 'MY_NON_DEFAULT_IMPACT_RELATION>') yield node, state " +
                        "RETURN  node, state" , null,
                r -> assertImpactResult_nodeState(r, "B", "FAILED") );
        // custom relation, with or
        testResult(db, "MATCH (a:Node) WHERE a.name='A' " +
                        "CALL neo4j.impact.compute(a, 'MY_NON_DEFAULT_IMPACT_RELATION>|WHATEVER') yield node, state " +
                        "RETURN  node, state" , null,
                r -> assertImpactResult_nodeState(r, "C", "FAILED") );
    }

    @Test
    public void testImpact_inputs() throws Exception {
        System.out.println("testImpact_inputs...");
        db.executeTransactionally(SETUP_NETWORK);
        //null node
        testResult(db, "CALL neo4j.impact.compute(null) yield node, state " +
                        "RETURN  node, state" , null,
                r -> assertImpactResult_resultCount(r, 0) );
        //single node
        testResult(db,"MATCH (c:Card) WHERE c.name='B-C1' " +
                        "CALL neo4j.impact.compute(c) yield node, state " +
                        "RETURN  node, state" , null,
                r -> assertImpactResult_resultCount(r, 6) );
        //single id
        testResult(db,"MATCH (c:Card) WHERE c.name='B-C1' " +
                        "CALL neo4j.impact.compute(id(c)) yield node, state " +
                        "RETURN  node, state" , null,
                r -> assertImpactResult_resultCount(r, 6) );
        //list of ids
        testResult(db,"MATCH (c:Card), (l:Link) WHERE c.name='B-C1' AND l.name ='protection link A-B' " +
                        "CALL neo4j.impact.compute([6, 10]) yield node, state " +
                        "RETURN  node, state" , null,
                r -> assertImpactResult_resultCount(r, 7) );
        //list of nodes
        testResult(db,"MATCH (c:Card), (l:Link) WHERE c.name='B-C1' AND l.name ='protection link A-B' " +
                        "CALL neo4j.impact.compute([id(c), id(l)]) yield node, state " +
                        "RETURN  node, state" , null,
                r -> assertImpactResult_resultCount(r, 7) );
        //nonsense : node node found
        try {
            testResult(db, "CALL neo4j.impact.compute(99999999) yield node, state " +
                            "RETURN  node, state", null,
                    r -> assertImpactResult_resultCount(r, 0));
        } catch (Exception e) {
            assertTrue( e.getMessage().contains("Unable to load NODE with id 9999"));
        }
        //nonsense : string
        try {
            testResult(db, "CALL neo4j.impact.compute('dumpInput') yield node, state " +
                            "RETURN  node, state", null,
                    r -> assertImpactResult_resultCount(r, 0));
        } catch (Exception e) {
            assertTrue( e.getMessage().contains("Unsupported data type for start parameter a Node or an Identifier (long) of a Node must be given!"));
        }
    }

    @Test
    public void testImpact_propagation_rules() throws Exception {
        System.out.println("testImpact_propagation_rules...");
        db.executeTransactionally(SETUP_SIMPLE_ABCD);
        //all default : MOST_SEVERE, null groups
        testResult(db, "MATCH (a:Node) WHERE a.name='A' " +
                        "CALL neo4j.impact.compute(a) yield node, state " +
                        "RETURN  node, state" , null,
                r -> assertImpactResult_nodeState(r, "C", "FAILED") );
        //LEAST_SEVERE
        db.executeTransactionally("MATCH (a)-[r:IMPACTS]-(c) SET r.impact_propagation = 'LEAST_SEVERE' ");
        testResult(db, "MATCH (a:Node) WHERE a.name='A' " +
                        "CALL neo4j.impact.compute(a) yield node, state " +
                        "RETURN  node, state" , null,
                r -> assertImpactResult_nodeState(r, "C", "WORKING") );
        //DEGRADATION
        db.executeTransactionally("MATCH (a)-[r:IMPACTS]-(c) SET r.impact_propagation = 'DEGRADATION' ");
        testResult(db, "MATCH (a:Node) WHERE a.name='A' " +
                        "CALL neo4j.impact.compute(a) yield node, state " +
                        "RETURN  node, state" , null,
                r -> assertImpactResult_nodeState(r, "C", "DEGRADED") );
        //RISK_PROPAGATION
        db.executeTransactionally("MATCH (a)-[r:IMPACTS]-(c) SET r.impact_propagation = 'RISK_PROPAGATION' ");
        testResult(db, "MATCH (a:Node) WHERE a.name='A' " +
                        "CALL neo4j.impact.compute(a) yield node, state " +
                        "RETURN  node, state" , null,
                r -> assertImpactResult_nodeState(r, "C", "AT_RISK") );
        //PROTECTION : 1 fail out of 3
        db.executeTransactionally("MATCH (a)-[r:IMPACTS]-(c) SET r.impact_propagation = 'PROTECTION' ");
        testResult(db, "MATCH (a:Node) WHERE a.name='A' " +
                        "CALL neo4j.impact.compute(a) yield node, state " +
                        "RETURN  node, state" , null,
                r -> assertImpactResult_nodeState(r, "C", "WORKING") );
        //PROTECTION  : 2 fail out of 3
        db.executeTransactionally("MATCH (a)-[r:IMPACTS]-(c) SET r.impact_propagation = 'PROTECTION' ");
        testResult(db, "MATCH (a:Node),  (b:Node)  WHERE a.name='A' AND b.name='B' " +
                        "CALL neo4j.impact.compute([a,b]) yield node, state " +
                        "RETURN  node, state" , null,
                r -> assertImpactResult_nodeState(r, "C", "AT_RISK") );
    }

    @Test
    public void testImpact_groups() throws Exception {
        System.out.println("testImpact_groups...");
        db.executeTransactionally(SETUP_SIMPLE_ABCD);
        //group 1 LEAST_SEVERE :(a, b)  / group 2  RISK_PROPAGATION: (d)
        //starting nodes : A & D
        db.executeTransactionally("MATCH (ab)-[r:IMPACTS]->(c) WHERE ab.name in ['A', 'B'] "+
                                    "SET r.impact_propagation = 'LEAST_SEVERE', r.impact_group=1 ");
        db.executeTransactionally("MATCH (d {name: 'D'})-[r:IMPACTS]->(c) SET r.impact_propagation = 'RISK_PROPAGATION', r.impact_group=2 ");
        testResult(db, "MATCH (a:Node),  (d:Node)  WHERE a.name='A' AND d.name='D' " +
                        "CALL neo4j.impact.compute([a, d]) yield node, state " +
                        "RETURN  node, state", null,
                r -> assertImpactResult_nodeState(r, "C", "AT_RISK"));
        //group 1 PROTECTION :(a, b)  / default group  MOST_SEVERE: (d)
        //starting nodes : A & D
        db.executeTransactionally("MATCH (ab)-[r:IMPACTS]->(c) WHERE ab.name in ['A', 'B'] "+
                "SET r.impact_propagation = 'PROTECTION', r.impact_group=1 ");
        db.executeTransactionally("MATCH (d {name: 'D'})-[r:IMPACTS]->(c) SET r.impact_propagation = 'MOST_SEVERE' ");
        testResult(db, "MATCH (a:Node),  (d:Node)  WHERE a.name='A' AND d.name='D' " +
                "CALL neo4j.impact.compute([a, d]) yield node, state " +
                        "RETURN  node, state", null,
                r -> assertImpactResult_nodeState(r, "C", "FAILED"));
    }

    @Test
    public void testImpact_limits() throws Exception {
        System.out.println("testImpact_limits...");
         db.executeTransactionally(SETUP_NETWORK);
        testResult(db, "MATCH (loc:Location {name:'loc1'}) " +
                        "CALL neo4j.impact.compute(loc, null, '3hops') yield node, state " +
                        "RETURN  node, state", null,
                r -> assertImpactResult_resultCount(r, 9));
        testResult(db, "MATCH (loc:Location {name:'loc1'}) " +
                        "CALL neo4j.impact.compute(loc, null, '10results') yield node, state " +
                        "RETURN  node, state", null,
                r -> assertImpactResult_resultCount(r, 10));
        testResult(db, "MATCH (loc:Location {name:'loc1'}) " +
                        "CALL neo4j.impact.compute(loc, null, '2hops,3results') yield node, state " +
                        "RETURN  node, state", null,
                r -> assertImpactResult_resultCount(r, 3));
        testResult(db, "MATCH (loc:Location {name:'loc1'}) " +
                        "CALL neo4j.impact.compute(loc, null, '3hops, 1000results, 50s') yield node, state " +
                        "RETURN  node, state", null,
                r -> assertImpactResult_resultCount(r, 9));
        try {
            testResult(db, "MATCH (loc:Location {name:'loc1'}) " +
                            "CALL neo4j.impact.compute(loc, null, '3hops,BS') yield node, state " +
                            "RETURN  node, state", null,
                    r -> assertImpactResult_resultCount(r, 9));
        } catch (Exception e) {
            assertTrue( e.getMessage().contains(" parameter 'limit' expects "));
        }

    }

    @Test
    public void testImpact_loops() throws Exception {
        System.out.println("testImpact_loops...");
        db.executeTransactionally(SETUP_NETWORK);
        testResult(db, "MATCH (loc:Location {name:'loc1'}), (p:Port {name:'X-1-1'}) " +
                        "CALL neo4j.impact.compute([loc,p]) yield node, state " +
                        "RETURN  node, state", null,
                r -> assert_Network(r,  "FAILED")
        );
        testResult(db, "MATCH (loc:Location {name:'loc1'}) " +
                        "CALL neo4j.impact.compute(loc) yield node, state " +
                        "RETURN  node, state", null,
                r -> assert_Network(r, "AT_RISK")
        );
        db.executeTransactionally(SETUP_ABC_LOOP);
        testResult(db, "MATCH (s:X {name:'START'}) " +
                        "CALL neo4j.impact.compute(s) yield node, state " +
                        "RETURN  node, state", null,
                r -> assertImpactResult_resultCount(r, 4)
        );
    }
    private void assert_Network(Result r, String e2eState) {
        assertImpactResult_nodeState(r, "e2e", e2eState);
    }
    private void assertImpactResult_nodeState(Result r, String nodeName, String expectedState) {
        Node node;
        String state ;
        for (Map<String, Object> map : Iterators.asList(r)) {
            node = (Node) map.get("node");
            if (node.getProperty("name").toString().equals(nodeName)) {
                state = (String) map.get("state");
                assertEquals(expectedState, state);
            }
        }
    }

    private void assertImpactResult_resultCount(Result r, int expectedCount) {
        //assertEquals(true, r.hasNext());
        List<Map<String, Object>> maps = Iterators.asList(r);
        assertEquals(expectedCount, maps.size());
        //Node n = (Node) maps.get(0).get("node");
        //assertEquals("Gene Hackman", n.getProperty("name"));
    }


    public static void testResult(GraphDatabaseService db, String call, Map<String,Object> params, Consumer<Result> resultConsumer) {
        try (Transaction tx = db.beginTx()) {
            Map<String, Object> p = (params == null) ? Collections.emptyMap() : params;
            Result result = tx.execute(call, p);
            resultConsumer.accept(result);
            tx.commit();
        } catch (RuntimeException e) {
            throw e;
        }
    }
    public static void registerProcedure(GraphDatabaseService db, Class<?>...procedures) {
        GlobalProcedures globalProcedures = ((GraphDatabaseAPI) db).getDependencyResolver().resolveDependency(GlobalProcedures.class);
        for (Class<?> procedure : procedures) {
            try {
                globalProcedures.registerProcedure(procedure, true);
                globalProcedures.registerFunction(procedure, true);
                globalProcedures.registerAggregationFunction(procedure, true);
            } catch (KernelException e) {
                throw new RuntimeException("while registering " + procedure, e);
            }
        }
    }
}
