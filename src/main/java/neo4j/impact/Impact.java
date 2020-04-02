package neo4j.impact;

import org.neo4j.graphdb.*;
import org.neo4j.graphdb.traversal.*;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.util.*;
import java.util.stream.Stream;
import org.neo4j.graphdb.Node;
import static org.neo4j.graphdb.Direction.*;


public class Impact  {

    public enum State  {
        WORKING(0), AT_RISK(1), DEGRADED(2), FAILED(3);

        private final int value;
        private State(int value) {
            this.value = value;
        }
        public int getStateValue() {
            return this.value;
        }
        public int compare(State that) {
            return Integer.compare(this.value, that.value);
        }
    }
    public enum ImpactRelationshipTypes implements RelationshipType
    {
        IMPACTS
    }
    public enum PropagationRule
    {
        MOST_SEVERE,
        DEGRADATION,
        RISK_PROPAGATION,
        LEAST_SEVERE,
        PROTECTION;
        public static PropagationRule validate(String s)
        {
            for(PropagationRule v:values())
                if (v.name().equals(s))
                    return v;
            return PropagationRule.MOST_SEVERE; //defaults to MOST_SEVERE
        }
    }

    @Context
    public GraphDatabaseService db;

    @Context
    public Transaction tx;

    @Context
    public Log log;

    @Procedure(value = "neo4j.impact.compute")
    @Description("neo4j.impact.compute(start <id>|Node|list, 'TYPE_OUT>|<TYPE_IN', maxLevel)\n"+
            "Computes impacts from the start node(s), following the given impact relationships, within the specified limits.\n"+
            "'start' : <id>|Node|list of Node|list of <id> \n"+
            "'relationshipFilter' : [<]RELATIONSHIP_TYPE1[>]|[<]RELATIONSHIP_TYPE2[>]|...\n"+
            "RELATIONSHIP_TYPES must be directional : INCOMING (<) or OUTGOING (>), "+
            "with OUTGOING assumed if direction is not specified. Defaults to 'IMPACTS>'. \n"+
            "'limits' : <hopLimit>hops,<timeout>s,<resultLimit>results\n"+
            "Allows limiting any of the following : "+
            "hop-depth of the traversals (ex: '5hops'), execution time (ex : '10s') or number of results (ex: '1000results'), "+
            "or any combination of them. Defaults to '10hops,60s,1000results'. \n" +
            "Returns the impacted nodes and their state.")
    public Stream<ImpactResult> compute(@Name("start") Object start,
                                         @Name(value="relationshipFilter", defaultValue = "")  String pathFilter,
                                         @Name(value="limits", defaultValue = "10hops,60s,1000results")  String limitsString
            ) throws Exception {
        long startTime = System.currentTimeMillis();
        log.debug("neo4j.impact.compute("+ start +", "+ pathFilter +", "+ limitsString +") started at "+startTime);

        //parse 'limits'
        long[] limits=parseLimits(limitsString);

        //parse 'start' with startToNodes (from PathExplorer) to support Node, nodeId, list of Nodes, list of nodeIds
        List<Node> nodes = this.startToNodes(start);
        HashMap<Node, State> nodesMap = new HashMap<Node, State>();
        for (Node n : nodes) { nodesMap.put(n, State.FAILED); }

        //parse 'relationshipFilter' : create a PathExpander
        PathExpander<State> pex;
        List<Pair<RelationshipType, Direction>> relsAndDirs;
        if (pathFilter != null && !pathFilter.trim().isEmpty()) {
            relsAndDirs = parsePathFilter(pathFilter.trim());
            pex = new RelationshipSequenceExpander(pathFilter.trim(), true); //copy of apoc CLass
        } else { //if no relationTypes specified, use default :IMPACTS
            pex = PathExpanders.forTypeAndDirection(ImpactRelationshipTypes.IMPACTS, Direction.OUTGOING);
            relsAndDirs = new ArrayList<>();
            relsAndDirs.add(Pair.of(ImpactRelationshipTypes.IMPACTS, Direction.OUTGOING));
        }

        //configure traversal
        TraversalDescription td = tx.traversalDescription(); //supposedly un-deprecated, due to Field riots
        td = td.breadthFirst(); //using breadthFirst to mitigate incomplete state computation when limits truncate the resultset
        td = td.expand(pex);
        td = td.evaluator(new ImpactEvaluator(nodes, nodesMap, relsAndDirs, limits[2], limits[1]));
        if (limits[0] > 0) {td = td.evaluator(Evaluators.toDepth((int) limits[0]));}
        td = td.uniqueness(Uniqueness.NODE_PATH); //NODE_PATH ensures the traversal does not loop back through its previous nodes
        // uniqueness should be set as last on the TraversalDescription
        Traverser traverser=  td.traverse(nodes);

        //run the traversal
        for(Path p: traverser) {
            //System.out.println(i++ +":"+System.currentTimeMillis()+" "+p.endNode()+" => "+ nodesMap.get(p.endNode()));
        }

        log.debug("neo4j.impact.compute completed after "+ (System.currentTimeMillis() - startTime) + "ms : " + nodesMap.size()+ " impacted nodes.");
        return nodesMap.entrySet().stream().map(e -> new ImpactResult(e.getKey(), e.getValue()));
    }

    private static long[] parseLimits(String limitsString) throws QueryExecutionException {
        long timeout=-1, maxLevel=-1, resultLimit=-1;
        String[] limits = limitsString.trim().split(",");
        for(int i=0; i< limits.length; i++) {
            String trimmedValue=limits[i].trim();
            if (trimmedValue.matches("^[0-9]+s$")) {
                timeout = Integer.parseInt(trimmedValue.replaceFirst("s", ""));
            } else if (trimmedValue.matches("^[0-9]+hops$")) {
                maxLevel = Integer.parseInt(trimmedValue.replaceFirst("hops", ""));
            } else if (trimmedValue.matches("^[0-9]+results$")) {
                resultLimit = Integer.parseInt(trimmedValue.replaceFirst("results", ""));
            } else {
                throw new QueryExecutionException("parameter 'limit' expects one (or several, with a comma separating them) of the following formats : limit in seconds (ex : '10s'), hop limit (ex: '5hops'), or result limit (ex: '1000results'). Found '"+ limits[i]+"'", null, "Neo.ClientError.Statement.SyntaxError");
            }
        }
        long[] limitsArray = {maxLevel, timeout, resultLimit};
        return limitsArray;
    }

    //uses a copy of apoc RelationshipTypeAndDirections (could be replaced by apoc dependency)
    private static List<Pair<RelationshipType, Direction>> parsePathFilter(String pathFilter) {
        List<Pair<RelationshipType, Direction>> relsAndDirs = new ArrayList<>();
        if (pathFilter == null) {
            relsAndDirs.add(Pair.of(null, BOTH));
        } else {
            String[] defs = pathFilter.split("\\|");
            for (String def : defs) {
                relsAndDirs.add(Pair.of(RelationshipTypeAndDirections.relationshipTypeFor(def), directionFor(def)));
            }
        }
        return relsAndDirs;
    }
    private static Direction directionFor(String type) {
        if (type.contains("<")) return INCOMING;
        if (type.contains(">")) return OUTGOING;
        return OUTGOING;   //defaults to OUTGOING, unlike in apoc.path (RelationshipTypeAndDirections)
    }

    //from apoc.path.PathExplorer : get list of nodes from 'start' input
    private List<Node> startToNodes(Object start) throws Exception {
        if (start == null) return Collections.emptyList();
        if (start instanceof Node) {
            return Collections.singletonList((Node) start);
        }
        if (start instanceof Number) {
            return Collections.singletonList(tx.getNodeById(((Number) start).longValue()));
        }
        if (start instanceof List) {
            List list = (List) start;
            if (list.isEmpty()) return Collections.emptyList();

            Object first = list.get(0);
            if (first instanceof Node) return (List<Node>)list;
            if (first instanceof Number) {
                List<Node> nodes = new ArrayList<>();
                for (Number n : ((List<Number>)list)) nodes.add(tx.getNodeById(n.longValue()));
                return nodes;
            }
        }
        throw new Exception("Unsupported data type for start parameter a Node or an Identifier (long) of a Node must be given!");
    }

    //result type
    public static class ImpactResult {
        // yield
        public final Node node;
        public final String state;

        public ImpactResult(Node node, State state) {
            this.node = node;
            this.state = state.name();
        }
    }
}