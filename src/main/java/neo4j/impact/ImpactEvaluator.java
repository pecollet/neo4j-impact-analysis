package neo4j.impact;


import org.neo4j.graphdb.*;
import org.neo4j.graphdb.traversal.Evaluation;
import org.neo4j.graphdb.traversal.Evaluator;
import org.neo4j.internal.helpers.collection.Pair;
import java.util.*;


public class ImpactEvaluator implements Evaluator {//extends PathEvaluator.Adapter<Integer>  {
    HashMap<Node, Impact.State> nodesMap = new HashMap<Node, Impact.State>();
    List<Node> startNodes;
    List<Pair<RelationshipType, Direction>> relsAndDirs;
    long resultLimit;
    long timeoutMs;
    long startTime;

    public ImpactEvaluator(List<Node> startNodes, HashMap nodesMap, List<Pair<RelationshipType, Direction>> relsAndDirs,
                            long resultLimit, long timeout) {
        this.nodesMap = nodesMap ;
        this.startNodes = startNodes ;
        this.relsAndDirs=relsAndDirs ;
        this.resultLimit=resultLimit ;
        this.timeoutMs = timeout * 1000 ;
        this.startTime = System.currentTimeMillis();
    }

    @Override
    public Evaluation evaluate(Path path) {
        //bail out if time or result limit reached
        if (this.timeoutMs > 0 && (System.currentTimeMillis()-this.startTime >= this.timeoutMs)) {
            return Evaluation.EXCLUDE_AND_PRUNE;
        }
        if (this.resultLimit > 0 &&  (this.nodesMap.size() >= this.resultLimit)) {
            return Evaluation.EXCLUDE_AND_PRUNE;
        }
        Node node = path.endNode();
        //System.out.println("Evaluating end node ("+node.getId() +") from path " + path.toString());

        //start nodes are FAILED, no need to compute their incoming impact state
        if (this.startNodes.contains(node)) {return Evaluation.INCLUDE_AND_CONTINUE;}

        //if node already exists as failed, it (& its impacted nodes) can't change state anymore. bail out already
        if (this.nodesMap.getOrDefault(node, Impact.State.WORKING) == Impact.State.FAILED ) {
            return Evaluation.EXCLUDE_AND_PRUNE;
        }

        HashMap<String, List> groups= new HashMap<String, List>();

        //for each impact relationType
        for (Pair<RelationshipType, Direction> relAndDir : this.relsAndDirs) {
            //for each relation of that type
            for (Relationship r : node.getRelationships(relAndDir.other().reverse(), relAndDir.first())) {
                //get the state of the impacting node (default to WORKING if node is not in nodesMAp)
                Impact.State incomingState = nodesMap.getOrDefault(r.getOtherNode(node), Impact.State.WORKING);
                //assign relation to a group, and add the incomingState to that group
                String groupKey = computeImpactGroupKey(r);
                List groupStates = groups.getOrDefault(groupKey, new ArrayList());
                groupStates.add(incomingState);
                groups.put(groupKey, groupStates);
                //System.out.println("    evaluating incoming relation from (" + r.getOtherNodeId(node.getId()) + ") : " + groupKey + " = " + incomingState);
            }
        }
        //iterate over groups in groups, and compute the group state, then get the worst computed state of all groups
        Impact.State worstState = Impact.State.WORKING;
        for(Map.Entry<String, List> entry : groups.entrySet()) {
            String groupKey = entry.getKey();
            List<Impact.State> states = entry.getValue();
            Impact.State groupState = computeImpactGroupState( groupKey, states);
            worstState = (groupState.getStateValue() > worstState.getStateValue()) ? groupState : worstState;
        }
        //System.out.println("    impact groups : "+groups.toString()+" => node state : "+worstState);
        this.nodesMap.put(node, worstState);

        return Evaluation.INCLUDE_AND_CONTINUE;
    }

    //compute a group key for the relation, based on impact_propagation and impact_group properties
    String computeImpactGroupKey(Relationship r) {
        Impact.PropagationRule propagationRule  = Impact.PropagationRule.validate((String) r.getProperty("impact_propagation", null));
        //if no impact_group specified : they're all together in a default group (per propagation rule) : ""
        return propagationRule.toString()+ "-" + r.getProperty("impact_group", "")   ;
    }

    Impact.State computeImpactGroupState (String key, List<Impact.State> states) {
        if (key.startsWith(Impact.PropagationRule.MOST_SEVERE.toString())) {
            return Collections.max(states, (a,b) -> a.compare(b));
        } else if (key.startsWith(Impact.PropagationRule.LEAST_SEVERE.toString())) {
            return Collections.min(states, (a,b) -> a.compare(b));
        } else if (key.startsWith(Impact.PropagationRule.PROTECTION.toString())) {
            int numWorking = Collections.frequency(states, Impact.State.WORKING);
            return (numWorking == 1) ? Impact.State.AT_RISK : Collections.min(states, (a,b) -> a.compare(b));
        } else if (key.startsWith(Impact.PropagationRule.DEGRADATION.toString())) {
            Impact.State maxState = Collections.max(states, (a,b) -> a.compare(b));
            return (maxState.getStateValue() > Impact.State.DEGRADED.getStateValue() ) ? Impact.State.DEGRADED : maxState ;
        } else if (key.startsWith(Impact.PropagationRule.RISK_PROPAGATION.toString())) {
            Impact.State maxState = Collections.max(states, (a,b) -> a.compare(b));
            return (maxState.getStateValue() > Impact.State.AT_RISK.getStateValue() ) ? Impact.State.AT_RISK : maxState ;
        } else {
            return Impact.State.WORKING;
        }
    }

}
