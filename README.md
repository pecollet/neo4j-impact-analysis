# neo4j.impact.analysis
stored procedures to perform impact analysis on a neo4j graph

## Procedure **neo4j.impact.compute**
Computes impacts from the start node(s), following the given impact relationships, within the specified limits.

## Usage
` CALL neo4j.impact.compute(start <id>|Node|list, 'TYPE_OUT>|<TYPE_IN', limits)`
### parameters 
* _start_ : `<id> | Node | list of Node | list of <id>`
* _relationshipFilter_ : string with format `'[<]RELATIONSHIP_TYPE1[>]|[<]RELATIONSHIP_TYPE2[>]|...'`

  RELATIONSHIP_TYPES **must** be directional : incoming (`<`) or outgoing (`>`), with outgoing assumed if direction is not specified. 
  
  Defaults to `IMPACTS>`.
* _limits_ : string with format `'<hopLimit>hops,<timeout>s,<resultLimit>results'`

  Allows limiting :
   * hop-depth of the traversals (ex: `'5hops'`), 
   * execution time in seconds (ex : `'10s'`) 
   * number of results (ex: `'1000results'`)
   
   or any comma-separated combination of them (will stop at whichever limit happens first). 
   
   Defaults to `'10hops,60s,1000results'`.
### output
Returns the impacted nodes and their state. 
* Impacted nodes are any node related, directly or indirectly, to the _start_ nodes, via relationships matching _relationshipFilter_, in the direction of impact. 
* Node states are one of the following (from least to most severe) : 'WORKING', 'AT_RISK', 'DEGRADED', 'FAILED'
  * 'WORKING' is the default state for all nodes in the graph. In the results of the procedure, only non-WORKING nodes are returned.
  * 'FAILED' is the state of the nodes designated by the _start_ parameter.
  * the states of the impacted nodes are computed according to the state propagation rules (see below), starting from the _start_ node(s).


### call examples
**Impacts from a single node, using the default :IMPACTS relationship type, and default limits :**
```
MATCH (a:Node) WHERE a.name='A'  
CALL neo4j.impact.compute(a) yield node, state 
RETURN  node, state
```
=> expects a model that looks like (a)-[:IMPACTS*]->()

---
**Impacts from a set of nodes :** 
```
MATCH (a:Node) WHERE n.name='A'
MATCH (b:Node) WHERE n.name='B'
CALL neo4j.impact.compute([a, b]) yield node, state " +
RETURN  node, state
```
---
**Impacts from a single node, with custom relationships :**
```
MATCH (a:Node) WHERE n.name='A'
CALL neo4j.impact.compute(a, '<CARRIED_BY|<USES') yield node, state " +
RETURN  node, state
```
=> expects a model that looks like (a)<-[:CARRIED_BY*]-() or (a)<-[:USES*]-() 

---

**Impacts from a single node, with custom limits :**
```
MATCH (a:Node) WHERE a.name='A'  
CALL neo4j.impact.compute(a, null, '5hops,20s') yield node, state 
RETURN  node, state
```
---
## State propagation 
The impact relationships are all the relationships in the model of the types specified with parameter _relationshipFilter_, 
or, if omitted, of the default type :IMPACTS.

(impactingNode)-[impactRel]->(impactedNode)

When computing the state of an impacted node, all the incoming impact relationships are evaluated in the following way :
 * incoming impact relationships are grouped, based on their _impact_propagation_ + _impact_group_ properties.
 * within each group, a resulting state is computed based on the propagation rule _impact_propagation_
 * the worst of all group states will be the impacted node state 

### Impact relationships properties :
All are optional
 * _impact_propagation_ : propagation rule. One of [MOST_SEVERE, DEGRADATION, RISK_PROPAGATION, LEAST_SEVERE, PROTECTION] 
   
   Specifies how to propagate the state from the impacting nodes to the impacted node
   * MOST_SEVERE (default) : the resulting state will be the most severe of the states of the impacting nodes (within the group)
   * DEGRADATION : same as MOST_SEVERE, except that the resulting state can only be DEGRADED at worst 
   * RISK_PROPAGATION : same as MOST_SEVERE, except that the resulting state can only be AT_RISK at worst
   * LEAST_SEVERE : the resulting state will be the least severe of the states of the impacting nodes (within the group)
   * PROTECTION : the resulting state will be AT_RISK if only one impacting nodes is WORKING. Otherwise it will be the least severe state. 
     That rule aims to simulate protection/redundancy situations, where the failure of one or several impacting nodes does not cause the failure of the impacted node as long as one impacting node still works. 
     But if there's only 1 impacted node left working, it can be useful to know there's no more protection left, hence the AT_RISK state.
 * _impact_group_ : a string or number to identify (together with _impact_propagation_) a group of relations, in the context of the same impacted node. If absent, the 'null' group is assumed. 

## Model examples
Here are a few example of data models.
### simple default model
![Simple model](https://github.com/pecollet/neo4j-impact-analysis/raw/master/pics/simple-model.svg "Simple Model")
To compute impacts on such a model : `CALL neo4j.impact.compute(start)`

### grouping & protection
![Grouping & protection](https://github.com/pecollet/neo4j-impact-analysis/raw/master/pics/grouping-protection.svg "Model with grouping & protection")
To compute impacts on such a model : `CALL neo4j.impact.compute(start, '<RUNS_ON|<HOSTED_ON|<DEPENDS_ON')`
