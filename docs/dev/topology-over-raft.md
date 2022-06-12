# Topology state machine

The topology state machine tracks all the nodes in a cluster,
their state, properties (topology, tokens, etc) and requested actions.

Node state can be one of those:
 none             - the node is not yet in the cluster
 bootstrapping    - the node is currently bootstrapping
 decommissioning  - the node is being decommissioned
 removing         - the node is being removed
 replacing        - the node is replacing another node
 normal           - the node is working normally
 left             - the node is left the cluster

Nodes in state left are never removed from the state.

A state may have additional parameters associated with it. For instance
'replacing' state has host id of a node been replaced as a parameter.

Tokens also can be in one of the states:

write_both_read_old - writes are going to new and old replica, but reads are from
             old replicas still
write_both_read_new - writes still going to old and new replicas but reads are
             from new replica
owner      - tokens are owned by the node and reads and write go to new
             replica set only

Tokens that needs to be move start in 'write_both_read_old' state. After entire
cluster learns about it streaming start. After the streaming tokens move
to 'write_both_read_new' state and again the whole cluster needs to learn about it
and make sure no reads started before that point exist in the system.
After that tokens may move to the 'owner' state.

topology_request is the field through which a topology operation request
can be issued to a node. A request is one of the topology operation
currently supported: join, leave, replace or remove. A request may also
have parameters associated with it.
