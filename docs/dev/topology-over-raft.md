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

write_only - writes are going to new and old replica, but reads are from
             old replicas still
read_write - writes still going to old and new replicas but reads are
             from new replica
owner      - tokens are owned by the node and reads and write go to new
             replica set only

Tokens that needs to be move start in 'write_only' state. After entire
cluster learns about it streaming start. After the streaming tokens move
to 'read_write' state and again the whole cluster needs to learn about it
and make sure no reads started before that point exist in the system.
After that tokens may move to the 'owner' state.

topology_request is the field through which a topology operation request
can be issued to a node. A request is one of the topology operation
currently supported: join, leave, replace or remove. A request may also
have parameters associated with it.

# Topology state persistence table

The in memory state's machine state is persisted in a local table system.topology.
The schema of the tables is:

CREATE TABLE system.topology (
    host_id uuid PRIMARY KEY,
    datacenter text,
    node_state text,
    rack text,
    release_version text,
    replaced_id uuid,
    tokens set<text>,
    replication_state text,
    topology_request text
)

Each node has a row in the table where its host_id is the primary key. The row contains:
 host_id            -  id of the node
 datacenter         -  a name of the datacenter the node belongs to
 rack               -  a name of the rack the node belongs to
 release_version    -  release version of the Scylla on the node
 node_state         -  current state of the node
 topology_request   -  if set contains one of the supported topology requests
 tokens             -  if set contains a list of tokens that belongs to the node
 replication_state  -  if set contains a state the state the token replication is now in
 replaced_id        -  if the node replacing or replaced another node here will be the id of that node
