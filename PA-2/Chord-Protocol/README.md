Chord Simulator


## Overview
In this project, we will be simulating chord protocol using FSharp Akka.net framework. We will be simulating the protocol using the algorithm mentioned in this paper - https://pdos.csail.mit.edu/papers/ton:chord/paper-ton.pdf

## Chord Protocol
Chord is a protocol and an algorithm for peer-to-peer distributed hash table. A distributed hash table is used to store the key-value pairs, where each key represents a node or data item in the system. A node stores values of all the data items with the keys for which it is responsible for. Chord protocol specifies how the nodes are positioned by assigning specific keys to them, and how a node can locate a value for a given key by first locating the node responsible for that key.

## Implementation
The implementation of our simulator consists of three sections.

### 1. Nodes creation
We have set the value of m to 15. So, the size of our identifier cirle which is computed as 2<sup>m</sup> is 2<sup>15</sup> = 32768. Thus, we have 32768 available positions available on our Chord Circle where we can place our nodes and the keys of data items. Based on nodeCount input from the command line, we will create that many Akka.net actors and compute position of each actor on the Chord circle. We generated random unique name for each actor, took its SHA1 hash and took its modulo with the identifier space size to calculate its position on the Chord Circle.
  * Node Position = SHA1(node_name) % 2<sup>m</sup>
  
 Once node positions are determined for all the actors, we computed the finger table of each node as per the algorithm mentioned in the <a href="https://pdos.csail.mit.edu/papers/ton:chord/paper-ton.pdf" target="_blank">reference paper</a>. A finger table stores key-value pairs, where each key represents the position of certain node in the system. Each finger table consists of maximum O(log n) key-values where n is number of nodes in the system. Once all the actors are initalized, we schedule each actor to make a request per second to find a random key in the system.

### 2. Key lookup
We have simulated two types of Key Lookups as mentioned in this <a href="https://pdos.csail.mit.edu/papers/ton:chord/paper-ton.pdf" target="_blank">reference paper</a>
  1. Simple Key Location - This is a simple linear search algorithm. In this algorithm, the node who is initiating the lookup sends a request to its successor and waits for the response. If the index of the lookup request is greater than the successor position, then the successor node forwards the request to its successor. If the successor node holds the requested index data, then it returns with the successful message else it returns with the failure response. These steps are recursivley executed until the end of the circle is reached or a failure message is returned. As we can see that, in the worst case we will traverse the whole network to find a data item which is not a very efficient solutionm. This algorithm works well if the identifier space and the number of nodes is small. As the identifier space and the node count increases, the lookup time will also increase.
  
  2. Scalable Key Location - This is extension over the Simple Key lookup algorithm for more efficient key lookup. The main issue with Simple Key lookup is that, each node only has the information about its successor node, thus it can forward the lookup request only to its neighbouring node. To solve this issue, each node maintains addition routing information in its routing table. A routing table holds routing information of maximum of O(log n) successive nodes. When initiating the lookup request, each node checks in its routing table if there is any closest successor node for the required data item and sends the request to that node. The same step is excecuted by each successor node until the last node of the chord circle is reached or data item is found. This drastically reduces the number of hops from O(n) to O(log n) to find a specific key on the Chord Circle.


### 3. New nodes joining and stabilization
Whenever a new node joins, we will first calculate its position on the Chord Circle using the same logic of generating a random name for the actor, taking its SHA1 hash and taking its modulo with the identifier space size. Suppose, we computed the new nodes position as <i>n</i> which is between n<sub>p</sub> and n<sub>s</sub> nodes. The node n will notify node n<sup>s</sup> and n<sup>s</sup> will set n as its predecessor. Periodically, we will send a stabilize message to all the actors. Whenever a actor receives a stabilize, message it will check its successor and update if required and also update its finger table. After this, all the nodes will have correct pointers for predecessor, successor and the nodes in the finger table.


## Instructions to run the simulation

 * ChordSimulator 1000 100

Command line params:
 1. numOfNode - number of nodes to be created
 2. numOfRequests - number of requests each node must perform
 3. lookupAlgorithm - simple/scalable. This is a optional parameter. By default the lookup algorithm will be scalable.

## Output
1. Simple Key lookup - 1010 Nodes 100 requests

     ```
     Each actors are done with processing the requests.
     Time elapsed to process the requests is 205503ms and average hops 248.96508
     ```
    
2. Scalable key lookup - 1010 Nodes 100 requests

     ```
     All actors are done with processing the requests.
     Time taken to process all the requests is 212503ms and average hops 5.730000
     ```
     

