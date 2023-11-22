module CommonUtil

open System
open System.Collections.Generic
open System.Security.Cryptography

let chordNodeName = "ChordName"
let chordSupervisiorName = "ChordSupervisior"
let chordSystemName = "ChordSimulator";
let nodeActorName = "ChordNode";
let unsignPrefix: byte[] = Array.zeroCreate 1

type LookupType = 
    | Simple
    | Scalable


type Message = 
    | SendRequest
    | Request of (int*string*int)
    | Response of (bool*int*int)
    | ActorConverged of (string*float)
    | InitActors of (Dictionary<string, SortedDictionary<int, string>>*int*int*LookupType)
    | Join of (LookupType)
    | Notify of (string*int)
    | InitRefresh
    | Refresh of (SortedDictionary<int, string>)
    | PredecessorRequest
    | PredecessorResponse of (string*int)


let computeNodePosition (nodeName: string) (m: int) =
    let hashBytes: byte[] = System.Text.Encoding.ASCII.GetBytes(nodeName) |> HashAlgorithm.Create("SHA1").ComputeHash
    let mutable hashInt: bigint = System.Numerics.BigInteger(Array.concat [ hashBytes ; unsignPrefix ])
    hashInt % System.Numerics.BigInteger(2.0 ** float m)


let createNodes (nodeCount: int) (chordSystem: Akka.Actor.ActorSystem) (m: int) = 
    let nodes: SortedDictionary<int, string> = new SortedDictionary<int, string>()
    for i in 1..nodeCount do
        let mutable nodeName = chordNodeName + Guid.NewGuid().ToString()
        let mutable nodePosition = computeNodePosition nodeName m |> int
        while nodes.ContainsKey(nodePosition) do
            nodeName <- chordNodeName + Guid.NewGuid().ToString()
            nodePosition <- computeNodePosition nodeName m |> int
        nodes.Add(nodePosition, nodeName)
    nodes.Add(int(2.0 ** float m), chordNodeName + Guid.NewGuid().ToString())
    nodes


let computeFingerTable (nodeIndex: int) (noodeName: string) (m: int) (nodeIndices: int list) (nodes: SortedDictionary<int, string>) = 
    let lastNode = nodeIndices |> List.rev |> List.head
    let mutable fingerTable: SortedDictionary<int, string> = new SortedDictionary<int, string>();
    let mutable counter = 0
    let mutable endReached: bool = false
    let mutable lastSuccessor = 0
        
    while counter < m && not(endReached) do
        let successor: int = nodeIndex + int(2.0 ** float counter) 
        counter <- counter + 1
        if (successor >= lastNode) then
            endReached <- true
        else 
            if (Seq.exists ((=) successor) nodeIndices) then
                if (successor <> lastSuccessor) then
                    lastSuccessor <- successor
                    fingerTable.Add(successor, nodes.[successor])
            else
                let mutable nodeFound: bool = false
                let mutable indexCounter = 0
                while not(nodeFound) && indexCounter < nodeIndices.Length do
                    let tempIndex = nodeIndices.[indexCounter]
                    indexCounter <- indexCounter + 1
                    if tempIndex > successor then
                        if (tempIndex <> lastSuccessor) then
                            fingerTable.Add(tempIndex, nodes.[tempIndex])
                            lastSuccessor <- tempIndex
                        nodeFound <- true
    fingerTable


let computeAllFingerTables (nodes: SortedDictionary<int, string>) (m: int) (nodesCount: int) = 
    let nodeIndices = nodes.Keys |> Seq.toList
    let fingerTableDict: Dictionary<string, SortedDictionary<int, string>> = new Dictionary<string, SortedDictionary<int, string>>()
    let mutable nodeCounter = 0

    for nodeNamePair in nodes do
        if (nodeCounter < nodesCount) then
            nodeCounter <- nodeCounter + 1
            let fingerTable = computeFingerTable nodeNamePair.Key nodeNamePair.Value m nodeIndices nodes        
            fingerTableDict.Add(nodeNamePair.Value, fingerTable)

    fingerTableDict


let generateNewNode (nodes: SortedDictionary<int, string>) (m: int) = 
    let mutable actorName = chordNodeName + Guid.NewGuid().ToString()
    let mutable position = int(computeNodePosition actorName m)
    while nodes.ContainsKey(int(position)) do
        actorName <- chordNodeName + Guid.NewGuid().ToString()
        position <- int(computeNodePosition actorName m)
    let nodeIndices = nodes.Keys |> Seq.toList
    let fingerTable = computeFingerTable position actorName m nodeIndices nodes
    let successorPos = (fingerTable.Keys |> Seq.toList).[0]
    let mutable successorName: string = nodes.[successorPos]
    (actorName, int(position), fingerTable, successorName, successorPos)
                            

let arrToString (input: int[]) = 
    let mutable output = "["
    for i in input do
        output <- output+(i.ToString()) + ","
    output <- output + "]"
    output
                    
            


