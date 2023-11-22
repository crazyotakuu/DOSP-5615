#time "on"
#r "nuget: Akka.FSharp"
#r "nuget: Akka.TestKit"

open System
open Akka.Actor
open Akka.FSharp
//open Akka.Configuration
//open System.Reflection

type Message =
    | A of int
    | RequestCompletion of int
    | SendRequest
    | ExitCircle of IActorRef // This can also essentially have just the id to the current node
    | StartRequesting //This message will start the scheduler which will then start sending request messages
    | RequestFwd of int*IActorRef*int
    | Request of IActorRef*int
    | SetFingerTable of Map<int,IActorRef>*Map<int,int>
    | SetRequests of int
    | Receipt
    | Join of int*IActorRef
    | UpdateFingerTable of Map<int, IActorRef>*Map<int,int>
    | UpdateSuccessors
    | RequestFingerTables of IActorRef
    | PrintFinger
    | StartUpdation

let system = ActorSystem.Create("System")

//Basic Actor Structure
type ProcessController(nodes: int) =
    inherit Actor()
    //Define required variables here
    let totalNodes = nodes
    let mutable completedNodes = 0
    let mutable totalHops = 0
    let mutable requests = 0

    override x.OnReceive(receivedMsg) =
        match receivedMsg :?> Message with 
            | RequestCompletion x ->
                completedNodes <- completedNodes + 1
                totalHops <-totalHops + x
                if(completedNodes = totalNodes) then
                    printfn $"All the nodes have completed the number of requests to be made with {(float(totalHops)/float(requests*totalNodes))} average hops"
                    Environment.Exit(-1)
            | SetRequests requests' ->
                requests <- requests'
            | _ -> ()

type Peer(processController: IActorRef, requests: int, numNodes: int, PeerID: int, N: int) =
    inherit Actor()
    //Define required variables here
    let totalPeers = numNodes
    let mutable cancelRequesting = false
    //HashTable of the type <Int, Peer>
    let mutable fingerTable = Map.empty<int, IActorRef>
    let mutable fingerPeerID = Map.empty<int, int>
    let mutable totalHops = 0
    //Counter to keep track of message requests sent by the given peer
    let mutable messageReceipts = 0
    
    let replace (key: int) (peerID: int) (currentPeerID: int) =
        let mutable distance = 0
        if(currentPeerID < peerID) then
            distance <- peerID - currentPeerID
        else
            distance <- currentPeerID + int(2. ** float N) - peerID
        
        let mutable closest = int(2. ** (Math.Log2(float distance)/Math.Log2(2.)))
        closest <- (closest + currentPeerID) % N
        match fingerPeerID.TryFind(closest) with
            | Some x ->
                if x = -1 then
                    true
                else
                    not(fingerPeerID.[closest] < peerID)
            | None -> true //This condition will, for the most part, never be accessed

    let checkDistance (currentKey: int) (curTableEntry: int) (incomingEntry: int) =
        //Fujction to measure table entry distance
        let currentDistance =
            if(currentKey < curTableEntry) then
                curTableEntry - currentKey
            else
                currentKey + int(2. ** float N) - curTableEntry
        let incomingDistance =
            if(currentKey < incomingEntry) then
                incomingEntry - currentKey
            else
                currentKey + int(2. ** float N) - incomingEntry
        if currentKey = curTableEntry then
            false
        elif currentKey = incomingEntry then
            true
        else
            incomingDistance < currentDistance

    override x.OnReceive(receivedMsg) =
        match receivedMsg :?> Message with
            | Request (actor, hops) ->
                // Function when the request is received at the receiving peer
                totalHops <- totalHops + hops
                actor <! Receipt

            | RequestFwd (reqID, requestingPeer, hops) ->
                match fingerTable.TryFind(reqID) with
                    | Some actor ->
                        if actor <> null then
                            actor <! Request(requestingPeer, hops + 1)
                        else
                            let mutable closest = -1
                            fingerTable |> Map.iter (fun _key _value -> if (_key < reqID || _key > closest) && _value<>null then closest <- _key)
                            fingerTable.[closest] <! RequestFwd(reqID, requestingPeer, hops + 1)

                    | None ->
                        let mutable closest = -1
                        fingerTable |> Map.iter (fun _key _value -> if (_key < reqID || _key > closest) then closest <- _key)
                        fingerTable.[closest] <! RequestFwd(reqID, requestingPeer, hops + 1)

            | StartRequesting ->
                //Starts Scheduler to schedule SendRequest Message to self mailbox
                Actor.Context.System.Scheduler.ScheduleTellRepeatedly(TimeSpan.FromSeconds(0.), TimeSpan.FromSeconds(1.), Actor.Context.Self, SendRequest)
            
            | StartUpdation ->
                Actor.Context.System.Scheduler.ScheduleTellRepeatedly(TimeSpan.FromSeconds(0.), TimeSpan.FromSeconds(1.), Actor.Context.Self, UpdateSuccessors)
                
            | SendRequest ->
                //Send a request for a random peer over here
                if messageReceipts >= requests then
                    ()
                    
                let randomPeer = Random().Next(totalPeers)
                
                match fingerTable.TryFind(randomPeer) with
                    | Some actor ->
                        if actor <> null then
                            actor <! Request(Actor.Context.Self, 1)
                        else
                            let mutable closest = -1
                            fingerTable |> Map.iter (fun _key _value -> if (_key < randomPeer || _key > closest) && _value<>null then closest <- _key)
                            fingerTable.[closest] <! RequestFwd(randomPeer, Actor.Context.Self, 1)
                    | None ->
                        let mutable closest = -1
                        fingerTable |> Map.iter (fun _key _value -> if (_key < randomPeer || _key > closest) && _value<>null then closest <- _key)
                        fingerTable.[closest] <! RequestFwd(randomPeer, Actor.Context.Self, 1)
                    
            | SetFingerTable (x, y)->
                fingerTable <- x
                fingerPeerID <- y

            | Receipt ->
                messageReceipts <- messageReceipts + 1
                if(messageReceipts = requests) then
                        cancelRequesting <- true
                        processController <! RequestCompletion(totalHops)
                printfn "Message Received at designated peer"
                
            | Join (peerID, peer) ->
                printfn $"Peer {peerID} has joined the ring"
                fingerTable |> Map.iter (fun _key _value ->
                    if (replace _key peerID PeerID && _key <> PeerID) || _key = peerID then
                        fingerTable <- fingerTable |> Map.add _key peer
                        fingerPeerID <- fingerPeerID |> Map.add _key peerID
                )
                fingerTable |> Map.iter (fun x y ->
                    if x <> PeerID && y <> null then
                        y <! UpdateFingerTable(fingerTable, fingerPeerID))
                peer <! UpdateFingerTable(fingerTable, fingerPeerID)
                
            | UpdateFingerTable (fingers, fingerPeers) ->
                //Function that takes an incoming PeerTable and matches it with the current peer table and updates values
                let mutable updateSuccessorRequest = false
                
                //This needs to be updated and replace function needs to be implemented here for updation
                fingerPeerID |> Map.iter (fun key' value' ->
                    let mutable lowestValue =
                        if value' = -1 then
                            int (2. ** float N)
                        else
                            value'
                    let mutable leastKey = int (2. ** float N)
                    //Implement something using distance function here
                    fingerPeers |> Map.iter (fun _key _value ->
                        if _value <> -1 && checkDistance key'  value' _value then
                            lowestValue <- _value
                            leastKey <- _key
                    )
                    if leastKey <> (int (2. ** float N)) && key'<>value' then
                        fingerPeerID <- fingerPeerID |> Map.add key' lowestValue
                        fingerTable <- fingerTable |> Map.add key' fingers.[leastKey]
                        updateSuccessorRequest <- true
                )
                if updateSuccessorRequest then
                    Actor.Context.Self <! UpdateSuccessors
            
            | UpdateSuccessors ->
                let curActor = Actor.Context.Self //I tried to put this in the message itself but for some reason it didn't except it
                fingerTable |> Map.iter (fun key' value' ->
                    if value' <> null && key'<>PeerID then
                        //printfn $"Peer {PeerID} sent request to peer {fingerPeerID.[key']}"
                        value' <! RequestFingerTables(curActor)
                    )
            
            | RequestFingerTables x ->
                x <! UpdateFingerTable(fingerTable, fingerPeerID)
                
            | PrintFinger ->
                printfn $"I am peer {PeerID}"
                printfn "%A" fingerTable
                printfn "%A" fingerPeerID
            | _ -> ()

//Actual Working starts here
let mutable numNodes = 15000//int (string (fsi.CommandLineArgs.GetValue 1))
let numRequests = 10//int (string (fsi.CommandLineArgs.GetValue 2))

let processController = system.ActorOf(Props.Create(typeof<ProcessController>, numNodes),"processController")

//If there needs to be any modification in the number of nodes, please do so here
//Function to return greater or equal nearest power of 2 for the table
let nearestPower n=
    if ((n > 0) && (n &&& (n-1) = 0)) then
        n
    else
        let mutable count = 0
        let mutable x = n
        while (x <> 0) do
            x <- x >>> 1
            count <- count + 1
        count

let nearestPow = nearestPower numNodes
let ringCapacity = int (2.**float nearestPow)
//numNodes <- ringCapacity

printfn $"Nearest Power: {nearestPow}, Ring Capacity: {ringCapacity}"
//_______________________________________________________________________________

processController <! SetRequests(numRequests)
//Initializing the entire ring as an array for now, until further progress

let ring = Array.zeroCreate(numNodes)

for i in [0 .. numNodes-1] do
    ring.[i] <- system.ActorOf(Props.Create(typeof<Peer>, processController, numRequests, numNodes, i, nearestPow), "Peer" + string i)

//Empty FingerTable Initialization
for i in [0 .. numNodes-1] do
    let mutable fingers = Map.empty<int, IActorRef>
    let mutable peerIDTable = Map.empty<int, int>
    fingers <- fingers |> Map.add i ring.[i]
    peerIDTable <- peerIDTable |> Map.add i i
    for j in [0 .. nearestPow - 1] do
        let x = (i + int (2. ** float j)) % int (2.** float nearestPow)
        fingers <- fingers |> Map.add x null
        peerIDTable <- peerIDTable |> Map.add x -1
//    printfn "%A" fingers
    ring.[i] <! SetFingerTable(fingers, peerIDTable)

//At this point we pick a base peer, which will be the entry point for all other peers with empty peer tables and then
//once the process of adding peers is complete, we will start sending of messages to all peers in which case even if a
//peer drops dead then the other peers will have all the info they need to continue the entire process

let baseActor = ring.[0]
for i in [1 .. numNodes-1] do
//    System.Threading.Thread.Sleep(TimeSpan.FromSeconds(1.))
    baseActor <! Join(i, ring.[i])

//Debugging Code
//baseActor <! Join (7, ring.[7])
//System.Threading.Thread.Sleep(TimeSpan.FromSeconds(1.))
//baseActor <! PrintFinger
System.Threading.Thread.Sleep(TimeSpan.FromSeconds(3.))
//ring.[7] <! PrintFinger
printfn "Starting table updation"
for i in [0 .. numNodes-1] do
//    System.Threading.Thread.Sleep(TimeSpan.FromSeconds(1.))
    ring.[i] <! StartUpdation
    
System.Threading.Thread.Sleep(TimeSpan.FromSeconds(10.))

//for i in [0 .. numNodes-1] do
//    System.Threading.Thread.Sleep(TimeSpan.FromSeconds(1.0))
//    ring.[i] <! PrintFinger
printfn "Commence Message Sending"
for i in [0 .. numNodes-1] do
    ring.[i] <! StartRequesting

Console.ReadLine()