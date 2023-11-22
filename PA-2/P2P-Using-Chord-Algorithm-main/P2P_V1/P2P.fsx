module P2P

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
    | StartRequesting
    | RequestFwd of int*IActorRef*int
    | Request of IActorRef*int
    | SetFingerTable of Map<int,IActorRef>*Map<int,int>
    | Receipt of int
    | Join of int*IActorRef
    | UpdateFingerTable of Map<int, IActorRef>*Map<int,int>
    | UpdateSuccessors
    | RequestFingerTables of IActorRef
    | StartUpdation

let system = ActorSystem.Create("System")

//Basic Actor Structure
type ProcessController(nodes: int, requests: int) =
    inherit Actor()
    //Define required variables here
    let totalMessages = nodes*requests
    let mutable completedMessages = 0
    let mutable totalHops = 0


    override x.OnReceive(receivedMsg) =
        match receivedMsg :?> Message with 
            | RequestCompletion x ->
                completedMessages <- completedMessages + 1
                totalHops <-totalHops + x
                if(completedMessages = totalMessages) then
                    printfn $"All the nodes have completed the number of requests to be made with {(float(totalHops)/float(totalMessages))} average hops"
                    Environment.Exit(-1)
            | _ -> ()

type Peer(processController: IActorRef, requests: int, numNodes: int, PeerID: int, N: int) =
    inherit Actor()
    //Define required variables here
    let totalPeers = numNodes
    let mutable fingerTable = Map.empty<int, IActorRef>
    let mutable fingerPeerID = Map.empty<int, int>
    let mutable msgQty = 0
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
            | None -> true

    let checkDistance (currentKey: int) (curTableEntry: int) (incomingEntry: int) =
        //Function to measure table entry distance
        let currentDistance =
            if(curTableEntry = 0) then
                int(2. ** float N) - currentKey
            elif(currentKey < curTableEntry) then
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
                actor <! Receipt(hops)

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
                Actor.Context.System.Scheduler.ScheduleTellRepeatedly(TimeSpan.FromSeconds(0.), TimeSpan.FromSeconds(5.), Actor.Context.Self, UpdateSuccessors)
                
            | SendRequest ->
                if msgQty >= requests then
                    ()
                let randomPeer = Random().Next(totalPeers)
                match fingerTable.TryFind(randomPeer) with
                    | Some actor ->
                        if actor <> null then
                            actor <! Request(Actor.Context.Self, 1)
                            msgQty <- msgQty + 1
                        else
                            let mutable closest = -1
                            fingerTable |> Map.iter (fun _key _value -> if (_key < randomPeer || _key > closest) && _value<>null then closest <- _key)
                            fingerTable.[closest] <! RequestFwd(randomPeer, Actor.Context.Self, 1)
                            msgQty <- msgQty + 1
                    | None ->
                        let mutable closest = -1
                        fingerTable |> Map.iter (fun _key _value -> if (_key < randomPeer || _key > closest) && _value<>null then closest <- _key)
                        fingerTable.[closest] <! RequestFwd(randomPeer, Actor.Context.Self, 1)
                        msgQty <- msgQty + 1
                    
            | SetFingerTable (x, y)->
                fingerTable <- x
                fingerPeerID <- y

            | Receipt x->
                processController <! RequestCompletion(x)
                
            | Join (peerID, peer) ->
                // printfn $"Peer {peerID} has joined the ring"
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
                fingerPeerID |> Map.iter (fun key' value' ->
                    let mutable lowestValue =
                        if value' = -1 then
                            int (2. ** float N)
                        else
                            value'
                    let mutable leastKey = int (2. ** float N)
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
                let curActor = Actor.Context.Self
                fingerTable |> Map.iter (fun key' value' ->
                    if value' <> null && key'<>PeerID then
                        value' <! UpdateFingerTable(fingerTable, fingerPeerID)
                        value' <! RequestFingerTables(curActor)
                    )
            
            | RequestFingerTables x ->
                x <! UpdateFingerTable(fingerTable, fingerPeerID)

            | _ -> ()

//Actual Working starts here
let mutable numNodes = int (string (fsi.CommandLineArgs.GetValue 1))
let numRequests = int (string (fsi.CommandLineArgs.GetValue 2))

let processController = system.ActorOf(Props.Create(typeof<ProcessController>, numNodes, numRequests),"processController")
let nearestPower n=
    if ((n > 0) && (n &&& (n-1) = 0)) then
        int (Math.Log (float n)) + 1
    else
        let mutable count = 0
        let mutable x = n
        while (x <> 0) do
            x <- x >>> 1
            count <- count + 1
        count
let nearestPow = nearestPower numNodes
let ringCapacity = int (2.**float nearestPow)

printfn $"Nearest Power: {nearestPow}, Ring Capacity: {ringCapacity}"

let ring = Array.zeroCreate(numNodes)

for i in [0 .. numNodes-1] do
    ring.[i] <- system.ActorOf(Props.Create(typeof<Peer>, processController, numRequests, numNodes, i, nearestPow), "Peer" + string i)

printfn $"Ring Size {ring.Length}"
//Empty FingerTable Initialization
for i in [0 .. numNodes-1] do
    let mutable fingers = Map.empty<int, IActorRef>
    let mutable peerIDTable = Map.empty<int, int>
    fingers <- fingers |> Map.add i ring.[i]
    peerIDTable <- peerIDTable |> Map.add i i
    for j in [0 .. nearestPow - 1] do
        let x = (i + int (2. ** float j)) % int (2.** float nearestPow)
        fingers <- fingers |> Map.add x ring.[0]
        peerIDTable <- peerIDTable |> Map.add x 0
    ring.[i] <! SetFingerTable(fingers, peerIDTable)

let baseActor = ring.[0]
for i in [1 .. numNodes-1] do
    baseActor <! Join(i, ring.[i])

for i in [0 .. numNodes-1] do
    ring.[i] <! StartUpdation
    

for i in [5 .. 0] do
    printfn $"The message sending will commence in T minus {i}s"
    System.Threading.Thread.Sleep(TimeSpan.FromSeconds(1.))
    
printfn "Commence Message Sending"
for i in [0 .. numNodes-1] do
    ring.[i] <! StartRequesting

Console.ReadLine()