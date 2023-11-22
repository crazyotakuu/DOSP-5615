#time "on"
#r "nuget: Akka.FSharp"
#r "nuget: Akka.TestKit"
#load "RandomString.fsx"

open System
open Akka.Actor
open Akka.FSharp
open Akka.Configuration
open RandomString

type Message = 
    | SetTotalNodes of int
    | RequestCompletion of int

type PeerMessage =
    | Init of int * string * string[] * Map<IActorRef, string>
    | InitFingerTable of Map<int, IActorRef>
    | SendRequest
    | ReceiveRequest of IActorRef * int * int * string
    | StartRequesting
    | RequestComplete of int    

let system = ActorSystem.Create("System")


type ProcessController(nodes : int) =
    inherit Actor()
    let mutable totalNodes = nodes
    let mutable completedNodes = 0
    let mutable numHops = 0

    override x.OnReceive(receivedMsg) =
        match receivedMsg :?> Message with
            | SetTotalNodes nodes ->
                totalNodes <- nodes
            | RequestCompletion hops ->
                completedNodes <- completedNodes + 1
                numHops <- numHops + hops
                //printfn "Complete numHops: %i" numHops
                if(completedNodes = totalNodes) then
                    let avgHops = (float numHops) / (float totalNodes)
                    printfn "All the nodes have completed the number of requests to be made"
                    printfn "Average number of hops: %.1f" avgHops
                    Environment.Exit(0)
                    //system.Terminate()
                    ()
            | _ -> ()



type Peer(processController: IActorRef, requests: int, numNodes: int) =
    inherit Actor()
    let totalRequests = requests
    let totalPeers = numNodes
    let mutable nodeID = 0
    let mutable messageRequests = 0
    let mutable message = ""
    let mutable allMessages = Array.zeroCreate(numNodes)
    let mutable messageTable = Map.empty<IActorRef, string>
    let mutable nodeLocation = ""
    let mutable totalHops = 0
    let mutable fingerTable = Map.empty<int, IActorRef>

    override x.OnReceive(receivedMsg) = 
        match receivedMsg :?> PeerMessage with
            | Init (id, msg, messages, messageMap) ->
                nodeID <- id
                nodeLocation <- "akka://system/user/Peer" + string nodeID
                message <- msg
                allMessages <- messages
                messageTable <- messageMap
                ()
            | InitFingerTable fingers ->
                fingerTable <- fingers
            | StartRequesting ->
                //Starts Scheduler to schedule SendRequest Message to self mailbox
                Actor.Context.System.Scheduler.ScheduleTellRepeatedly(TimeSpan.FromSeconds(0.), TimeSpan.FromSeconds(1.), Actor.Context.Self, SendRequest)
            | SendRequest ->
                //Send a request for a random peer over here
                let randomPeer = Random().Next(totalPeers)
                let desiredMsg = allMessages.[randomPeer]
                match fingerTable.TryFind(randomPeer) with
                | Some peer ->
                   let peerMsg = messageTable.[peer]
                   //printfn "desired: %s query: %s" desiredMsg peerMsg
                   if(peerMsg = desiredMsg) then
                       peer <! RequestComplete 0
                   else
                       let mutable closest = -1;
                       fingerTable |> Map.iter (fun _key _value -> if (_key < randomPeer || _key > closest) then closest <- _key)
                       fingerTable.[closest] <! ReceiveRequest (Actor.Context.Self, randomPeer, 0, desiredMsg)  
                | None ->
                    let mutable closest = -1;
                    fingerTable |> Map.iter (fun _key _value -> if (_key < randomPeer || _key > closest) then closest <- _key)
                    fingerTable.[closest] <! ReceiveRequest (Actor.Context.Self, randomPeer, 0, desiredMsg)
                //printfn "Send Node: %s: %i" nodeLocation messageRequests
                ()
            | ReceiveRequest (originalNode, desiredID, hops, desiredMsg) ->
                let numHops = hops + 1
                match fingerTable.TryFind(desiredID) with
                | Some peer ->
                    let peerMsg = messageTable.[peer]
                    printfn "desired: %s query: %s" desiredMsg peerMsg
                    if(peerMsg.Equals(desiredMsg)) then
                        peer <! RequestComplete numHops
                    else
                        let mutable closest = -1;
                        fingerTable |> Map.iter (fun _key _value -> if (_key < desiredID || _key > closest) then closest <- _key)
                        fingerTable.[closest] <! ReceiveRequest (originalNode, desiredID, numHops, desiredMsg)
                | None ->
                    let mutable closest = -1;
                    fingerTable |> Map.iter (fun _key _value -> if (_key < desiredID || _key > closest) then closest <- _key)
                    fingerTable.[closest] <! ReceiveRequest (originalNode, desiredID, numHops, desiredMsg)
                ()
            | RequestComplete hops ->
                messageRequests <- messageRequests + 1
                totalHops <- totalHops + hops
                printfn "%i" totalHops
                if(messageRequests >= requests) then
                    processController <! RequestCompletion totalHops
                ()
            | _ -> ()
    


//Actual Working starts here
let mutable numNodes = 2//int (string (fsi.CommandLineArgs.GetValue 1))
let numRequests = 4//int (string (fsi.CommandLineArgs.GetValue 2))

let processController = system.ActorOf(Props.Create(typeof<ProcessController>, numNodes),"processController")

//If there needs to be any modification in the number of nodes, please do so here


//_______________________________________________________________________________

processController <! SetTotalNodes(numNodes) //Initializing the total number of nodes in the entire system

//Initializing the entire ring as an array for now, until further progress

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
let ringCapacity = (int)(2. ** (float)nearestPow)
numNodes <- ringCapacity

let ring = Array.zeroCreate(numNodes)

//Create peers
for i in [0 .. numNodes-1] do
    ring.[i] <- system.ActorOf(Props.Create(typeof<Peer>, processController, numRequests, numNodes), "Peer" + string i)

//Create String messages
let messages = Array.zeroCreate(numNodes)

for i in [0 .. numNodes-1] do
    messages.[i] <- randomStr (i+1)
    printfn "msga: %s" messages.[i]

let mutable peerMessages = Map.empty<IActorRef, string>
for i in [0 .. numNodes-1] do
    peerMessages <- peerMessages |> Map.add ring.[i] messages.[i]

//Initialize Peers
for i in [0 .. numNodes-1] do
    ring.[i] <! Init(i, messages.[i], messages, peerMessages)
let randomPeer = Random().Next(numNodes)

//Initialize finger table for each peer
for i in [0 .. numNodes-1] do
    let mutable fingers = Map.empty<int, IActorRef> 
    for j in [0 .. nearestPow - 1] do
        let x = (i + (int)(2. ** (float)j)) % (int)(2.** (float)nearestPow)
        fingers <- fingers |> Map.add x ring.[j]
    ring.[i] <! InitFingerTable fingers

//Start requesting info for a random peer for each peer
let nodePeer = "akka://system/user/Peer" + string randomPeer
for i in [0 .. numNodes-1] do
     ring.[i] <! StartRequesting

Console.ReadLine()