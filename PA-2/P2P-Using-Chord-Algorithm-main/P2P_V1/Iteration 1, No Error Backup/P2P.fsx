open System.Reflection

#time "on"
#r "nuget: Akka.FSharp"
#r "nuget: Akka.TestKit"

open System
open Akka.Actor
open Akka.FSharp
open Akka.Configuration

type Message =
    | A of int
    | SetTotalNodes of int
    // | PeerRing of IActorRef[]
    | RequestCompletion of int
    | SendRequest
    | ExitCircle of IActorRef // This can also essentially have just the id to the current node
    | StartRequesting //This message will start the scheduler which will then start sending request messages
    | RequestFwd of int*IActorRef*int
    | Request of IActorRef*int
    | SetFingerTable of Map<int,IActorRef>
    | SetRequests of int
    | Receipt

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
            | SetTotalNodes nodes ->
                // totalNodes <- nodes
                printfn "This is just an initialization might not even be needed"
            | RequestCompletion x ->
                completedNodes <- completedNodes + 1
                totalHops <-totalHops + x
                if(completedNodes = totalNodes) then
                    printfn "All the nodes have completed the number of requests to be made with %.1f average hops" (float(totalHops)/(float)(requests*totalNodes))
                    Environment.Exit(-1)
            | SetRequests requests' ->
                requests <- requests'
            | _ -> ()

type Peer(processController: IActorRef, requests: int, numNodes: int, PeerID: int) =
    inherit Actor()
    //Define required variables here
    let totalPeers = numNodes
    let mutable cancelRequesting = false
    //HashTable of the type <Int, Peer>
    let mutable fingerTable = Map.empty<int, IActorRef>
    let mutable totalHops = 0

    //Counter to keep track of message requests sent by the given peer
    let mutable messageReceipts = 0

    override x.OnReceive(receivedMsg) =
        match receivedMsg :?> Message with
            | Request (actor, hops) ->
                // Function when the request is received at the receiving peer
                totalHops <- totalHops + hops
                actor <! Receipt

            | RequestFwd (reqID, requestingPeer, hops) ->
                // Hey Marcus check these for resources
                // https://fsharp.github.io/fsharp-core-docs/reference/fsharp-collections-fsharpmap-2.html
                // https://stackoverflow.com/questions/67128805/what-is-the-best-way-to-enhance-map-in-f-such-that-it-reports-the-key-when-rais
                match fingerTable.TryFind(reqID) with
                    | Some actor ->
                        actor <! Request(requestingPeer, hops + 1)
                        // printfn " "
                    | None ->
                        let mutable closest = -1
                        fingerTable |> Map.iter (fun _key _value -> if (_key < reqID || _key > closest) then closest <- _key)
                        // for entry in fingerTable do
                        //     if entry.Key < reqID && entry.Key > closest then
                        //         closest <- entry.Key
                        fingerTable.[closest] <! RequestFwd(reqID, requestingPeer, hops + 1)

            | StartRequesting ->
                //Starts Scheduler to schedule SendRequest Message to self mailbox
                Actor.Context.System.Scheduler.ScheduleTellRepeatedly(TimeSpan.FromSeconds(0.), TimeSpan.FromSeconds(1.), Actor.Context.Self, SendRequest)
                
            | SendRequest ->
                //Send a request for a random peer over here
                let randomPeer = Random().Next(totalPeers)
                // printfn "XXXXXXXXX %i" randomPeer
                
                match fingerTable.TryFind(randomPeer) with
                    | Some actor ->
                        actor <! Request(Actor.Context.Self, 1)
                    | None ->
                        let mutable closest = -1
                        fingerTable |> Map.iter (fun _key _value -> if (_key < randomPeer || _key > closest) then closest <- _key)
                        // for entry in fingerTable do
                        //     if entry.Key < randomPeer && entry.Key > closest then
                        //         closest <- entry.Key
                        // printfn "Closest Element where the error occurs ideally:  XXXXXX  %i  XXXXXX" closest
                        fingerTable.[closest] <! RequestFwd(randomPeer, Actor.Context.Self, 1)

            | SetFingerTable x ->
                fingerTable <- x

            | Receipt ->
                messageReceipts <- messageReceipts + 1
                if(messageReceipts = requests) then
                        cancelRequesting <- true
                        processController <! RequestCompletion(totalHops)
                
                printfn "Message Received at designated peer"

            | _ -> ()

//Actual Working starts here
let mutable numNodes = 5//int (string (fsi.CommandLineArgs.GetValue 1))
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
let ringCapacity = (int)(2. ** (float)nearestPow)
numNodes <- ringCapacity

printfn "Nearest Power: %i, Ring Capacity: %i" nearestPow ringCapacity
//_______________________________________________________________________________

processController <! SetTotalNodes(numNodes) //Initializing the total number of nodes in the entire system
processController <! SetRequests(numRequests)
//Initializing the entire ring as an array for now, until further progress

let ring = Array.zeroCreate(numNodes)

for i in [0 .. numNodes-1] do
    ring.[i] <- system.ActorOf(Props.Create(typeof<Peer>, processController, numRequests, numNodes, i), "Peer" + string i)

//Temporary FingerTable Initialization
for i in [0 .. numNodes-1] do
    let mutable fingers = Map.empty<int, IActorRef> 
    for j in [0 .. nearestPow - 1] do
        let x = (i + (int)(2. ** (float)j)) % (int)(2.** (float)nearestPow)
        fingers <- fingers |> Map.add x ring.[x]
    ring.[i] <! SetFingerTable(fingers)

for i in [0 .. numNodes-1] do
    ring.[i] <! StartRequesting

Console.ReadLine()