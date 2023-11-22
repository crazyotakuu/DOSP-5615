
open System.Reflection
open Akka.Actor
open Akka.Actor

#time "on"
#r "nuget: Akka.FSharp"
#r "nuget: Akka.TestKit"

open System
open Akka.Actor
open Akka.FSharp
open System.Reflection
type Message =
    | Request of IActorRef*int
    | SetFingerTable of Map<int, IActorRef>*Map<int,int>
    | Something
    
type Peer(peerID: int) =
    inherit Actor()
    let mutable fingersTable = Map.empty<int, IActorRef>
    let mutable fingerPeerID = Map.empty<int, int>
    override x.OnReceive(receivedMsg) =
        match receivedMsg :?> Message with
            | Request (actor, hops) ->
                printfn ""
            | SetFingerTable (x, y) ->
                fingersTable <- x
                fingerPeerID <- y
                
            | _ -> ()
            
let system = ActorSystem.Create("System")

let ring = Array.zeroCreate(16)
let nearestPow = 4
for i in [0 .. 15] do
    ring.[i] <- system.ActorOf(Props.Create(typeof<Peer>, i), "Peer" + string i)
    
for i in [0 .. 100-1] do
    let mutable fingers = Map.empty<int, IActorRef>
    let mutable fingerPeerID = Map.empty<int, int>
    for j in [0 .. nearestPow - 1] do
        let x = (i + (int)(2. ** (float)j)) % (int)(2.** (float)nearestPow)
        fingers <- fingers |> Map.add x ring.[x]
        fingerPeerID <- fingerPeerID |> Map.add x x
    ring.[i] <! SetFingerTable(fingers, fingerPeerID)

let mutable fingers = Map.empty<int, IActorRef>
let mutable fingerPeerID = Map.empty<int, int>
for i in [0 .. 16] do
    fingers <- fingers |> Map.add (i) ring.[i]
    fingerPeerID <- fingerPeerID |> Map.add i i

let replace (key: int) (curValue: int) (peerID: int) (currentPeer: int) =
    let mutable distance = 0
    if(currentPeer < peerID) then
        distance <- peerID - currentPeer
    else
        distance <- currentPeer + (int)(2. ** 4.) - peerID
    
    let mutable closest = (int)(2. ** (Math.Log2((float)distance)/Math.Log2(2.)))
    closest <- (closest + currentPeer) % 4
    match fingerPeerID.TryFind(closest) with
        | Some x ->
            if x = -1 then
                true
            else
                not(fingerPeerID.[closest] < peerID)
        | None -> true //This condition will, for the most part, never be accessed
        
let mutable i = 1
let peerID = 5
let peerIDPeer = system.ActorOf(Props.Create(typeof<Peer>, peerID), "Peer" + string peerID)
let currentPeer = 1
//fingers |> Map.iter (fun _key _value -> if (_key < 1 || _key > 2) then  <- _key)
fingers |> Map.iter (fun _key _value ->
    if replace _key fingerPeerID.[_key] peerID currentPeer then
        fingers <- fingers |> Map.add _key peerIDPeer
        fingerPeerID <- fingerPeerID |> Map.add _key peerID
        printfn "Printing from if"
    else
        printfn "Printing from else"
    )
