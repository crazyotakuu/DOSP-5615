module SupervisorActor

open Akka
open Akka.FSharp
open System
open System.Collections.Generic
open System.IO
open CommonUtil
open ChordActor

let supervisorActor nodeCount (nodesDict: SortedDictionary<int, string>) (m: int) (joinCount: int) (identifierSpace: int) (requestsCount: int) (mailbox: Actor<Message>) =
    let mutable averageHops = 0.0
    let mutable convergedCount = 0
    let nodes = nodesDict
    let mutable localNodeCount = nodeCount
    let mutable newlyJoinedNodes = 0
    let stopwatch = System.Diagnostics.Stopwatch() 

    let rec loop() = actor {
        let! message = mailbox.Receive()
        match message with
        | InitActors(nodeFingerTables, identifierSpace, requestsCount, lookupType) ->
            let actorPos = nodes.Keys |> Seq.toList
            let actorNames = nodes.Values |> Seq.toList
            let mutable counter = 0

            for nodePair in nodes do
                let mutable successorName: string = null
                let mutable successorPos: int = -1
                let mutable predecessorName: string = null
                let mutable predecessorPos: int = -1

                if counter < actorNames.Length-1 then
                    successorName <- actorNames.[counter+1]
                    successorPos <- actorPos.[counter+1]

                if counter > 0 then
                    predecessorName <- actorNames.[counter-1]
                    predecessorPos <- actorPos.[counter-1]

                let fingerTable = (if nodeFingerTables.ContainsKey(nodePair.Value) then (nodeFingerTables.[nodePair.Value]) else (new SortedDictionary<int, string>()))
                let chordActorRef = spawn mailbox.Context.System nodePair.Value (
                    nodeActor fingerTable nodePair.Key identifierSpace requestsCount chordSystemName predecessorName predecessorPos successorName successorPos lookupType m
                )

                mailbox.Context.System.Scheduler.ScheduleTellRepeatedly(
                    System.TimeSpan.FromMilliseconds(1000.0), 
                    System.TimeSpan.FromMilliseconds(1000.0), 
                    chordActorRef, 
                    SendRequest, 
                    chordActorRef
                )
                counter <- counter + 1
            stopwatch.Start()
        | ActorConverged(actorName, avgHops) ->
            convergedCount <- convergedCount + 1;
            averageHops <- averageHops + avgHops
            printfn "Actor %s processed all the requests in %f average hops." actorName avgHops
            if convergedCount = localNodeCount then
                stopwatch.Stop()
                averageHops <- averageHops / float(localNodeCount)
                printfn "All actors are done with processing the requests. \n Time taken to process all the requests is %d and average hops %f" (stopwatch.ElapsedMilliseconds) averageHops
        | Join(lookupType) ->
            if newlyJoinedNodes < joinCount then
                let name, position, fingerTable, successorName, successorPos = generateNewNode nodes m
                nodes.Add(position, name)
                let newActorRef = spawn mailbox.Context.System name (
                    nodeActor fingerTable position identifierSpace requestsCount chordSystemName null -1 successorName successorPos lookupType m
                )
                mailbox.Context.System.Scheduler.ScheduleTellRepeatedly(
                    System.TimeSpan.FromMilliseconds(1000.0), 
                    System.TimeSpan.FromMilliseconds(1000.0), 
                    newActorRef, 
                    SendRequest, 
                    newActorRef
                )
                if successorName <> null then
                    select ("akka://" + chordSystemName + "/user/" + successorName) mailbox.Context.System <! Notify(name, position)
                newlyJoinedNodes <- newlyJoinedNodes + 1
                localNodeCount <- localNodeCount + 1
        | InitRefresh ->
            for actor in nodes.Values do
                select ("akka://" + chordSystemName + "/user/" + actor) mailbox.Context.System <! Refresh(new SortedDictionary<int, string>(nodes)) 
        | _ -> printfn "Invalid message received"
        return! loop()
    }
    loop()

