module ChordActor

open System
open System.IO
open Akka.FSharp
open System.Collections.Generic
open CommonUtil

let rec nodeActor (fingerTable: SortedDictionary<int, string>) index identifierSpace requestsCount systemName
        predecessorName predecessorPos successorName successorPos lookupType m (mailbox: Actor<Message>) =
    let mutable localFingerTable = fingerTable
    let mutable processedRequestsCount = 0
    let mutable requestInProgress: bool = false
    let tempNode: bool = (localFingerTable.Count = 0)
    let mutable totalHops = 0
    let mutable predecessorActorName: string = predecessorName
    let mutable predecessorActorPos: int = predecessorPos
    let mutable successorActorName: string = successorName
    let mutable successorActorPos: int = successorPos

    let rec loop() = actor {
        let! message = mailbox.Receive()

        match message with
        | SendRequest ->
            if processedRequestsCount < requestsCount && not(requestInProgress) && not(tempNode) then
                let requestedData = Random().Next(index+1, identifierSpace)
                match lookupType with
                | Simple ->
                    if requestedData <= successorActorPos then
                        totalHops <- totalHops + 1
                        processedRequestsCount <- processedRequestsCount + 1
                        if processedRequestsCount = requestsCount then
                            select ("akka://" + chordSystemName + "/user/" + chordSupervisiorName) mailbox.Context.System <! ActorConverged(mailbox.Self.Path.Name, float(totalHops)/float(requestsCount))
                        requestInProgress <- false
                    else
                        select ("akka://" + chordSystemName + "/user/" + successorActorName) mailbox.Context.System<! Request(requestedData, (mailbox.Self.Path.ToStringWithAddress()), 1)
                        requestInProgress <- true
                | Scalable ->
                    let keys = localFingerTable.Keys |> Seq.toList
                    let mutable requestProcessed = false
                    let mutable successor = -1
                    let mutable counter = 0

                    while counter < localFingerTable.Count && not(requestProcessed) do
                        if (keys.[counter] = requestedData) || (counter = 0 && keys.[counter] > requestedData) then
                            totalHops <- totalHops + 1
                            processedRequestsCount <- processedRequestsCount + 1
                            if processedRequestsCount = requestsCount then
                                select ("akka://" + systemName + "/user/" + chordSupervisiorName) mailbox.Context.System <! ActorConverged(mailbox.Self.Path.Name, (float(totalHops)/float(requestsCount))) 
                            requestProcessed <- true
                        elif keys.[counter] > successor && keys.[counter] < requestedData then
                            successor <- keys.[counter]
                        counter <- counter + 1

                    if successor <> -1 then
                        select ("akka://" + systemName + "/user/" + localFingerTable.[successor]) mailbox.Context.System <! Request(requestedData, (mailbox.Self.Path.ToStringWithAddress()), 1)
                        requestInProgress <- true

        | Request(requestedData, initActor, hopCount) ->
            match lookupType with
            | Scalable ->
                let keys = localFingerTable.Keys |> Seq.toList
                let mutable requestProcessed: bool = false
                let mutable counter = 0
                let mutable closestSuccessor = -1

                while counter < keys.Length && not(requestProcessed) do
                    if counter = 0 then
                        if keys.[counter] >= requestedData then
                            select initActor mailbox.Context.System <! Response(true, hopCount+1, requestedData)
                            requestProcessed <- true
                    
                    if keys.[counter] > closestSuccessor && keys.[counter] < requestedData then
                        closestSuccessor <- keys.[counter]

                    counter <- counter + 1

                if not(requestProcessed) then
                    if closestSuccessor <> -1 then
                        select ("akka://" + systemName + "/user/" + localFingerTable.[closestSuccessor]) mailbox.Context.System <! Request(requestedData, initActor, hopCount+1)
                    else
                        select initActor mailbox.Context.System <! Response(false, 0, requestedData)
            | Simple ->
                if requestedData <= successorActorPos then
                    select initActor mailbox.Context.System <! Response(true, hopCount+1, requestedData)
                elif successorPos = -1 then
                    select initActor mailbox.Context.System <! Response(false, 0, requestedData)
                else
                    select ("akka://" + chordSystemName + "/user/" + successorActorName) mailbox.Context.System <! Request(requestedData, initActor, hopCount+1)
        | Response(status, hopCount, requestedData) ->
            requestInProgress <- false
            if status then
                processedRequestsCount <- processedRequestsCount + 1
                totalHops <- totalHops + hopCount
                if processedRequestsCount = requestsCount then
                    let averageHops = float(totalHops)/float(requestsCount)
                    select ("akka://" + systemName + "/user/" + chordSupervisiorName) mailbox.Context.System <! ActorConverged(mailbox.Self.Path.Name, averageHops)
        | Notify(predecessorName, predecessorPos) ->
            predecessorActorName <- predecessorName
            predecessorActorPos <- predecessorPos
        | Refresh(nodes) ->
            if not(tempNode) then
                if successorActorPos <> -1 then
                    select ("akka://" + chordSystemName + "/user/" + successorActorName) mailbox.Context.System <! PredecessorRequest

                let nodeIndices = nodes.Keys |> Seq.toList
                localFingerTable <- computeFingerTable index mailbox.Self.Path.Name m nodeIndices nodes
        | PredecessorRequest ->
            if predecessorName <> null then
                mailbox.Sender() <! PredecessorResponse(predecessorActorName, predecessorActorPos)
        | PredecessorResponse(predecessorName, predecessorPos) ->
            if mailbox.Self.Path.Name <> predecessorName then
                successorActorName <- predecessorName
                successorActorPos <- predecessorPos
                select ("akka://" + chordSystemName + "/user/" + successorActorName) mailbox.Context.System <! Notify(mailbox.Self.Path.Name, index)
            0 |> ignore
        | _ -> printfn "Invalid message received"
        return! loop()
    }
    loop()

