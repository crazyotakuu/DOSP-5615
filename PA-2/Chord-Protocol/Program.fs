module ChordSimulator

open CommonUtil
open ChordActor
open SupervisorActor
open Akka
open Akka.FSharp
open System
open System.Collections.Generic

[<EntryPoint>]
let main argv = 
    let m = 20
    let joinCount = 10
    let identifierSpace = int(2.0 ** float m)
    let nodeCount = (argv.[0] |> int)
    let requestsCount = (argv.[1] |> int)
    let mutable lookupType: LookupType = Scalable

    if argv.Length = 3 then
        lookupType <- if argv.[2] = "simple" then Simple else Scalable

    let chordSystem = System.create chordSystemName <| Configuration.defaultConfig()

    let nodes: SortedDictionary<int, string> = createNodes nodeCount chordSystem m
    let nodeFingerTables: Dictionary<string, SortedDictionary<int, string>> = computeAllFingerTables nodes m nodeCount

    let supervisor = spawn chordSystem chordSupervisiorName (supervisorActor nodeCount nodes m joinCount identifierSpace requestsCount)
    supervisor <! InitActors(nodeFingerTables, identifierSpace, requestsCount, lookupType)

    chordSystem.Scheduler.ScheduleTellRepeatedly(
        System.TimeSpan.FromMilliseconds(5000.0), 
        System.TimeSpan.FromMilliseconds(10000.0), 
        supervisor, 
        Join(lookupType), 
        supervisor
    )

    chordSystem.Scheduler.ScheduleTellRepeatedly(
        System.TimeSpan.FromMilliseconds(5000.0), 
        System.TimeSpan.FromMilliseconds(5000.0), 
        supervisor, 
        InitRefresh, 
        supervisor
    )
    
    Console.ReadLine() |> ignore

    0//return dummy int
