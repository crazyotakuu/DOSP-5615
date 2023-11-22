// Import necessary namespaces
open System
open Akka.Actor
open Akka.FSharp
open System.Collections.Generic

let system = ActorSystem.Create("GossipAlgoSimulator", Configuration.defaultConfig())
let random = Random()
let mutable cubeNodes = -1
let neighbours = new List<List<int>>()

type Information = 
    // StartGossip will have the string that is the message or the roumor passed
    | StartGossip of (string)
    // WorkDone will be used to terminate the 2 algorithms
    | WorkDone of (int)
    // Input will be used for an actor reference, it acts as a handle to an actor
    // used to send messages to an actor
    | Input of (IActorRef list)
    // input is given to the algorithm which includes 
    // numberOfNodes, algorithm, and neighbour list based on topology given 
    | InputToAlgorithm of (int * string * List<List<int>>)
    // StartPushSum will contain a double value that is 10 ** (-10) for termination of algorithm
    | StartPushSum of (double)
    // ComputePushSum will have computed sum, weight, and value [10 ** (-10)] for termination of algorithm
    | ComputePushSum of (double * double * double)

let Node (listener : IActorRef) (neighbor: List<int>) (nodeNum: int) (mailbox:Actor<_>) =
    let mutable neighbours: IActorRef list = [];
    let mutable numberOfRumorsHeard = 0;
    let mutable sum = (nodeNum |> double);
    // initially w = 1 (given)
    let mutable weight = 1.0;
    // this variable is used for the termination of Push Sum Algorithm
    // Stroing number of rounds, after 3 rounds it terminates
    let mutable rounds = 1;
    let mutable terminated = false;

    let rec loop ()  = actor {
        let! message = mailbox.Receive ()
        match message with
            // reference of an actor, used to send messages to an actor
            | Input(actorRef) ->
                neighbours <- actorRef

            | StartGossip(roumor) -> 
                // incrementing the number of message heard by one
                // after 10 messages the algorithm need to be stopped
                numberOfRumorsHeard <- numberOfRumorsHeard + 1
                if(numberOfRumorsHeard = 10) then
                    // sending message that the work has been completed therefore increase the counter by one in boss
                    listener <! WorkDone(nodeNum)
                if (numberOfRumorsHeard <= 10) then
                    for i in 1..5  do
                        // selecting a random neighbor and sending the roumor
                        // calling the StartGossip method again up till we heard 10 roumors
                        let randomNum = (((new System.Random()).Next(0, neighbor.Count)) |> int) % (neighbor.Count)
                        neighbours.[neighbor.Item(randomNum) - 1] <! StartGossip(roumor)

            | StartPushSum(delta) ->
                    // selecting random neighbor and sending the message
                    let randomNum = (((new System.Random()).Next(0, neighbor.Count)) |> int) % (neighbor.Count)
                    // half of s (sum) and w (weight) is kept by the sending actor 
                    sum <- sum / 2.0 
                    weight <- weight / 2.0
                    neighbours.[neighbor.Item(randomNum) - 1] <! ComputePushSum(sum, weight, delta)

            | ComputePushSum(s, w, delta) ->
                    // calculating the new sum and weight and then finding the ratio
                    // since the values s and w never converge, only the ratio does    
                    let mutable newSum = sum + s
                    let mutable newWeight = weight + w
                    let sumEstimate = ((sum / weight) - (newSum / newWeight))
                    let finalSum = Math.Abs(sumEstimate)
                    // this if will ensure that the algorithm runs continously untill the ratio converges more than 10 ** (-10)    
                    if(not terminated && finalSum > delta) then 
                        rounds <- 0
                        sum <- sum + s
                        weight <- weight + w
                        sum <- sum / 2.0
                        weight <- weight / 2.0
                        // selecting random neighbor and sending the message again
                        let randomNum = (((new System.Random()).Next(0, neighbor.Count)) |> int) % (neighbor.Count)
                        neighbours.[neighbor.Item(randomNum) - 1] <! ComputePushSum(sum, weight, delta)

                    else if (rounds = 3) then
                        // terminate if an actors ratio s/w did not change more than 10 ** (-10) in 3 consecutive rounds
                        if(not terminated) then
                            listener <! WorkDone(nodeNum)
                        terminated <- true
                        sum <- sum / 2.0
                        weight <- weight / 2.0
                        let selectedNeiIdx = (((new System.Random()).Next(0, neighbor.Count))|>int) % (neighbor.Count)
                        neighbours.[neighbor.Item(selectedNeiIdx)-1]  <! ComputePushSum(sum, weight, delta)
                    // the ratio is not converged or is less that 10 ** (-10)
                    // and number of rounds are also less than 3
                    // continue calling ComputePushSum  
                    else 
                        sum <- sum / 2.0
                        weight <- weight / 2.0
                        let randomNum = (((new System.Random()).Next(0, neighbor.Count)) |> int) % (neighbor.Count)
                        neighbours.[neighbor.Item(randomNum) - 1]  <! ComputePushSum(sum, weight, delta)
                        rounds <- rounds + 1
                    
            | _ -> 
                printfn "Invalid Message!"
                Environment.Exit 1

        return! loop ()
    }
    loop ()

let BossActor (numberOfNodes: int) (topology: string) (mailbox:Actor<_>) =
    // keeping the counter to check whether all the nodes have been traveresed or not
    let mutable count = 0
    // keeping the variable to store time elapsed
    let mutable stopWatch = System.Diagnostics.Stopwatch()

    // if the topology is line or full the loop will run up till number of nodes
    if (topology = "line" || topology = "full") then  
        // one to one mapping
        let childList = 
                [1 .. numberOfNodes]
                |> List.map(fun id -> Node mailbox.Context.Self (neighbours.Item(id - 1)) (id) |> spawn system ("Node" + string(id)))
        for i in 1 .. numberOfNodes do
            childList.Item(i - 1) <! Input(childList)
        let rec loop () = actor {
            let! message = mailbox.Receive()
            match message with 

            // information needed by the algorithm
            | InputToAlgorithm (numberOfNodes, algorithm, neighbours) -> 
                // storing the roumor for the algorithm
                let roumor = "There is only one letter that doesn't appear in any US state name, the letter Q"
                let randomNum = (new System.Random()).Next(0, numberOfNodes)
                // starting the stopwatch and calling the algorithm according to the input
                stopWatch.Start()
                if (algorithm = "gossip") then
                    childList.Item(randomNum) <! StartGossip (roumor)
                else if (algorithm = "push-sum") then
                    childList.Item(randomNum) <! StartPushSum (0.0000000001)   
                else 
                    printfn "Invalid Algorithm!"

            | WorkDone (yes) -> 
                count <- count + 1

            | _ -> ()

            // now count is equal to numberOfNodes therefore we terminate and print the time of convergence    
            if (count = numberOfNodes) then
                    stopWatch.Stop()
                    printfn("Gossip Algo Convergence Time : %f") stopWatch.Elapsed.TotalMilliseconds
                    mailbox.Context.System.Terminate() |> ignore
        
            return! loop()
        }
        loop()
    // if the topology is 3D or imp3D the loop will run upttill cubeNodes
    // that is up till the nearest perfect cube of the given number of nodes
    else
        let childList = 
                [1 .. cubeNodes]
                |> List.map(fun id -> Node mailbox.Context.Self (neighbours.Item(id - 1)) (id) |> spawn system ("Node" + string(id)))
        for i in 1 .. cubeNodes do
            childList.Item(i - 1) <! Input(childList)
        let rec loop () = actor {
            let! message = mailbox.Receive()
            match message with 

            | InputToAlgorithm (numberOfNodes, algorithm, neighbours) ->    
                let roumor = "There is only one letter that doesn't appear in any US state name, the letter Q"
                let randomNum = (new System.Random()).Next(0, numberOfNodes)
                stopWatch.Start()
                if (algorithm = "gossip") then
                    childList.Item(randomNum) <! StartGossip (roumor)
                else if (algorithm = "push-sum") then
                    childList.Item(randomNum) <! StartPushSum (0.0000000001)   
                else 
                    printfn "Invalid Algorithm!"
                    
            | WorkDone (yes) -> 
                count <- count + 1

            | _ -> ()

            if (count = numberOfNodes) then
                    stopWatch.Stop()
                    printfn("Time to converge : %f") stopWatch.Elapsed.TotalMilliseconds
                    mailbox.Context.System.Terminate() |> ignore
        
            return! loop()
        }
        loop()

// Main entry point of the application
[<EntryPoint>]
let main args =
    // Parsing command-line arguments
    let numberOfNodes = args.[0] |> int
    let topology = args.[1]
    let algorithm = args.[2]

    if (topology = "line") then
        for i in 1..numberOfNodes do
            // creating a temporary list for storing the neighbors of the current node
            let neighbor = new List<int>()
            // if it is the first node then connecting to second node only 
            // becuase it will have only one neighbor
            if (i = 1) then 
                neighbor.Add(2)
            // if it is the last node then connecting to second last node / previous node 
            // becuase it will also have only one neighbor
            else if (i = numberOfNodes) then
                neighbor.Add(numberOfNodes - 1)
            // if the i is in between 1 and last node then adding neighbor
            // for each of that node neighbor will be one back and one in front
            else
                neighbor.Add(i - 1)
                neighbor.Add(i + 1)
            // adding all the neighbor for each i
            neighbours.Add(neighbor)

    else if (topology = "full") then

        for i in 1..numberOfNodes do
            // creating a temporary list for storing the neighbors of the ith node
            let neighbor = new List<int>()
            for j in 1..numberOfNodes do
                // in this type of topology each node is connected to every other node except by itself
                if (j <> i) then
                    neighbor.Add(j)
            neighbours.Add(neighbor)

    else if (topology = "3D" || topology = "imp3D") then

        // finding the nearest perfect cube for the given number of nodes
        let cbRoot = int ((float numberOfNodes) ** (1.0/3.0)) + 1;
        let side =  cbRoot;
        let finalNodes = int ((float side) ** (3.0));
        cubeNodes <- finalNodes
        let surface = int ((float side) ** (2.0));

        for i in 1..finalNodes do

            let neighbor = new List<int>();
            let remSide = i % side; 
            let remSurf = i % surface;

            // 8 corners of the cube
            if (i = 1) then
                neighbor.Add(i + 1);
                neighbor.Add(i + side);
                neighbor.Add(i + surface);
                // printfn "Corner 1"
            else if (i = side) then
                neighbor.Add(i - 1);
                neighbor.Add(i + side);
                neighbor.Add(i + surface);
                // printfn "Corner 2"
            else if (i = (surface - side + 1)) then
                neighbor.Add(i + 1);
                neighbor.Add(i - side);
                neighbor.Add(i + surface);
                // printfn "Corner 3"
            else if (i = surface) then
                neighbor.Add(i - 1);
                neighbor.Add(i - side);
                neighbor.Add(i + surface);
                // printfn "Corner 4"
            else if (i = 1 + ((side - 1) * surface)) then
                neighbor.Add(i + 1);
                neighbor.Add(i + side);
                neighbor.Add(i - surface);
                // printfn "Corner 5"
            else if (i = side + ((side - 1) * surface)) then
                neighbor.Add(i - 1);
                neighbor.Add(i + side);
                neighbor.Add(i - surface);
                // printfn "Corner 6"
            else if (i = surface - side + 1 + ((side - 1) * surface)) then
                neighbor.Add(i + 1);
                neighbor.Add(i - side);
                neighbor.Add(i - surface);
                // printfn "Corner 7"
            else if (i = finalNodes) then
                neighbor.Add(i - 1);
                neighbor.Add(i - side);
                neighbor.Add(i - surface);
                // printfn "Corner 8"

            // 12 edges of the cube
            else if (i > 1 && i < side) then
                neighbor.Add(i - 1);
                neighbor.Add(i + 1);
                neighbor.Add(i + side);
                neighbor.Add(i + surface);
                // printfn "Edge 1"
            else if (i < surface - side && i > 1 && remSide = 1) then
                neighbor.Add(i + 1);
                neighbor.Add(i + side);
                neighbor.Add(i - side);
                neighbor.Add(i + surface);
                // printfn "Edge 2"
            else if (i > side && i < surface && remSide = 0) then
                neighbor.Add(i - 1);
                neighbor.Add(i - side);
                neighbor.Add(i + side);
                neighbor.Add(i + surface);
                // printfn "Edge 3"
            else if (i > surface - side + 1  && i < surface) then
                neighbor.Add(i - side);
                neighbor.Add(i - 1);
                neighbor.Add(i + 1);
                neighbor.Add(i + surface);
                // printfn "Edge 4"
            else if (i > 1 && i <= finalNodes - surface && remSurf = 1) then
                neighbor.Add(i + 1);
                neighbor.Add(i + side);
                neighbor.Add(i + surface);
                neighbor.Add(i - surface);
                // printfn "Edge 5"
            else if (i > surface && i <= finalNodes - surface && remSurf = side) then
                neighbor.Add(i - 1);
                neighbor.Add(i + side);
                neighbor.Add(i - surface);
                neighbor.Add(i + surface);
                // printfn "Edge 6"
            else if (i > surface && i <= finalNodes - surface && remSurf = surface - side + 1) then
                neighbor.Add(i + 1);
                neighbor.Add(i - side);
                neighbor.Add(i - surface);
                neighbor.Add(i + surface);
                // printfn "Edge 7"
            else if (i > surface && i <= finalNodes - surface && remSurf = 0) then
                neighbor.Add(i - 1);
                neighbor.Add(i - side);
                neighbor.Add(i - surface);
                neighbor.Add(i + surface);
                // printfn "Edge 8"
            else if (i > 1 + (side - 1) * surface && i < side + (side - 1) * surface) then
                neighbor.Add(i - 1);
                neighbor.Add(i + 1);
                neighbor.Add(i + side);
                neighbor.Add(i - surface);
                // printfn "Edge 9"
            else if (i < surface - side + (side - 1) * surface && i > 1 + (side - 1) * surface && remSide = 1) then
                neighbor.Add(i + 1);
                neighbor.Add(i + side);
                neighbor.Add(i - side);
                neighbor.Add(i - surface);
                // printfn "Edge 10"
            else if (i > side + (side - 1) * surface && i < surface + (side - 1) * surface && remSide = 0) then
                neighbor.Add(i - 1);
                neighbor.Add(i - side);
                neighbor.Add(i + side);
                neighbor.Add(i - surface);
                // printfn "Edge 11"
            else if (i > surface - side + 1 + (side - 1) * surface && i < surface + (side - 1) * surface) then
                neighbor.Add(i - side);
                neighbor.Add(i - 1);
                neighbor.Add(i + 1);
                neighbor.Add(i - surface);
                // printfn "Edge 12"

            // 6 surfaces of the cube
            else if (i > side + 1 && i < (side - 1) * side && remSide > 1) then
                neighbor.Add(i + 1); 
                neighbor.Add(i - 1);
                neighbor.Add(i + side);
                neighbor.Add(i - side);
                neighbor.Add(i + surface);
                // printfn "Surface 1"
            else if (i > side + 1 + (side - 1) * surface && i < (side - 1) * side + (side - 1) * surface && remSide > 1) then
                neighbor.Add(i + 1);
                neighbor.Add(i - 1);
                neighbor.Add(i + side);
                neighbor.Add(i - side);
                neighbor.Add(i - surface);
                // printfn "Surface 2"
            else if (i > 1 + surface && i <= finalNodes - surface - side && remSide = 1 && remSurf <> 1 && remSurf <> surface - side + 1) then
                neighbor.Add(i + side);
                neighbor.Add(i - side);
                neighbor.Add(i + surface);
                neighbor.Add(i - surface);
                neighbor.Add(i + 1);
                // printfn "Surface 3"
            else if (i > 1 + side + surface && i <= finalNodes - surface && remSide = 0 && remSurf <> side && remSide <> 0) then
                neighbor.Add(i + side);
                neighbor.Add(i - side);
                neighbor.Add(i + surface);
                neighbor.Add(i - surface);
                neighbor.Add(i - 1);
                // printfn "Surface 4"
            else if (i > 1 + surface && i < finalNodes - 2 * surface + side && remSurf > 1 && remSurf < side) then
                neighbor.Add(i + 1);
                neighbor.Add(i - 1);
                neighbor.Add(i + surface);
                neighbor.Add(i - surface);
                neighbor.Add(i + side);
                // printfn "Surface 5"
            else if (i > 2 * surface - side + 1 && i < finalNodes - surface && remSurf > surface - side + 1 && remSurf <> 0) then
                neighbor.Add(i + 1);
                neighbor.Add(i - 1);
                neighbor.Add(i + surface);
                neighbor.Add(i - surface);
                neighbor.Add(i - side);
                // printfn "Surface 6"
            else
                neighbor.Add(i + 1);
                neighbor.Add(i - 1);
                neighbor.Add(i + side);
                neighbor.Add(i - side);
                neighbor.Add(i + surface);
                neighbor.Add(i - surface);

            // for Imperfect 3D generating a radom number between 1 and finalNode that is not already added to neighbor list 
            // Add that to neighbor then
            if (topology = "imp3D") then
                let mutable found = true;
                let mutable r = -1;
                while (found = true) do
                    r <- random.Next(1, finalNodes + 1);
                    found <- neighbor.Contains(r);
                neighbor.Add(r);

            neighbours.Add(neighbor);  

    else 
        printfn "Invalid Topology!"

    // ... (rest of your script's logic goes here, adapted as necessary)
    let boss = spawn system "boss" (fun mailbox -> BossActor numberOfNodes topology mailbox)

    (boss <? InputToAlgorithm (numberOfNodes, algorithm, neighbours)) |> ignore

    // Wait for actor system termination
    system.WhenTerminated.Wait() |> ignore
    0 // Return an integer exit code
