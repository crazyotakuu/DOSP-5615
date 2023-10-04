open System
open System.Net
open System.Net.Sockets
open System.IO
open System.Text
open System.Collections.Generic
open System.Threading

let port = 12345
let mutable index = 0

let parseClientMessage (clientMessage: string) =
    // Split the client's message into parts
    let clientMessageParts = clientMessage.Split(' ')
    // Check for the number of inputs after operand
    let numInputs = clientMessageParts.Length - 1
    let operator = clientMessageParts.[0]

    if not (operator = "add") && not (operator = "subtract") && not (operator = "multiply") && not (operator = "bye") && not (operator = "terminate") then
        "-1"
    elif numInputs < 2 && not (operator = "bye") && not (operator = "terminate") then
        // Return error code -2: Number of inputs is less than two
        "-2"
    elif numInputs > 4 && not (operator = "bye") && not (operator = "terminate") then
        // Return error code -3: Number of inputs is more than four
        "-3"
    else
        match operator with
        | "add" ->
            // Parse and perform addition for multiple inputs
            let result = Array.skip 1 clientMessageParts
                        |> Array.map Int32.Parse
                        |> Array.sum
            result.ToString()
        | "subtract" ->
            // Parse and perform subtraction for multiple inputs
            let inputs = Array.skip 1 clientMessageParts
                        |> Array.map Int32.Parse
            let result = Array.fold (fun acc x -> acc - x) inputs.[0] (Array.skip 1 inputs)
            result.ToString()
        | "multiply" ->
            // Parse and perform multiplication for multiple inputs
            let result = Array.skip 1 clientMessageParts
                        |> Array.map Int32.Parse
                        |> Array.fold (*) 1
            result.ToString()
        | "bye" ->
            // exit code
            "-5"
        | "terminate" ->
            // exit code
            "-5"
        | _ ->
            // Invalid command or insufficient inputs, return error code -1 (highest priority)
            "-1"

let handleClient (client : TcpClient) (clients : List<TcpClient>) (server : TcpListener)=
    let stream = client.GetStream()
    let reader = new StreamReader(stream)
    let writer = new StreamWriter(stream)

    // Send a greeting to the client
    writer.WriteLine("Hello!")
    writer.Flush()
    let i = Interlocked.Increment(&index)

    let rec processClient() =
        try
            let clientMessage = reader.ReadLine()
            Console.WriteLine("Received: " + clientMessage)
            if String.IsNullOrWhiteSpace(clientMessage) then
                Console.WriteLine("Responding to client " + i.ToString() + " with result: -1")
                writer.WriteLine("-1")
                writer.Flush()
                processClient()
            else
                // Parse the client's message
                let response = parseClientMessage clientMessage
                // Send the response to the client

                let operation = clientMessage.Split(' ').[0]

                if operation = "bye" then
                    Console.WriteLine("Responding to client " + i.ToString() + " with result: -5")
                    writer.WriteLine(response)
                    writer.Flush()
                    // close client
                    client.Close()
                elif operation = "terminate" then
                    Console.WriteLine("Responding to client " + i.ToString() + " with result: -5")
                    writer.WriteLine(response)
                    writer.Flush()
                    // close all clients and server
                    for c in clients do
                        c.Close()
                // actually message should have been here but now it in execption handling in main()      
                //    Console.WriteLine("Server Stopped and number of clients are " + clients.Count.ToString())
                    server.Stop()
                else
                    Console.WriteLine("Responding to client " + i.ToString() + " with result: " + response)
                    writer.WriteLine(response)
                    writer.Flush()
                    processClient()

        with
        | :? FormatException ->
            // Handle non-integer inputs
            Console.WriteLine("Responding to client " + i.ToString() + " with result: -4")
            writer.WriteLine("-4") // Non-integer input error code
            writer.Flush()
            processClient()
        | _ ->
            // Handle other errors
            // uncomment the below and comment server closure above and try "terminate", i suspect dangling threads or something, you can try to comment out the | _ -> case entirely but ...
            Console.WriteLine("Responding to client " + i.ToString() + " with result: -100")
            writer.WriteLine("-1") // Generic error code (highest priority)
            writer.Flush()
            processClient()

    processClient()

[<EntryPoint>]
let main argv =
    let server = new TcpListener(IPAddress.Any, port)
    server.Start()
    Console.WriteLine("Server is running and listening on port " + port.ToString())

    let clients = new List<TcpClient>()

    let rec acceptClients () =
        let client = server.AcceptTcpClient()
        clients.Add(client)
        System.Threading.Tasks.Task.Run(fun () -> handleClient client clients server) |> ignore
        acceptClients()

    try
        acceptClients()
    with
    | :? System.ObjectDisposedException -> () // Ignore when the server is disposed
    | _ -> Console.WriteLine("Server Stopped") // probably not the right way, comment this line entirely and run and should actually return 0 clients

    // Close all client connections
    for client in clients do
        client.Close()

    // for logging purpose  
    Console.WriteLine("Server Stopped and number of clients are " + clients.Count.ToString()) // probably all connections are closed but list just has previous elements
    // Close the server
    server.Stop()

    0