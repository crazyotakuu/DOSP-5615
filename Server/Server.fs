open System
open System.Net
open System.Net.Sockets
open System.IO
open System.Text
open System.Collections.Generic
open System.Threading

let port = 28732
let mutable index = 0

let parseClientMessage (clientMessage: string) =
    let clientMessageParts = clientMessage.Split(' ')
    let numInputs = clientMessageParts.Length - 1
    let operator = clientMessageParts.[0]

    if not (operator = "add") && not (operator = "subtract") && not (operator = "multiply") && not (operator = "bye") && not (operator = "terminate") then
        "-1"
    elif numInputs < 2 && not (operator = "bye") && not (operator = "terminate") then
        "-2"
    elif numInputs > 4 && not (operator = "bye") && not (operator = "terminate") then
        "-3"
    else
        match operator with
        | "add" ->
            let result = Array.skip 1 clientMessageParts
                        |> Array.map Int32.Parse
                        |> Array.sum
            result.ToString()
        | "subtract" ->
            let inputs = Array.skip 1 clientMessageParts
                        |> Array.map Int32.Parse
            let result = Array.fold (fun acc x -> acc - x) inputs.[0] (Array.skip 1 inputs)
            result.ToString()
        | "multiply" ->
            let result = Array.skip 1 clientMessageParts
                        |> Array.map Int32.Parse
                        |> Array.fold (*) 1
            result.ToString()
        | "bye" ->
            "-5"
        | "terminate" ->
            "-5"
        | _ ->
            "-1"

let handleClient (client : TcpClient) (clients : List<TcpClient>) (server : TcpListener)=
    let stream = client.GetStream()
    let reader = new StreamReader(stream)
    let writer = new StreamWriter(stream)

    writer.WriteLine("Hello! You are connected to the Server")
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
                let response = parseClientMessage clientMessage

                let operation = clientMessage.Split(' ').[0]

                if operation = "bye" then
                    Console.WriteLine("Responding to client " + i.ToString() + " with result: -5")
                    writer.WriteLine(response)
                    writer.Flush()
                    client.Close()
                elif operation = "terminate" then
                    Console.WriteLine("Responding to client " + i.ToString() + " with result: -5")
                    writer.WriteLine(response)
                    writer.Flush()
                    for c in clients do
                        c.Close()
                    server.Stop()
                else
                    Console.WriteLine("Responding to client " + i.ToString() + " with result: " + response)
                    writer.WriteLine(response)
                    writer.Flush()
                    processClient()

        with
        | :? FormatException ->
            Console.WriteLine("Responding to client " + i.ToString() + " with result: -4")
            writer.WriteLine("-4") 
            writer.Flush()
            processClient()
        | _ ->
            Console.WriteLine("Responding to client " + i.ToString() + " with result: -100")
            writer.WriteLine("-1") 
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
    | :? System.ObjectDisposedException -> () 
    | _ -> Console.WriteLine("Server Stopped")

    for client in clients do
        client.Close()
    // Console.WriteLine("Server Stopped and number of clients are " + clients.Count.ToString()) // probably all connections are closed but list just has previous elements
    server.Stop()

    0