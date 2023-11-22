open System
open System.Net
open System.Net.Sockets
open System.IO
open System.Text

let serverIP = "127.0.0.1"
let serverPort = 28732

let clientHandler () =
    try
        let client = new TcpClient(serverIP, serverPort)
        let stream = client.GetStream()
        let reader = new StreamReader(stream)
        let writer = new StreamWriter(stream)

        let serverResponse = reader.ReadLine()
        Console.WriteLine(serverResponse)

        let rec processCommands () =
            try
                Console.Write("Enter a command: ")
                let command = Console.ReadLine()

                writer.WriteLine(command)
                writer.Flush()

                let response = reader.ReadLine()
                Console.WriteLine("Server response: " + response)

                if response = "-5" then
                    Console.Write("Exit")
                    writer.Close()
                    reader.Close()
                    stream.Close()
                    client.Close()
                else
                    processCommands()
            with
            | :? IOException ->
                Console.WriteLine("Connection to the server is lost.")
            | _ ->
                Console.WriteLine("An error occurred.")

        processCommands()
    with
    | :? SocketException ->
        Console.WriteLine("Could not connect to the server.")
    | _ ->
        Console.WriteLine("An error occurred.")

[<EntryPoint>]
let main argv =
    clientHandler()
    0
