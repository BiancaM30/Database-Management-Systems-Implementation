import socket


server_address = ('localhost', 8989)
client_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
client_socket.connect(server_address)
bufferSize = 1024
command = ''



while command != "exit":
    command = input("Enter command: ")
    client_socket.sendall(command.encode())
    server_response = client_socket.recvfrom(bufferSize)
    print(server_response[0].decode())
    #
    # if server_response[0].decode() != "SELECT":
    #     print(server_response[0].decode())
    # else:
    #     f = open("databases/select.txt", "r")
    #     print(f.read())
    #     f.close()