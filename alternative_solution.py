import socket  # noqa: F401
import asyncio


class kafkaObject:
    def __init__(self):
        self.host = "localhost"
        self.port = 9092

    async def start(self):
        server = await asyncio.start_server(self.handleClient, self.host, self.port)
        addr = server.sockets[0].getsockname()
        print(f"Serveur en écoute sur {addr}")

        async with server:
            await server.serve_forever()

    async def handleClient(self, reader, writer):
        addr = writer.get_extra_info("peername")
        # print(f"Connexion de {addr}")

        try:
            while True:
                data = await reader.read(1024)
                if not data:
                    break
                writer.write(b"\x00\x00\x00\x04\x00\x00\x00\x07")
        except Exception as e:
            print(f"Erreur avec {addr}: {e}")


async def main():
    # You can use print statements as follows for debugging,
    # they'll be visible when running tests.
    print("Logs from your program will appear here!")

    kafka = kafkaObject()
    await kafka.start()


if __name__ == "__main__":
    asyncio.run(main())
