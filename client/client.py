from time import time
import rpyc
import time
import asyncio

async def main():
    # connect to the load balancer
    conn = rpyc.connect("load_balancer", 18861)
    loop = asyncio.get_running_loop()

    remote = conn.root

    try:
        while True:
            # read input words
            word = await loop.run_in_executor(None, input, "Input: ")
            if word.lower() == "exit":
                break

            # call remote method in a thread to avoid blocking the event loop
            result = await asyncio.to_thread(lambda: remote.count_words(word))
            print("Result:", result)
    finally:
        conn.close()


if __name__ == "__main__":
    asyncio.run(main())