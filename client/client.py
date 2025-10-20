from time import sleep
import rpyc
from time import perf_counter
import asyncio
import sys
import socket

RETRIES = 3
BACKOFF = 0.5  # seconds
socket.setdefaulttimeout(1.0)


# Use LB
def _call_count_words(keyword: str):
    for attempt in range(RETRIES):
        try:
            # timeout -> LB or server is unreachable
            with rpyc.connect("load_balancer", 18861) as conn:
                return conn.root.count_words(keyword)
        except (EOFError, ConnectionResetError, ConnectionRefusedError,
                socket.timeout, OSError) as e:
            if attempt < RETRIES - 1:
                sleep(BACKOFF * (2 ** attempt))
                continue
            # Final attempt failed
            raise


# Directly to server
# def _call_count_words(keyword: str) -> int:
#     with rpyc.connect("rpyc_server1", 18862) as conn:
#         return conn.root.count_words(keyword)

async def main(bench: bool):
    loop = asyncio.get_running_loop()

    # Interactive mode
    if not bench:
        while True:
            # read input words
            word = await loop.run_in_executor(None, input, "Input: ")
            if word.lower() == "exit":
                break
            
            # Each request uses a new connection (enables LB round-robin)
            result = await asyncio.to_thread(_call_count_words, word)
            print("Result:", result)
    
    # Benchmark mode       
    else:
        for case in ["100", "1000", "10000"]:
            query_list = []
            with open(case + "_rep.txt", encoding="utf-8") as f:
                for line in f:
                    word = line.strip()
                    if not word:
                        continue
                    if word.lower() == "exit":
                        break
                    query_list.append(word)
            
            # Dispatch all RPCs in parallel; each uses its own connection.
            start = perf_counter()
            results = await asyncio.gather(
                *[asyncio.to_thread(_call_count_words, q) for q in query_list]
            )
            end = perf_counter()
            # for word, count in zip(query_list, results):
            #     print(f"Result: {word} -> {count}")
            print(f"[CL] Total execution latency for {case} words: {end - start}")
            

if __name__ == "__main__":
    args = [a for a in sys.argv if a.startswith("--mode=")]
    if len(args) != 1:
        raise RuntimeError("Exactly one mode should be specified")
    mode_name = args[0].split("=")[1]
    
    if (mode_name == "interactive"):
        asyncio.run(main(False))
    elif (mode_name == "benchmark"):
        asyncio.run(main(True))
    else:
        raise RuntimeError("Only 'interactive' and 'benchmark' are permitted modes")
    
