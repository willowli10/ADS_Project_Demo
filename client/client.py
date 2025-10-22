from time import sleep
import rpyc
from time import perf_counter
import asyncio
import sys
import socket
import numpy as np

RETRIES = 3
BACKOFF = 0.5  # seconds
socket.setdefaulttimeout(1.0)


# Use LB
# def _call_count_words(keyword: str):
#     for attempt in range(RETRIES):
#         try:
#             # timeout -> LB or server is unreachable
#             with rpyc.connect("load_balancer", 18861) as conn:
#                 return conn.root.count_words(keyword)
#         except (EOFError, ConnectionResetError, ConnectionRefusedError,
#                 socket.timeout, OSError) as e:
#             if attempt < RETRIES - 1:
#                 sleep(BACKOFF * (2 ** attempt))
#                 continue
#             # Final attempt failed
#             raise


# Directly to server
def _call_count_words(keyword: str) -> int:
    with rpyc.connect("rpyc_server1", 18862) as conn:
        return conn.root.count_words(keyword)
    
    
async def _timed_query(word: str):
    start = perf_counter()
    try:
        result = await asyncio.to_thread(_call_count_words, word)
        return word, result, perf_counter() - start, None
    except Exception as exc:
        return word, None, perf_counter() - start, exc
    

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
            try:
                start = perf_counter()
                result = await asyncio.to_thread(_call_count_words, word)
                print(f"Result: {result}, took {perf_counter()-start}")
            except Exception as e:
                print("[CL] No backend available (or connection failed):", e)
                # keep looping so the user can try again after servers recover
                continue
    
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
            combined = await asyncio.gather(
                *(_timed_query(q) for q in query_list)
            )

            results = [c[1] for c in combined if c[3] is None]
            latencies = [c[2] for c in combined if c[3] is None]
            ok = len(results)
            fail = len(combined) - ok
            print(f"[CL] Completed {ok} ok / {fail} failed for {case} words")

            end = perf_counter()
            # for word, count in zip(query_list, results):
            #     print(f"Result: {word} -> {count}")
            print(f"[CL] Total execution latency for {case} words: {end - start}")
            if latencies:
                print(f"[CL] 99th percentile latency for {case} words: {np.quantile(latencies, 0.99)}")
            else:
                print(f"[CL] 99th percentile latency for {case} words: N/A (no successful results)")
            

if __name__ == "__main__":
    args = [a for a in sys.argv if a.startswith("--mode=")]
    if len(args) != 1:
        raise RuntimeError("Exactly one mode should be specified")
    mode_name = args[0].split("=")[1]
    
    if (mode_name == "interactive"):
        asyncio.run(main(bench=False))
    elif (mode_name == "benchmark"):
        asyncio.run(main(bench=True))
    else:
        raise RuntimeError("Only 'interactive' and 'benchmark' are permitted modes")
    
