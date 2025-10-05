from time import time
import rpyc
from time import perf_counter
import asyncio
import sys

def _call_count_words(keyword: str) -> int:
    # Open a fresh connection per request so the LB can round-robin
    with rpyc.connect("load_balancer", 18861) as conn:
        return conn.root.count_words(keyword)


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
        query_list = []
        with open("queries.txt", encoding="utf-8") as f:
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
        for word, count in zip(query_list, results):
            print(f"Result: {word} -> {count}")
        print(f"[CL] Total execution latency: {end - start}")

        # TODO: make this code interchangeable, with or without a load balancer
        # TODO: sanity check results
        # TODO: ask teammates about using gather

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
    
