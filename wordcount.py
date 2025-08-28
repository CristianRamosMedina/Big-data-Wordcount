import sys
import time
from datetime import datetime
from collections import Counter
from concurrent.futures import ProcessPoolExecutor, as_completed  
import os

def map_count(chunk: str) -> Counter:
    words = chunk.split()
    return Counter(words)

def main():
    if len(sys.argv) < 2:
        print(f"Uso: python {sys.argv[0]} <archivo> [num_workers] [salida.txt]")
        sys.exit(1)

    filename = sys.argv[1]
    num_workers = int(sys.argv[2]) if len(sys.argv) > 2 else os.cpu_count()
    out_path = sys.argv[3] if len(sys.argv) > 3 else f"{filename}.wordcount2.txt"

    block_size = 32 * 1024 * 1024  

    start_wall = datetime.now()
    t0 = time.perf_counter()

    total = Counter()

    with open(filename, "r", encoding="utf-8", errors="ignore") as f, \
         ProcessPoolExecutor(max_workers=num_workers) as executor:

        futures = []
        while True:
            data = f.read(block_size)
            if not data:
                break
            futures.append(executor.submit(map_count, data))

        for fut in as_completed(futures):
            total.update(fut.result())  

    total_palabras = sum(total.values())
    distintas = len(total)

    t1 = time.perf_counter()
    end_wall = datetime.now()
    dur = t1 - t0

    with open(out_path, "w", encoding="utf-8") as out:
        out.write(f"# WordCount Map-Reduce con procesos\n")
        out.write(f"# Archivo: {filename}\n")
        out.write(f"# Workers: {num_workers}\n")
        out.write(f"# Inicio:  {start_wall.isoformat(sep=' ', timespec='seconds')}\n")
        out.write(f"# Fin:     {end_wall.isoformat(sep=' ', timespec='seconds')}\n")
        out.write(f"# Duración (s): {dur:.2f}\n")
        out.write(f"# Total de palabras: {total_palabras}\n")
        out.write(f"# Palabras distintas: {distintas}\n")
        out.write("# ---- palabra\tconteo ----\n")
        for w, n in total.items():
            out.write(f"{w}\t{n}\n")

    print(f"TOTAL PALABRAS: {total_palabras}")
    print(f"PALABRAS DISTINTAS: {distintas}")
    print(f"Tiempo: {dur:.2f} s")
    print(f"Salida: {out_path}")

if __name__ == "__main__":
    main()
