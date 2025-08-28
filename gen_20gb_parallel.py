import argparse
import math
import os
import tempfile
from multiprocessing import Process, Queue, cpu_count
from faker import Faker

TARGET_SIZE = 20 * 1024 * 1024 * 1024  
BLOCK_TARGET = 64 * 1024 * 1024         

def gen_block(fake: Faker, approx_bytes: int) -> bytes:
    """Genera líneas fake hasta aproximar approx_bytes y devuelve un bloque bytes."""
    out = []
    total = 0
    
    while total < approx_bytes:
        s = fake.text(max_nb_chars=500) + "\n"
        out.append(s)
        total += len(s.encode('utf-8'))
    return "".join(out).encode("utf-8")

def worker(idx: int, bytes_goal: int, tmp_dir: str, locale: str, q: Queue):
    """Cada proceso genera ~bytes_goal a un archivo temporal."""
    fake = Faker(locale) if locale else Faker()
    written = 0
    tmp_path = os.path.join(tmp_dir, f"parte_{idx:03d}.txt")

    with open(tmp_path, "wb", buffering=8*1024*1024) as f:
        while written < bytes_goal:
            remaining = bytes_goal - written
            block_size = BLOCK_TARGET if remaining > BLOCK_TARGET else remaining
            block = gen_block(fake, block_size)
            f.write(block)
            written += len(block)
    q.put((idx, tmp_path, written))

def concat_files(part_paths, final_path):
    """Concatena los archivos de part_paths en final_path de forma streaming."""
    with open(final_path, "wb", buffering=16*1024*1024) as out:
        for p in part_paths:
            with open(p, "rb", buffering=16*1024*1024) as src:
                while True:
                    chunk = src.read(16*1024*1024)
                    if not chunk:
                        break
                    out.write(chunk)

def main():
    ap = argparse.ArgumentParser(description="Genera archivo de texto de 20GB en paralelo con Faker.")
    ap.add_argument("salida", help="Ruta del archivo final (ej. C:\\ruta\\archivo_20gb.txt)")
    ap.add_argument("--workers", type=int, default=0, help="# de procesos (0 = todos los núcleos)")
    ap.add_argument("--locale", type=str, default="es_ES", help="Locale de Faker (ej. es_ES, en_US)")
    ap.add_argument("--size_gb", type=float, default=20.0, help="Tamaño objetivo en GB (por si quieres otro tamaño)")
    args = ap.parse_args()

    total_bytes = int(args.size_gb * 1024 * 1024 * 1024)
    procs = args.workers if args.workers and args.workers > 0 else (cpu_count() or 1)

    
    base = total_bytes // procs
    resto = total_bytes % procs
    quotas = [base + (1 if i < resto else 0) for i in range(procs)]

    print(f"➡ Generando {args.size_gb:.2f} GB con {procs} procesos (locale: {args.locale})…")

    tmp_dir = tempfile.mkdtemp(prefix="gen20gb_")
    q = Queue()
    processes = []
    for i, quota in enumerate(quotas):
        p = Process(target=worker, args=(i, quota, tmp_dir, args.locale, q))
        p.start()
        processes.append(p)

    results = []
    for _ in processes:
        results.append(q.get())

    for p in processes:
        p.join()

    
    results.sort(key=lambda x: x[0])
    part_paths = [path for _, path, _ in results]
    produced = sum(w for _, _, w in results)

    print(f"✅ Partes generadas: {len(part_paths)} | Bytes totales: {produced:,}")
    print("➡ Concatenando partes en el archivo final…")
    concat_files(part_paths, args.salida)

    final_size = os.path.getsize(args.salida)
    print(f"🎉 Listo: {args.salida} ({final_size:,} bytes)")

  
    for p in part_paths:
        try:
            os.remove(p)
        except Exception:
            pass
    try:
        os.rmdir(tmp_dir)
    except Exception:
        pass

if __name__ == "__main__":
    main()
