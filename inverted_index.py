import argparse, os, re, time, tempfile, heapq
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

TOKEN_RE = re.compile(r"[A-Za-zÀ-ÖØ-öø-ÿ0-9']+")

def tokenize(s: str):
    for m in TOKEN_RE.finditer(s):
        yield m.group(0).lower()

def map_count_docs(doc_texts, start_doc_id):
    postings = defaultdict(Counter)
    did = start_doc_id
    for txt in doc_texts:
        if not txt:
            did += 1
            continue
        ctr = Counter(tokenize(txt))
        if ctr:
            for term, tf in ctr.items():
                postings[term][did] += tf
        did += 1
    return postings, len(doc_texts)

def spill_sorted_chunk(postings, tmp_dir, chunk_idx):
    rows = []
    for term, docs in postings.items():
        for doc_id, tf in docs.items():
            rows.append((term, doc_id, tf))
    rows.sort(key=lambda x: (x[0], x[1]))
    path = os.path.join(tmp_dir, f"chunk_{chunk_idx:06d}.tsv")
    with open(path, "w", encoding="utf-8") as w:
        for term, doc_id, tf in rows:
            w.write(f"{term}\t{doc_id}\t{tf}\n")
    return path, len(rows)

def merge_sorted_chunks(chunk_paths, outdir, shard_mb=256, basename="index"):
    os.makedirs(outdir, exist_ok=True)
    files = [open(p, "r", encoding="utf-8") for p in chunk_paths]

    def gen(i, fh):
        for line in fh:
            term, doc, tf = line.rstrip("\n").split("\t")
            yield (term, int(doc), int(tf), i)

    iters = [gen(i, fh) for i, fh in enumerate(files)]
    heap = []
    for it in iters:
        try:
            term, doc, tf, src = next(it)
            heap.append((term, doc, tf, src, it))
        except StopIteration:
            pass
    heapq.heapify(heap)

    shard_idx = 0
    shard_path = os.path.join(outdir, f"{basename}_{shard_idx:03d}.tsv")
    out = open(shard_path, "w", encoding="utf-8")
    written_in_shard = 0
    records = 0

    while heap:
        term, doc, tf, src, it = heapq.heappop(heap)
        out.write(f"{term}\t{doc}\t{tf}\n")
        records += 1
        written_in_shard += len(term) + len(str(doc)) + len(str(tf)) + 3

        try:
            nterm, ndoc, ntf, _ = next(it)
            heapq.heappush(heap, (nterm, ndoc, ntf, src, it))
        except StopIteration:
            pass

        if written_in_shard >= shard_mb * 1024 * 1024:
            out.close()
            shard_idx += 1
            shard_path = os.path.join(outdir, f"{basename}_{shard_idx:03d}.tsv")
            out = open(shard_path, "w", encoding="utf-8")
            written_in_shard = 0

    out.close()
    for fh in files:
        fh.close()
    return records, shard_idx + 1

def build_inverted_index(input_path, outdir, threads, lines_per_doc, batch_docs, shard_mb):
    os.makedirs(outdir, exist_ok=True)
    tmp_dir = tempfile.mkdtemp(prefix="invidx_")

    if threads <= 0:
        threads = os.cpu_count() or 1

    t0 = time.perf_counter()
    total_docs = 0
    chunk_paths = []
    chunk_idx = 0

    with ThreadPoolExecutor(max_workers=threads) as ex, \
         open(input_path, "r", encoding="utf-8", errors="ignore") as f:

        futures = []
        doc_buf, line_buf = [], []
        next_doc_id = 0

        def flush_docs(docs, start_id):
            return ex.submit(map_count_docs, list(docs), start_id)

        for line in f:
            line_buf.append(line)
            if len(line_buf) >= lines_per_doc:
                doc_text = "".join(line_buf)
                doc_buf.append(doc_text)
                line_buf = []
                if len(doc_buf) >= batch_docs:
                    futures.append(flush_docs(doc_buf, next_doc_id))
                    next_doc_id += len(doc_buf)
                    doc_buf = []

        if line_buf:
            doc_buf.append("".join(line_buf))
            line_buf = []
        if doc_buf:
            futures.append(flush_docs(doc_buf, next_doc_id))
            next_doc_id += len(doc_buf)
            doc_buf = []

        for fut in as_completed(futures):
            postings, ndocs = fut.result()
            total_docs += ndocs
            path, rows = spill_sorted_chunk(postings, tmp_dir, chunk_idx)
            chunk_paths.append(path)
            chunk_idx += 1

    rows, nshards = merge_sorted_chunks(chunk_paths, outdir, shard_mb=shard_mb, basename="index")

    for p in chunk_paths:
        try: os.remove(p)
        except: pass
    try: os.rmdir(tmp_dir)
    except: pass

    t1 = time.perf_counter()

    # txt
    summary_path = os.path.join(outdir, "resumen.txt")
    with open(summary_path, "w", encoding="utf-8") as s:
        s.write(f"Índice invertido en '{outdir}'\n")
        s.write(f"Docs (bloques de {lines_per_doc} líneas): {total_docs:,}\n")
        s.write(f"Postings                        : {rows:,}\n")
        s.write(f"Shards                          : {nshards}\n")
        s.write(f"Tiempo total                    : {t1 - t0:.2f}s\n")
    print(f"Proceso completado. Resumen guardado en {summary_path}")

def main():
    ap = argparse.ArgumentParser(description="Índice invertido: cada documento = N líneas contiguas.")
    ap.add_argument("input", help="Archivo grande de texto")
    ap.add_argument("--outdir", required=True, help="Carpeta de salida para shards TSV y resumen.txt")
    ap.add_argument("--threads", type=int, default=0, help="Hilos para map (0=auto)")
    ap.add_argument("--lines-per-doc", type=int, default=10, help="Líneas por documento (default=10)")
    ap.add_argument("--batch-docs", type=int, default=50_000, help="Docs por batch antes de derramar")
    ap.add_argument("--shard-mb", type=int, default=256, help="Tamaño de shard TSV (MB)")
    args = ap.parse_args()

    build_inverted_index(
        args.input, args.outdir,
        threads=args.threads,
        lines_per_doc=args.lines_per_doc,
        batch_docs=args.batch_docs,
        shard_mb=args.shard_mb,
    )

if __name__ == "__main__":
    main()
