import linecache

from pipelines.utils.logger import log


def display_top(snapshot, key_type="lineno", limit=50):
  top_stats = snapshot.statistics(key_type)

  log(f"Top {limit} lines")
  out = []
  for index, stat in enumerate(top_stats[:limit], 1):
    frame = stat.traceback[0]
    out.append(f"#{index}: {frame.filename}:{frame.lineno}: {(stat.size / 1024):.1f} KiB")
    line = linecache.getline(frame.filename, frame.lineno).strip()
    if line:
      out.append(f"    {line}")
  log("\n".join(out))

  other = top_stats[limit:]
  if other:
    size = sum(stat.size for stat in other)
    log(f"{len(other)} other: {(size / 1024):.1f} KiB")
  total = sum(stat.size for stat in top_stats)
  log(f"Total allocated size: {(total / 1024 / 1024):.1f} MiB")
