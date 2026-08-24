/**
 * Progress watcher: turns the last lines of a job's mirrored stdout into a
 * compact progress string shown in `colab cluster list`. The user supplies a
 * regex at submit time; we apply it per tick and keep only the LAST match —
 * training logs are append-only, so the latest match is the freshest state.
 *
 * A pattern that never matches is not an error; progress stays unset.
 */

/**
 * Compile `pattern` and return the last matching line's capture (or the whole
 * match when there are no capture groups). Returns undefined when the pattern
 * is invalid (logged once by the caller) or matches nothing.
 */
export function extractProgress(buffer: string, pattern: string): string | undefined {
  let re: RegExp;
  try {
    re = new RegExp(pattern, 'gm');
  } catch {
    return undefined;
  }
  let last: RegExpExecArray | null = null;
  let m: RegExpExecArray | null;
  while ((m = re.exec(buffer)) !== null) {
    last = m;
    if (m[0].length === 0) re.lastIndex++; // never loop on zero-width matches
  }
  if (!last) return undefined;
  return (last.length > 1 ? last.slice(1).join('/') : last[0]).trim();
}
