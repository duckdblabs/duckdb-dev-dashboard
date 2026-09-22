const MONTHS = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];

// Evidence normalizes timestamp strings without a timezone before passing them to ECharts.
// Local getters preserve those UTC wall-clock values instead of applying the browser timezone.
export const chartDate = (value) => {
  if (value instanceof Date) return value;
  if (typeof value === 'number') return new Date(value);
  return new Date(String(value).replace(' ', 'T').replace(/Z$/, ''));
};

export const chartTime = (value) => chartDate(value).getTime();

export const shortDate = (value) => {
  const date = chartDate(value);
  return Number.isNaN(date.getTime()) ? String(value) : `${MONTHS[date.getMonth()]} ${date.getDate()}`;
};

const twoDigits = (value) => String(value).padStart(2, '0');

export const fullTimestamp = (value) => {
  const date = chartDate(value);
  if (Number.isNaN(date.getTime())) return String(value);
  return `${date.getFullYear()}-${twoDigits(date.getMonth() + 1)}-${twoDigits(date.getDate())} ${twoDigits(date.getHours())}:${twoDigits(date.getMinutes())}:${twoDigits(date.getSeconds())} UTC`;
};

export const isCommitSha = (value) => /^[0-9a-f]{7,40}$/i.test(value ?? '');

export const escapeHtml = (value) => String(value ?? '')
  .replaceAll('&', '&amp;')
  .replaceAll('<', '&lt;')
  .replaceAll('>', '&gt;')
  .replaceAll('"', '&quot;')
  .replaceAll("'", '&#039;');

// The release line a run belongs to: 'v2.0' for v2.0.0-alpha41489, 'v2.1' for v2.1.0-alpha41770.
// Two lines are benchmarked side by side (main plus a maintenance branch receiving backports), and
// they must be plotted and compared separately or their points interleave into one noisy series.
export const versionLine = (version) => {
  const match = /^v?(\d+\.\d+)/.exec(String(version ?? ''));
  return match ? `v${match[1]}` : 'unknown';
};

// Colours per release line. Fixed rather than palette-assigned so a line keeps its colour whichever
// lines happen to be in the selected window; unlisted lines fall back to the spare colours in order.
const VERSION_LINE_COLORS = { 'v2.0': '#2563eb', 'v2.1': '#7c3aed' };
const SPARE_LINE_COLORS = ['#db2777', '#ca8a04', '#64748b'];

export const versionLineColors = (lines) => {
  let spare = 0;
  return Object.fromEntries(
    [...new Set(lines)].map((line) => [line, VERSION_LINE_COLORS[line] ?? SPARE_LINE_COLORS[spare++ % SPARE_LINE_COLORS.length]])
  );
};

export const versionLineOrder = (lines) => [...new Set(lines)].sort((a, b) =>
  a.localeCompare(b, undefined, { numeric: true }));

export const previousCommitMap = (rows) => {
  // Predecessors are assigned within a release line: the diff that explains a point's movement is
  // the one against the previous commit on the same branch, not against whatever other branch
  // happened to be benchmarked just before it.
  //
  // A commit can have multiple benchmark runs. De-duplicate it before assigning predecessors so
  // repeated runs compare against the previous benchmarked commit, not against each other.
  const byLine = new Map();
  for (const row of rows) {
    if (!isCommitSha(row.commit_sha)) continue;
    const line = versionLine(row.duckdb_version);
    if (!byLine.has(line)) byLine.set(line, []);
    byLine.get(line).push(row);
  }
  const previous = new Map();
  for (const lineRows of byLine.values()) {
    const commits = [...new Set(
      lineRows
        .sort((a, b) => chartTime(a.merge_commit_date) - chartTime(b.merge_commit_date)
          || a.commit_sha.localeCompare(b.commit_sha))
        .map((row) => row.commit_sha)
    )];
    commits.slice(1).forEach((commitSha, index) => previous.set(commitSha, commits[index]));
  }
  return previous;
};

export const commitLinks = (row, previousCommitSha) => {
  const commitSha = row?.commit_sha ?? '';
  const commitLabel = escapeHtml(row?.commit ?? commitSha.slice(0, 8));
  const commitLink = isCommitSha(commitSha)
    ? `<a href="https://github.com/duckdb/duckdb/commit/${commitSha}" target="_blank" rel="noopener noreferrer" style="text-decoration: underline;">${commitLabel}</a>`
    : 'Unknown';
  const comparisonLinks = previousCommitSha && isCommitSha(previousCommitSha)
    ? ` (<a href="https://github.com/duckdb/duckdb/compare/${previousCommitSha}..${commitSha}" target="_blank" rel="noopener noreferrer" style="text-decoration: underline;">range</a>, <a href="https://github.com/duckdb/duckdb/compare/${previousCommitSha}...${commitSha}" target="_blank" rel="noopener noreferrer" style="text-decoration: underline;">PRs</a>)`
    : '';
  return `${commitLink}${comparisonLinks}`;
};

export const formatSeconds = (value) => Number.isFinite(Number(value))
  ? Number(value).toFixed(3)
  : 'Unknown';
