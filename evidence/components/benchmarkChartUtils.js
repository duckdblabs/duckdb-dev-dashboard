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

export const previousCommitMap = (rows) => {
  // A commit can have multiple benchmark runs. De-duplicate it before assigning predecessors so
  // repeated runs compare against the previous benchmarked commit, not against each other.
  const commits = [...new Set(
    rows
      .filter((row) => isCommitSha(row.commit_sha))
      .sort((a, b) => chartTime(a.merge_commit_date) - chartTime(b.merge_commit_date)
        || a.commit_sha.localeCompare(b.commit_sha))
      .map((row) => row.commit_sha)
  )];
  return new Map(commits.slice(1).map((commitSha, index) => [commitSha, commits[index]]));
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
