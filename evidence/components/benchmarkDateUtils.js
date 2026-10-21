const YYYY_MM_DD = /^(\d{4})-(\d{2})-(\d{2})$/;

// Evidence stores a CalendarDate by converting local midnight to an ISO date. In timezones east
// of UTC that moves the stored value to the previous day. Convert it back to the calendar date
// before putting it in a URL, otherwise the destination DateRange applies the shift a second time.
export const dateRangeUrlValue = (value, timezoneOffsetMinutes = undefined) => {
  const match = YYYY_MM_DD.exec(String(value ?? ''));
  if (!match) return value;

  const year = Number(match[1]);
  const monthIndex = Number(match[2]) - 1;
  const day = Number(match[3]);
  const offset = timezoneOffsetMinutes
    ?? new Date(year, monthIndex, day).getTimezoneOffset();

  if (offset >= 0) return value;

  return new Date(Date.UTC(year, monthIndex, day + 1)).toISOString().slice(0, 10);
};
