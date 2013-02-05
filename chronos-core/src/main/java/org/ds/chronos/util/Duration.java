package org.ds.chronos.util;

import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 
 * Utility class for representing a duration of time. Supports short hand codes
 * for durations such as 1d, 1h, or 1h30m.
 * 
 * @author Dan Simpson
 */
public class Duration {

  private static final long MILLIS_IN_SECOND = 1000l;
  private static final long MILLIS_IN_MINUTE = MILLIS_IN_SECOND * 60;
  private static final long MILLIS_IN_HOUR = MILLIS_IN_MINUTE * 60;
  private static final long MILLIS_IN_DAY = MILLIS_IN_HOUR * 24;
  private static final long MILLIS_IN_WEEK = MILLIS_IN_DAY * 7;
  private static final long MILLIS_IN_YEAR = MILLIS_IN_DAY * 365;

  private static final Map<String, Long> codes = new ConcurrentSkipListMap<String, Long>();
  static {
    codes.put("y", MILLIS_IN_YEAR);
    codes.put("w", MILLIS_IN_WEEK);
    codes.put("d", MILLIS_IN_DAY);
    codes.put("h", MILLIS_IN_HOUR);
    codes.put("m", MILLIS_IN_MINUTE);
    codes.put("s", MILLIS_IN_SECOND);
    codes.put("ms", 1l);
  }

  /**
   * Regex represents a number followed by a time code from the abbreviations
   * map above. Eg: 1d, 1d2h, 1d 3h, 1d 1d 1d... odd
   */
  private static final Pattern pattern = Pattern
      .compile("(\\d+)\\s*([a-z]{1,2})");

  private long millis;

  /**
   * 
   * @param number
   *          of milliseconds for duration
   */
  public Duration(long millis) {
    assert (millis > 0);
    this.millis = millis;
  }

  /**
   * 
   * Parse a duration string into milliseconds.
   * <p>
   * Examples: 1d = 1 day 3d4h = 3 days 4 hours
   * <p>
   * units: y = year, w = week, d = day, h = hour, m = minute, s = second, ms =
   * milliseconds.
   * 
   * @param duration
   *          abbreviation
   */
  public Duration(String shorthand) {
    this(parse(shorthand));
  }

  /**
   * @return The number of milliseconds in this duration
   */
  public long getMillis() {
    return millis;
  }

  /**
   * Reduce a number into a sub unit and return the rest
   * 
   * @param value
   * @param target
   * @param code
   * @param b
   * @return
   */
  private long reduce(long value, long target, String code, StringBuilder b) {
    if (value >= target) {
      b.append(value / target);
      b.append(code);
      return value % target;
    }
    return value;
  }

  /**
   * Generate a string representation for duration
   */
  public String toString() {
    if (millis <= 0) {
      return "0ms";
    }

    long acc = millis;

    StringBuilder b = new StringBuilder();
    acc = reduce(acc, MILLIS_IN_YEAR, "y", b);
    acc = reduce(acc, MILLIS_IN_WEEK, "w", b);
    acc = reduce(acc, MILLIS_IN_DAY, "d", b);
    acc = reduce(acc, MILLIS_IN_HOUR, "h", b);
    acc = reduce(acc, MILLIS_IN_MINUTE, "m", b);
    acc = reduce(acc, MILLIS_IN_SECOND, "s", b);
    acc = reduce(acc, 1l, "ms", b);
    return b.toString();
  }

  /**
   * Use the regex to match the pattern and sum the output
   * 
   * @param duration
   * @return
   */
  private static long parse(String duration) {
    long result = 0;

    Matcher matcher = pattern.matcher(duration.toLowerCase());
    while (matcher.find()) {
      if (matcher.groupCount() >= 2) {
        result += Long.parseLong(matcher.group(1))
            * codes.get(matcher.group(2));
      }
    }

    return result;
  }

  /**
   * Justify date to the previous (past) interval for this duration
   * 
   * @param input
   *          the date
   * @return justified date
   */
  public final Date justifyPast(Date input) {
    return new Date(justifyPast(input.getTime()));
  }

  /**
   * @param input
   *          timetamp
   * @return
   */
  public final long justifyPast(long input) {
    return input - (input % millis);
  }

  /**
   * Justify date to the previous or current interval if the current time
   * happens to fall on an interval line.
   * 
   * @param input
   * @return
   */
  public final Date justifyPastOrNow(Date input) {
    return new Date(justifyPastOrNow(input.getTime()));
  }

  /**
   * @param input
   *          timetamp
   * @return
   */
  public final long justifyPastOrNow(long input) {
    long diff = (input % millis);
    if (diff == millis) {
      return input;
    }
    return input - diff;
  }

  /**
   * Justify date to the next (future) interval for this duration
   * 
   * @param input
   *          the date
   * @return justified date
   */
  public Date justifyFuture(Date input) {
    long remainder = input.getTime() % millis;
    if (remainder == 0) {
      return input;
    }
    return new Date(input.getTime() + millis - remainder);
  }

  /**
   * Increment date by the given duration
   * 
   * @param input
   * @return new date
   */
  public Date add(Date input) {
    return new Date(input.getTime() + millis);
  }

  /**
   * Decrement date by given duration
   * 
   * @param input
   * @return new date
   */
  public Date subtract(Date input) {
    return new Date(input.getTime() - millis);
  }
}