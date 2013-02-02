package org.ds.chronos;

import java.util.Calendar;
import java.util.Date;

import org.ds.chronos.api.ChronosException;
import org.ds.chronos.util.Duration;
import org.ds.support.TestBase;
import org.junit.Assert;
import org.junit.Test;

public class DurationTest extends TestBase {

  @Test
  public void simpleTest() throws ChronosException {
    Duration duration = new Duration(1000);
    Assert.assertEquals(1000, duration.getMillis());
  }

  @Test
  public void testYears() {
    Duration duration = new Duration("1y");
    Assert.assertEquals(1000l * 60 * 60 * 24 * 365, duration.getMillis());
  }

  @Test
  public void test2Years() {
    Duration duration = new Duration("2y");
    Assert.assertEquals(1000l * 60 * 60 * 24 * 365 * 2, duration.getMillis());
  }

  @Test
  public void testWeek() {
    Duration duration = new Duration("1w");
    Assert.assertEquals(1000 * 60 * 60 * 24 * 7, duration.getMillis());
  }

  @Test
  public void testDay() {
    Duration duration = new Duration("1d");
    Assert.assertEquals(1000 * 60 * 60 * 24, duration.getMillis());
  }

  @Test
  public void testHour() {
    Duration duration = new Duration("1h");
    Assert.assertEquals(1000 * 60 * 60, duration.getMillis());
  }

  @Test
  public void testMinute() {
    Duration duration = new Duration("1m");
    Assert.assertEquals(1000 * 60, duration.getMillis());
  }

  @Test
  public void testSecond() {
    Duration duration = new Duration("1s");
    Assert.assertEquals(1000, duration.getMillis());
  }

  @Test
  public void testMillis() {
    Duration duration = new Duration("10ms");
    Assert.assertEquals(10, duration.getMillis());
  }

  @Test
  public void testSpaced() {
    Duration duration = new Duration("10 ms");
    Assert.assertEquals(10, duration.getMillis());
  }

  @Test
  public void testCaps() {
    Duration duration = new Duration("10MS");
    Assert.assertEquals(10, duration.getMillis());
  }

  @Test
  public void testComplex() {
    Duration duration = new Duration("1h30m3ms");
    Assert.assertEquals(1000 * 60 * 90 + 3, duration.getMillis());
  }

  @Test
  public void testComplexSpaced() {
    Duration duration = new Duration("1h 30m 3ms");
    Assert.assertEquals(1000 * 60 * 90 + 3, duration.getMillis());
  }

  @Test
  public void testStrings() {
    Assert.assertEquals("999ms", new Duration("999ms").toString());
    Assert.assertEquals("1s", new Duration("1000ms").toString());
    Assert.assertEquals("59s", new Duration("59s").toString());
    Assert.assertEquals("1m", new Duration("60s").toString());
    Assert.assertEquals("59m", new Duration("59m").toString());
    Assert.assertEquals("1h", new Duration("60m").toString());
    Assert.assertEquals("6d", new Duration("6d").toString());
    Assert.assertEquals("1w", new Duration("7d").toString());
    Assert.assertEquals("1w", new Duration("1w").toString());
    Assert.assertEquals("1y", new Duration("1y").toString());
    Assert.assertEquals("1y1w1d1h1m1s15ms", new Duration(
        "1y 1w 1d 1h 1m 1s 15ms").toString());
  }

  @Test
  public void testPastJustify() {
    Calendar c = getCal();
    Date expected = c.getTime();
    c.set(2012, 1, 5, 15, 13, 22);
    Date test = c.getTime();

    Duration duration = new Duration("1h");
    Assert.assertEquals(expected, duration.justifyPast(test));
    Assert.assertEquals(expected,
        duration.justifyPast(duration.justifyPast(test)));
  }

  @Test
  public void testFutureJustify() {
    Calendar c = getCal();
    c.add(Calendar.HOUR, 1);
    Date expected = c.getTime();
    c.add(Calendar.MINUTE, -25);
    Date test = c.getTime();

    Duration duration = new Duration("1h");
    Assert.assertEquals(expected, duration.justifyFuture(test));
    Assert.assertEquals(expected,
        duration.justifyFuture(duration.justifyFuture(test)));
  }

  @Test
  public void testAdd() {
    Calendar c = getCal();
    Date expected = c.getTime();
    c.add(Calendar.HOUR, -1);
    Date test = c.getTime();
    Assert.assertEquals(expected, new Duration("1h").add(test));
  }

  @Test
  public void testSubtract() {
    Calendar c = getCal();
    Date expected = c.getTime();
    c.add(Calendar.HOUR, 1);
    Date test = c.getTime();
    Assert.assertEquals(expected, new Duration("1h").subtract(test));
  }

  private Calendar getCal() {
    Calendar c = Calendar.getInstance();
    c.set(2012, 1, 5, 15, 0, 0);
    c.set(Calendar.MILLISECOND, 0);
    return c;
  }
}
