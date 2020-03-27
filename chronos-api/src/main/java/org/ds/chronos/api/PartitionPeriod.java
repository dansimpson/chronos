package org.ds.chronos.api;

import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.TimeZone;

/**
 * A period of time for use with partitioning data over multiple keys
 * 
 * @author Dan Simpson
 * 
 */
public enum PartitionPeriod {

	HOUR(Calendar.HOUR),
	DAY(Calendar.DAY_OF_MONTH),
	MONTH(Calendar.MONTH),
	YEAR(Calendar.YEAR);

	private int calendarField;

	private PartitionPeriod(int calendarField) {
		this.calendarField = calendarField;
	}

	public final int getCalendarField() {
		return calendarField;
	}

	public final String getPeriodKey(String key, long timestamp) {
		return getPeriodKey(key, getCalendar(timestamp));
	}

	public final String getPeriodKey(String key, Calendar date) {
		switch (this) {
		case HOUR:
			return String.format("%s-%04d-%02d-%02d-%02d", key, date.get(Calendar.YEAR), date.get(Calendar.MONTH) + 1,
			    date.get(Calendar.DAY_OF_MONTH), date.get(Calendar.HOUR_OF_DAY));
		case DAY:
			return String.format("%s-%04d-%02d-%02d", key, date.get(Calendar.YEAR), date.get(Calendar.MONTH) + 1,
			    date.get(Calendar.DAY_OF_MONTH));
		case MONTH:
			return String.format("%s-%04d-%02d", key, date.get(Calendar.YEAR), date.get(Calendar.MONTH) + 1);
		case YEAR:
			return String.format("%s-%04d", key, date.get(Calendar.YEAR));
		default:
			return null;
		}
	}

	public final List<String> getPeriodKeys(String prefix, Date d1, Date d2) {
		return getPeriodKeys(prefix, d1.getTime(), d2.getTime());
	}

	private static final TimeZone TZ = TimeZone.getTimeZone("UTC");

	public final List<String> getPeriodKeys(String prefix, long t1, long t2) {
		if (t1 > t2) {
			List<String> keys = getPeriodKeys(prefix, t2, t1);
			Collections.reverse(keys);
			return keys;
		}

		Calendar calendar = getCalendar(t1);

		LinkedList<String> keys = new LinkedList<String>();
		while (calendar.getTimeInMillis() <= t2) {
			keys.add(getPeriodKey(prefix, calendar));
			calendar.add(calendarField, 1);
			clearCal(calendar);
		}
		return keys;
	}

	private Calendar getCalendar(long timestamp) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTimeZone(TZ);
		calendar.setTimeInMillis(timestamp);
		clearCal(calendar);
		return calendar;
	}

	private void clearCal(Calendar calendar) {
		calendar.set(Calendar.MINUTE, 0);
		calendar.set(Calendar.SECOND, 0);
		calendar.set(Calendar.MILLISECOND, 0);

		if (this != HOUR) {
			calendar.set(Calendar.AM_PM, 0);
			calendar.set(Calendar.HOUR, 0);
		}

		// We need to fetch every key which could have data. If we
		// have a query that falls on the 8th, this will account for
		// that, and make sure we get all keys
		if (this == MONTH || this == YEAR) {
			calendar.set(Calendar.DAY_OF_MONTH, 1);
		}

		if (this == YEAR) {
			calendar.set(Calendar.MONTH, 0);
		}
	}
}