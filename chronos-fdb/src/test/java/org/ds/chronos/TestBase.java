package org.ds.chronos;

import java.util.TimeZone;

import org.junit.BeforeClass;

import com.foundationdb.Database;
import com.foundationdb.FDB;

public class TestBase {

	private static FDB fdb;

	@BeforeClass
	public static void setupBasic() {
		TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
		fdb = FDB.selectAPIVersion(21);
	}

	public static void cleanup() {
		fdb.dispose();
	}

	public static Database getDatabase() {
		return fdb.open().get();
	}

}
