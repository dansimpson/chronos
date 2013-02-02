package org.ds.support;

import java.util.TimeZone;

import org.junit.BeforeClass;

public class TestBase {

  @BeforeClass
  public static void setupBasic() {
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
  }

}
