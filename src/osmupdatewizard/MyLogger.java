package osmupdatewizard;

import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 *
 * @author Sven Petsche
 */
public class MyLogger {

  private boolean enabled;
  private static MyLogger instance = null;
  private final int lvl;

  private MyLogger() {
    if (Config.getInstance().getValue("logger").equalsIgnoreCase("enabled")) {
      this.enabled = true;
    } else {
      this.enabled = false;
    }
    this.lvl = Integer.parseInt(Config.getInstance().getValue("logLevel"));
  }

  public static MyLogger getInstance() {
    if (instance == null) {
      instance = new MyLogger();
    }
    return instance;
  }

  public boolean isEnabled() {
    return this.enabled;
  }

  public void setEnabled(boolean enabled) {
    this.enabled = enabled;
  }

  public void print(int level, String message) {
    if (this.checkIfPrint(level)) {
      System.out.println(message);
    }
  }

  public void print(int level, String message, boolean timestamp) {
    if (this.checkIfPrint(level)) {
      Date time = new Timestamp(Calendar.getInstance().getTime().getTime());
      DateFormat df = new SimpleDateFormat("HH:mm:ss: ");
      System.out.println(df.format(time) + message);
    }
  }

  private boolean checkIfPrint(int level) {
    return this.enabled && level <= this.lvl;
  }
}
