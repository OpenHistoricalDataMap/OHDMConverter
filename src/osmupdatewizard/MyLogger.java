package osmupdatewizard;

import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * Singleton
 *
 * @author Sven Petsche
 */
public class MyLogger {

  private boolean enabled;
  private static MyLogger instance = new MyLogger();
  private final int lvl;

  /**
   * Loads generel information from Config.
   */
  private MyLogger() {
    this.enabled = Config.getInstance().getValue("logger").equalsIgnoreCase("enabled");
    this.lvl = Integer.parseInt(Config.getInstance().getValue("logLevel"));
  }

  /**
   *
   * @return instance of this object
   */
  public static MyLogger getInstance() {
    return instance;
  }

  public boolean isEnabled() {
    return this.enabled;
  }

  public void setEnabled(boolean enabled) {
    this.enabled = enabled;
  }

  public int getLevel() {
    return this.lvl;
  }

  /**
   * Prints the message if the printlevel is good enough.
   *
   * @param level
   * @param message
   */
  public void print(int level, String message) {
    if (this.checkIfPrint(level)) {
      System.out.println(message);
    }
  }

  /**
   * Prints the message with a timestamp in front if the printlevel is enough.
   *
   * @param level
   * @param message
   * @param timestamp
   */
  public void print(int level, String message, boolean timestamp) {
    if (this.checkIfPrint(level)) {
      Date time = new Timestamp(Calendar.getInstance().getTime().getTime());
      DateFormat df = new SimpleDateFormat("HH:mm:ss: ");
      System.out.println(df.format(time) + message);
    }
  }

  /**
   * Private method to check if message should be printed.
   *
   * @param level
   * @return
   */
  private boolean checkIfPrint(int level) {
    return this.enabled && level <= this.lvl;
  }
}
