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
public class Logger {

  private boolean enabled;
  private static Logger instance = null;

  private Logger() {
    if (Config.getInstance().getValue("logger").equalsIgnoreCase("enabled")) {
      this.enabled = true;
    } else {
      this.enabled = false;
    }
  }

  public static Logger getInstance() {
    if (instance == null) {
      instance = new Logger();
    }
    return instance;
  }

  public boolean isEnabled() {
    return this.enabled;
  }

  public void setEnabled(boolean enabled) {
    this.enabled = enabled;
  }

  public void print(String message) {
    if (this.enabled) {
      System.out.println(message);
    }
  }
  
  public void print(String message, boolean timestamp){
    if (this.enabled){
      Date time = new Timestamp(Calendar.getInstance().getTime().getTime());
      DateFormat df = new SimpleDateFormat("HH:mm:ss: ");
      System.out.println(df.format(time) + message);
    }
  }
}
