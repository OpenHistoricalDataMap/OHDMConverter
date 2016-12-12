package osmupdatewizard;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;

/**
 *
 * @author thsc
 */
public interface ImportCommandBuilder {

  public void addNode(HashMap<String, String> attributes, ArrayList<TagElement> tags) throws SQLException;

  public void addWay(HashMap<String, String> attributes, ArrayList<NodeElement> nds, ArrayList<TagElement> tags) throws SQLException;

  public void addRelation(HashMap<String, String> attributes, ArrayList<MemberElement> members, ArrayList<TagElement> tags) throws SQLException;

  public void flush() throws SQLException;
  
  public void printStatus();
}
