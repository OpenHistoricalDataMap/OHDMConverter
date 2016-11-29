package osmupdatewizard;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

/**
 *
 * @author thsc
 */
public interface ImportCommandBuilder {

  public void addNode(HashMap<String, String> attributes, ArrayList<TagElement> tags);

  public void addWay(HashMap<String, String> attributes, ArrayList<NodeElement> nds, ArrayList<TagElement> tags);

  public void addRelation(HashMap<String, String> attributes, ArrayList<MemberElement> members, ArrayList<TagElement> tags);

  public void flush();
  
  public void printStatus();
}
