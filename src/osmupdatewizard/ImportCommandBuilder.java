package osmupdatewizard;

import java.util.HashMap;
import java.util.HashSet;

/**
 *
 * @author thsc
 */
public interface ImportCommandBuilder {

  public void addNode(HashMap<String, String> attributes, HashSet<TagElement> tags);

  public void addWay(HashMap<String, String> attributes, HashSet<NodeElement> nds, HashSet<TagElement> tags);

  public void addRelation(HashMap<String, String> attributes, HashSet<MemberElement> members, HashSet<TagElement> tags);

  public void flush();
  
  public void printStatus();
}
