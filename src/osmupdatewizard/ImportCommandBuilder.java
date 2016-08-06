package osmupdatewizard;

import java.util.HashMap;
import java.util.HashSet;

/**
 *
 * @author thsc
 */
public interface ImportCommandBuilder {

  public void addNode(HashMap<String, String> attributes, HashSet<TagElement> tags);

  public void addWay(HashMap<String, String> attributes, HashSet<NDElement> nds, HashSet<TagElement> tags);

  public void addRelation(HashMap<String, String> attributes, HashSet<MemberElement> members, HashSet<TagElement> tags);

  public void printStatus();
}
