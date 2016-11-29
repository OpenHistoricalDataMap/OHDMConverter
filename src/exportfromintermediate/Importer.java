package exportfromintermediate;

/**
 *
 * @author thsc
 */
public interface Importer {

    public void importWay(OHDMWay way);

    public void importRelation(OHDMRelation relation);

    public void importNode(OHDMNode node);
    
}
