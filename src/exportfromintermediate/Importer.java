package exportfromintermediate;

/**
 *
 * @author thsc
 */
public interface Importer {

    public boolean importWay(OHDMWay way);

    public boolean importRelation(OHDMRelation relation);

    public boolean importNode(OHDMNode node);
    
}
