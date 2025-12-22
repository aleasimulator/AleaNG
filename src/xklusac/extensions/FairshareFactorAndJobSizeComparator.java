package xklusac.extensions;
import java.util.Comparator;
import xklusac.environment.GridletInfo;

/**
 * Class WallclockComparator<p>
 * Compares two gridlets according to their wallclock value.
 * @author Dalibor Klusacek
 */
public class FairshareFactorAndJobSizeComparator implements Comparator {
    /**
     * Compares two gridlets according to their FF value. Higher goes first.
     */
    @Override
    public int compare(Object o1, Object o2) {
        GridletInfo g1 = (GridletInfo) o1;
        GridletInfo g2 = (GridletInfo) o2;
        
        // Primary: FF descending
        int ffCompare = Double.compare(g2.getPriority(), g1.getPriority()); 
        if (ffCompare != 0) {
            return ffCompare;
        }

        // Secondary: id ascending (int)
        return Integer.compare((g2.getPpn()*g2.getNumNodes()), (g1.getPpn()*g1.getNumNodes()));
    }
  
    
    

}
