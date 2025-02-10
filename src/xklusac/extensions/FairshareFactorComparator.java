package xklusac.extensions;
import java.util.Comparator;
import xklusac.environment.GridletInfo;

/**
 * Class WallclockComparator<p>
 * Compares two gridlets according to their wallclock value.
 * @author Dalibor Klusacek
 */
public class FairshareFactorComparator implements Comparator {
    /**
     * Compares two gridlets according to their FF value. Higher goes first.
     */
    public int compare(Object o1, Object o2) {
        GridletInfo g1 = (GridletInfo) o1;
        GridletInfo g2 = (GridletInfo) o2;
        double priority1 = (Double) g1.getPriority();
        double priority2 = (Double) g2.getPriority();
        if(priority2 > priority1) return 1;
        if(priority1 == priority2) return 0;
        if(priority2 < priority1) return -1;
        return 0;
    }
    

}
