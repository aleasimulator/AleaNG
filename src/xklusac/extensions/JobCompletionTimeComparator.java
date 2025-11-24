package xklusac.extensions;
import gridsim.ResGridlet;
import java.util.Comparator;
import xklusac.environment.ComplexGridlet;
/**
 * Class JobCompletionTimeComparator<p>
 * Compares two gridlets according to expected completion time (smaller first).
 * @author Dalibor Klusacek
 */
public class JobCompletionTimeComparator implements Comparator {
    
    /**
     * Compares two gridlets according to their expected completion time (smaller first)
     */
    public int compare(Object o1, Object o2) {
        ResGridlet g1 = (ResGridlet) o1;
        ResGridlet g2 = (ResGridlet) o2;
        double priority1 = ((ComplexGridlet) g1.getGridlet()).getExpectedFinishTime();
        double priority2 = ((ComplexGridlet) g2.getGridlet()).getExpectedFinishTime();
        if(priority1 > priority2) return 1;
        if(priority1 == priority2) return 0;
        if(priority1 < priority2) return -1;
        return 0;
    }
    
}
