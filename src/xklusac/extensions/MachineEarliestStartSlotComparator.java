package xklusac.extensions;
import gridsim.ResGridlet;
import java.util.Comparator;
import xklusac.environment.ComplexGridlet;
import xklusac.environment.MachineWithRAMandGPUs;
/**
 * Class JobCompletionTimeComparator<p>
 * Compares two gridlets according to expected completion time (smaller first).
 * @author Dalibor Klusacek
 */
public class MachineEarliestStartSlotComparator implements Comparator {
    
    /**
     * Compares two machines according to their expected earliest job start time (smaller first)
     */
    public int compare(Object o1, Object o2) {
        MachineWithRAMandGPUs g1 = (MachineWithRAMandGPUs) o1;
        MachineWithRAMandGPUs g2 = (MachineWithRAMandGPUs) o2;
        double priority1 = g1.getEst();
        double priority2 = g2.getEst();
        if(priority1 > priority2) return 1;
        if(priority1 == priority2) return 0;
        if(priority1 < priority2) return -1;
        return 0;
    }
    
}
