/*
 * QueuePriorityComparator.java
 *
 *
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package xklusac.extensions;

import java.util.Comparator;

/**
 * Class QueuePriorityComparator<p>
 * Compares two queues wrt. their priority
 * @author Dalibor Klusacek
 */
public class QueuePriorityComparator implements Comparator {
    
    /**
     * Compares two queues according to their priority
     */
    public int compare(Object o1, Object o2) {
        Queue g1 = (Queue) o1;
        Queue g2 = (Queue) o2;
        double priority1 = g1.getPriority()*1.0;
        double priority2 = g2.getPriority()*1.0;
        if(priority1 < priority2) return 1;
        if(priority1 == priority2) return 0;
        if(priority1 > priority2) return -1;
        return 0;
    }
    
}
