/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package xklusac.algorithms.queue_based.multi_queue;

import java.util.Date;
import java.util.Collections;
import java.util.LinkedList;
import xklusac.algorithms.SchedulingPolicy;
import xklusac.environment.ExperimentSetup;
import xklusac.environment.GridletInfo;
import xklusac.environment.ResourceInfo;
import xklusac.environment.Scheduler;
import xklusac.environment.User;
import xklusac.extensions.FairshareFactorAndJobIDComparator;

/**
 * Class FairshareAggressiveBackfilling<p>
 * Implements aggressive backfilling algorithm which does not use job reservation. It supports multiple queues and fair-sharing.
 *
 * @author Dalibor Klusacek
 */
public class FairshareAggressiveBackfilling implements SchedulingPolicy {

    private Scheduler scheduler;

    public FairshareAggressiveBackfilling(Scheduler scheduler) {
        this.scheduler = scheduler;
    }

    @Override
    public void addNewJob(GridletInfo gi) {
        double runtime1 = new Date().getTime();
        int index = Scheduler.all_queues_names.indexOf(gi.getQueue());
        if (index == -1 || ExperimentSetup.by_queue == false) {
            index = 0;
        }
        LinkedList queue = Scheduler.all_queues.get(index);
        queue.addLast(gi);
        Scheduler.runtime += (new Date().getTime() - runtime1);
        //System.out.println(gi.getQueue() + " New job has been received in active_scheduling_queue " + index);
    }

    @Override
    public int selectJob() {
        boolean backfill = false;
        
        int scheduled = 0;
        ResourceInfo r_cand = null;
        
        int currently_free_CPUs = 0;

        for (int j = 0; j < Scheduler.resourceInfoList.size(); j++) {
            ResourceInfo ri = (ResourceInfo) Scheduler.resourceInfoList.get(j);
            currently_free_CPUs += ri.getNumFreePE();

        }
        if (currently_free_CPUs == 0) {
            System.out.println("Backfill terminated immediately due to no free resources: " + currently_free_CPUs + " free CPUs.");
            return 0;
        }               

        for (int q = 0; q < Scheduler.all_queues.size(); q++) {
            Scheduler.active_scheduling_queue = Scheduler.all_queues.get(q);
            if (ExperimentSetup.use_fairshare) {
                // reset temp usage if time passed since last time
                scheduler.resetTemporaryUsageAndUpdateFF();
                System.setProperty("java.util.Arrays.useLegacyMergeSort", "true");
                Collections.sort(Scheduler.active_scheduling_queue, new FairshareFactorAndJobIDComparator());
                
            }
            
            for (int i = 0; i < Scheduler.active_scheduling_queue.size(); i++) {
                GridletInfo gi = (GridletInfo) Scheduler.active_scheduling_queue.get(i);
                for (int j = 0; j < Scheduler.resourceInfoList.size(); j++) {
                    ResourceInfo ri = (ResourceInfo) Scheduler.resourceInfoList.get(j);

                    if (Scheduler.isSuitable(ri, gi) && ri.canExecuteNow(gi)) {
                        r_cand = ri;
                        break;
                    }
                }
                if (r_cand != null) {
                    gi = (GridletInfo) Scheduler.active_scheduling_queue.remove(i);
                    //System.err.println(gi.getID()+" PEs size = "+gi.PEs.size());
                    r_cand.addGInfoInExec(gi);
                    // set the resource ID for this gridletInfo (this is the final scheduling decision)
                    gi.setResourceID(r_cand.resource.getResourceID());
                    
                    User u = ExperimentSetup.users.get(gi.getUser());
                    u.setQueued_jobs(u.getQueued_jobs() - 1);
                    
                    
                    if (ExperimentSetup.use_fairshare) {
                        // add additional usage for this user to update his or her fairshare factor
                        scheduler.updateTemporaryUsageAndFFuponJobStart(gi);
                    }
                    //System.out.println(gi.getID()+" start in active_scheduling_queue "+gi.getQueue()+", avail:"+ExperimentSetup.queues.get(gi.getQueue()).getQueueAvailCPUs()+" of "+ExperimentSetup.queues.get(gi.getQueue()).getLimit()+" req:"+gi.getNumPE());
                    scheduler.submitJob(gi.getGridlet(), r_cand.resource.getResourceID());
                    r_cand.is_ready = true;
                    //scheduler.sim_schedule(GridSim.getEntityId("Alea_Job_Scheduler"), 0.0, AleaSimTags.GRIDLET_SENT, gi);
                    scheduled++;
                    //System.out.println(gi.getID()+": submit on "+r_cand.resource.getResourceName());
                    r_cand = null;
                    i--;
                    if (backfill) {
                        gi.getGridlet().setBackfilled(1);
                        ExperimentSetup.backfilled++;
                        //System.out.println(gi.getID() + ": backfilled. Queue size = " + Scheduler.active_scheduling_queue.size());
                    }
                    return scheduled;
                } else {
                    // we skip this job and will try to backfill (if there are some jobs)
                    backfill = true;                    
                }
            }
        }

        return scheduled;
    }
}
