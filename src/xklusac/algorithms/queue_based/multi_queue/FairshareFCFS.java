/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package xklusac.algorithms.queue_based.multi_queue;

import gridsim.GridSim;
import java.util.Date;
import java.util.Collections;
import java.util.LinkedList;
import xklusac.algorithms.SchedulingPolicy;
import xklusac.environment.ExperimentSetup;
import xklusac.environment.GridletInfo;
import xklusac.environment.ResourceInfo;
import xklusac.environment.Scheduler;
import xklusac.environment.User;
import xklusac.extensions.FairshareFactorComparator;

/**
 * Class FairshareFCFS<p>
 * Implements FCFS algorithm with fairsharing (queue re-ordering).
 *
 * @author Dalibor Klusacek
 */
public class FairshareFCFS implements SchedulingPolicy {

    private Scheduler scheduler;

    public FairshareFCFS(Scheduler scheduler) {
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
        //System.out.println("New job has been received by SJF");
    }

    @Override
    public int selectJob() {
        //System.out.println("Selecting job by fairFCFS...");
        int scheduled = 0;
        ResourceInfo r_cand = null;
        for (int q = 0; q < Scheduler.all_queues.size(); q++) {
            Scheduler.queue = Scheduler.all_queues.get(q);
            if (ExperimentSetup.use_fairshare) {
                // reset temp usage if time passed since last time
                scheduler.resetTemporaryUsageAndUpdateFF();
                System.setProperty("java.util.Arrays.useLegacyMergeSort", "true");
                Collections.sort(Scheduler.queue, new FairshareFactorComparator());
                /*if (Scheduler.queue.size() > 0 && ExperimentSetup.users.size()>1) {
                    System.out.print("User_A > User_B "+(ExperimentSetup.users.get("User_A").getFairshare_factor()>ExperimentSetup.users.get("User_B").getFairshare_factor()));
                    System.out.print(" | User_A ff: "+ExperimentSetup.users.get("User_A").getFairshare_factor()+" User_B ff: "+ExperimentSetup.users.get("User_B").getFairshare_factor());
                    System.out.println(" | User_A has "+ExperimentSetup.users.get("User_A").getQueued_jobs()+" User_B has "+ExperimentSetup.users.get("User_B").getQueued_jobs());
                    System.out.println(Scheduler.queue.get(0).getID()+" priority: "+ Scheduler.queue.get(0).getPriority() + " of first job, owner " + Scheduler.queue.get(0).getUser());
                }*/
            }
            for (int i = 0; i < Scheduler.queue.size(); i++) {
                GridletInfo gi = (GridletInfo) Scheduler.queue.get(i);
                for (int j = 0; j < Scheduler.resourceInfoList.size(); j++) {
                    ResourceInfo ri = (ResourceInfo) Scheduler.resourceInfoList.get(j);

                    /*if (Scheduler.isSuitable(ri, gi) && !ri.canExecuteNow(gi)) {
                        System.out.println(gi.getID()+" cannot run now on "+ri.resource.getResourceName()+": "+ri.printNodeStatus());
                    } */
                    if (Scheduler.isSuitable(ri, gi) && ri.canExecuteNow(gi)) {
                        r_cand = ri;
                        break;
                    }
                }

                if (r_cand != null) {
                    gi = (GridletInfo) Scheduler.queue.remove(i);
                    //System.err.println(gi.getID()+" PEs size = "+gi.PEs.size());
                    r_cand.addGInfoInExec(gi);
                    // set the resource ID for this gridletInfo (this is the final scheduling decision)
                    gi.setResourceID(r_cand.resource.getResourceID());

                    User u = ExperimentSetup.users.get(gi.getUser());
                    u.setQueued_jobs(u.getQueued_jobs() - 1);
                    //u.setStarted_jobs(u.getStarted_jobs()+1);

                    /*for(int us = 0; us < ExperimentSetup.user_logins.size(); us++){
                        User usr = ExperimentSetup.users.get(ExperimentSetup.user_logins.get(us));
                        System.out.print("User "+usr.getName()+"("+usr.getQueued_jobs()+") FF:"+usr.getFairshare_factor()+", ");
                    }
                    System.out.println();
                    System.out.println(gi.getID() + " starting, owner " + gi.getUser()+" remaining jobs "+u.getQueued_jobs()+" ff: "+ExperimentSetup.users.get(gi.getUser()).getFairshare_factor()+" time: "+GridSim.clock());
                     */
                    if (ExperimentSetup.use_fairshare) {
                        // add additional usage for this user to update his or her fairshare factor
                        scheduler.updateTemporaryUsageAndFFuponJobStart(gi);
                    }
                    //System.out.println(" new ff: "+ExperimentSetup.users.get(gi.getUser()).getFairshare_factor()+" time: "+GridSim.clock());
                    //System.out.println("-------end--------");
                    scheduler.submitJob(gi.getGridlet(), r_cand.resource.getResourceID());

                    r_cand.is_ready = true;
                    //scheduler.sim_schedule(GridSim.getEntityId("Alea_Job_Scheduler"), 0.0, AleaSimTags.GRIDLET_SENT, gi);
                    scheduled++;
                    r_cand = null;
                    i--;
                    return scheduled;
                } else {
                    //System.out.println(gi.getID()+" cannot run, nodes "+gi.getNumNodes()+" ppn "+gi.getPpn()+" GPU "+gi.getGpus_per_node());
                    return scheduled;
                }
            }
        }

        return scheduled;
    }
}
