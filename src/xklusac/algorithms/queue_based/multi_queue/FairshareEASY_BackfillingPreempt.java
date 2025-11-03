/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package xklusac.algorithms.queue_based.multi_queue;

import alea.core.AleaSimTags;
import gridsim.GridSim;
import gridsim.GridSimTags;
import gridsim.Gridlet;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedList;
import xklusac.algorithms.SchedulingPolicy;
import xklusac.environment.ExperimentSetup;
import xklusac.environment.GridletInfo;
import xklusac.environment.ResourceInfo;
import xklusac.environment.Scheduler;
import xklusac.environment.User;
import xklusac.extensions.FairshareFactorComparator;
import xklusac.extensions.WallclockComparator;

/**
 * Class FairshareEASY_Backfilling<p>
 * Implements EASY Backfilling with fairshare. It supports multiple queues and
 * fair-sharing.
 *
 * @author Dalibor Klusacek
 */
public class FairshareEASY_BackfillingPreempt implements SchedulingPolicy {

    private Scheduler scheduler;

    public FairshareEASY_BackfillingPreempt(Scheduler scheduler) {
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
        //System.out.println("New job has been received by EASY Backfilling");
    }

    @Override
    public int selectJob() {
        //System.out.println("Selecting job by EASY Backfilling...");
        int scheduled = 0;
        boolean succ = false;
        double est = 0.0;
        ResourceInfo r_cand = null;
        int r_cand_speed = 0;
        int currently_free_CPUs = 0;

        for (int j = 0; j < Scheduler.resourceInfoList.size(); j++) {
            ResourceInfo ri = (ResourceInfo) Scheduler.resourceInfoList.get(j);
            currently_free_CPUs += ri.getNumFreePE();

        }

        if (!ExperimentSetup.use_preemption) {

            if (currently_free_CPUs == 0) {
                System.out.println("Backfill terminated immediately due to no free resources: " + currently_free_CPUs + " free CPUs.");
                return 0;
            }
        }

        for (int q = 0; q < Scheduler.all_queues.size(); q++) {
            Scheduler.active_scheduling_queue = Scheduler.all_queues.get(q);
            if (ExperimentSetup.use_fairshare) {
                scheduler.resetTemporaryUsageAndUpdateFF();
                System.setProperty("java.util.Arrays.useLegacyMergeSort", "true");
                Collections.sort(Scheduler.active_scheduling_queue, new FairshareFactorComparator());
            }

            if (Scheduler.active_scheduling_queue.size() > 0) {
                //System.out.println("---BF start---");
                /*for (int us = 0; us < ExperimentSetup.user_logins.size(); us++) {
                    User usr = ExperimentSetup.users.get(ExperimentSetup.user_logins.get(us));
                    System.out.print("User " + usr.getName() + "(queued:" + usr.getQueued_jobs() + ") FF:" + usr.getFairshare_factor() + ", ");
                }
                System.out.println(" << end for queue: " + q);
                 */
                String top_user = Scheduler.active_scheduling_queue.getFirst().getUser();

                int u_index = 0;

                GridletInfo gi = (GridletInfo) Scheduler.active_scheduling_queue.get(u_index);

                // just a simple check - we should never reach this situation since we do reservation for first job later!
                if (!gi.getUser().equals(top_user)) {
                    System.exit(0);
                    break;
                }

                for (int j = 0; j < Scheduler.resourceInfoList.size(); j++) {
                    ResourceInfo ri = (ResourceInfo) Scheduler.resourceInfoList.get(j);
                    if (Scheduler.isSuitable(ri, gi) && ri.canExecuteNow(gi)) {
                        int speed = ri.peRating;
                        if (speed > r_cand_speed) {
                            r_cand = ri;
                            r_cand_speed = speed;
                        }
                    }
                }
                
                boolean is_active_queue_eligible = q < Scheduler.all_queues.size() - 1;
                if (is_active_queue_eligible && ExperimentSetup.use_preemption && r_cand == null && gi.isCheckpoint_limit_eligible() && !gi.getGridlet().isPreempted()) {
                    int gridlet_priority_level = ExperimentSetup.queues.get(gi.getQueue()).getPriority();

                    for (int j = 0; j < Scheduler.resourceInfoList.size(); j++) {
                        ResourceInfo ri = (ResourceInfo) Scheduler.resourceInfoList.get(j);
                        if (Scheduler.isSuitable(ri, gi)) {
                            LinkedList<GridletInfo> checkpointed_jobs = ri.findAndCheckpointJobs(gi, gridlet_priority_level);
                            boolean success = false;
                            if (checkpointed_jobs != null) {
                                success = true;
                                System.out.println("===========================");
                                System.out.println(gi.getID() + " needs " + gi.getNumPE() + " CPUs and resource_" + ri.resource.getResourceName() + " now has " + ri.getNumFreePE() + " at time: " + GridSim.clock());
                                System.out.println(gi.getID() + ": EASY -> PREEMPT these " + printCheckpointedJobs(checkpointed_jobs) + " jobs. This job has status = " + gi.getGridlet().getGridletStatusString());
                                scheduler.sim_schedule(ri.resource.getResourceID(), 0.0, AleaSimTags.POLICY_CHECKPOINT, (checkpointed_jobs));
                            }
                            if (success) {
                                return scheduled;
                            }
                        }
                    }
                }
                

                if (r_cand != null) {
                    gi = (GridletInfo) Scheduler.active_scheduling_queue.remove(u_index);
                    r_cand.addGInfoInExec(gi);
                    // set the resource ID for this gridletInfo (this is the final scheduling decision)
                    gi.setResourceID(r_cand.resource.getResourceID());
                    User u = ExperimentSetup.users.get(gi.getUser());
                    u.setQueued_jobs(u.getQueued_jobs() - 1);

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
                    // tell the JSS where to send which gridlet
                    scheduler.submitJob(gi.getGridlet(), r_cand.resource.getResourceID());
                    succ = true;
                    r_cand.is_ready = true;
                    //scheduler.sim_schedule(GridSim.getEntityId("Alea_Job_Scheduler"),  0.0, AleaSimTags.GRIDLET_SENT, gi);
                    return 1;
                } else {
                    u_index++;
                    if (u_index >= Scheduler.active_scheduling_queue.size()) {
                        //this queue is empty/tried - continue with another queue
                        //ALERT - no reservation if only one job remains in HIGH priority queue
                        continue;
                    }
                }
                //}
            } else {
                //this queue is empty/tried - continue with another queue
                continue;
            }
            // try backfilling procedure (this active queue is not empty/tried)
            // ALERT will not pass this test if only one job remains in HIGH priority queue
            if (!succ && Scheduler.active_scheduling_queue.size() > 1) {
                boolean removed = false;
                // do not create reservation for job that cannot be executed
                for (int j = 0; j < Scheduler.active_scheduling_queue.size(); j++) {

                    GridletInfo gi = (GridletInfo) Scheduler.active_scheduling_queue.get(j);
                    if (gi.isExecutable()) {
                        break;
                    } else {
                        // kill such job
                        System.out.println(Math.round(GridSim.clock()) + " gi:" + gi.getID() + ": KILLED BY EASY-BACKFILLING: [" + gi.getProperties() + "] CPUs=" + gi.getNumPE());
                        try {
                            gi.getGridlet().setGridletStatus(Gridlet.CANCELED);
                        } catch (Exception ex) {
                            ex.printStackTrace();
                        }
                        removed = true;
                        scheduler.sim_schedule(GridSim.getEntityId("Alea_Job_Scheduler"), 0.0, GridSimTags.GRIDLET_RETURN, gi.getGridlet());
                        Scheduler.active_scheduling_queue.remove(j);
                        j--;
                    }
                }
                // EASY will be called again when killed jobs return to Scheduler - no waiting will happen.
                if (removed) {
                    return 0;

                }
                // head of active_scheduling_queue - gridlet with reservation
                GridletInfo grsv = (GridletInfo) Scheduler.active_scheduling_queue.get(0);
                //System.out.println("Backfilling....");

                //System.out.println(Scheduler.all_queues_names.get(q)+" has "+Scheduler.active_scheduling_queue.size()+" jobs and tries to backfill... ");
                // reserved machine (i.e. Earliest Available)
                ResourceInfo rsv_res = findReservedResource(grsv);
                LinkedList<ResourceInfo> eligible_resources = new LinkedList();
                for (int e = 0; e < Scheduler.resourceInfoList.size(); e++) {
                    ResourceInfo re = (ResourceInfo) Scheduler.resourceInfoList.get(e);
                    if (re.getNumFreePE() > 0) {
                        eligible_resources.add(re);
                    }
                }

                // try backfilling on all gridlets in active_scheduling_queue except for head (grsv)
                for (int j = 1; j < Scheduler.active_scheduling_queue.size(); j++) {
                    GridletInfo gi = (GridletInfo) Scheduler.active_scheduling_queue.get(j);
                    int req = gi.getNumPE();
                    /*if (gi.getNumPE() >= grsv.getNumPE()) {
                     continue; // such gridlet will never succeed (not true if requirements used)
                     TODO
                     }*/

                    if (req > currently_free_CPUs) {
                        //System.out.println("Gridlet "+gi.getID()+" skipped in EASY. Total free CPUs = "+currently_free_CPUs+" while gridlet requests: "+gi.getNumPE()+" CPUs.");
                        continue;
                    }

                    ResourceInfo ri = findResourceBF(gi, grsv, rsv_res, eligible_resources);

                    if (ri != null) {
                        Scheduler.active_scheduling_queue.remove(j);
                        ri.addGInfoInExec(gi);
                        // set the resource ID for this gridletInfo (this is the final scheduling decision)
                        gi.setResourceID(ri.resource.getResourceID());

                        User u = ExperimentSetup.users.get(gi.getUser());
                        u.setQueued_jobs(u.getQueued_jobs() - 1);

                        if (ExperimentSetup.use_fairshare) {
                            // add additional usage for this user to update his or her fairshare factor
                            scheduler.updateTemporaryUsageAndFFuponJobStart(gi);
                        }
                        // submit job
                        System.out.println("Job: "+gi.getID()+" backfilled around reserved job: "+grsv.getID() + " in queue: "+Scheduler.all_queues_names.get(q)+" at time: " + GridSim.clock());
                        scheduler.submitJob(gi.getGridlet(), ri.resource.getResourceID());
                        gi.getGridlet().setBackfilled(1);
                        ExperimentSetup.backfilled++;
                        ri.is_ready = true;
                        succ = true;
                        //scheduler.sim_schedule(GridSim.getEntityId("Alea_Job_Scheduler"), 0.0, AleaSimTags.GRIDLET_SENT, gi);

                        scheduled++;
                        j--; //to get correct gridlet from active_scheduling_queue in next round. The active_scheduling_queue was shortened...
                        return 1;

                    }
                }
                eligible_resources = null;
            }

            //continue with the next active_scheduling_queue
        }

        /*if (min_prop.equals("1:ppn=1:mem=409600KB:vmem=137438953472KB:scratch_type=any:scratch_volume=1024mb:x86:nfs4:debian7")) {
         System.out.println("EASY scheduled " + scheduled + " gridlets, minimal requirement was " + min_CPUs_req + " CPUs with properties = (" + min_prop + "), while total available is: " + currently_free_CPUs + " CPUs, max per cluster = " + max_per_cluster);
         currently_free_CPUs = 0;
         for (int j = 0; j < Scheduler.resourceInfoList.size(); j++) {
         ResourceInfo ri = (ResourceInfo) Scheduler.resourceInfoList.get(j);
         currently_free_CPUs += ri.getNumFreePE();
            
         }
         System.out.println("------CHECK------"+ currently_free_CPUs);
         }*/
        return scheduled;
    }

    /**
     * auxiliary method needed for easy/edf backfilling
     */
    private ResourceInfo findResourceBF(GridletInfo gi, GridletInfo grsv, ResourceInfo rsv_res, LinkedList eligible_resources) {
        ResourceInfo r_cand = null;
        int r_cand_speed = 0;
        if (!ExperimentSetup.use_speeds) { // choose the first-fit resource since all have the same speed
            for (int j = 0; j < eligible_resources.size(); j++) {
                ResourceInfo ri = (ResourceInfo) eligible_resources.get(j);
                if (Scheduler.isSuitable(ri, gi) && ri.canExecuteNow(gi) && ri.resource.getResourceID() != rsv_res.resource.getResourceID()) {
                    return ri;
                } else if (Scheduler.isSuitable(ri, gi) && ri.canExecuteNow(gi) && ri.resource.getResourceID() == rsv_res.resource.getResourceID()) {
                    double eft = GridSim.clock() + gi.getJobRuntime(ri.peRating);
                    // either the filler job terminates before start time of the first job (reservation=OK) or it can use other free CPUs (no collision with reservation)
                    if ((eft < rsv_res.est) || rsv_res.usablePEs >= gi.getNumPE()) {
                        return ri;
                    }
                }
            }
            return r_cand;
        } else { // resources do not have the same speed - choose the fastest one
            for (int j = 0; j < eligible_resources.size(); j++) {
                ResourceInfo ri = (ResourceInfo) eligible_resources.get(j);
                if (Scheduler.isSuitable(ri, gi) && ri.canExecuteNow(gi) && ri.resource.getResourceID() != rsv_res.resource.getResourceID()) {
                    int speed = ri.peRating;
                    if (speed >= r_cand_speed) {
                        r_cand = ri;
                        r_cand_speed = speed;
                    }

                } else if (Scheduler.isSuitable(ri, gi) && ri.canExecuteNow(gi) && ri.resource.getResourceID() == rsv_res.resource.getResourceID()) {
                    double eft = GridSim.clock() + gi.getJobRuntime(ri.peRating);
                    if ((eft < rsv_res.est) || rsv_res.usablePEs >= gi.getNumPE()) {
                        int speed = ri.peRating;
                        if (speed > r_cand_speed) {
                            r_cand = ri;
                            r_cand_speed = speed;
                        }
                    }
                }
            }
            return r_cand;
        }
    }

    /**
     * Auxiliary method for easy/edf backfilling
     */
    private ResourceInfo findReservedResource(GridletInfo grsv) {
        double est = Double.MAX_VALUE;
        ResourceInfo found = null;
        for (int j = 0; j < Scheduler.resourceInfoList.size(); j++) {
            ResourceInfo ri = (ResourceInfo) Scheduler.resourceInfoList.get(j);
            if (ri.getNumRunningPE() >= grsv.getNumPE()) {
                double ri_est = ri.getEarliestStartTime(grsv, GridSim.clock());
                // select minimal EST
                if (ri_est <= est) {
                    est = ri_est;
                    found = ri;
                }

            } else if (ri.resource.getNumPE() >= grsv.getNumPE()) {
                double ri_est = Double.MAX_VALUE - 10.0;
                // select minimal EST
                if (ri_est <= est) {
                    est = ri_est;
                    found = ri;
                }
            } else {
                continue; // this is not suitable cluster

            }
        }
        return found;
    }
    
    private String printCheckpointedJobs(LinkedList<GridletInfo> checkpointed_jobs) {
        String jobs = "";
        for (int i = 0; i < checkpointed_jobs.size(); i++) {
            jobs += checkpointed_jobs.get(i).getID() + ", ";
        }
        return jobs;
    }
    
    private void printQueuedJobs(LinkedList<GridletInfo> jobs, String qname) {
        System.out.print(qname+": [");
        for (int i = 0; i < jobs.size(); i++) {
            System.out.print(jobs.get(i).getID() + "("+jobs.get(i).getUser()+"),");
        }
        System.out.println("] End of queue ");
    }
}
