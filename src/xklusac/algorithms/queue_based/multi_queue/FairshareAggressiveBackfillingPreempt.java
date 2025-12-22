/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package xklusac.algorithms.queue_based.multi_queue;

import alea.core.AleaSimTags;
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
import xklusac.extensions.FairshareFactorAndJobIDComparator;

/**
 * Class FairshareAggressiveBackfilling<p>
 * Implements aggressive backfilling algorithm which does not use job
 * reservation. It supports multiple queues and fair-sharing.
 *
 * @author Dalibor Klusacek
 */
public class FairshareAggressiveBackfillingPreempt implements SchedulingPolicy {

    private Scheduler scheduler;

    public FairshareAggressiveBackfillingPreempt(Scheduler scheduler) {
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
        

        if (!ExperimentSetup.use_preemption) {
            int currently_free_CPUs = 0;

            for (int j = 0; j < Scheduler.resourceInfoList.size(); j++) {
                ResourceInfo ri = (ResourceInfo) Scheduler.resourceInfoList.get(j);
                currently_free_CPUs += ri.getNumFreePE();

            }
            if (currently_free_CPUs == 0) {
                //System.out.println("Backfill terminated immediately due to no free resources: " + currently_free_CPUs + " free CPUs.");
                return 0;
            }
        }

        for (int q = 0; q < Scheduler.all_queues.size(); q++) {
            Scheduler.active_scheduling_queue = Scheduler.all_queues.get(q);
            //System.out.println(Scheduler.all_queues_names.get(q) + " has " + Scheduler.active_scheduling_queue.size() + " jobs and priority " + ExperimentSetup.queues.get(Scheduler.all_queues_names.get(q)).getPriority());
            
            if (ExperimentSetup.use_fairshare) {
                // reset temp usage if time passed since last time
                scheduler.resetTemporaryUsageAndUpdateFF();
                System.setProperty("java.util.Arrays.useLegacyMergeSort", "true");
                Collections.sort(Scheduler.active_scheduling_queue, new FairshareFactorAndJobIDComparator());

            }
            //printQueuedJobs(Scheduler.active_scheduling_queue, Scheduler.all_queues_names.get(q));

            for (int i = 0; i < Scheduler.active_scheduling_queue.size(); i++) {
                ResourceInfo r_cand = null;
                GridletInfo gi = (GridletInfo) Scheduler.active_scheduling_queue.get(i);
                for (int j = 0; j < Scheduler.resourceInfoList.size(); j++) {
                    ResourceInfo ri = (ResourceInfo) Scheduler.resourceInfoList.get(j);

                    if (Scheduler.isSuitable(ri, gi) && ri.canExecuteNow(gi)) {
                        r_cand = ri;
                        break;
                    }
                }

                // try preemption (only non-preempted job from higher priority queue with OK user/queue limits can evict other jobs)
                boolean is_active_queue_eligible = q < Scheduler.all_queues.size() - 1;
                // isCheckpoint_limit_eligible() - checks whether this job is not subject to quota limits already 
                // isPreempted() checks that this is not the previously preempted job
                if (is_active_queue_eligible && ExperimentSetup.use_preemption && r_cand == null && gi.isCheckpoint_limit_eligible() && !gi.getGridlet().isPreempted()) {
                    int gridlet_priority_level = ExperimentSetup.queues.get(gi.getQueue()).getPriority();

                    for (int j = 0; j < Scheduler.resourceInfoList.size(); j++) {
                        ResourceInfo ri = (ResourceInfo) Scheduler.resourceInfoList.get(j);
                        if (Scheduler.isSuitable(ri, gi)) {
                            System.out.println("====== find preemptable jobs for: " + gi.getID()+" ["+ gi.getNumNodes()+ "x"+gi.getPpn()+"] from "+gi.getQueue()+" at position "+i);
                            LinkedList<GridletInfo> checkpointed_jobs = ri.findAndCheckpointJobs(gi, gridlet_priority_level);
                            //System.out.println("=========end of finding==========");
                            boolean success = false;
                            if (checkpointed_jobs != null) {
                                success = true;
                                System.out.println("=========== initiate preemption ================");
                                System.out.println(gi.getID() + " needs " + gi.getNumNodes()+ "x"+gi.getPpn()+" CPUs, resource_" + ri.resource.getResourceName() + " now has " + ri.getNumFreePE() + " at time: " + GridSim.clock());
                                System.out.println(gi.getID() + ": Aggressive Backfill -> PREEMPT these " + printCheckpointedJobs(checkpointed_jobs) + " jobs. This job has status = " + gi.getGridlet().getGridletStatusString());
                                if (checkpointed_jobs.size() > 0) {
                                    Scheduler.waiting_for_preempted_job_to_arrive = true;
                                }
                                scheduler.sim_schedule(ri.resource.getResourceID(), 0.0, AleaSimTags.POLICY_CHECKPOINT, (checkpointed_jobs));
                                System.out.println("=============== done ================");
                            }
                            if (success) {
                                // we can quit now because next time there will be free resources and gi will run
                                return scheduled;
                            }
                        }
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
                    //System.out.println(gi.getID()+": submitting to "+r_cand.resource.getResourceName());
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

    private String printCheckpointedJobs(LinkedList<GridletInfo> checkpointed_jobs) {
        String jobs = "";
        for (int i = 0; i < checkpointed_jobs.size(); i++) {
            GridletInfo gi = checkpointed_jobs.get(i);
            jobs += gi.getID() + "["+gi.getNumNodes()+"x"+gi.getPpn()+"](nodes: "+gi.getGridlet().assigned_machines+"), ";
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
