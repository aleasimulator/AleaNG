/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package xklusac.algorithms.queue_based.multi_queue;

import alea.core.AleaSimTags;
import gridsim.GridSim;
import gridsim.GridSimTags;
import gridsim.Gridlet;
import gridsim.MachineList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedList;
import xklusac.algorithms.SchedulingPolicy;
import xklusac.environment.ExperimentSetup;
import xklusac.environment.GridletInfo;
import xklusac.environment.MachineWithRAMandGPUs;
import xklusac.environment.ResourceInfo;
import xklusac.environment.Scheduler;
import xklusac.environment.User;
import xklusac.extensions.FairshareFactorAndJobIDComparator;

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

        deletePreviousReservations();

        for (int j = 0; j < Scheduler.resourceInfoList.size(); j++) {
            ResourceInfo ri = (ResourceInfo) Scheduler.resourceInfoList.get(j);
            currently_free_CPUs += ri.getNumFreePE();

        }

        if (!ExperimentSetup.use_preemption) {

            if (currently_free_CPUs == 0) {
                //System.out.println("Backfill terminated immediately due to no free resources: " + currently_free_CPUs + " free CPUs.");
                return 0;
            }
        }

        GridletInfo gridlet_with_reservation = null;

        for (int q = 0; q < Scheduler.all_queues.size(); q++) {
            Scheduler.active_scheduling_queue = Scheduler.all_queues.get(q);
            //System.out.println(Scheduler.all_queues_names.get(q) + " has " + Scheduler.active_scheduling_queue.size() + " jobs and priority " + ExperimentSetup.queues.get(Scheduler.all_queues_names.get(q)).getPriority());
            
            if (ExperimentSetup.use_fairshare) {
                scheduler.resetTemporaryUsageAndUpdateFF();
                System.setProperty("java.util.Arrays.useLegacyMergeSort", "true");
                Collections.sort(Scheduler.active_scheduling_queue, new FairshareFactorAndJobIDComparator());
            }

            if (Scheduler.active_scheduling_queue.size() > 0 && gridlet_with_reservation == null) {
                //System.out.println("START EASY in queue: " + Scheduler.all_queues_names.get(q) + " with " + Scheduler.active_scheduling_queue.size() + " jobs at:" + Math.round(GridSim.clock()));

                //System.out.println("---BF start---");
                /*for (int us = 0; us < ExperimentSetup.user_logins.size(); us++) {
                    User usr = ExperimentSetup.users.get(ExperimentSetup.user_logins.get(us));
                    System.out.print("User " + usr.getName() + "(queued:" + usr.getQueued_jobs() + ") FF:" + usr.getFairshare_factor() + ", ");
                }
                System.out.println(" << end for queue: " + q);
                 */
                String top_user = Scheduler.active_scheduling_queue.getFirst().getUser();

                int job_position = 0;

                // select the first job in a given queue
                GridletInfo gi = (GridletInfo) Scheduler.active_scheduling_queue.get(job_position);

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

                // only higher priority queue can preempt... from the next queue
                boolean is_active_queue_eligible = q < Scheduler.all_queues.size() - 1;
                // isCheckpoint_limit_eligible() - checks whether this job is not subject to quota limits already 
                // isPreempted() checks that this is not the previously preempted job
                if (is_active_queue_eligible && ExperimentSetup.use_preemption && r_cand == null && gi.isCheckpoint_limit_eligible() && !gi.getGridlet().isPreempted()) {
                    int gridlet_priority_level = ExperimentSetup.queues.get(gi.getQueue()).getPriority();

                    for (int j = 0; j < Scheduler.resourceInfoList.size(); j++) {
                        ResourceInfo ri = (ResourceInfo) Scheduler.resourceInfoList.get(j);
                        if (Scheduler.isSuitable(ri, gi)) {
                            System.out.println("====== find preemptable jobs for: " + gi.getID()+" from "+gi.getQueue()+" at position "+job_position);
                            LinkedList<GridletInfo> checkpointed_jobs = ri.findAndCheckpointJobs(gi, gridlet_priority_level);
                            //System.out.println("=========end of finding==========");
                            boolean success = false;
                            if (checkpointed_jobs != null) {
                                success = true;
                                System.out.println("=========== initiate preemption ================");
                                System.out.println(gi.getID() + " needs " + gi.getNumNodes()+ "x"+gi.getPpn()+" CPUs, resource_" + ri.resource.getResourceName() + " now has " + ri.getNumFreePE() + " at time: " + GridSim.clock());
                                System.out.println(gi.getID() + ": EASY -> PREEMPT these " + printCheckpointedJobs(checkpointed_jobs) + " jobs. This job has status = " + gi.getGridlet().getGridletStatusString());
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
                    gi = (GridletInfo) Scheduler.active_scheduling_queue.remove(job_position);
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
                    System.out.println("Job: " + gi.getID() + " scheduled from queue: " + Scheduler.all_queues_names.get(q) + " at time: " + Math.round(GridSim.clock()));
                    scheduler.submitJob(gi.getGridlet(), r_cand.resource.getResourceID());
                    succ = true;
                    r_cand.is_ready = true;
                    //scheduler.sim_schedule(GridSim.getEntityId("Alea_Job_Scheduler"),  0.0, AleaSimTags.GRIDLET_SENT, gi);
                    return 1;
                } else {
                    job_position++;
                    if (job_position >= Scheduler.active_scheduling_queue.size()) {
                        //this queue is empty/tried - continue with another queue
                        if (gridlet_with_reservation == null) {
                            gridlet_with_reservation = (GridletInfo) Scheduler.active_scheduling_queue.get(0);
                        }

                        //System.out.println(q + ": Continue with another queue... gridlet_with_reservation: " + gridlet_with_reservation.getID());
                        //ALERT - no reservation if only one job remains in HIGH priority queue
                        //Example:
                        //>>> 10 arrived, in queue/schedule 4 jobs, requiring 84 CPUs, held jobs 0 running 6 jobs, free CPUs 7, free RAM 17408 GB, free GPUs 272, tried jobs 0, Day: 01-01-2025 [01:00] SimClock: 18
                        //START EASY in queue: priority_queue with 1 jobs at:18
                        //START EASY in queue: default_queue with 3 jobs at:18
                        continue; //(with another queue)
                    }
                }

            }

            // try backfilling procedure (this active queue is not empty/tried)
            // 
            // Possible solution - remember the first job ALWAYS (gridlet_with_reservation not first job from active queue but from first queue)
            // Done, this works, next ALERT: will not create more reservations for multiple queues. Only for one queue.
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
                if (gridlet_with_reservation == null) {
                    gridlet_with_reservation = (GridletInfo) Scheduler.active_scheduling_queue.get(0);
                }

                //System.out.println(Scheduler.all_queues_names.get(q) + " has " + Scheduler.active_scheduling_queue.size() + " jobs and tries to backfill around " + gridlet_with_reservation.getID());
                // reserved machine (i.e. Earliest Available)
                ResourceInfo reserved_resource = findReservedResource(gridlet_with_reservation);
                LinkedList<ResourceInfo> eligible_resources = new LinkedList();
                for (int e = 0; e < Scheduler.resourceInfoList.size(); e++) {
                    ResourceInfo re = (ResourceInfo) Scheduler.resourceInfoList.get(e);
                    if (re.getNumFreePE() > 0) {
                        eligible_resources.add(re);
                    }
                }

                // try backfilling on all gridlets in active_scheduling_queue except for head (gridlet_with_reservation)
                for (int j = 1; j < Scheduler.active_scheduling_queue.size(); j++) {
                    GridletInfo gi = (GridletInfo) Scheduler.active_scheduling_queue.get(j);
                    int req = gi.getNumPE();
                    /*if (gi.getNumPE() >= gridlet_with_reservation.getNumPE()) {
                     continue; // such gridlet will never succeed (not true if requirements used)
                     TODO
                     }*/

                    if (req > currently_free_CPUs) {
                        //System.out.println("Gridlet "+gi.getID()+" skipped in EASY. Total free CPUs = "+currently_free_CPUs+" while gridlet requests: "+gi.getNumPE()+" CPUs.");
                        continue;
                    }

                    //ResourceInfo ri = findResourceBF(gi, gridlet_with_reservation, reserved_resource, eligible_resources);
                    ResourceInfo ri = backfillAroundReservation(gi, gridlet_with_reservation, reserved_resource, eligible_resources);

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
                        System.out.println("Job: " + gi.getID() + " backfilled around reserved job: " + gridlet_with_reservation.getID() + " in queue: " + Scheduler.all_queues_names.get(q) + " at time: " + GridSim.clock());
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

        return scheduled;
    }

    private ResourceInfo backfillAroundReservation(GridletInfo gi, GridletInfo grsv, ResourceInfo rsv_res, LinkedList eligible_resources) {
        ResourceInfo r_cand = null;

        // first try different resources than those reserved
        for (int j = 0; j < eligible_resources.size(); j++) {
            ResourceInfo ri = (ResourceInfo) eligible_resources.get(j);
            if (Scheduler.isSuitable(ri, gi) && ri.canExecuteNow(gi) && ri.resource.getResourceID() != rsv_res.resource.getResourceID()) {
                //System.out.println("BANS ignored, different resource used ");
                double eft = GridSim.clock() + gi.getJobRuntime(ri.peRating);
                gi.setExpectedFinishTime(eft);
                gi.getGridlet().setIs_using_reserved_resource(false);
                return ri;
            }
        }

        // next, check those reserved
        for (int j = 0; j < eligible_resources.size(); j++) {
            ResourceInfo ri = (ResourceInfo) eligible_resources.get(j);
            if (Scheduler.isSuitable(ri, gi) && ri.canExecuteNow(gi) && ri.resource.getResourceID() == rsv_res.resource.getResourceID()) {
                double eft = GridSim.clock() + gi.getJobRuntime(ri.peRating);
                gi.setExpectedFinishTime(eft);

                // either the filler job terminates before start time of the first job (reservation=OK) or it can use other free CPUs (no collision with reservation)
                //if ((eft < reserved_resource.est) || reserved_resource.usablePEs >= gi.getNumPE()) {
                if ((eft < rsv_res.est)) {
                    //System.out.println("BANS ignored, end "+eft+" < EST " + rsv_res.est);
                    gi.getGridlet().setIs_using_reserved_resource(false);
                    return ri;
                } else {
                    boolean it_fits = ri.canGridletFitNextToReservation(gi, grsv);
                    if (it_fits) {
                        //System.out.println("MUST RESPECT BANS");
                        gi.getGridlet().setIs_using_reserved_resource(true);
                        return ri;
                    }

                }
            }
        }
        return r_cand;

    }

    /**
     * Auxiliary method for easy/edf backfilling
     */
    private ResourceInfo findReservedResource(GridletInfo grsv) {
        double est = Double.MAX_VALUE;
        ResourceInfo found = null;
        //System.out.println(grsv.getID()+" START to look for reservation --------------");
        for (int j = 0; j < Scheduler.resourceInfoList.size(); j++) {
            ResourceInfo ri = (ResourceInfo) Scheduler.resourceInfoList.get(j);
            if (Scheduler.isSuitable(ri, grsv)) {
                ri.getEarliestStartTimeForTopJob(grsv, GridSim.clock());
                double ri_est = ri.est;
                // select minimal EST
                if (ri_est <= est) {
                    est = ri_est;
                    found = ri;
                }
            }
        }
        //System.out.println(grsv.getID()+" END looking for reservation -------------- EST ("+est+")");
        return found;
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
        System.out.print(qname + ": [");
        for (int i = 0; i < jobs.size(); i++) {
            System.out.print(jobs.get(i).getID() + "(" + jobs.get(i).getUser() + "),");
        }
        System.out.println("] End of queue ");
    }

    // deletes old reservations from previous invocations
    private void deletePreviousReservations() {
        for (int j = 0; j < Scheduler.resourceInfoList.size(); j++) {
            ResourceInfo ri = (ResourceInfo) Scheduler.resourceInfoList.get(j);
            MachineList machines = ri.resource.getMachineList();
            //System.out.println(this.resource.getResourceName()+ " Machines: "+machines.size());
            for (int i = 0; i < machines.size(); i++) {
                MachineWithRAMandGPUs machine = (MachineWithRAMandGPUs) machines.get(i);
                // cannot use such machine
                machine.banned_PEs = 0;
                machine.banned_GPUs = 0;
            }

        }
    }

}
