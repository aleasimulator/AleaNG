package xklusac.environment;

import gridsim.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import org.jfree.data.time.TimeSeries;
import xklusac.extensions.BinaryHeap;
import xklusac.extensions.FairshareGroup;
import xklusac.extensions.HeapNode;
import xklusac.extensions.Hole;
import xklusac.extensions.MachineEarliestStartSlotComparator;
import xklusac.extensions.MachineFreeCpuAvailabilityComparator;
import xklusac.extensions.StartComparator;

/**
 * Class ResourceInfo<p>
 * This class stores dynamic information about each resource. E.g., prepared
 * schedule for this resource, list of gridletDescriptions of jobs in
 * execution/waiting on machine. It also provides methods to calculate various
 * parameters based on the knowledge of the schedule/active_scheduling_queue and
 * resource status, e.g. expected makespan, machine usage, first free slot, etc.
 *
 * @author Dalibor Klusacek
 */
public class ResourceInfo {

    /**
     * Resource char. object - used to get information about Resource
     */
    public ComplexResourceCharacteristics resource;
    /**
     * Denotes the total number of PE on Resource
     */
    public int numPE;
    /**
     * List of gridletInfos "on Resource"
     */
    public ArrayList<GridletInfo> resInExec;
    /**
     * List representing schedule for this resource (gridletInfos)
     */
    public ArrayList<GridletInfo> resSchedule;
    /**
     * Sum of tardiness of all finished jobs
     */
    protected double prev_tard = 0.0;
    /**
     * Denotes if previously selected gridlet was succesfully sended by JSS -
     * prevents anticipating of gridlets
     */
    public boolean is_ready = true;
    /**
     * denotes previous deadline score of already finished jobs
     */
    protected int prev_score = 0;
    /**
     * current tardiness of jobs
     */
    public double resource_tardiness = 0.0;
    /**
     * current makespan
     */
    public double resource_makespan = 0.0;
    /**
     * number of nondelayed jobs so far
     */
    public int resource_score = 0;
    /**
     * number of nondelayed jobs in schedule
     */
    public int expected_score = 0;
    /**
     * number of delayed jobs in schedule
     */
    public int expected_fails = 0;
    /**
     * stable == true means that currently stored resource-related information
     * are correct and up-to-date
     */
    public boolean stable = false;
    /**
     * previous time when update was performed
     */
    protected double prev_clock = 0.0;
    /**
     * when will be PEs free
     */
    public double finishTimeOnPE[] = null;
    /**
     * earliest start time (active_scheduling_queue only)
     */
    public double est = Double.MAX_VALUE - 10;
    public int usablePEs = 0;
    /**
     * list of holes (gaps) in schedule
     */
    public LinkedList holes = new LinkedList();
    /**
     * total lenght of holes
     */
    protected double holes_length = 0.0;
    /**
     * total MIPS as available in holes
     */
    protected double holes_mips = 0.0;
    /**
     * actual resource usage
     */
    public double res_usage = 0.0;
    /**
     * avg start time
     */
    public double accum_start_time = 0.0;
    /**
     * average slowdown
     */
    public double accum_sd = 0.0;
    /**
     * average wait time
     */
    public double accum_wait = 0.0;
    /**
     * average response time
     */
    public double accum_resp = 0.0;
    /**
     * This resource's PE rating
     */
    public int peRating = 0;
    protected boolean stable_w = false;
    protected boolean stable_s = false;
    protected boolean stable_free = false;
    double[] r_tuwt;
    double[] r_tusa;
    public MachineList virt_machines;

    public int nowFreePE = 0;
    public double first_job_start = -1;
    TimeSeries series_usage;
    TimeSeries series_used;

    public ArrayList reserved_machines = null;

    /**
     * Creates a new instance of ResourceInfo with "in schedule" and "on
     * resource" lists of gridletInfos
     *
     * @param resource Resource characteristics (number of CPU, rating, etc.)
     */
    public ResourceInfo(ComplexResourceCharacteristics resource) {
        this.resource = resource;
        this.numPE = resource.getNumPE();
        this.finishTimeOnPE = new double[resource.getNumPE()];
        this.resInExec = new ArrayList();
        this.resSchedule = new ArrayList();
        this.peRating = resource.getMIPSRatingOfOnePE();
        this.stable_w = false;
        this.stable_s = false;

        AdvancedSpaceSharedWithRAM policy = null;
        for (int i = 0; i < ExperimentSetup.local_schedulers.size(); i++) {
            policy = (AdvancedSpaceSharedWithRAM) ExperimentSetup.local_schedulers.get(i);
            if (policy.resource_.getResourceID() == this.resource.getResourceID()) {
                System.out.println(i + "th allocation policy '" + policy.name + "' is used for the resource no. " + this.resource.getResourceID() + " " + this.resource.getResourceName());
                break;
            }
        }

        series_usage = new TimeSeries(resource.getResourceName());
        series_used = new TimeSeries(resource.getResourceName());

    }

    /**
     * Removes GridletInfo from list of "Gridlets on Resource" (only GridletInfo
     * there not gridlets)
     *
     * @param gi gridletInfo to be removed
     */
    public void lowerResInExec(int id, int owner_id) {
        boolean removed = false;
        for (int j = 0; j < resInExec.size(); j++) {
            GridletInfo giRes = (GridletInfo) resInExec.get(j);

            if (giRes.getID() == id && giRes.getOwnerID() == owner_id) {
                resInExec.remove(j);
                stable = false;
                stable_w = false;
                stable_s = false;
                stable_free = false;
                removed = true;
                break;
            }
        }
        if (!removed) {
            System.out.println("Error removing gi: " + id + " from InExec list.");
        }
    }

    /**
     * Removes GridletInfo from Resource schedule
     *
     * @param gi gridletInfo to be removed
     */
    public void lowerResScheduleList(GridletInfo gi) {
        for (int j = 0; j < resSchedule.size(); j++) {
            GridletInfo giRes = (GridletInfo) resSchedule.get(j);
            if (giRes.getID() == gi.getID() && giRes.getOwnerID() == gi.getOwnerID()) {
                resSchedule.remove(j);
                stable = false;
                stable_w = false;
                stable_s = false;
                break;
            }
        }
    }

    public String printNodeStatus() {
        String s = "";
        ArrayList machines = new ArrayList<>(this.resource.getMachineList());

        for (int i = 0; i < machines.size(); i++) {
            MachineWithRAMandGPUs machine = (MachineWithRAMandGPUs) machines.get(i);
            s += "[" + machine.getNumFreePE() + "," + machine.getFreeGPUs() + "]";
        }

        return s;
    }

    /**
     * Gets the number of currently free CPUs on a resource.
     *
     * @return number of currently free CPUs
     */
    public int getNumFreePE() {
        if (stable_free) {
            return nowFreePE;
        } else {
            //int freePE = this.numPE;
            int freePE = getNumRunningPE();
            // just testing
            freePE = Math.min(freePE, this.numPE);
            for (int j = 0; j < resInExec.size(); j++) {
                GridletInfo gi = (GridletInfo) resInExec.get(j);
                if (gi.getStatus() != Gridlet.SUCCESS) {
                    freePE = freePE - gi.getNumPE();
                }
            }
            stable_free = true;
            nowFreePE = Math.max(0, freePE);
            return Math.max(0, freePE);
        }
    }

    public int getNumAvailGpus() {
        //MachineList machines = this.resource.getMachineList();
        ArrayList machines = new ArrayList<>(this.resource.getMachineList());
        int gpus = 0;

        for (int i = 0; i < machines.size(); i++) {
            MachineWithRAMandGPUs machine = (MachineWithRAMandGPUs) machines.get(i);
            gpus += machine.getGpus();
        }
        return gpus;
    }

    /**
     * Gets the number of currently working (not failed) CPUs on a resource.
     */
    public int getNumRunningPE() {
        int running = 0;

        if (ExperimentSetup.failures) {
            //MachineList mlist = this.resource.getMachineList();
            ArrayList mlist = new ArrayList<>(this.resource.getMachineList());

            for (int i = 0; i < mlist.size(); i++) {
                MachineWithRAMandGPUs m = (MachineWithRAMandGPUs) mlist.get(i);
                if (m.getFailed() == false) {
                    running += m.getNumPE();
                }
            }
            return running;
        } else {
            return this.resource.getNumPE();
        }
    }

    /**
     * This method tests whether given resource (i.e., set of machines in a
     * cluster) can start to execute given gridlet. Both CPU and RAM
     * requirements are considered simultaneously.
     *
     * @param gi Gridlet to be executed on that resource
     * @return true if there are suffcient free CPUs and RAM to execute given
     * gridlet
     */
    public boolean canExecuteNow(GridletInfo gi) {
        boolean excl = gi.getProperties().contains("excl");

        if (ExperimentSetup.use_multiple_queues) {
            int avail = ExperimentSetup.queues.get(gi.getQueue()).getQueueAvailCPUs();
            if (avail < gi.getNumPE()) {
                gi.setCheckpoint_limit_eligible(false);
                //System.out.println(gi.getID()+": Queue limit FAIL: "+avail+" avail CPUs of limit: "+ExperimentSetup.queues.get(gi.getQueue()).getQueue_CPU_limit()+" for job requiring="+gi.getNumPE()+" CPUs");
                return false;
            } else {
                gi.setCheckpoint_limit_eligible(true);
                //System.out.println(gi.getID()+": Queue limit OK: "+avail+" avail CPUs of limit: "+ExperimentSetup.queues.get(gi.getQueue()).getQueue_CPU_limit()+" for job requiring="+gi.getNumPE()+" CPUs");
            }
        }

        if (ExperimentSetup.use_user_groups) {
            FairshareGroup g = ExperimentSetup.groups.get(gi.getGroupID());
            User u = ExperimentSetup.users.get(gi.getUser());
            int avail = g.getFreeQuota();
            int user_avail = u.getFreeQuota();
            if (avail < gi.getNumPE()) {
                gi.setCheckpoint_limit_eligible(false);
                //System.out.println(gi.getID()+": Quota limit FAIL: only "+avail+" CPUs available of total limit: "+g.getQuota()+" for group "+g.getName()+", for job requiring="+gi.getNumPE()+" CPUs");
                return false;
            } else if (user_avail < gi.getNumPE()) {
                gi.setCheckpoint_limit_eligible(false);
                //System.out.println(gi.getID() + " User Quota FAIL: only =" + user_avail + " (of "+u.getUserQuota()+" total) < requested=" + gi.getNumPE() + " for user " + u.getName());
                return false;
            } else {
                gi.setCheckpoint_limit_eligible(true);
                //System.out.println(gi.getID()+": Quota limit OK: "+avail+" CPUs available of total limit: "+g.getQuota()+" for group "+g.getName()+", for job requiring="+gi.getNumPE()+" CPUs");
            }
        }

        if (this.getNumFreePE() < 1) {
            //System.out.println(gi.getID()+": No need to test nodespec - no free CPUs at all now at:"+this.resource.getResourceName());
            return false;
        }

        long ram = gi.getRam();
        int ppn = gi.getPpn();
        int numNodes = gi.getNumNodes();
        int GPUs_per_node = gi.getGpus_per_node();

       ArrayList machines = new ArrayList<>(this.resource.getMachineList());
        if (ExperimentSetup.anti_starvation == true) {
            machines = new ArrayList<>(virt_machines);
        }
        int allocateNodes = numNodes;
        //System.out.println(this.resource.getResourceName()+ " Machines: ");
        for (int i = 0; i < machines.size(); i++) {
            MachineWithRAMandGPUs machine = (MachineWithRAMandGPUs) machines.get(i);
            // cannot use such machine
            if (machine.getFailed()) {
                continue;
            }
            // cannot use machine with job assigned if exclusive use required
            if (excl && machine.getNumFreePE() < machine.getNumPE()) {
                //System.out.println(gi.getID() + " cannot execute on " + this.resource.getResourceName() + ", because prop=" + gi.getProperties()+ " and mach"+i+" has "+machine.getNumBusyPE()+" used CPUs");
                continue;
            }
            if (ExperimentSetup.anti_starvation == false) {
                if (machine.getNumFreePE() >= ppn && machine.getFreeRam() >= ram && machine.getFreeGPUs() >= GPUs_per_node) {
                    allocateNodes--;
                    //System.out.println(gi.getID() + " nodes="+numNodes+" ncpus="+ppn+" to_be_allocated_nodes="+allocateNodes);
                } else {
                    //System.out.println(gi.getID() + " Cannot use this node, nodes="+numNodes+" ncpus="+ppn+" this has CPUs ="+machine.getNumFreePE()+" and free RAM="+(machine.getFreeRam()/(1024.0*1024)));
                }
            } else {
                if (machine.getNumFreeVirtualPE() >= ppn && machine.getFreeRam() >= ram) {
                    allocateNodes--;

                }
            }

            if (allocateNodes <= 0) {
                //System.out.println(gi.getID() + " nodes="+numNodes+" ncpus="+ppn+" to_be_allocated_nodes="+allocateNodes);
                return true;
            }
        }

        //do only when packing
        if (ExperimentSetup.resource_spec_packing && numNodes > 1) {
            //System.out.println(gi.getID() + " !!! START !!! of job packing. ppn=" + gi.getPpn() + " node=" + gi.getNumNodes());
            // pocet uzlu je liche cislo a to neumim rozumne transformovat
            if (numNodes % 2 != 0) {
                ppn = ppn * numNodes;
                ram = ram * numNodes;
                numNodes = 1;
                int cpu_per_m = this.resource.getNumPE() / this.resource.getNumMachines();
                if (cpu_per_m >= ppn && ram <= this.resource.getRamOnOneMachine()) {
                    //System.out.println(gi.getID() + " test of (lichy) job packing: " + gi.getProperties() + " ppn=" + ppn + " node=" + numNodes + " CPU-per-m: " + cpu_per_m);

                    machines = new ArrayList<>(this.resource.getMachineList());
                    if (ExperimentSetup.anti_starvation == true) {
                        machines = new ArrayList<>(virt_machines);
                    }
                    allocateNodes = numNodes;

                    for (int i = 0; i < machines.size(); i++) {
                        MachineWithRAMandGPUs machine = (MachineWithRAMandGPUs) machines.get(i);
                        // cannot use such machine
                        if (machine.getFailed()) {
                            continue;
                        }
                        // cannot use machine with job assigned if exclusive use required
                        if (excl && machine.getNumFreePE() < machine.getNumPE()) {
                            //System.out.println(gi.getID() + " cannot execute on " + this.resource.getResourceName() + ", because prop=" + gi.getProperties()+ " and mach"+i+" has "+machine.getNumBusyPE()+" used CPUs");
                            continue;
                        }

                        if (ExperimentSetup.anti_starvation == false) {
                            if (machine.getNumFreePE() >= ppn && machine.getFreeRam() >= ram) {
                                allocateNodes--;
                            }
                        } else {
                            if (machine.getNumFreeVirtualPE() >= ppn && machine.getFreeRam() >= ram) {
                                allocateNodes--;
                            }
                        }

                        if (allocateNodes <= 0) {
                            gi.setPpn(ppn);
                            gi.setNumNodes(numNodes);
                            gi.setRam(ram);
                            gi.getGridlet().setRam(ram);
                            gi.getGridlet().setNumNodes(numNodes);
                            gi.getGridlet().setPpn(ppn);
                            System.out.println(gi.getID() + " Job packing SUCCESS: " + gi.getProperties() + " ppn=" + gi.getPpn() + " node=" + gi.getNumNodes());
                            return true;
                        }
                    }

                }
                // pocet uzlu je sude cislo
            } else {
                while (numNodes > 1) {
                    ppn = ppn * 2;
                    ram = ram * 2;
                    numNodes = numNodes / 2;

                    int cpu_per_m = this.resource.getNumPE() / this.resource.getNumMachines();
                    if (cpu_per_m >= ppn && ram <= this.resource.getRamOnOneMachine()) {
                        //System.out.println(gi.getID() + " test of (sudy) job packing: " + gi.getProperties() + " ppn=" + ppn + " node=" + numNodes + " CPU-per-m: " + cpu_per_m);
                        //System.out.println(gi.getID() + " Job packing tested: " + gi.getProperties() + " ppn=" + gi.getPpn() + " node=" + gi.getNumNodes());

                        machines = new ArrayList<>(this.resource.getMachineList());
                        if (ExperimentSetup.anti_starvation == true) {
                            machines = new ArrayList<>(virt_machines);
                        }
                        allocateNodes = numNodes;

                        for (int i = 0; i < machines.size(); i++) {
                            MachineWithRAMandGPUs machine = (MachineWithRAMandGPUs) machines.get(i);
                            // cannot use such machine
                            if (machine.getFailed()) {
                                continue;
                            }
                            // cannot use machine with job assigned if exclusive use required
                            if (excl && machine.getNumFreePE() < machine.getNumPE()) {
                                //System.out.println(gi.getID() + " cannot execute on " + this.resource.getResourceName() + ", because prop=" + gi.getProperties()+ " and mach"+i+" has "+machine.getNumBusyPE()+" used CPUs");
                                continue;
                            }
                            if (ExperimentSetup.anti_starvation == false) {
                                if (machine.getNumFreePE() >= ppn && machine.getFreeRam() >= ram) {
                                    allocateNodes--;
                                }
                            } else {
                                if (machine.getNumFreeVirtualPE() >= ppn && machine.getFreeRam() >= ram) {
                                    allocateNodes--;
                                }
                            }

                            if (allocateNodes <= 0) {
                                gi.setPpn(ppn);
                                gi.setNumNodes(numNodes);
                                gi.setRam(ram);
                                gi.getGridlet().setRam(ram);
                                gi.getGridlet().setNumNodes(numNodes);
                                gi.getGridlet().setPpn(ppn);
                                System.out.println(gi.getID() + " Job packing SUCCESS: " + gi.getProperties() + " ppn=" + gi.getPpn() + " node=" + gi.getNumNodes());
                                return true;
                            }
                        }
                    }
                }
            }
        }

        // all machines tested and not enough CPUs/RAM found to execute job now
        return false;

    }

    // oznaci a odecte CPU a RAM u vsech virt uzlu, kde muze bezet uloha
    public void markSuitableNodes(GridletInfo gi) {
        MachineList machines = virt_machines;

        long ram = gi.getRam();
        int ppn = gi.getPpn();
        int numNodes = gi.getNumNodes();
        int gpus = gi.getGpus_per_node();

        // pro kazdy pozadavek z nodespec udelam zvlast rezervaci
        for (int nod = 0; nod < numNodes; nod++) {
            // rezervaci udelam na kazdem vyhovujicim stroji
            for (int i = 0; i < machines.size(); i++) {
                MachineWithRAMandGPUs machine = (MachineWithRAMandGPUs) machines.get(i);
                if (machine.getNumPE() >= ppn && machine.getRam() >= ram && machine.getGpus() >= gpus) {
                    machine.setNumFreeVirtualPE(machine.getNumFreeVirtualPE() - ppn);
                    machine.setUsedRam(machine.getUsedRam() + ram);
                    machine.setUsed_gpus(machine.getUsed_gpus() + gpus);
                }
            }
        }

    }
    // oznaci a odecte CPU a RAM u vsech virt uzlu, kde muze bezet uloha - vybere vsak jen ty nejdrive volne

    public void markOptimalSuitableNodes(GridletInfo gi) {
        MachineList machines = virt_machines;

        long ram = gi.getRam();
        int ppn = gi.getPpn();
        int numNodes = gi.getNumNodes();
        int gpus = gi.getGpus_per_node();

        // pro kazdy pozadavek z nodespec udelam zvlast rezervaci
        for (int nod = 0; nod < numNodes; nod++) {
            // rezervaci udelam na stroji, ktery bude nejdrive volny
            double earliest_start_time = Double.MAX_VALUE;
            MachineWithRAMandGPUs earliest_machine = null;
            for (int i = 0; i < machines.size(); i++) {
                MachineWithRAMandGPUs machine = (MachineWithRAMandGPUs) machines.get(i);
                if (machine.getNumPE() >= ppn && machine.getRam() >= ram && machine.getGpus() >= gpus) {
                    double local_est = machine.getEarliestStartTimeForNodeJob(ppn);
                    if (local_est < earliest_start_time) {
                        earliest_start_time = local_est;
                        earliest_machine = machine;
                    }
                }
            }
            if (earliest_machine != null) {
                earliest_machine.setNumFreeVirtualPE(earliest_machine.getNumFreeVirtualPE() - ppn);
                earliest_machine.setUsedRam(earliest_machine.getUsedRam() + ram);
                earliest_machine.setUsed_gpus(earliest_machine.getUsed_gpus() + gpus);
                // TO DO - ted natvrdo navysim na Double.MAXVALUE (uvnitr rutiny updateFirst...)
                //System.out.println(gi.getID()+" Allocating nodes on "+this.resource.getResourceName()+" mach: "+earliest_machine.getMachineID());
                earliest_machine.updateFirstFreeTimeAfterNodeJobAllocation(ppn, gi.getJobLimit());
            }
        }

    }
    // oznaci a odecte CPU a RAM u vsech virt uzlu, kde muze bezet uloha

    public void markLimitedSuitableNodes(GridletInfo gi) {
        MachineList machines = virt_machines;

        long ram = gi.getRam();
        int ppn = gi.getPpn();
        int numNodes = gi.getNumNodes();
        int max_overhead = 2;
        int overhead = 0;
        int gpus = gi.getGpus_per_node();

        max_overhead = Math.max(max_overhead, gi.getNumPE() / 5);

        // pro kazdy pozadavek z nodespec udelam zvlast rezervaci
        for (int nod = 0; nod < numNodes; nod++) {
            overhead = 0;
            // rezervaci udelam na kazdem vyhovujicim stroji maximalne tolikrat kolik je max_overhead
            for (int i = 0; i < machines.size(); i++) {
                MachineWithRAMandGPUs machine = (MachineWithRAMandGPUs) machines.get(i);
                if (machine.getNumPE() >= ppn && machine.getRam() >= ram && machine.getGpus() >= gpus) {
                    machine.setNumFreeVirtualPE(machine.getNumFreeVirtualPE() - ppn);
                    machine.setUsedRam(machine.getUsedRam() + ram);
                    machine.setUsed_gpus(machine.getUsed_gpus() + gpus);
                    overhead++;
                }
                if (overhead >= max_overhead) {
                    break;
                }
            }
        }

    }

    // pouziva se pri stradani - vynuluje rezervace a nastavi stav obrazu stroju podle skutecneho zaplneni
    public void deleteReservations() {
        //MachineList machines = this.resource.getMachineList();
        ArrayList machines = new ArrayList<>(this.resource.getMachineList());
        virt_machines = new AdvancedMachineList();

        for (int i = 0; i < machines.size(); i++) {
            MachineWithRAMandGPUs machine = (MachineWithRAMandGPUs) machines.get(i);
            PEList peList = new PEList();
            for (int k = 0; k < machine.getNumPE(); k++) {
                // need to store PE id and MIPS Rating
                peList.add(new PE(k, machine.getMIPSRating()));
            }
            MachineWithRAMandGPUs virt_machine = new MachineWithRAMandGPUs(i, peList, machine.getRam(), machine.getGpus());

            // cannot use such machine
            if (machine.getFailed()) {
                virt_machine.setFailed(true);
            }
            virt_machine.setNumFreeVirtualPE(machine.getNumFreePE());
            virt_machine.setUsedRam(machine.getUsedRam());
            virt_machine.setUsed_gpus(machine.getUsed_gpus());
            double[] ests = Arrays.copyOf(machine.getFirstFreeTimeArray(), machine.getFirstFreeTimeArray().length);
            virt_machine.setFirstFreeTimeArray(ests);
            virt_machines.add(virt_machine);
        }
    }

    

    public boolean executedHere(GridletInfo gi) {
        if (gi.getOsRequired().contains(this.resource.getResourceName())) {
            return true;
        } else {
            return false;
        }
    }

    public boolean canExecuteEver(GridletInfo gi) {

        if (ExperimentSetup.enforce_partition == false) {
            long ram = gi.getRam();
            int ppn = gi.getPpn();
            int numNodes = gi.getNumNodes();
            int gpus_per_node = gi.getGpus_per_node();

            //MachineList machines = this.resource.getMachineList();
            ArrayList machines = new ArrayList<>(this.resource.getMachineList());
            int allocateNodes = numNodes;

            for (int i = 0; i < machines.size(); i++) {
                MachineWithRAMandGPUs machine = (MachineWithRAMandGPUs) machines.get(i);
                // cannot use such machine
                if (machine.getFailed()) {
                    continue;
                }
                if (machine.getNumPE() >= ppn && machine.getRam() >= ram && machine.getGpus() >= gpus_per_node) {
                    allocateNodes--;
                }
                if (allocateNodes <= 0) {
                    return true;
                }
            }

            // all machines tested and not enough CPUs/RAM found to execute job now
            return false;
        }

        String[] req_nodes = gi.getProperties().split(":");

        // returns false if this resource partition is not the one required by job in SWF
        if (ExperimentSetup.enforce_partition && !gi.getProperties().contains("all")) {
            String partition = this.resource.getProperties();
            if (!partition.equals(req_nodes[0])) {
                //System.out.println(gi.getID() + " requires partition: " + gi.getProperties() + ", but this resource partition is: " + partition+" (cluster = "+this.resource.getResourceName()+")");
                return false;
            }
        }

        long ram = gi.getRam();
        int ppn = gi.getPpn();
        int numNodes = gi.getNumNodes();
        int GPUs_per_node = gi.getGpus_per_node();

        //MachineList machines = this.resource.getMachineList();
        ArrayList machines = new ArrayList<>(this.resource.getMachineList());
        int allocateNodes = numNodes;

        for (int i = 0; i < machines.size(); i++) {
            MachineWithRAMandGPUs machine = (MachineWithRAMandGPUs) machines.get(i);
            // cannot use such machine
            if (machine.getFailed()) {
                continue;
            }
            if (machine.getNumPE() >= ppn && machine.getRam() >= ram && machine.getGpus() >= GPUs_per_node) {
                allocateNodes--;
            }
            if (allocateNodes <= 0) {
                return true;
            }
        }

        // all machines tested and not enough CPUs/RAM found to execute job now
        return false;
    }

    /**
     * This method returns the amount of RAM on single machine within a cluster.
     *
     * @return the amount of RAM in KB
     */
    public long getRamOfOneMachine() {
        //MachineList machines = this.resource.getMachineList();
        ArrayList machines = new ArrayList<>(this.resource.getMachineList());
        for (int i = 0; i < machines.size(); i++) {
            MachineWithRAMandGPUs machine = (MachineWithRAMandGPUs) machines.get(i);
            return machine.getRam();
        }
        return 0;
    }

    /**
     * This method returns the amount of free RAM of the first machine within a
     * cluster.
     *
     * @return
     */
    public long getFreeRamOfOneMachine() {
        //MachineList machines = this.resource.getMachineList();
        ArrayList machines = new ArrayList<>(this.resource.getMachineList());
        for (int i = 0; i < machines.size(); i++) {
            MachineWithRAMandGPUs machine = (MachineWithRAMandGPUs) machines.get(i);
            return machine.getFreeRam();
        }
        return 0;
    }

    /*
     * Gets the number of currently busy CPUs on a resource.
     */
    public int getNumBusyPE() {
        int busy = 0;
        //MachineList mlist = this.resource.getMachineList();
        ArrayList mlist = new ArrayList<>(this.resource.getMachineList());

        for (int i = 0; i < mlist.size(); i++) {
            MachineWithRAMandGPUs m = (MachineWithRAMandGPUs) mlist.get(i);
            if (m.getFailed() == false) {
                busy += m.getNumBusyPE();
            }
        }
        return busy;
    }

    public int getNumAllocatedPE() {
        int busy = 0;
        //MachineList mlist = this.resource.getMachineList();
        ArrayList mlist = new ArrayList<>(this.resource.getMachineList());

        for (int i = 0; i < mlist.size(); i++) {
            MachineWithRAMandGPUs m = (MachineWithRAMandGPUs) mlist.get(i);
            if (m.getFailed() == false) {
                if (m.isUsed_exclusively()) {
                    busy += m.getNumPE();
                } else {
                    busy += m.getNumBusyPE();
                }
            }
        }
        return busy;
    }

    /**
     * Updates start times in the "CPU field" according to release date of
     * multi-CPU gridlet - auxiliary method
     *
     * @param finishTimeOnPE[] field representing earliest free slot of each CPU
     * on machine
     * @param start_time either current time or release date - according to what
     * is higher
     */
    private void updateMultiStartTime(double finishTimeOnPE[], double start_time) {

        for (int j = 0; j < finishTimeOnPE.length; j++) {
            if (finishTimeOnPE[j] < start_time && finishTimeOnPE[j] > -998.0) {
                finishTimeOnPE[j] = start_time;
            }
        }
    }

    /**
     * Selects index of the last CPU necessary to run multi-CPU gridlet.
     * Auxiliary method
     *
     * @param finishTimeOnPE[] field representing earliest free slot of each CPU
     * on machine
     * @param gi gridletInfo describing the multi-CPU gridlet
     */
    private int findFirstFreeSlot(double finishTimeOnPE[], GridletInfo gi) {
        int index = 0;
        double min = Double.MAX_VALUE - 10;
        //gi.getPEs().clear();

        for (int i = 0; i < gi.getNumPE(); i++) {
            for (int j = 0; j < finishTimeOnPE.length; j++) {
                // if other PE needed to run gridlet - be carefull when comparing 2 double values
                if (finishTimeOnPE[j] < min && finishTimeOnPE[j] > -998) {
                    min = finishTimeOnPE[j];
                    index = j;
                }
            }

            //reset min value if not the last PE allocated
            min = Double.MAX_VALUE - 10; //here remember hole

            if (i != (gi.getNumPE() - 1)) {
                //gi.getPEs().add(index);
                finishTimeOnPE[index] = -999;
            }
        }
        //gi.getPEs().add(index);
        //System.out.println("time = "+finishTimeOnPE[index]);
        return index;
    }

    /**
     * Auxiliary method for EASY Backfilling
     */
    private int findUsablePEs(int index, double finishTimeOnPE[], GridletInfo gi) {

        int usable = 0;
        double earliest = 0.0;

        earliest = finishTimeOnPE[index];
        finishTimeOnPE[index] = -999;
        //System.out.println("----usable----");
        // count usable
        for (int j = 0; j < finishTimeOnPE.length; j++) {
            // if other PE needed to run gridlet - be carefull when comparing 2 double values
            if (finishTimeOnPE[j] == earliest) {
                //System.out.println(finishTimeOnPE[j]);
                usable++;
            }
        }
        //System.out.println("----usable----");
        return usable;
    }

    /*
     * Auxiliary function - predicts when and which PEs will be used for this
     * gi.
     *
     */
    private void predictPEs(double finishTimeOnPE[], GridletInfo gi) {
        int index = 0;
        double min = Double.MAX_VALUE - 10;
        ArrayList<Integer> PEs = new ArrayList();

        for (int i = 0; i < gi.getNumPE(); i++) {
            for (int j = 0; j < finishTimeOnPE.length; j++) {
                // if other PE needed to run gridlet - be carefull when comparing 2 double values
                if (finishTimeOnPE[j] < min && finishTimeOnPE[j] > -998) {
                    min = finishTimeOnPE[j];
                    index = j;
                }
            }

            //reset min value if not the last PE allocated
            min = Double.MAX_VALUE - 10; //here remember hole

            if (i != (gi.getNumPE() - 1)) {
                PEs.add(index);
                finishTimeOnPE[index] = -999;
            }
        }
        PEs.add(index);
        gi.setPEs(PEs);
        gi.setPlannedPEs(PEs, "");
        System.out.println("PEs computation for: " + gi.getID() + " cpu=" + gi.getNumPE() + " PEs=" + PEs.size() + " status=" + gi.getGridlet().getGridletStatusString() + ")");
    }

    /*
     * Auxiliary function for correct Hole (Gap) creation. Generates gaps at the
     * end of schedule.
     *
     */
    private int createLastGaps(double finishTimeOnPE2[], int numPE) {
        int index = 0;
        double min = Double.MAX_VALUE - 10;
        double hole_start = -1.0;
        int hole_size = 1;
        //gi.getPEs().clear();
        //System.out.println("finding gaps over = "+numPE);

        for (int i = 0; i < numPE; i++) {
            for (int j = 0; j < finishTimeOnPE2.length; j++) {
                // if other PE needed to run gridlet - be carefull when comparing 2 double values
                if (finishTimeOnPE2[j] < min && finishTimeOnPE2[j] > -998) {
                    min = finishTimeOnPE2[j];
                    index = j;
                }
            }
            if (hole_start <= 0.0) { // hole not started yet

                hole_start = min; // possible hole start

            } else { // finish hole or continue creating it?

                if (hole_start != min) {
                    // we have a new hole in schedule - store it.
                    double length = min - hole_start;
                    Hole h = new Hole(hole_start, min, length, (length * peRating), hole_size, null);
                    holes_length += length * hole_size;
                    holes_mips += length * peRating * hole_size;
                    holes.addLast(h);
                    hole_size++;
                    hole_start = min;
                } else {
                    hole_size++;
                }
            }

            //reset min value if not the last PE allocated
            min = Double.MAX_VALUE - 10; //here remember hole

            if (i != (numPE - 1)) {
                //gi.getPEs().add(index);
                finishTimeOnPE2[index] = -999;
            }
        }

        Hole h_last = new Hole(finishTimeOnPE2[index], Double.MAX_VALUE, Double.MAX_VALUE, Double.MAX_VALUE,
                numPE, null);
        holes.addLast(h_last);
        //gi.getPEs().add(index);
        return index;
    }

    /**
     * This method creates last gaps at the end of schedule
     */
    private void createLastGapsFast(double finishTimeOnPE2[], int numPE, double min, double max) {
        int hole_size = 0;

        for (int j = 0; j < finishTimeOnPE2.length; j++) {
            if (finishTimeOnPE2[j] <= min) {
                hole_size++;
            }
        }
        double length = max - min;
        if (length > 0.0) {
            Hole h = new Hole(min, max, length, (length * peRating), hole_size, null);
            holes_length += length * hole_size;
            holes_mips += length * peRating * hole_size;
            holes.addLast(h);
        }
        Hole h_last = new Hole(max, Double.MAX_VALUE, Double.MAX_VALUE, Double.MAX_VALUE,
                numPE, null);
        holes.addLast(h_last);
    }

    /**
     * This method constructs the binary heap that stores information about
     * first free slots on CPUs.
     */
    private BinaryHeap createBinaryHeap() {
        BinaryHeap slots = new BinaryHeap();
        HashMap<Double, ArrayList> time_slots = new HashMap<Double, ArrayList>();
        double time = 0.0;

        for (int i = 0; i < finishTimeOnPE.length; i++) {
            time = finishTimeOnPE[i];
            if (!time_slots.containsKey(time)) {
                ArrayList<Integer> cpus = new ArrayList<Integer>();
                cpus.add(i);
                time_slots.put(time, cpus);
            } else {
                ArrayList<Integer> cpus = time_slots.get(time);
                cpus.add(i);
                time_slots.put(time, cpus);
            }
        }
        Object[] keys = time_slots.keySet().toArray();
        for (int i = 0; i < time_slots.size(); i++) {
            ArrayList<Integer> cpus = time_slots.get((Double) keys[i]);
            slots.insert(new HeapNode((Double) keys[i], cpus));
        }
        return slots;
    }

    /**
     * Selects index of the last CPU necessary to run multi-CPU gridlet. Also,
     * it builds the hole-list during execution. Auxiliary method.
     *
     * @param finishTimeOnPE[] field representing earliest free slot of each CPU
     * on machine
     * @param gi gridletInfo describing the multi-CPU gridlet
     * @param slots Binary heap with first free slots
     */
    private int findFirstFreeSlotForWaitingJob(double finishTimeOnPE[], GridletInfo gi, BinaryHeap slots, String who) {
        int index = 0;
        double min = Double.MAX_VALUE - 10;
        double hole_start = -1.0;
        int hole_size = 1;
        double earl_job_start = 0.0;
        if (gi.getID() == 65000) {
            //System.out.println(GridSim.clock() + ": "+this.resource.getResourceName()+"("+who+"): " + gi.getID() + "["+gi.getNumPE()+"cpus]: clearing following " + gi.getPlannedPEs().size() + " planned PEs... " + gi.getPlannedPEsString());
            System.out.println(gi.getID() + ": ------- R-info avail times:-------");
            for (int j = 0; j < finishTimeOnPE.length; j++) {
                System.out.println("[" + j + "]: " + finishTimeOnPE[j] + " ");
            }
            System.out.println();
        }

        gi.getPlannedPEs().clear();
        ArrayList<Integer> usedIDs = new ArrayList<Integer>();

        if (ExperimentSetup.use_heap) {
            int needed = gi.getNumPE();
            int found = 0;
            hole_size = 0;

            while (needed > 0) {

                //System.out.println(gi.getID()+":  needed = "+needed);
                HeapNode hn = (HeapNode) slots.findMin();
                earl_job_start = hn.getTime();
                ArrayList<Integer> cpuIDs = hn.getCpuIDs(gi.getID());
                // all ids in this node
                if (cpuIDs.size() >= needed) {
                    //System.out.println("prior size = "+cpuIDs.size()+" used = "+usedIDs.size());
                    for (int i = 0; i < needed; i++) {
                        usedIDs.add(cpuIDs.remove(0));
                        //System.out.println(i+": size = "+cpuIDs.size()+" used = "+usedIDs.size());
                    }
                    //System.out.println("post size = "+cpuIDs.size()+" used = "+usedIDs.size());
                    needed = 0;
                    // vyprazdneny uzel odstranime
                    if (cpuIDs.size() == 0) {
                        //hn.setCpuIDs(cpuIDs);
                        slots.deleteMin();
                    } else {
                        hn.setCpuIDs(cpuIDs);
                    }
                    //if(gi.getID()>4000) System.out.println(gi.getID()+": OK needed = "+needed+" of "+gi.getNumPE()+" Res_cpus = "+finishTimeOnPE.length+" heap size = "+slots.size()+" so far = "+usedIDs.size());
                    //slots.printCPUcount("all found for "+gi.getID());

                    // more nodes needed => empty the whole node while gap will appear as a side effect
                } else {
                    int founded = cpuIDs.size();
                    needed = needed - founded;
                    hole_size += founded;
                    //if(gi.getID()>4000) System.out.println(gi.getID()+": prior "+founded+" steps have "+usedIDs.size());
                    for (int i = 0; i < founded; i++) {

                        usedIDs.add(cpuIDs.remove(0));
                        //if(gi.getID()>4000) System.out.println(gi.getID()+": in "+i+" th step have "+usedIDs.size());
                    }
                    //if(gi.getID()>4000) System.out.println(gi.getID()+": past "+founded+" steps have "+usedIDs.size());
                    //hn.setCpuIDs(cpuIDs);
                    hole_start = hn.getTime();
                    // delete empty node
                    slots.deleteMin();
                    //create hole
                    //if(gi.getID()>4000) System.out.println(gi.getID()+": KO needed = "+needed+" of "+gi.getNumPE()+" Res_cpus = "+finishTimeOnPE.length+" heap size = "+slots.size()+" so far = "+usedIDs.size());
                    //slots.printCPUcount("more needed for "+gi.getID());
                    HeapNode hnext = (HeapNode) slots.findMin();
                    min = hnext.getTime();
                    double length = min - hole_start;
                    Hole h = new Hole(hole_start, min, length, (length * peRating), hole_size, gi);
                    //System.out.println(gi.getID()+" New hole s = "+Math.round(hole_start)+" size = "+hole_size+" length = "+Math.round(length));
                    holes_length += length * hole_size;
                    holes_mips += length * peRating * hole_size;
                    holes.addLast(h);
                }
            }
            // update of binary heap structure
            double glFinishTime = gi.getJobRuntime(peRating);
            if (glFinishTime < 1.0) {
                glFinishTime = 1.0;
            }
            int roundUpTime = (int) (glFinishTime + 1);
            double end = earl_job_start + roundUpTime;
            //if(gi.getID()>4000) System.out.println(gi.getID()+": Insert new node with CPUids "+usedIDs.size()+" == "+gi.getNumPE());
            slots.insert(new HeapNode(end, usedIDs));
            // update of old structure - compatibility reasons
            int lasti = usedIDs.get(usedIDs.size() - 1);

            for (int i = 0; i < usedIDs.size(); i++) {
                index = usedIDs.get(i);
                if (i != (usedIDs.size() - 1)) {
                    finishTimeOnPE[index] = -999;
                }

            }
            //gi.setPlannedPEs(usedIDs, who);
            /*if (gi.getID() == 69) {
                System.out.println(GridSim.clock() + ": "+this.resource.getResourceName()+"("+who+"): "+ gi.getID() + "["+gi.getNumPE()+"cpus]: Resetting following " + gi.getPlannedPEs().size() + " planned PEs... " + gi.getPlannedPEsString());
            }*/
            index = lasti;

        } else {
            // classical array will be used instead of Binary Heap
            for (int i = 0; i < gi.getNumPE(); i++) {
                for (int j = 0; j < finishTimeOnPE.length; j++) {
                    // if other PE needed to run gridlet - be carefull when comparing 2 double values
                    /*if (finishTimeOnPE[j] < min && finishTimeOnPE[j] > -998) {
                        min = finishTimeOnPE[j];
                        index = j;*/
                    if (Math.round(finishTimeOnPE[j]) < min && finishTimeOnPE[j] > -998) {
                        min = Math.round(finishTimeOnPE[j]);
                        index = j;

                    }
                }
                if (hole_start <= 0.0) { // hole not started yet

                    hole_start = min; // possible hole start

                } else { // finish hole or continue creating it?

                    if (hole_start != min) {
                        // we have a new hole in schedule - store it.
                        double length = min - hole_start;
                        Hole h = new Hole(hole_start, min, length, (length * peRating), hole_size, gi);
                        holes_length += length * hole_size;
                        holes_mips += length * peRating * hole_size;
                        holes.addLast(h);
                        hole_size++;
                        hole_start = min;
                    } else {
                        hole_size++;
                    }
                }

                //reset min value if not the last PE allocated
                min = Double.MAX_VALUE - 10; //here remember hole

                if (i != (gi.getNumPE() - 1)) {
                    //gi.getPEs().add(index);
                    usedIDs.add(index);
                    finishTimeOnPE[index] = -999;
                }
            }
            //gi.getPEs().add(index);
            usedIDs.add(index);
            //gi.setPlannedPEs(usedIDs, who);
        }
        // SCC modification
        //predictPEs(finishTimeOnPE, gi);
        ArrayList<Integer> PEs = new ArrayList(usedIDs);
        gi.setPEs(PEs);
        gi.setExpectedStartTime(finishTimeOnPE[index]);
        gi.setPlannedPEs(usedIDs, who + ":" + this.resource.getResourceName());

        if (first_job_start > 0 && Math.max(0.0, (finishTimeOnPE[index] - first_job_start)) > ExperimentSetup.max_schedule_length) {
            //System.out.println("Schedule is tooo long....: "+(finishTimeOnPE[index] - first_job_start)+ " end "+finishTimeOnPE[index]+" fj "+ first_job_start);
            Scheduler.schedule_too_long = true;
        } else {
            //System.out.println("Schedule is first/short.: "+(finishTimeOnPE[index] - first_job_start)+ " end "+finishTimeOnPE[index]+" fj "+ first_job_start);
            Scheduler.schedule_too_long = false;
        }

        return index;
    }

    private int findFirstFreeSlotForWaitingJobCheckCPUs(double finishTimeOnPE[], GridletInfo gi, BinaryHeap slots, String who) {
        int index = 0;
        double min = Double.MAX_VALUE - 10;
        double hole_start = -1.0;
        int hole_size = 1;

        if (gi.getID() == ExperimentSetup.debug_job) {
            //System.out.println(GridSim.clock() + ": "+this.resource.getResourceName()+"("+who+"): " + gi.getID() + "["+gi.getNumPE()+"cpus]: clearing following " + gi.getPlannedPEs().size() + " planned PEs... " + gi.getPlannedPEsString());
            System.out.println(gi.getID() + ": ------- R-info avail times:------- at time: " + GridSim.clock());
            for (int j = 0; j < finishTimeOnPE.length; j++) {
                //    System.out.println("[" + j + "]: " + finishTimeOnPE[j] + " ");
            }
            //System.out.println("compare 202 vs 334: " + Double.compare(finishTimeOnPE[202], finishTimeOnPE[334]) + ", 202>334 " + (finishTimeOnPE[334] < finishTimeOnPE[202]));
        }

        ArrayList<Integer> usedIDs = new ArrayList<Integer>();
        double backup[] = new double[resource.getNumPE()];
        for (int j = 0; j < finishTimeOnPE.length; j++) {
            backup[j] = finishTimeOnPE[j];
        }

        // classical array will be used instead of Binary Heap
        for (int i = 0; i < gi.getNumPE(); i++) {
            for (int j = 0; j < finishTimeOnPE.length; j++) {
                // if other PE needed to run gridlet - be carefull when comparing 2 double values
                /*if (finishTimeOnPE[j] < min && finishTimeOnPE[j] > -998) {
                        min = finishTimeOnPE[j];
                        index = j;*/
                if (finishTimeOnPE[j] < min && finishTimeOnPE[j] > -998) {
                    min = finishTimeOnPE[j];
                    index = j;
                }
            }
            if (ExperimentSetup.use_gaps) {
                if (hole_start <= 0.0) { // hole not started yet

                    hole_start = min; // possible hole start

                } else { // finish hole or continue creating it?

                    if (hole_start != min) {
                        // we have a new hole in schedule - store it.
                        double length = min - hole_start;
                        Hole h = new Hole(hole_start, min, length, (length * peRating), hole_size, gi);
                        holes_length += length * hole_size;
                        holes_mips += length * peRating * hole_size;
                        holes.addLast(h);
                        hole_size++;
                        hole_start = min;
                    } else {
                        hole_size++;
                    }
                }
            }

            //reset min value if not the last PE allocated
            min = Double.MAX_VALUE - 10; //here remember hole

            if (i != (gi.getNumPE() - 1)) {
                //gi.getPEs().add(index);
                usedIDs.add(index);
                finishTimeOnPE[index] = -999;
                //System.out.println(gi.getID() +" marking CPU "+index);
            }
        }
        usedIDs.add(index);

        double new_start = finishTimeOnPE[index];
        int max_index = -1;
        double max = -1.0;
        //perform check
        boolean update = true;

        if (gi.getResourceID() == resource.getResourceID()) {
            //System.out.println(gi.getID() + "----"+ gi.getResourceID());
            //System.out.println(gi.getPlannedPEs().size());
            for (int pi = 0; pi < gi.getPlannedPEs().size(); pi++) {
                //System.out.println("Opening " + pi + " of " + gi.getPlannedPEs().size() + " value " + gi.getPlannedPEs().get(pi) + " backup size " + backup.length + " res " + gi.getResourceID());
                if (backup[gi.getPlannedPEs().get(pi)] <= (new_start + 0.0)) {
                    if (backup[gi.getPlannedPEs().get(pi)] > max) {
                        max = backup[gi.getPlannedPEs().get(pi)];
                        max_index = gi.getPlannedPEs().get(pi);
                    }
                    update = false;
                } else {

                    update = true;
                    break;
                }
            }
        } else {
            update = true;
        }
        //System.out.println("----");

        //update = true;
        if (update) {
            // SCC modification
            double ps = gi.getExpectedStartTime();
            gi.getPlannedPEs().clear();
            ArrayList<Integer> PEs = new ArrayList(usedIDs);
            gi.setPEs(PEs);
            gi.setExpectedStartTime(finishTimeOnPE[index]);
            gi.setPlannedPEs(usedIDs, who + ":" + this.resource.getResourceName());
            if (gi.getID() == ExperimentSetup.debug_job) {
                System.out.println("Update was needed... for: " + gi.getID() + " new start = " + finishTimeOnPE[index] + " prev start = " + ps + " sch:" + gi.getPlannedPEsString());
            }

        } else {
            for (int j = 0; j < finishTimeOnPE.length; j++) {
                finishTimeOnPE[j] = backup[j];
            }

            for (int j = 0; j < gi.getPlannedPEs().size(); j++) {
                finishTimeOnPE[gi.getPlannedPEs().get(j)] = -999;
            }
            index = max_index;
            finishTimeOnPE[index] = max;

            double abs_diff = Math.abs(new_start - finishTimeOnPE[index]);

            if (new_start != finishTimeOnPE[index] && abs_diff > 0.0) {
                //System.out.println(gi.getID() + " has different start times!!! " + new_start + " vs old " + finishTimeOnPE[index]);
            }
            ArrayList<Integer> PEs = new ArrayList(gi.getPlannedPEs());
            gi.setPEs(PEs);
            gi.setExpectedStartTime(finishTimeOnPE[index]);
            //gi.setExpectedStartTime(new_start);
            gi.getPlannedPEs().clear();
            gi.setPlannedPEs(new ArrayList(PEs), who + ":" + this.resource.getResourceName());
            if (gi.getID() == ExperimentSetup.debug_job) {
                System.out.println("NO update needed... for: " + gi.getID() + " old start = " + finishTimeOnPE[index] + " found start = " + new_start + " sch:" + gi.getPlannedPEsString());
            }

        }

        if (first_job_start > 0 && Math.max(0.0, (finishTimeOnPE[index] - first_job_start)) > ExperimentSetup.max_schedule_length) {
            //System.out.println("Schedule is tooo long....: "+(finishTimeOnPE[index] - first_job_start)+ " end "+finishTimeOnPE[index]+" fj "+ first_job_start);
            Scheduler.schedule_too_long = true;
        } else {
            //System.out.println("Schedule is first/short.: "+(finishTimeOnPE[index] - first_job_start)+ " end "+finishTimeOnPE[index]+" fj "+ first_job_start);
            Scheduler.schedule_too_long = false;
        }

        /*if (gi.getID() == 66) {
            //System.out.println(GridSim.clock() + ": "+this.resource.getResourceName()+"("+who+"): " + gi.getID() + "["+gi.getNumPE()+"cpus]: clearing following " + gi.getPlannedPEs().size() + " planned PEs... " + gi.getPlannedPEsString());
            System.out.println(gi.getID() + ": ------- R-info avail times COMPLETED:------- at time: "+GridSim.clock());
            for (int j = 0; j < finishTimeOnPE.length; j++) {
                System.out.println("[" + j + "]: " + finishTimeOnPE[j] + " ");
            }
            System.out.println();
        }*/
        return index;
    }

    /*
     * Auxiliary function predicting completion time of running jobs.
     *
     */
    private void predictFirstFreeSlots(double current_time) {
        int peIndex = 0;

        // first - failed machines must have finishTimeOnPE[peIndex] = MAX_VALUE
        if (ExperimentSetup.failures) {
            //MachineList list = resource.getMachineList();
            ArrayList list = new ArrayList<>(this.resource.getMachineList());
            for (int i = 0; i < list.size(); i++) {
                Machine m = (Machine) list.get(i);
                if (!m.getFailed()) {
                    //numPE += m.getNumPE();
                    for (int p = 0; p < m.getNumPE(); p++) {
                        //System.out.print(index_id+".");
                        finishTimeOnPE[peIndex] = current_time;
                        peIndex++;
                    }
                } else {
                    for (int p = 0; p < m.getNumPE(); p++) {
                        //System.out.print(index_id+".");
                        finishTimeOnPE[peIndex] = Double.MAX_VALUE;
                        peIndex++;
                    }
                }

            }
        } else {
            for (int i = 0; i < finishTimeOnPE.length; i++) {
                finishTimeOnPE[i] = current_time;
                /*if (i == 0) {
                    System.out.println(i + " is set to slot = " + finishTimeOnPE[i] + " at time " + GridSim.clock());
                }*/
            }
        }
        peIndex = 0;

        for (int j = 0; j < resInExec.size(); j++) {
            GridletInfo gi = (GridletInfo) resInExec.get(j);
            List<Integer> PEs = gi.getPEs();
            // System.out.println(gi.getID()+": planned PEs size: "+PEs.size()+" at time: "+Math.round(current_time));
            if (gi.getStatus() == Gridlet.INEXEC) {
                double run_time = current_time - gi.getGridlet().getExecStartTime();
                double time_remaining = Math.max(0.0, (gi.getJobRuntime(peRating) - run_time));
                double glFinishTime = time_remaining;

                int roundUpTime = (int) (glFinishTime + 1);

                // update all PE-finish-time that will run this gridlet
                if (gi.getID() == ExperimentSetup.debug_job) {
                    System.out.println(gi.getID() + ": will finish at: " + (GridSim.clock() + glFinishTime) + " remains: " + (glFinishTime) + " est. runtime: " + gi.getJobRuntime(peRating));
                }

                if (PEs.size() < gi.getNumPE()) {
                    System.out.println(gi.getID() + " NO planned PEs WEIRD status=" + gi.getGridlet().getGridletStatusString() + " resource=" + resource.getResourceName() + " at clock=" + GridSim.clock());
                    // ALERT THIS DOES NOT WORK on topology aware jobs (nodes, ppn)
                    predictPEs(finishTimeOnPE, gi);
                    PEs = gi.getPEs();
                }
                for (int k = 0; k < gi.getNumPE(); k++) {

                    finishTimeOnPE[PEs.get(k)] += (glFinishTime);
                    //
                    if (gi.getID() == ExperimentSetup.debug_job) {
                        System.out.print("[" + PEs.get(k) + "]" + finishTimeOnPE[PEs.get(k)] + ",");
                    }

                    peIndex++;
                }
                //
                if (gi.getID() == ExperimentSetup.debug_job) {
                    System.out.println(" STOP");
                }

                double giTard = Math.max(0.0, finishTimeOnPE[PEs.get(0)] - gi.getDue_date());
                gi.setExpectedFinishTime(finishTimeOnPE[PEs.get(0)]);
                gi.setTardiness(giTard);
            } else if (gi.getStatus() != Gridlet.SUCCESS && gi.getStatus() != Gridlet.INEXEC && gi.getStatus() != Gridlet.QUEUED && gi.getStatus() != Gridlet.FAILED_RESOURCE_UNAVAILABLE) {

                //System.out.println(gi.getID() + " not executing, WEIRD status=" + gi.getGridlet().getGridletStatusString() + " resource=" + resource.getResourceName() + " at clock=" + GridSim.clock());
                if (PEs.size() < gi.getNumPE()) {
                    System.out.println(gi.getID() + " NO planned PEs WEIRD status=" + gi.getGridlet().getGridletStatusString() + " resource=" + resource.getResourceName() + " at clock=" + GridSim.clock());
                    // ALERT THIS DOES NOT WORK on topology aware jobs (nodes, ppn)
                    predictPEs(finishTimeOnPE, gi);
                }
                double max = 0.0;
                PEs = gi.getPEs();
                for (int k = 0; k < gi.getNumPE(); k++) {
                    if (max < finishTimeOnPE[PEs.get(k)]) {
                        max = finishTimeOnPE[PEs.get(k)];
                    }
                }
                for (int k = 0; k < gi.getNumPE(); k++) {
                    //System.out.println(gi.getID() + " stat=" + gi.getGridlet().getGridletStatusString() + " setting PEid: " + PEs.get(k) + " to complete: " + (max + gi.getJobRuntime(peRating)));
                    //finishTimeOnPE[PEs.get(k)] = max + gi.getJobRuntime(peRating);
                    finishTimeOnPE[PEs.get(k)] = max + gi.getJobRuntime(peRating);
                }
                double giTard = Math.max(0.0, finishTimeOnPE[PEs.get(0)] - gi.getDue_date());
                gi.setExpectedFinishTime(finishTimeOnPE[PEs.get(0)]);
                gi.setTardiness(giTard);
            } else if (gi.getStatus() == Gridlet.QUEUED) {
                // this if branch should not really execute. If so, something is wrong.
                System.out.println(gi.getID() + ": ++++++++++++++++++++++++ QUEUED");
                // return the last needed PE index (others finish-time set to -999)
                peIndex = findFirstFreeSlot(finishTimeOnPE, gi);
                finishTimeOnPE[peIndex] += gi.getJobRuntime(peRating);
                double giTard = Math.max(0.0, finishTimeOnPE[peIndex] - gi.getDue_date());
                gi.setExpectedFinishTime(finishTimeOnPE[peIndex]);
                gi.setTardiness(giTard);
                // update all PE-finish-time that will run this gridlet
                for (int k = 0; k < finishTimeOnPE.length; k++) {
                    if (finishTimeOnPE[k] < -998) {
                        finishTimeOnPE[k] = finishTimeOnPE[peIndex];
                    }
                }
            }
        }
    }

    public void printRunningJobsPEs() {
        boolean[] freeID = new boolean[finishTimeOnPE.length];
        for (int i = 0; i < freeID.length; i++) {
            freeID[i] = true;
        }
        for (int i = 0; i < resInExec.size(); i++) {
            GridletInfo gi = (GridletInfo) resInExec.get(i);
            System.out.println(gi.getID() + " runs till:" + gi.getExpectedFinishTime() + " remain:" + Math.round(gi.getExpectedFinishTime() - GridSim.clock()) + " PEs:" + gi.getPlannedPEsString());
            for (int j = 0; j < gi.getPlannedPEs().size(); j++) {
                freeID[gi.getPlannedPEs().get(j)] = false;
            }
        }
        for (int i = 0; i < freeID.length; i++) {
            if (freeID[i] == true) {
                System.out.println(i + " Free at time:" + GridSim.clock());
            }
        }
    }

    /**
     * This method update information about schedule such as job start/finish
     * time, number of nondelayed jobs, makespan, etc. Usefull for
     * schedule-based methods mainly. It can be easily modified to provide more
     * information about e.g., slowdown. If there is no change since the last
     * computation it is not performed to save time.
     *
     */
    public void update(double current_time) {
        update(current_time, "");
    }

    public void update(double current_time, String message) {

        double total_tardiness = 0.0;
        double tardiness = 0.0;
        int nondelayed = 0;
        double start_hole_min = Double.MAX_VALUE;
        double start_hole_max = 0.0;
        int idUns[] = new int[resSchedule.size()];
        //System.out.println("------------------------------------------------------");
        //printRunningJobsPEs();
        //System.out.println(GridSim.clock() + ": START updating schedule: " + printScheduleIDs());
        if (ExperimentSetup.algID == 0) {
            return;
        }
        if (prev_clock == current_time && stable) {
            // no change - so save computational time
            //System.out.println(GridSim.clock() + ": stable schedule...");
            return;
        } else {
            stable_w = false;
            stable_s = false;
            Scheduler.schedule_too_long = false;
            // setup the field representing CPUs earliest free slot times
            holes.clear();
            holes_length = 0.0;
            holes_mips = 0.0;
            res_usage = 0.0;
            accum_start_time = 0.0;
            accum_sd = 0.0;
            accum_wait = 0.0;
            accum_resp = 0.0;

            // initialize the free slot array (must be done)
            predictFirstFreeSlots(current_time); //OK works
            BinaryHeap slots = null;
            if (ExperimentSetup.use_heap) {
                slots = createBinaryHeap();
            }
            /*System.out.println("----st-----");
                        for (int ff = 0; ff < finishTimeOnPE.length; ff++) {
                                    System.out.println(ff+"\t"+(finishTimeOnPE[ff]));
                                    //proc mam 52 nul, kdyz by jich melo byt jen 40?
                                }
            System.out.println("----end----");*/
            // calculate all required values for jobs in schedule
            for (int j = 0; j < resSchedule.size(); j++) {
                GridletInfo gi = (GridletInfo) resSchedule.get(j);

                idUns[j] = gi.getID();
                //update the res_usage value
                res_usage += gi.getJobRuntime(peRating) * peRating * gi.getNumPE();
                int index = -1;
                //System.out.println(gi.getID() + " (" + gi.getNumPE() + " cpu): Schedule UPDATE, expstart: " +gi.getExpectedStartTime()+" now:"+GridSim.clock());

                // if job start time is very near, do not change CPU allocation
                // apply only for first job or job already pinned
                boolean pin = true;
                //&& (j == 0 || gi.isPinned())
                if (ExperimentSetup.pinJob && gi.getExpectedStartTime() > 0.0) {
                    double expstart = gi.getExpectedStartTime();
                    if (expstart <= (current_time + ExperimentSetup.pin_duration)) {
                        if (expstart - current_time > 0) {
                            //System.out.println(gi.getID() + " (" + gi.getNumPE() + " cpu): Difference is small: " + Math.round(expstart - current_time) + " should start at:" + expstart + " now:" + GridSim.clock());
                        }
                        for (int pp = 0; pp < gi.getPlannedPEs().size(); pp++) {
                            if (finishTimeOnPE[gi.getPlannedPEs().get(pp)] > (expstart + 0.1)) {
                                //System.out.println(GridSim.clock() + ": Previous job extended - do not pin gi: " + gi.getID() + "(" + gi.getNumPE() + " cpu) diff:" + (finishTimeOnPE[gi.getPlannedPEs().get(pp)] - expstart) + " old pred.start:" + expstart + " free:" + this.getNumFreePE());
                                pin = false;
                                gi.setPinned(false);
                                break;
                            }
                        }
                        if (gi.getPlannedPEs().size() < gi.getNumPE()) {
                            pin = false;
                        }
                        // no job extension detected, go ahead and update finishTimeOnPE and update index
                        if (pin) {
                            for (int pp = 0; pp < gi.getPlannedPEs().size(); pp++) {
                                finishTimeOnPE[gi.getPlannedPEs().get(pp)] = expstart;
                            }
                            index = gi.getPlannedPEs().get(0);
                            //System.out.println(GridSim.clock() + ": Pinning gi: " + gi.getID() + "[index:"+j+"] on " + gi.getNumPE() + " CPUs | message: "+message);
                            gi.setPinned(true);
                        }
                    } else {
                        pin = false;
                    }
                } else {
                    pin = false;
                }

                // simulate the FCFS attitude of LRM on the resource
                if (!pin) {
                    index = findFirstFreeSlotForWaitingJobCheckCPUs(finishTimeOnPE, gi, slots, "up");
                    //System.out.println("Classic slot for gi: " + gi.getID() + " starting on CPU:"+index);
                }

                if (j == 0) {
                    first_job_start = gi.getExpectedStartTime();
                }
                gi.setInit(false);
                // set expected start time wrt. current schedule
                //System.out.println(gi.getID()+" index = "+index+" finishTimeOnPE length="+finishTimeOnPE.length);
                gi.setExpectedStartTime(finishTimeOnPE[index]);

                /*if (gi.getID() == 828 || gi.getID() == 834 || gi.getID() == 830 || gi.getID() == 705) {
                    System.out.println(gi.getID() + "|" + gi.getPlannedPEsString() + " start=" + gi.getExpectedStartTime()+" pinned:"+gi.isPinned()+" time:"+GridSim.clock());
                }*/
                accum_start_time += finishTimeOnPE[index];

                double glFinishTime = gi.getJobRuntime(peRating);
                if (glFinishTime < 1.0) {
                    //glFinishTime = 1.0;
                }
                int roundUpTime = (int) (glFinishTime + 1);

                double earliestNextTime = finishTimeOnPE[index];

                // time when the gridlet will be probably finished on CPU #index
                //finishTimeOnPE[index] += roundUpTime;
                finishTimeOnPE[index] += glFinishTime;
                // sets expected finish time
                gi.setExpectedFinishTime(finishTimeOnPE[index]);
                // tardiness of this gridlet in this schedule
                tardiness = Math.max(0.0, finishTimeOnPE[index] - gi.getDue_date());
                accum_wait += Math.max(0.0, gi.getExpectedStartTime() - gi.getRelease_date());
                accum_resp += Math.max(0.0, gi.getExpectedFinishTime() - gi.getRelease_date());
                accum_sd += (Math.max(1.0, (finishTimeOnPE[index] - gi.getRelease_date()))) / Math.max(1.0, roundUpTime);

                gi.setTardiness(tardiness); // after this method we know each gridlet's tardiness

                if (tardiness <= 0.0) {
                    nondelayed++;
                }
                total_tardiness += tardiness;

                // update also the rest of PEs finish-time required to run this gridlet
                for (int k = 0; k < finishTimeOnPE.length; k++) {
                    if (finishTimeOnPE[k] < -998) {
                        finishTimeOnPE[k] = finishTimeOnPE[index];
                    }
                }
                if (pin) {
                    for (int pp = 0; pp < gi.getPlannedPEs().size(); pp++) {
                        finishTimeOnPE[gi.getPlannedPEs().get(pp)] = finishTimeOnPE[index];
                        //System.out.println("Pinning gi: " + gi.getID() + " on CPU:" + gi.getPlannedPEs().get(pp));
                    }
                }
                //start_hole = earliestNextTime;
                start_hole_max = finishTimeOnPE[index];
            }

            // prepare min and max starting points for last gap
            for (int i = 0; i < finishTimeOnPE.length; i++) {
                if (finishTimeOnPE[i] > start_hole_max) {
                    start_hole_max = finishTimeOnPE[i];
                }
                if (finishTimeOnPE[i] < start_hole_min) {
                    start_hole_min = finishTimeOnPE[i];
                }
            }

            expected_fails = resSchedule.size() - nondelayed;

            // add expected tardiness of running jobs
            for (int j = 0; j < resInExec.size(); j++) {
                GridletInfo gi = (GridletInfo) resInExec.get(j);
                total_tardiness += gi.getTardiness();
                if (gi.getTardiness() <= 0.0) {
                    nondelayed++;
                }
            }

            // calculate makespan
            double makespan = 0.0;
            for (int j = 0; j < finishTimeOnPE.length; j++) {
                if (finishTimeOnPE[j] > makespan) {
                    makespan = finishTimeOnPE[j];
                }
            }

            // add tardiness and score of already finished jobs
            expected_score = nondelayed;
            total_tardiness += prev_tard;
            nondelayed += prev_score;

            // set the variables to new values
            resource_tardiness = total_tardiness;
            resource_score = nondelayed;
            resource_makespan = makespan;

            stable = true;
            prev_clock = current_time;

            //Hole h_last = new Hole(start_hole, Double.MAX_VALUE, Double.MAX_VALUE, Double.MAX_VALUE, numPE, last);
            //holes.addLast(h_last);
            //System.arraycopy(est, numPE, est, numPE, numPE);
            double finishTimeOnPE2[] = new double[resource.getNumPE()];
            for (int i = 0; i < finishTimeOnPE.length; i++) {
                finishTimeOnPE2[i] = finishTimeOnPE[i];
            }
            // add hole to the end of schedule (infinite hole)
            //createLastHoles(finishTimeOnPE2, numPE);
            if (ExperimentSetup.use_gaps) {
                createLastGapsFast(finishTimeOnPE2, numPE, start_hole_min, start_hole_max);
            }

            //sort the schedule via start times - so that less gaps will appear
            Collections.sort(resSchedule, new StartComparator());

            for (int j = 0; j < resSchedule.size(); j++) {
                GridletInfo gi = (GridletInfo) resSchedule.get(j);
                if (gi.getID() != idUns[j]) {
                    //System.out.println(j+"th job: Sorted, gaps corrupted..." + gi.getID() + "/" + idUns[j]);
                }
            }
            first_job_start = -1;
            //System.out.println(GridSim.clock() + ": END updating schedule: " + printScheduleIDs());
        }

    }

    public void forceUpdate(double current_time) {
        double total_tardiness = 0.0;
        double tardiness = 0.0;
        int nondelayed = 0;
        double start_hole_max = 0.0;
        double start_hole_min = Double.MAX_VALUE;
        GridletInfo last = null;
        // setup the field representing CPUs earliest free slot times
        holes.clear();
        holes_length = 0.0;
        holes_mips = 0.0;
        res_usage = 0.0;
        accum_start_time = 0.0;
        accum_sd = 0.0;
        accum_wait = 0.0;
        accum_resp = 0.0;
        int idUns[] = new int[resSchedule.size()];
        stable_w = false;
        stable_s = false;
        Scheduler.schedule_too_long = false;
        if (ExperimentSetup.algID == 0) {
            return;
        }

        // initialize the free slot array (must be done)
        predictFirstFreeSlots(current_time); //OK works
        BinaryHeap slots = null;
        if (ExperimentSetup.use_heap) {
            slots = createBinaryHeap();
        }

        // calculate all required values for jobs in schedule
        //System.out.println(GridSim.clock() + ": force updating schedule: " + printScheduleIDs());
        //System.out.println("------------------------------------------------------");
        //System.out.println(GridSim.clock() + ": FORCE updating schedule: " + printScheduleIDs());
        for (int j = 0; j < resSchedule.size(); j++) {
            GridletInfo gi = (GridletInfo) resSchedule.get(j);
            /*if (gi.getID() == 319) {
                System.out.println(gi.getID() + "|" + gi.getPlannedPEsString() + " start=" + gi.getExpectedStartTime() + " FUpinned:" + gi.isPinned());
            }*/
            idUns[j] = gi.getID();
            //update the res_usage value
            res_usage += gi.getJobRuntime(peRating) * peRating * gi.getNumPE();

            int index = -1;

            // if job start time is very near, do not change CPU allocation
            // apply only for first job or job already pinned
            boolean pin = true;
            //System.out.println(gi.getID() + " (" + gi.getNumPE() + " cpu): FU schedule FORCEup, expstart: " +gi.getExpectedStartTime()+" now:"+GridSim.clock());
            //&& (j == 0 || gi.isPinned())
            if (ExperimentSetup.pinJob && gi.getExpectedStartTime() > 0.0) {
                double expstart = gi.getExpectedStartTime();
                if (expstart <= (current_time + ExperimentSetup.pin_duration)) {
                    //System.out.println(gi.getID() + " (" + gi.getNumPE() + " cpu): FU Difference is small: " + Math.round(expstart - current_time) + " should start at:" + expstart+" now:"+GridSim.clock());

                    for (int pp = 0; pp < gi.getPlannedPEs().size(); pp++) {
                        if (finishTimeOnPE[gi.getPlannedPEs().get(pp)] > (expstart + 0.1)) {
                            //System.out.println("FU Previous job extended - do not pin gi: " + gi.getID());
                            pin = false;
                            gi.setPinned(false);
                            break;
                        }
                    }
                    if (gi.getPlannedPEs().size() < gi.getNumPE()) {
                        pin = false;
                    }
                    // no job extension detected, go ahead and update finishTimeOnPE and update index
                    if (pin) {
                        for (int pp = 0; pp < gi.getPlannedPEs().size(); pp++) {
                            finishTimeOnPE[gi.getPlannedPEs().get(pp)] = expstart;
                            //System.out.println("Pinning gi: " + gi.getID() + " on CPU:" + gi.getPlannedPEs().get(pp));
                        }
                        index = gi.getPlannedPEs().get(0);
                        //System.out.println(GridSim.clock() + ": FPinning gi: " + gi.getID() + "[index:"+j+"] on " + gi.getNumPE() + " CPUs, expstart: "+expstart);
                        gi.setPinned(true);
                    }
                } else {
                    pin = false;
                }
            } else {
                pin = false;
            }

            // simulate the FCFS attitude of LRM on the resource
            if (!pin) {
                index = findFirstFreeSlotForWaitingJobCheckCPUs(finishTimeOnPE, gi, slots, "up");
                //System.out.println("Classic slot for gi: " + gi.getID() + " starting on CPU:"+index);
            }

            gi.setInit(false);
            if (j == 0) {
                first_job_start = gi.getExpectedStartTime();
            }
            gi.setExpectedStartTime(finishTimeOnPE[index]);
            accum_start_time += finishTimeOnPE[index];

            double glFinishTime = gi.getJobRuntime(peRating);
            if (glFinishTime < 1.0) {
                //glFinishTime = 1.0;
            }
            int roundUpTime = (int) (glFinishTime + 1);

            double earliestNextTime = finishTimeOnPE[index];

            // time when the gridlet will be probably finished on CPU #index
            //finishTimeOnPE[index] += roundUpTime;
            finishTimeOnPE[index] += glFinishTime;
            gi.setExpectedFinishTime(finishTimeOnPE[index]);

            // tardiness of this gridlet in this schedule
            tardiness = Math.max(0.0, finishTimeOnPE[index] - gi.getDue_date());
            accum_wait += Math.max(0.0, gi.getExpectedStartTime() - gi.getRelease_date());
            accum_resp += Math.max(0.0, gi.getExpectedFinishTime() - gi.getRelease_date());
            accum_sd += (Math.max(1.0, (finishTimeOnPE[index] - gi.getRelease_date()))) / Math.max(1.0, roundUpTime);

            gi.setTardiness(tardiness); // after this method we know each gridlet's tardiness

            if (tardiness <= 0.0) {
                nondelayed++;
            }
            total_tardiness += tardiness;

            // update also the rest of PEs finish-time required to run this gridlet
            for (int k = 0; k < finishTimeOnPE.length; k++) {
                if (finishTimeOnPE[k] < -998) {
                    finishTimeOnPE[k] = finishTimeOnPE[index];
                }
                /*
                 * else if(finishTimeOnPE[k] < earliestNextTime){ // since it is
                 * FCFS resource, do no allow earlier starts finishTimeOnPE[k] =
                 * earliestNextTime; }
                 */
            }
            if (pin) {
                for (int pp = 0; pp < gi.getPlannedPEs().size(); pp++) {
                    finishTimeOnPE[gi.getPlannedPEs().get(pp)] = finishTimeOnPE[index];
                    //System.out.println("Pinning gi: " + gi.getID() + " on CPU:" + gi.getPlannedPEs().get(pp));
                }
            }
            start_hole_max = earliestNextTime;
        }
        // prepare min and max starting points for last gap
        for (int i = 0; i < finishTimeOnPE.length; i++) {
            if (finishTimeOnPE[i] > start_hole_max) {
                start_hole_max = finishTimeOnPE[i];
            }
            if (finishTimeOnPE[i] < start_hole_min) {
                start_hole_min = finishTimeOnPE[i];
            }
        }

        expected_fails = resSchedule.size() - nondelayed;

        // add expected tardiness of running jobs
        for (int j = 0; j < resInExec.size(); j++) {
            GridletInfo gi = (GridletInfo) resInExec.get(j);
            total_tardiness += gi.getTardiness();
            if (gi.getTardiness() <= 0.0) {
                nondelayed++;
            }
        }

        double makespan = 0.0;
        for (int j = 0; j < finishTimeOnPE.length; j++) {
            if (finishTimeOnPE[j] > makespan) {
                makespan = finishTimeOnPE[j];
            }
        }

        // add tardiness and score of already finished jobs
        expected_score = nondelayed;
        total_tardiness += prev_tard;
        nondelayed += prev_score;

        // set the variables to new values
        resource_tardiness = total_tardiness;
        resource_score = nondelayed;
        resource_makespan = makespan;

        stable = true;
        prev_clock = current_time;
        //Hole h_last = new Hole(start_hole, Double.MAX_VALUE, Double.MAX_VALUE, Double.MAX_VALUE, numPE, last);
        //holes.addLast(h_last);

        double finishTimeOnPE2[] = new double[resource.getNumPE()];
        for (int i = 0; i < finishTimeOnPE.length; i++) {
            finishTimeOnPE2[i] = finishTimeOnPE[i];
        }
        // add hole to the end of schedule (infinite hole)
        //createLastHoles(finishTimeOnPE2, numPE);
        if (ExperimentSetup.use_gaps) {
            createLastGapsFast(finishTimeOnPE2, numPE, start_hole_min, start_hole_max);
        }
        //sort the schedule via start times - so that less gaps will appear
        Collections.sort(resSchedule, new StartComparator());
        /*for (int j = 0; j < resSchedule.size(); j++) {
            GridletInfo gi = (GridletInfo) resSchedule.get(j);
            if (gi.getID() != idUns[j]) {
                System.out.println("Force: Sorted, gaps corrupted..." + gi.getID() + "/" + idUns[j]);
            }
        }*/
        first_job_start = -1;
        //System.out.println(GridSim.clock() + ": END force updating schedule: " + printScheduleIDs());
    }

    public void update_working(double current_time) {
        if (false) {
            forceUpdate(current_time);

        } else {

            double total_tardiness = 0.0;
            double tardiness = 0.0;
            int nondelayed = 0;
            double start_hole_min = Double.MAX_VALUE;
            double start_hole_max = 0.0;
            double end_hole = 0.0;
            int size_hole = 0;
            GridletInfo last = null;
            int idUns[] = new int[resSchedule.size()];

            if (prev_clock == current_time && stable) {
                // no change - so save computational time
                return;
            } else {
                stable_w = false;
                stable_s = false;
                Scheduler.schedule_too_long = false;
                // setup the field representing CPUs earliest free slot times
                holes.clear();
                holes_length = 0.0;
                holes_mips = 0.0;
                res_usage = 0.0;
                accum_start_time = 0.0;
                accum_sd = 0.0;
                accum_wait = 0.0;
                accum_resp = 0.0;

                // initialize the free slot array (must be done)
                predictFirstFreeSlots(current_time); //OK works
                BinaryHeap slots = null;
                if (ExperimentSetup.use_heap) {
                    slots = createBinaryHeap();
                }

                // calculate all required values for jobs in schedule
                //System.out.println(GridSim.clock() + ": updating schedule: " + printScheduleIDs());
                for (int j = 0; j < resSchedule.size(); j++) {
                    GridletInfo gi = (GridletInfo) resSchedule.get(j);
                    idUns[j] = gi.getID();
                    //update the res_usage value
                    res_usage += gi.getJobRuntime(peRating) * peRating * gi.getNumPE();

                    // simulate the FCFS attitude of LRM on the resource
                    int index = findFirstFreeSlotForWaitingJobCheckCPUs(finishTimeOnPE, gi, slots, "up");

                    if (j == 0) {
                        first_job_start = gi.getExpectedStartTime();
                    }
                    gi.setInit(false);
                    // set expected start time wrt. current schedule
                    gi.setExpectedStartTime(finishTimeOnPE[index]);

                    accum_start_time += finishTimeOnPE[index];

                    double glFinishTime = gi.getJobRuntime(peRating);
                    if (glFinishTime < 1.0) {
                        //glFinishTime = 1.0;
                    }
                    int roundUpTime = (int) (glFinishTime + 1);

                    double earliestNextTime = finishTimeOnPE[index];

                    // time when the gridlet will be probably finished on CPU #index
                    //finishTimeOnPE[index] += roundUpTime;
                    finishTimeOnPE[index] += glFinishTime;
                    // sets expected finish time
                    gi.setExpectedFinishTime(finishTimeOnPE[index]);
                    // tardiness of this gridlet in this schedule
                    tardiness = Math.max(0.0, finishTimeOnPE[index] - gi.getDue_date());
                    accum_wait += Math.max(0.0, gi.getExpectedStartTime() - gi.getRelease_date());
                    accum_resp += Math.max(0.0, gi.getExpectedFinishTime() - gi.getRelease_date());
                    accum_sd += (Math.max(1.0, (finishTimeOnPE[index] - gi.getRelease_date()))) / Math.max(1.0, roundUpTime);

                    gi.setTardiness(tardiness); // after this method we know each gridlet's tardiness

                    if (tardiness <= 0.0) {
                        nondelayed++;
                    }
                    total_tardiness += tardiness;

                    // update also the rest of PEs finish-time required to run this gridlet
                    for (int k = 0; k < finishTimeOnPE.length; k++) {
                        if (finishTimeOnPE[k] < -998) {
                            finishTimeOnPE[k] = finishTimeOnPE[index];
                        }
                    }
                    //start_hole = earliestNextTime;
                    start_hole_max = finishTimeOnPE[index];
                }

                // prepare min and max starting points for last gap
                for (int i = 0; i < finishTimeOnPE.length; i++) {
                    if (finishTimeOnPE[i] > start_hole_max) {
                        start_hole_max = finishTimeOnPE[i];
                    }
                    if (finishTimeOnPE[i] < start_hole_min) {
                        start_hole_min = finishTimeOnPE[i];
                    }
                }

                expected_fails = resSchedule.size() - nondelayed;

                // add expected tardiness of running jobs
                for (int j = 0; j < resInExec.size(); j++) {
                    GridletInfo gi = (GridletInfo) resInExec.get(j);
                    total_tardiness += gi.getTardiness();
                    if (gi.getTardiness() <= 0.0) {
                        nondelayed++;
                    }
                }

                // calculate makespan
                double makespan = 0.0;
                for (int j = 0; j < finishTimeOnPE.length; j++) {
                    if (finishTimeOnPE[j] > makespan) {
                        makespan = finishTimeOnPE[j];
                    }
                }

                // add tardiness and score of already finished jobs
                expected_score = nondelayed;
                total_tardiness += prev_tard;
                nondelayed += prev_score;

                // set the variables to new values
                resource_tardiness = total_tardiness;
                resource_score = nondelayed;
                resource_makespan = makespan;

                stable = true;
                prev_clock = current_time;

                //Hole h_last = new Hole(start_hole, Double.MAX_VALUE, Double.MAX_VALUE, Double.MAX_VALUE, numPE, last);
                //holes.addLast(h_last);
                //System.arraycopy(est, numPE, est, numPE, numPE);
                double finishTimeOnPE2[] = new double[resource.getNumPE()];
                for (int i = 0; i < finishTimeOnPE.length; i++) {
                    finishTimeOnPE2[i] = finishTimeOnPE[i];
                }
                // add hole to the end of schedule (infinite hole)
                //createLastHoles(finishTimeOnPE2, numPE);
                if (ExperimentSetup.use_gaps) {
                    createLastGapsFast(finishTimeOnPE2, numPE, start_hole_min, start_hole_max);
                }

                //sort the schedule via start times - so that less gaps will appear
                Collections.sort(resSchedule, new StartComparator());

                /*for (int j = 0; j < resSchedule.size(); j++) {
                    GridletInfo gi = (GridletInfo) resSchedule.get(j);
                    if (gi.getID() != idUns[j]) {
                        System.out.println("Sorted, gaps corrupted..." + gi.getID() + "/" + idUns[j]);
                    }
                }*/
                first_job_start = -1;
                //System.out.println(GridSim.clock() + ": END updating schedule: " + printScheduleIDs());
            }
        }
    }

    /**
     * This method updates all information about the schedule even when no
     * change appears - more time overhead. The functionality is the same as in
     * case of update(current_time);
     *
     */
    public void forceUpdate_working(double current_time) {
        double total_tardiness = 0.0;
        double tardiness = 0.0;
        int nondelayed = 0;
        double start_hole_max = 0.0;
        double start_hole_min = Double.MAX_VALUE;
        GridletInfo last = null;
        // setup the field representing CPUs earliest free slot times
        holes.clear();
        holes_length = 0.0;
        holes_mips = 0.0;
        res_usage = 0.0;
        accum_start_time = 0.0;
        accum_sd = 0.0;
        accum_wait = 0.0;
        accum_resp = 0.0;
        int idUns[] = new int[resSchedule.size()];
        stable_w = false;
        stable_s = false;
        Scheduler.schedule_too_long = false;

        // initialize the free slot array (must be done)
        predictFirstFreeSlots(current_time); //OK works
        BinaryHeap slots = null;
        if (ExperimentSetup.use_heap) {
            slots = createBinaryHeap();
        }

        // calculate all required values for jobs in schedule
        //System.out.println(GridSim.clock() + ": force updating schedule: " + printScheduleIDs());
        for (int j = 0; j < resSchedule.size(); j++) {
            GridletInfo gi = (GridletInfo) resSchedule.get(j);
            idUns[j] = gi.getID();
            //update the res_usage value
            res_usage += gi.getJobRuntime(peRating) * peRating * gi.getNumPE();

            // simulate the FCFS attitude of LRM on the resource
            int index = findFirstFreeSlotForWaitingJobCheckCPUs(finishTimeOnPE, gi, slots, "fup");
            gi.setInit(false);
            if (j == 0) {
                first_job_start = gi.getExpectedStartTime();
            }
            gi.setExpectedStartTime(finishTimeOnPE[index]);
            accum_start_time += finishTimeOnPE[index];

            double glFinishTime = gi.getJobRuntime(peRating);
            if (glFinishTime < 1.0) {
                //glFinishTime = 1.0;
            }
            int roundUpTime = (int) (glFinishTime + 1);

            double earliestNextTime = finishTimeOnPE[index];

            // time when the gridlet will be probably finished on CPU #index
            //finishTimeOnPE[index] += roundUpTime;
            finishTimeOnPE[index] += glFinishTime;
            gi.setExpectedFinishTime(finishTimeOnPE[index]);

            // tardiness of this gridlet in this schedule
            tardiness = Math.max(0.0, finishTimeOnPE[index] - gi.getDue_date());
            accum_wait += Math.max(0.0, gi.getExpectedStartTime() - gi.getRelease_date());
            accum_resp += Math.max(0.0, gi.getExpectedFinishTime() - gi.getRelease_date());
            accum_sd += (Math.max(1.0, (finishTimeOnPE[index] - gi.getRelease_date()))) / Math.max(1.0, roundUpTime);

            gi.setTardiness(tardiness); // after this method we know each gridlet's tardiness

            if (tardiness <= 0.0) {
                nondelayed++;
            }
            total_tardiness += tardiness;

            // update also the rest of PEs finish-time required to run this gridlet
            for (int k = 0; k < finishTimeOnPE.length; k++) {
                if (finishTimeOnPE[k] < -998) {
                    finishTimeOnPE[k] = finishTimeOnPE[index];
                }
                /*
                 * else if(finishTimeOnPE[k] < earliestNextTime){ // since it is
                 * FCFS resource, do no allow earlier starts finishTimeOnPE[k] =
                 * earliestNextTime; }
                 */
            }
            start_hole_max = earliestNextTime;
        }
        // prepare min and max starting points for last gap
        for (int i = 0; i < finishTimeOnPE.length; i++) {
            if (finishTimeOnPE[i] > start_hole_max) {
                start_hole_max = finishTimeOnPE[i];
            }
            if (finishTimeOnPE[i] < start_hole_min) {
                start_hole_min = finishTimeOnPE[i];
            }
        }

        expected_fails = resSchedule.size() - nondelayed;

        // add expected tardiness of running jobs
        for (int j = 0; j < resInExec.size(); j++) {
            GridletInfo gi = (GridletInfo) resInExec.get(j);
            total_tardiness += gi.getTardiness();
            if (gi.getTardiness() <= 0.0) {
                nondelayed++;
            }
        }

        double makespan = 0.0;
        for (int j = 0; j < finishTimeOnPE.length; j++) {
            if (finishTimeOnPE[j] > makespan) {
                makespan = finishTimeOnPE[j];
            }
        }

        // add tardiness and score of already finished jobs
        expected_score = nondelayed;
        total_tardiness += prev_tard;
        nondelayed += prev_score;

        // set the variables to new values
        resource_tardiness = total_tardiness;
        resource_score = nondelayed;
        resource_makespan = makespan;

        stable = true;
        prev_clock = current_time;
        //Hole h_last = new Hole(start_hole, Double.MAX_VALUE, Double.MAX_VALUE, Double.MAX_VALUE, numPE, last);
        //holes.addLast(h_last);

        double finishTimeOnPE2[] = new double[resource.getNumPE()];
        for (int i = 0; i < finishTimeOnPE.length; i++) {
            finishTimeOnPE2[i] = finishTimeOnPE[i];
        }
        // add hole to the end of schedule (infinite hole)
        //createLastHoles(finishTimeOnPE2, numPE);
        if (ExperimentSetup.use_gaps) {
            createLastGapsFast(finishTimeOnPE2, numPE, start_hole_min, start_hole_max);
        }
        //sort the schedule via start times - so that less gaps will appear
        Collections.sort(resSchedule, new StartComparator());
        /*for (int j = 0; j < resSchedule.size(); j++) {
            GridletInfo gi = (GridletInfo) resSchedule.get(j);
            if (gi.getID() != idUns[j]) {
                System.out.println("Force: Sorted, gaps corrupted..." + gi.getID() + "/" + idUns[j]);
            }
        }*/
        first_job_start = -1;
        //System.out.println(GridSim.clock() + ": END force updating schedule: " + printScheduleIDs());
    }

    /**
     * Auxiliary method - once schedule is changed it is not stable until update
     * method is called
     */
    public boolean removeGInfo(GridletInfo gi) {
        stable = false;
        stable_w = false;
        stable_s = false;
        holes.clear();
        return resSchedule.remove(gi);
    }

    /**
     * Auxiliary method - once schedule is changed it is not stable until update
     * method is called
     */
    public GridletInfo removeGInfoIndex(int index) {
        stable = false;
        stable_w = false;
        stable_s = false;
        holes.clear();
        return (GridletInfo) resSchedule.remove(index);
    }

    /**
     * Auxiliary method - once schedule is changed it is not stable until update
     * method is called
     */
    public GridletInfo removeFirstGI() {
        stable = false;
        stable_w = false;
        stable_s = false;
        GridletInfo gi = (GridletInfo) resSchedule.remove(0);
        holes.clear();
        return gi;
    }

    /**
     * Auxiliary method - once schedule is changed it is not stable until update
     * method is called
     */
    public void addLastGInfo(GridletInfo gi) {
        stable = false;
        stable_w = false;
        stable_s = false;
        resSchedule.add(gi);
        gi.getPEs().clear();
        holes.clear();
    }

    /**
     * Auxiliary method - once schedule is changed it is not stable until update
     * method is called
     */
    public boolean addGInfo(int index, GridletInfo gi) {
        stable = false;
        stable_w = false;
        stable_s = false;
        resSchedule.add(index, gi);
        gi.getPEs().clear();
        holes.clear();
        return true;
    }

    /**
     * Auxiliary method - once schedule is changed it is not stable until update
     * method is called
     */
    public void addGInfoInExec(GridletInfo gi) {
        stable = false;
        stable_w = false;
        stable_s = false;
        stable_free = false;
        resInExec.add(gi);
        holes.clear();

    }
    
    public boolean is_running_exclusive_job_blocking_preemption(GridletInfo cg) {

        boolean candidate_exclusive = false;
        if(cg.getProperties().contains(":excl")){
            candidate_exclusive = true;
        }        
        int candidate_priority = ExperimentSetup.queues.get(cg.getQueue()).getPriority();
        
        for (int j = 0; j < resInExec.size(); j++) {
            GridletInfo gi = (GridletInfo) resInExec.get(j);
            int running_priority = ExperimentSetup.queues.get(gi.getQueue()).getPriority();
            boolean running_exclusive = gi.getProperties().contains(":excl");
            
            if(running_priority >= candidate_priority && running_exclusive){
                // running job is exclusive and blocks preemption
                return true;
            }
            if(running_priority >= candidate_priority && candidate_exclusive){
                // running job cannot be preempted and blocks preemption for this exclusive job
                return true;
            }            
        }
        return false;
    }

    public LinkedList<GridletInfo> findAndCheckpointJobs(GridletInfo gi, int priority_level) {

        //MachineList machines = this.resource.getMachineList();
        ArrayList machines = new ArrayList<>(this.resource.getMachineList());
        MachineList machines_sorted = new AdvancedMachineList();
        for (int i = 0; i < machines.size(); i++) {
            MachineWithRAMandGPUs machine = (MachineWithRAMandGPUs) machines.get(i);
            machines_sorted.add(machine);
        }
        
        if(gi.getNumNodes()==this.resource.getNumMachines() && is_running_exclusive_job_blocking_preemption(gi)){
            System.out.println("Running job blocks preemption... ");
            return null;
        }

        // sort machines such that free machines come first
        // TO DO: once we build checkpointed list, this order may not hold anymore 
        // (we do not notice that a job will free other machines since we only check them in a fixed order)
        // (ideally, next machine to check should be those from the terminated job used_machine_list)
        System.setProperty("java.util.Arrays.useLegacyMergeSort", "true");
        Collections.sort(machines_sorted, new MachineFreeCpuAvailabilityComparator());

        long ram = gi.getRam();
        int ppn = gi.getPpn();
        int numNodes = gi.getNumNodes();
        int GPUs_per_node = gi.getGpus_per_node();

        LinkedList<GridletInfo> checkpointed_jobs = new LinkedList<>();

        int allocateNodes = numNodes;

        for (int i = 0; i < machines_sorted.size(); i++) {
            MachineWithRAMandGPUs machine = (MachineWithRAMandGPUs) machines_sorted.get(i);
            //System.out.println(machine.getMachineID() + " PRE machine has " + machine.getNumFreePE() + " free CPUs and " + machine.getFreeGPUs() + " GPUs");
        }

        boolean was_ready_already = true;
        //System.out.println(this.resource.getResourceName()+ " Machines: "+machines.size());
        for (int i = 0; i < machines_sorted.size(); i++) {
            MachineWithRAMandGPUs machine = (MachineWithRAMandGPUs) machines_sorted.get(i);
            //either use it if machine is free or checkpoint jobs as needed
            //System.out.println(machine.getMachineID() + " DURING machine has " + machine.getNumFreePE() + " free CPUs");

            // exclusive job requires all PEs on a machine
            if (gi.getProperties().contains(":excl")) {
                ppn = machine.getNumPE();
            }

            if (machine.getNumFreePE() >= ppn && machine.getFreeRam() >= ram && machine.getFreeGPUs() >= GPUs_per_node) {
                allocateNodes--;
            } else {
                was_ready_already = false;
                LinkedList<GridletInfo> local_jobs = checkpointJobsOnOneMachine(checkpointed_jobs, machine, gi, priority_level);
                if (local_jobs != null) {
                    checkpointed_jobs.addAll(local_jobs);
                    allocateNodes--;
                    //System.out.println("Jobs to preempt size: "+local_jobs.size());
                    //quick fix for the whole node job scenario only
                    if (ExperimentSetup.all_jobs_allocate_whole_nodes && local_jobs.size() > 0) {
                        GridletInfo to_be_preempted_job = local_jobs.getFirst();
                        //System.out.println("testing: "+to_be_preempted_job.getID());
                        if (to_be_preempted_job.getNumNodes() > allocateNodes) {
                            //System.out.println("Stopping sooner!");
                            //killing such job will free enough resources for whole node jobs so quit now
                            allocateNodes = 0;
                        } else {
                            //System.out.println("Job: "+to_be_preempted_job.getID()+" is not sufficient: "+(to_be_preempted_job.getNumNodes() > allocateNodes)+" "+(to_be_preempted_job.getRam() >= ram));
                        }
                    }
                }
            }
            if (allocateNodes <= 0) {
                System.out.println("Done: job " + gi.getID() + " needs [" + numNodes + "x" + ppn + "] CPUs, # of required checkpointed jobs: " + checkpointed_jobs.size());

                return checkpointed_jobs;
            }
            
            if(i==machines_sorted.size()-1){
                System.out.println("No suitable checkpoint available, only "+allocateNodes+" nodes found.");
            }

        }
        return null;
    }

    private LinkedList<GridletInfo> checkpointJobsOnOneMachine(LinkedList<GridletInfo> previously_checkpointed_jobs, MachineWithRAMandGPUs machine, GridletInfo gi, int priority_level) {
        LinkedList<GridletInfo> checkpointed_jobs = new LinkedList<>();
        String added = "";
        String reused = "";
        long ram = gi.getRam();
        int ppn = gi.getPpn();
        int GPUs_per_node = gi.getGpus_per_node();

        long free_ram = machine.getFreeRam();
        int freePE = machine.getNumFreePE();
        int freeGPUs = machine.getFreeGPUs();

        // exclusive job requires all PEs on a machine
        if (gi.getProperties().contains(":excl")) {
            ppn = machine.getNumPE();
        }

        // update available capacity wrt. already checkpointed jobs
        for (int i = 0; i < previously_checkpointed_jobs.size(); i++) {
            ComplexGridlet g = (ComplexGridlet) previously_checkpointed_jobs.get(i).getGridlet();
            GridletInfo gin = previously_checkpointed_jobs.get(i);
            //System.out.println(gin.getID() + " is already checkpointed, using PEs: " + g.getPEs().toString() + " this mach: " + machine.getMachineID() + " runs: " + machine.getRunningJobsString());
            if (machine.containsJob(g)) {
                //System.out.println(gin.getID() + " runs on " + machine.getMachineID());
                reused += gin.getID() + " ";

                free_ram += gin.getRam();
                freePE += gin.getPpn();
                freeGPUs += gin.getGpus_per_node();
            }
        }
        // kill as many running jobs as necessary
        ArrayList<ResGridlet> running_jobs = machine.running_jobs;
        for (int i = 0; i < running_jobs.size(); i++) {
            ComplexGridlet cg = (ComplexGridlet) running_jobs.get(i).getGridlet();
            GridletInfo checkpointed_job = findGiInResInExec(cg);
            // only consider this running job if it has not been used (checkpointed on a previous machine) already! (as a previously_chckp_job)
            if (!previously_checkpointed_jobs.contains(checkpointed_job)) {
                //System.out.println(checkpointed_job.getID() + " runs first here " + machine.getMachineID());
                checkpointed_job.machines_used += machine.getMachineID() + " ";

                // check job priority level...
                int ge_p = ExperimentSetup.queues.get(checkpointed_job.getQueue()).getPriority();
                if (ge_p < priority_level) {
                    free_ram += cg.getRam();
                    freePE += cg.getPpn();
                    freeGPUs += cg.getGpus_per_node();
                    if (!previously_checkpointed_jobs.contains(checkpointed_job)) {
                        added += cg.getGridletID() + "(" + cg.getPpn() + "),";
                        checkpointed_jobs.add(checkpointed_job);
                    }
                }else{
                    //System.out.println("Cannot preempt "+checkpointed_job.getID()+" with priority "+ge_p+" on machine: "+machine.getMachineID());
                }
            }

            if (free_ram >= ram && freePE >= ppn && freeGPUs >= GPUs_per_node) {
                //System.out.println("machine " + machine.getMachineID() + " should have " + freePE + " CPUs after chckp. Added: " + added + " Reused: " + reused);
                return checkpointed_jobs;

            }
        }

        return null;
    }

    public GridletInfo findGiInResInExec(ComplexGridlet cg) {

        for (int j = 0; j < resInExec.size(); j++) {
            GridletInfo gi = (GridletInfo) resInExec.get(j);
            if (gi.getGridlet().equals(cg)) {
                return gi;
            }
        }
        return null;
    }

    public LinkedList<GridletInfo> findAndCheckpointJobsWhenWholeNodesAllocations(GridletInfo gi, int priority_level) {
        int required = Math.max(0, (gi.getNumPE() - getNumFreePE()));
        int freed = 0;
        LinkedList<GridletInfo> checkpointed_jobs = new LinkedList<>();
        for (int i = resInExec.size() - 1; i >= 0; i--) {
            GridletInfo ge = resInExec.get(i);
            int ge_p = ExperimentSetup.queues.get(ge.getQueue()).getPriority();
            if (ge_p < priority_level) {
                // ALERT - this is not sufficient if jobs do not allocate whole nodes!
                freed += ge.getNumPE();
                checkpointed_jobs.add(ge);
                if (freed >= required) {
                    //kill such job at AdvSpaceShared
                    //super.sim_schedule(this.resource.getResourceID(), 0.0, AleaSimTags.POLICY_CHECKPOINT, null);
                    return checkpointed_jobs;
                }
            } else {
                //System.out.println(ge.getID() + " has " + ge_p + " priority thus cannot be checkpointed with a job having priority " + priority_level);
            }
        }
        //System.out.println(gi.getID() + " could not use C/R, only "+freed+" found ");   
        return null;
    }

    /**
     * This method force recomputation of jobs-on-resource status. It also
     * updates information about their expected finish time, tardiness etc.
     */
    public void updateFinishTimeOfAssignedGridlets(double current_time) {
        // setup the field representing CPUs earliest free slot times
        predictFirstFreeSlots(current_time);
    }

    /**
     * Queue only method (not to be used with schedules) - gets first available
     * start time for the TOP job (gi).
     */
    public void getEarliestStartTimeForTopJob(GridletInfo gi, double current_time) {
        this.reserved_machines = null;
        // updates finishTimeOnPE
        this.updateFinishTimeOfAssignedGridlets(current_time);

        // get EST according to gi PE count
        // ALERT does this respect nodes and chunks? I don't think so.
        // THIS DOES not work for topology-aware jobs (numnodes, ppn)
        //int index = findFirstFreeSlot(finishTimeOnPE, gi);
        //this.est = finishTimeOnPE[index]; // Earl. Start Time for head of active_scheduling_queue
        ArrayList machines_sorted = getReservationTimeForTopJob(gi);
        this.est = ((MachineWithRAMandGPUs) machines_sorted.get(gi.getNumNodes() - 1)).getEst();
        //this.usablePEs = findUsablePEs(index, finishTimeOnPE, gi);

        this.reserved_machines = machines_sorted;
    }

    /**
     * New method to espablish a reservation for TOP JOB
     *
     */
    public ArrayList getReservationTimeForTopJob(GridletInfo gi) {
        //MachineList machines = this.resource.getMachineList();
        ArrayList machines = new ArrayList<>(this.resource.getMachineList());
        ArrayList machines_sorted = new ArrayList<>(new AdvancedMachineList());

        long ram = gi.getRam();
        int ppn = gi.getPpn();
        int numNodes = gi.getNumNodes();
        int GPUs_per_node = gi.getGpus_per_node();
        ArrayList<Double> earliest_start_times = new ArrayList();

        int allocateNodes = numNodes;
        //System.out.println(this.resource.getResourceName()+ " Machines: "+machines.size());
        for (int i = 0; i < machines.size(); i++) {
            MachineWithRAMandGPUs machine = (MachineWithRAMandGPUs) machines.get(i);

            // exclusive job must use all PEs
            if (gi.getProperties().contains(":excl")) {
                ppn = machine.getNumPE();
            }

            // cannot use such machine
            if (machine.getFailed() || machine.getNumPE() < ppn) {
                //System.out.println(gi.getID() + " skipping this machine " + i);
                continue;
            }
            machine.banned_PEs = 0;
            machine.banned_GPUs = 0;
            boolean success = machine.calculateMachineCapacityInTime(GridSim.clock());
            //System.out.println(gi.getID()+" success on machine "+i);
            for (int c = 0; c < machine.capacity_list.size(); c++) {
                Capacity cap = machine.capacity_list.get(c);
                if (cap.getCpus() >= ppn && cap.getRam() >= ram && cap.getGpus() >= GPUs_per_node) {
                    earliest_start_times.add(cap.getStart_time());
                    machine.setEst(cap.getStart_time());
                    machines_sorted.add(machine);
                    if (c == 0) {
                        // System.out.println(gi.getID() + " adding to est list, size = " + earliest_start_times.size() + " on machine " + i+" usable free CAP = "+(cap.getCpus()-ppn));
                    }
                    break;
                }
            }
        }

        // now sort them (FUTURE WORK: consider best-fit selection that will create less gaps). Now we choose those earliest...
        Collections.sort(earliest_start_times);
        System.setProperty("java.util.Arrays.useLegacyMergeSort", "true");
        Collections.sort(machines_sorted, new MachineEarliestStartSlotComparator());
        double earliest_start_time = earliest_start_times.get(numNodes - 1);
        //System.out.println(gi.getID() + " finding res for numnodes=" + numNodes + " ncpus=" + ppn + " on Res " + this.resource.getResourceName() + " that has machines = " + machines.size() + " and est list size: " + earliest_start_times.size() + " EST = " + earliest_start_time+" at time: "+GridSim.clock());

        //ban reserved nodes and their CPUs and set their EST time (reservation time)
        for (int b = 0; b < numNodes; b++) {
            MachineWithRAMandGPUs machine = (MachineWithRAMandGPUs) machines_sorted.get(b);
            // exclusive job must use all PEs
            if (gi.getProperties().contains(":excl")) {
                ppn = machine.getNumPE();
            }
            if (machine.getNumFreePE() > 0) {
                machine.banned_PEs = ppn;
                machine.banned_GPUs = GPUs_per_node;
                //System.out.println(gi.getID() + " Banning " + machine.banned_PEs + " CPUs on cl: "+this.resource.getResourceName()+" machine with EST: " + machine.getEst() + " machine id:" + machine.getMachineID()+" free: "+machine.getNumFreePE());
                machine.setEst(earliest_start_time);
            }

        }

        return machines_sorted;
    }

    public boolean canGridletFitNextToReservation(GridletInfo gi, GridletInfo grsv) {
        ArrayList machines = reserved_machines;

        long ram = gi.getRam();
        int ppn = gi.getPpn();
        int numNodes = gi.getNumNodes();
        int GPUs_per_node = gi.getGpus_per_node();
        int allocateNodes = numNodes;

        gi.getGridlet().getAllowed_machine_ids().clear();

        int reserved_ppn = grsv.getPpn();

        //System.out.println(this.resource.getResourceName()+ " Machines: "+machines.size());
        for (int i = 0; i < machines.size(); i++) {
            MachineWithRAMandGPUs machine = (MachineWithRAMandGPUs) machines.get(i);
            int freePpn = Math.max(0, (machine.getNumFreePE() - machine.banned_PEs));
            int freeGPUs = Math.max(0, (machine.getFreeGPUs() - machine.banned_GPUs));

            if (gi.getProperties().contains(":excl")) {
                ppn = machine.getNumPE();
            }
            // cannot use such machine
            if (machine.getFailed() || freePpn < ppn || freeGPUs < GPUs_per_node) {
                //System.out.println(gi.getID() + " skipping this machine " + i);
                continue;
            }
            if (freePpn >= ppn && machine.getFreeRam() >= ram && freeGPUs >= GPUs_per_node) {
                allocateNodes--;
                // add this machine to allowed list
                gi.getGridlet().getAllowed_machine_ids().add(machine.getMachineID());
                //System.out.println(gi.getID() + ": adding allowed machine: "+machine.getMachineID()+" that has free: "+machine.getNumFreePE()+" minus banned: "+machine.banned_PEs);

                if (allocateNodes <= 0) {
                    //System.out.println(gi.getID() + " fits around "+grsv.getID()+" reservation, wants: nodes=" + numNodes + " and ncpus=" + ppn + 
                    //        " on Res " + this.resource.getResourceName() + " and gets "+gi.getGridlet().getAllowed_machine_ids().size()+" machines, time: "+GridSim.clock());
                    return true;
                }
            }
        }

        return false;
    }

    /**
     * This method tries to find a suitable hole (gap) for gridlet gi in current
     * schedule. Suitable == long enough, large enough wrt. PEs.
     */
    public boolean findHoleForGridlet(GridletInfo gi) {
        if (gi.getNumPE() > this.numPE) {
            return false;
        }
        double mips = gi.getJobRuntime(peRating) * peRating;
        Hole candidate = null;
        double prev_end = Double.MAX_VALUE;

        for (int i = 0; i < holes.size(); i++) {
            Hole h = (Hole) holes.get(i);

            if (h.getSize() >= gi.getNumPE() && h.getStart() <= prev_end) {
                if (candidate == null) {
                    // new candidate hole
                    candidate = h;
                }
                // next hole has to start right after this hole
                prev_end = h.getEnd();

                // hole(s) are large enough
                if (mips <= h.getMips()) {
                    // what is the candidate position in schedule
                    GridletInfo nextGi = (GridletInfo) candidate.getPosition(); // because of this Gi the hole(s) were created

                    int index = 0;
                    if (nextGi == null) {
                        index = resSchedule.size();
                    } else {
                        index = resSchedule.indexOf(nextGi);
                    }

                    this.addGInfo(index, gi);
                    return true;
                } else {
                    // hole(s) are still small
                    // decrease remaining length of hole
                    mips = mips - h.getMips();
                }
            } else {
                // restart search for hole - this one is not good (small PEs size)
                candidate = null;
                mips = gi.getJobRuntime(peRating) * peRating;
                prev_end = Double.MAX_VALUE;
                // this gap is candidate in the next round (otherwise it would be skipped, resulting in a possible error)
                if (h.getSize() >= gi.getNumPE()) {
                    i--;
                }
            }
        }
        System.out.println("No hole found for gi=" + gi.getID() + " which is weird because holes=" + holes.size());
        return false;
    }

    /**
     * Method that print all holes in current schedule.
     */
    protected void printHole() {
        if (holes.size() > 0) {
            Hole h = (Hole) holes.getFirst();
            GridletInfo gi = (GridletInfo) resSchedule.get(0);
            System.out.println(holes.size() + "\t" + resSchedule.size() + " | " + h.getPosition().getID()
                    + "," + gi.getID() + " | " + h.getLength());
        }
    }

    /**
     * Method that print current schedule.
     */
    protected void printSchedule() {
        System.out.print("Schedule on " + this.resource.getResourceName() + ": ");
        if (resSchedule.size() == 0) {
            System.out.print("Empty schedule (no planning here)");
        } else {
            for (int j = 0; j < resSchedule.size(); j++) {
                GridletInfo gi = (GridletInfo) resSchedule.get(j);
                String start_date = new java.text.SimpleDateFormat("kk:mm dd-MM-yyyy").format(new java.util.Date(Math.round(gi.getExpectedStartTime()) * 1000));
                String end_date = new java.text.SimpleDateFormat("kk:mm dd-MM-yyyy").format(new java.util.Date(Math.round(gi.getExpectedFinishTime()) * 1000));
                //print [jobID (nodes)(start)(end)]
                System.out.print("[" + gi.getID() + "(node:" + gi.getPlannedPEsString() + ")(s:" + start_date + ")(e:" + end_date + ")] ");
            }
        }
        System.out.println(" END.");
    }

    public String printScheduleIDs() {
        String sch = "";
        if (resSchedule.size() == 0) {
            System.out.print("Empty schedule (no planning here)");
        } else {
            for (int j = 0; j < resSchedule.size(); j++) {
                GridletInfo gi = (GridletInfo) resSchedule.get(j);
                sch += ("[" + gi.getID() + "(" + gi.getExpectedStartTime() + ")cpu:" + gi.getNumPE() + "] ");
            }
        }
        sch += " END.";
        return sch;
    }

    /*
     * Auxiliary function to realize whether this resource supports job's
     * requirements
     *
     */
    protected boolean supportProperty(String prop) {
        String supported = this.resource.getProperties();
        //System.out.println("SUPPROTED by RI = "+supported);
        if (supported.contains(prop)) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * Finds the position of specified gridlet in the resource's schedule.
     */
    public int findGridletInfoPosition(GridletInfo gi) {
        for (int j = 0; j < resSchedule.size(); j++) {
            GridletInfo gs = (GridletInfo) resSchedule.get(j);
            if (gs.getID() == gi.getID() && gs.getUser().equals(gi.getUser())) {
                return j;
            }
        }
        return -1;
    }

    /**
     * Updates Fairness related criteria.
     */
    public double[] updateFairness(double[] tuwt, double[] tusa) {
        // update when internal inf. is stable and the int. array is not, or when the size of internal array is different from Scheduler's array size, or the int. inf. is not stable
        if (r_tuwt == null | (stable && !stable_w) || (r_tuwt.length != (tuwt.length * 2)) || !stable) {
            int size = tuwt.length;

            // initialize new array
            r_tuwt = new double[size * 2];
            for (int i = 0; i < r_tuwt.length; i++) {
                r_tuwt[i] = 0.0;
            }

            for (int i = 0; i < resInExec.size(); i++) {
                GridletInfo gi = resInExec.get(i);
                int user_index = Scheduler.users.indexOf(new String(gi.getUser()));
                r_tuwt[user_index] += Math.max(0.0, gi.getGridlet().getExecStartTime() - gi.getRelease_date());//*gi.getNumPE();
                r_tuwt[user_index + size] += gi.getNumPE() * gi.getJobRuntime(peRating);
            }
            for (int i = 0; i < resSchedule.size(); i++) {
                GridletInfo gi = resSchedule.get(i);
                int user_index = Scheduler.users.indexOf(new String(gi.getUser()));
                r_tuwt[user_index] += Math.max(0.0, gi.getExpectedStartTime() - gi.getRelease_date());//*gi.getNumPE();
                r_tuwt[user_index + size] += gi.getNumPE() * gi.getJobRuntime(peRating);
            }
            stable_w = true;
        }

        return r_tuwt;
    }
}
