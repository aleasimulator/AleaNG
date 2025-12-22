package xklusac.environment;

import alea.core.AleaSimTags;
import eduni.simjava.Sim_entity;
import eduni.simjava.Sim_event;
import gridsim.*;
import java.io.IOException;
import java.util.*;
import org.jfree.data.time.TimeSeries;
import org.jfree.data.time.TimeSeriesCollection;
import xklusac.objective_functions.CommonObjectives;
import xklusac.extensions.*;

/**
 * Class Scheduler<p>
 * This class represents the Scheduler. It takes the GridletInfo object from
 * JobLoader and determines where to send the Gridlet. It uses both
 * active_scheduling_queue-based (FCFS, EDF, Easy Backfilling, EDF-Backfilling
 * and PBS-pro like algorithm) and schedule-based (Backfill-like solution with
 * Local search based optimization) scheduling algorithms.<br> body() method is
 * responsible for communication while scheduling algorithms are placed outside
 * this method. <br> Schedule of each Resource is placed in objects of
 * ResourceInfo class. One ResourceInfo object represents one Resource.
 *
 * @author Dalibor Klusacek
 */
public class Scheduler extends GridSim {

    public static boolean waiting_for_preempted_job_to_arrive = false;
    /**
     * list of Resources
     */
    private LinkedList resList;

    public static String scheduling_algorithm = "";
    
    public static double final_makespan = 0.1;

    /**
     * list of ResourceInfo objects
     */
    public static ArrayList resourceInfoList;
    /**
     * Resource count
     */
    private int totalResource;
    /**
     * list of Resource's IDs
     */
    private int resourceID[];
    /**
     * list of Resource names
     */
    private String resourceName[];
    private int prev_scheduled = 0;
    /**
     * Denotes time required for schedule generation, i.e. time = Sum(clock2 -
     * clok1)
     */
    private double clock = 0.0;
    /**
     * Time before the start of scheduling
     */
    private double clock1 = 0.0;
    /**
     * Time after the end of scheduling
     */
    private double clock2 = 0.0;
    /**
     * Tag signaling that gridlet/job was sent
     */
    private static LinkedList schedQueue = new LinkedList();
    public static LinkedList schedQueue2 = new LinkedList();
    /**
     * incoming job active_scheduling_queue
     */
    public static LinkedList<GridletInfo> active_scheduling_queue = new LinkedList();

    public static LinkedList<GridletInfo> hold_queue = new LinkedList();

    public static LinkedList<GridletInfo> tried_queue = new LinkedList();

    public static LinkedList<GridletInfo> DAG_queue = new LinkedList();
    public static LinkedList<Integer> unfinished_predecessors = new LinkedList();

    public static LinkedList<String> all_queues_names = new LinkedList();

    public static boolean schedule_too_long = false;
    /**
     * incoming job active_scheduling_queue
     */
    public static LinkedList q10 = new LinkedList();
    /**
     * incoming job active_scheduling_queue
     */
    public static LinkedList q5 = new LinkedList();
    /**
     * incoming job active_scheduling_queue
     */
    public static LinkedList q2 = new LinkedList();
    /**
     * incoming job active_scheduling_queue
     */
    public static LinkedList q7 = new LinkedList();
    /**
     * incoming job active_scheduling_queue
     */
    public static LinkedList q8 = new LinkedList();
    /**
     * incoming job active_scheduling_queue
     */
    public static LinkedList q9 = new LinkedList();
    /**
     * incoming job active_scheduling_queue
     */
    public static LinkedList q6 = new LinkedList();
    /**
     * incoming job active_scheduling_queue
     */
    public static LinkedList q11 = new LinkedList();
    /**
     * incoming job active_scheduling_queue
     */
    public static LinkedList q1 = new LinkedList();
    /**
     * incoming job active_scheduling_queue
     */
    public static LinkedList q4 = new LinkedList();
    /**
     * incoming job active_scheduling_queue
     */
    public static LinkedList q3 = new LinkedList();
    /**
     * incoming job active_scheduling_queue
     */
    public static LinkedList short_queue = new LinkedList();
    /**
     * incoming job active_scheduling_queue
     */
    public static LinkedList normal_queue = new LinkedList();
    /**
     * incoming job active_scheduling_queue
     */
    public static LinkedList quark_queue = new LinkedList();
    /**
     * incoming job active_scheduling_queue
     */
    public static LinkedList long_queue = new LinkedList();
    /**
     * incoming job active_scheduling_queue
     */
    public static LinkedList ncbr_queue = new LinkedList();
    /**
     * incoming job active_scheduling_queue
     */
    public static LinkedList iti_queue = new LinkedList();
    /**
     * incoming job active_scheduling_queue
     */
    public static LinkedList cpmd_queue = new LinkedList();
    /**
     * List of all incoming job queues
     */
    public static LinkedList<LinkedList> all_queues = new LinkedList();
    /**
     * denotes active_scheduling_queue/schedule strategy
     */
    public static int global_policy = 1;
    /**
     * denotes used algorithm
     */
    private int algorithm = 1;
    /**
     * denotes active_scheduling_queue/schedule size
     */
    private int waiting_size = 0;
    /**
     * denotes best machine's MIPS rating
     */
    double bestMachine = 0.0;
    /**
     * denotes # of active PEs
     */
    static double activePEs = 0.0;
    /**
     * denotes # of available PEs
     */
    public static double availPEs = 0.0;
    public static double classic_activePEs = 0.0;
    /**
     * denotes # of available PEs
     */
    public static double classic_availPEs = 0.0;
    /**
     * denotes # of PEs requested by currently present jobs
     */
    static double requestedPEs = 0.0;
    /**
     * denotes load of the system
     */
    static double load = 0.0;
    static double classic_load = 0.0;
    /**
     * denotes maximal possible load of the system
     */
    static double max_load = 0.0;
    /**
     * denotes last time event when the load was updated
     */
    static double last_event = 0.0;
    /**
     * denotes start time of the simulation
     */
    static double start_event = -10.0;
    /**
     * denotes restart of simulation - variables will be reset
     */
    private boolean restart = true;
    /**
     * auxiliary variable
     */
    double wait = 0.0;
    /**
     * auxiliary variable
     */
    static Random perm_rnd = new Random(1024768);
    /**
     * auxiliary variable
     */
    Input r = new Input();
    /**
     * auxiliary variable
     */
    String folder_prefix = "";
    /**
     * auxiliary variable
     */
    LinkedList<String> cpus = new LinkedList();
    /**
     * auxiliary variable
     */
    Output out = new Output();
    /**
     * auxiliary variable
     */
    int current_gl = 0;
    /**
     * auxiliary variable
     */
    double date1 = 0.0;
    /**
     * auxiliary variable
     */
    double date2 = 0.0;
    /**
     * auxiliary variable
     */
    int priority = 1;
    /**
     * auxiliary variable
     */
    private int received;

    private int started;
    /**
     * auxiliary variable
     */
    public static double runtime = 0.0;
    /**
     * auxiliary variable
     */
    private int in_job_counter = 0;
    /**
     * auxiliary variable
     */
    int day_count = 0;
    /**
     * auxiliary variable
     */
    int hour_count = 0;
    /**
     * auxiliary variable
     */
    public static Random rand = new Random(1223);
    /**
     * auxiliary variable
     */
    public static String data_set = "";
    /**
     * auxiliary variable
     */
    boolean end_of_submission = false;
    /**
     * auxiliary variable
     */
    String suff = "";
    /**
     * auxiliary variable
     */
    boolean repeated = false;
    /**
     * auxiliary variable
     */
    static LinkedList<String> avail_properties = new LinkedList();
    /**
     * auxiliary variable
     */
    public int succ_m = 0;
    /**
     * auxiliary variable
     */
    int bad = 0;
    /**
     * auxiliary variable
     */
    static double failure_time = 0.0;
    /**
     * auxiliary variable
     */
    double wavail_time = 0.0;
    /**
     * auxiliary variable
     */
    static double wfailure_time = 0.0;
    /**
     * auxiliary variable
     */
    double av_PEs = 0.0;
    /**
     * auxiliary variable
     */
    double wav_PEs = 0.0;
    /**
     * auxiliary variable
     */
    double day_usage = 0.0;
    /**
     * auxiliary variable
     */
    double week_usage = 0.0;
    /**
     * auxiliary variable
     */
    int week_count = 0;
    /**
     * auxiliary variable
     */
    public static LinkedList<String> users = new LinkedList();
    /**
     * auxiliary variable
     */
    public static LinkedList<Double> users_CPUtime = new LinkedList();
    public static LinkedList<Double> users_MAX = new LinkedList();
    public static LinkedList<Double> users_SQRT = new LinkedList();
    public static LinkedList<Double> final_users_CPUtime = new LinkedList();
    /**
     * auxiliary variable
     */
    public static LinkedList<Double> users_multiplications = new LinkedList();
    /**
     * auxiliary variable
     */
    public static LinkedList<Double> users_length = new LinkedList();
    /**
     * auxiliary variable
     */
    public static LinkedList<Double> users_last_length = new LinkedList();
    /**
     * auxiliary variable
     */
    public static LinkedList<Double> users_P_length = new LinkedList();
    /**
     * auxiliary variable
     */
    public static LinkedList<Double> users_percentage_length = new LinkedList();
    /**
     * auxiliary variable
     */
    public static LinkedList<Double> total_uwt = new LinkedList();
    public static LinkedList<Double> final_total_uwt = new LinkedList();
    /**
     * auxiliary variable
     */
    public static LinkedList<Double> total_uram = new LinkedList();
    public static LinkedList<Double> final_total_uram = new LinkedList();
    /**
     * auxiliary variable
     */
    public static LinkedList<Integer> users_jobs = new LinkedList();
    /**
     * auxiliary variable
     */
    public static LinkedList<Integer> users_P_jobs = new LinkedList();
    /**
     * auxiliary variable
     */
    Visualizator anim = null;
    /**
     * auxiliary variable
     */
    LinkedList<Visualizator> windows = null;
    /**
     * auxiliary variable
     */
    LinkedList<Integer> days = new LinkedList();
    /**
     * auxiliary variable
     */
    LinkedList<Double> util = new LinkedList();
    /**
     * auxiliary variable
     */
    LinkedList<Double> cl_util = new LinkedList();
    /**
     * auxiliary variable
     */
    LinkedList<Double> cl_status = new LinkedList();
    /**
     * auxiliary variable
     */
    LinkedList<Double> hour_cl_util = new LinkedList();
    /**
     * auxiliary variable
     */
    public LinkedList<String> cl_names = new LinkedList();
    /**
     * auxiliary variable
     */
    public LinkedList<Integer> cl_CPUs = new LinkedList();
    /**
     * auxiliary variable
     */
    LinkedList<Integer> running = new LinkedList();
    /**
     * auxiliary variable
     */
    LinkedList<Integer> waiting = new LinkedList();
    /**
     * auxiliary variable
     */
    LinkedList<Integer> requested = new LinkedList();
    /**
     * auxiliary variable
     */
    LinkedList<Integer> used = new LinkedList();
    /**
     * auxiliary variable
     */
    LinkedList<Double> day_util = new LinkedList();
    /**
     * auxiliary variable
     */
    LinkedList<Integer> hours = new LinkedList();
    /**
     * auxiliary variable
     */
    int reqc = 0;
    /**
     * auxiliary variable
     */
    public static int busyc = 0;
    /**
     * auxiliary variable
     */
    int availCPUS = 0;
    /**
     * auxiliary variable
     */
    LinkedList<Integer> availCPUs = new LinkedList();
    /**
     * auxiliary variable
     */
    long tt = new Date().getTime();
    /**
     * determines whether graphical output will be generated
     */
    boolean visualize = false;
    /**
     * auxiliary variable
     */
    ResultCollector rc = null;
    /**
     * auxiliary variable
     */
    int total_machines = 0;
    /**
     * auxiliary variable
     */
    boolean failures;
    /**
     * auxiliary variable
     */
    int submitted = 0;
    /**
     * auxiliary variable
     */
    int maxPE = 0;
    /**
     * auxiliary variable
     */
    int event_opt = 0;
    int last_job_id = 0;
    public static long start_date = -1;
    static TimeSeries series_system_usage;
    static TimeSeries series_alloc_usage;

    /**
     * auxiliary variable
     */
    LinkedList<GridletInfo> job_queue = new LinkedList();

    /**
     * Creates a new instance of Scheduler - there is only one in whole
     * simulation.
     *
     * @param name name of the scheduler
     * @param baudRate bandwith speed
     * @param totalJSS number of JobLoaders communicating with this Scheduler
     * @param results results of this simulation that will be printed (in
     * ExperimentSetup class)
     * @param algorithm selects active_scheduling_queue/schedule based algorithm
     * (FCFS, EDF, BackFilling, Tabu Search...)
     * @param data_set name of the data set file
     * @param total_gridlet number of gridlets to be executed
     * @param suff name of the scheduling algorithm
     * @param windows list of references on the GUI windows
     * @param rc reference to a result collector instance
     * @param rnd random number generator seed
     *
     */
    public Scheduler(String name, double baudRate, int totalJSS, LinkedList results, int algorithm, String data_set,
            String suff, LinkedList windows, ResultCollector rc, int rnd) throws Exception {
        super(name, baudRate);

        folder_prefix = System.getProperty("user.dir");

        this.failures = ExperimentSetup.failures;

        this.windows = windows;
        if (windows.size() > 0) {
            visualize = true;
        }
        this.perm_rnd = new Random(1223 + rnd + 1);
        this.rand = new Random(1024768 + rnd + 1);
        this.algorithm = algorithm;
        this.data_set = data_set;
        this.suff = suff;
        this.repeated = false;

        this.current_gl = 0;

        date1 = new Date().getTime();

        this.rc = rc;

        // reset user and job related data before new experiment starts
        users.clear();
        users_CPUtime.clear();
        users_MAX.clear();
        users_SQRT.clear();
        final_users_CPUtime.clear();
        users_multiplications.clear();
        total_uwt.clear();
        final_total_uwt.clear();
        total_uram.clear();
        final_total_uram.clear();
        users_length.clear();
        users_last_length.clear();
        users_P_length.clear();
        users_percentage_length.clear();
        users_jobs.clear();
        users_P_jobs.clear();
        series_system_usage = new TimeSeries("System Usage %");
        series_alloc_usage = new TimeSeries("System Allocated %");
    }

    /**
     * The body() method communicates with other entities and directs the whole
     * Scheduler's behavior.<p>
     * This method receives the gridlets from JobLoader. When new gridlet arrive
     * FCFS, EDF, BackFilling, etc. create the active_scheduling_queue/schedule.
     * When some gridlets are finished they are received. This is a signal for
     * the scheduler to try to schedule waiting gridlets according to prepared
     * active_scheduling_queue/schedule (gridlet is finished = resource load has
     * lowered).
     */
    public void body() {

        send(this.get_id(), 10.0, AleaSimTags.RESOURCE_INIT_WAIT);

        // Accept events until the simulation is finished
        while (!end_of_submission || received < in_job_counter) {

            Sim_event ev = new Sim_event();
            sim_get_next(ev);

            if (ev.get_tag() == AleaSimTags.RESOURCE_INIT_WAIT) {
                System.out.println("Start Scheduler at: " + GridSim.clock() + " sending RESOURCE_INIT_DONE with a delay of 2 seconds.");
                super.sim_schedule(GridSim.getEntityId(this.getEntityName()), 2.0, AleaSimTags.RESOURCE_INIT_DONE);

                resList = super.getGridResourceList();

                resourceInfoList = new ArrayList();

                //System.out.println("Current time: " + GridSim.clock());
                totalResource = resList.size();
                System.out.println("GridResource/Cluster count: " + totalResource);
                resourceID = new int[totalResource];
                resourceName = new String[totalResource];
                // a loop to get all the resources available
                int i = 0;
                availPEs = 0.0;
                classic_availPEs = 0.0;

                for (i = 0; i < totalResource; i++) {
                    //System.out.println(i + ": Current time: " + GridSim.clock());
                    // Resource list contains list of resource IDs
                    resourceID[i] = ((Integer) resList.get(i)).intValue();
                    // get their names as well
                    resourceName[i] = GridSim.getEntityName(resourceID[i]);
                    int res_id = ((Integer) resList.get(i)).intValue();
                    // Get Resource Characteristic Info
                    //ComplexResourceCharacteristics res = (ComplexResourceCharacteristics) super.getResourceCharacteristics(res_id);
                    Sim_entity entity = eduni.simjava.Sim_system.get_entity(res_id);

// 2. Cast it to a GridResource (assuming your resources are GridResources)
                    if (entity instanceof GridResource) {
                        GridResource gridRes = (GridResource) entity;

                        // 3. Get the characteristics directly from the object field
                        ResourceCharacteristics rawChar = gridRes.getResourceCharacteristics();

                        // 4. Cast to your specific type
                        ComplexResourceCharacteristics res = (ComplexResourceCharacteristics) rawChar;

                        // Continue as normal...
                        ResourceInfo ri = new ResourceInfo(res);
                        
                        // increase number of available PEs
                        availPEs += ri.resource.getNumPE() * ri.resource.getMIPSRatingOfOnePE();
                        classic_availPEs += ri.resource.getNumPE();
                        availCPUS += ri.resource.getNumPE();
                        if (ri.resource.getMIPSRatingOfOnePE() > maxPE) {
                            maxPE = ri.resource.getMIPSRatingOfOnePE();
                        }
                        // store machines according CPU count and their performance
                        if (resourceInfoList.size() > 0) {
                            for (int j = 0; j < resourceInfoList.size(); j++) {
                                ResourceInfo rj = (ResourceInfo) resourceInfoList.get(j);
                                if (ri.resource.getNumPE() >= rj.resource.getNumPE()) {
                                    if (ri.resource.getNumPE() == rj.resource.getNumPE() && ri.resource.getMIPSRatingOfOnePE() > rj.resource.getMIPSRatingOfOnePE()) {
                                        resourceInfoList.add(j, ri);
                                        cl_names.add(j, ri.resource.getResourceName());
                                        cl_CPUs.add(j, ri.resource.getNumPE());
                                        break;
                                    }
                                    if (ri.resource.getNumPE() > rj.resource.getNumPE()) {
                                        resourceInfoList.add(j, ri);
                                        cl_names.add(j, ri.resource.getResourceName());
                                        cl_CPUs.add(j, ri.resource.getNumPE());
                                        break;
                                    }
                                }
                                if (j == resourceInfoList.size() - 1) {
                                    resourceInfoList.add(ri);
                                    cl_names.add(ri.resource.getResourceName());
                                    cl_CPUs.add(ri.resource.getNumPE());
                                    break;
                                }
                            }
                        } else {
                            resourceInfoList.add(ri);
                            cl_names.add(ri.resource.getResourceName());
                            cl_CPUs.add(ri.resource.getNumPE());
                        }
                        //System.out.println(i + ": end Current time: " + GridSim.clock());
                    }
                }
                ResourceInfo best = (ResourceInfo) resourceInfoList.get(0);
                bestMachine = best.resource.getMIPSRatingOfOnePE();
                //System.out.println("Current time: " + GridSim.clock());

                //System.out.println(perm_rnd.nextDouble() + " next");
                System.out.println("=======================================================================================================");
                System.out.println("List of resources:");
                for (i = 0; i < resourceInfoList.size(); i++) {
                    ResourceInfo ri = (ResourceInfo) resourceInfoList.get(i);
                    total_machines += ri.resource.getNumMachines();
                    System.out.println("id = " + ri.resource.getResourceID() + ", name = " + ri.resource.getResourceName() + ", CPUs = " + ri.resource.getNumPE() + ", CPU rating = "
                            + ri.resource.getMIPSRatingOfOnePE() + ", nodes = " + ri.resource.getNumMachines() + ", props = " + ri.resource.getProperties() + ", RAM = " + (ri.resource.getRamOnOneMachine() / (1024.0 * 1024)) + " GB per node, GPUs = " + ri.getNumAvailGpus());
                }
                System.out.println("Total available MIPS power = " + availPEs + " MIPS in " + classic_availPEs + " CPUs, total machines = " + total_machines);
                System.out.println("=======================================================================================================");
                System.out.println("");
                //System.out.println("Current time: " + GridSim.clock());
                wav_PEs = availPEs;
                av_PEs = classic_availPEs;
                continue;
            }

            if (ev.get_tag() == AleaSimTags.RESOURCE_INIT_DONE) {
                System.out.println("RESOURCE_INIT_DONE Finished at t=" + GridSim.clock());
                // start periodical logging of results and visualization
                if (visualize) {
                    super.sim_schedule(this.getEntityId(this.getEntityName()), 0.0, AleaSimTags.SCHEDULER_COLLECT);
                }
                // start periodical optimization of schedule
                super.sim_schedule(GridSim.getEntityId(this.getEntityName()), 300.0, AleaSimTags.EVENT_OPTIMIZE);
                // start the FailureLoader
                if (failures) {
                    super.sim_schedule(this.getEntityId(data_set + "_FailureLoader"), 0.0, AleaSimTags.EVENT_WAKE);
                }
                // start the JobLoader
                System.out.println("Wake JOB LOADER up event at time: " + GridSim.clock());
                super.sim_schedule(this.getEntityId(data_set + "_JobLoader"), 5.0, AleaSimTags.EVENT_WAKE);

                // periodic logging of current throughput (# of jobs completed so far)
                super.sim_schedule(this.getEntityId(this.getEntityName()), 0.0, AleaSimTags.SCHEDULER_PRINT_THROUGHPUT);
                //super.sim_schedule(this.getEntityId(this.getEntityName()), 0.0, AleaSimTags.SCHEDULER_PRINT_SCHEDULE);

                // fairshare algorithm
                if (ExperimentSetup.use_fairshare) {
                    super.sim_schedule(this.getEntityId(this.getEntityName()), 0.0, AleaSimTags.FAIRSHARE_UPDATE);
                }

                // periodic decrease of old fairshare weights
                if (ExperimentSetup.use_decay && ExperimentSetup.use_fairshare) {
                    super.sim_schedule(this.getEntityId(this.getEntityName()), 0.0, AleaSimTags.FAIRSHARE_WEIGHT_DECAY);
                }
                continue;
            }

            if (ev.get_tag() == AleaSimTags.SCHEDULER_PRINT_FIRST_JOB_IN_QUEUE) {
                if (this.all_queues.getFirst().size() > 0) {
                    GridletInfo gf = (GridletInfo) (this.all_queues.getFirst()).getFirst();
                    double wait = Math.round(clock() - gf.getRelease_date()) / 3600.0;
                    if (wait > 24) {
                        System.out.println("-------------------------------------------");
                        System.out.println("Long queue waiting: first job " + gf.getID() + " in queue waits for " + wait + " hours. Requires " + gf.getNumPE() + " CPUs and properties = " + gf.getProperties() + "");
                        for (int r = 0; r < resourceInfoList.size(); r++) {
                            ResourceInfo rri = (ResourceInfo) resourceInfoList.get(r);
                            System.out.println(rri.resource.getResourceName() + " has now " + rri.getNumFreePE() + " free CPUs.");
                        }
                        System.out.println("-------------------------------------------");
                    }
                }
                super.sim_schedule(this.getEntityId(this.getEntityName()), (24 * 3600.0), AleaSimTags.SCHEDULER_PRINT_FIRST_JOB_IN_QUEUE);
                continue;
            }

            if (ev.get_tag() == AleaSimTags.SCHEDULER_PRINT_THROUGHPUT) {
                if (Scheduler.start_date > -1) {
                    if (!ExperimentSetup.use_fairshare) {
                        ExperimentSetup.result_collector.recordUserUsage(clock(), ExperimentSetup.users);
                    }
                    ExperimentSetup.result_collector.recordClusterUsage(clock());

                }
                super.sim_schedule(this.getEntityId(this.getEntityName()), (ExperimentSetup.sample_tick), AleaSimTags.SCHEDULER_PRINT_THROUGHPUT);
                continue;
            }

            if (ev.get_tag() == AleaSimTags.FAIRSHARE_UPDATE) {
                if (Scheduler.start_date > -1) {
                    resetTemporaryUsageAndUpdateFF();
                    this.updateFairShare();
                    //ExperimentSetup.result_collector.recordSystemThroughput(clock(), ExperimentSetup.users); //received
                    //ExperimentSetup.result_collector.recordFairshareFactor(clock(), ExperimentSetup.users);
                    ExperimentSetup.result_collector.recordUserUsage(clock(), ExperimentSetup.users);
                }
                super.sim_schedule(this.getEntityId(this.getEntityName()), ExperimentSetup.fairshare_update_interval, AleaSimTags.FAIRSHARE_UPDATE);
                continue;
            }

            if (ev.get_tag() == AleaSimTags.SCHEDULER_PRINT_SCHEDULE) {
                String dated = new java.text.SimpleDateFormat("kk:mm dd-MM-yyyy").format(new java.util.Date(Math.round(clock()) * 1000));
                System.out.println("PRINTING schedule at simulation time: " + dated + "-------------------------");
                for (int i = 0; i < resourceInfoList.size(); i++) {
                    ResourceInfo ri = (ResourceInfo) resourceInfoList.get(i);
                    ri.printSchedule();
                }
                System.out.println("End of schedule at simulation time: " + dated + "--//-----------------------");
                super.sim_schedule(this.getEntityId(this.getEntityName()), (3600.0 * 6), AleaSimTags.SCHEDULER_PRINT_SCHEDULE);
                continue;
            }

            if (ev.get_tag() == AleaSimTags.FAIRSHARE_WEIGHT_DECAY && ExperimentSetup.use_fairshare) {
                applyDecay();
                super.sim_schedule(this.getEntityId(this.getEntityName()), (ExperimentSetup.decay_interval * 3600.0), AleaSimTags.FAIRSHARE_WEIGHT_DECAY);
                continue;
            }

            if (ev.get_tag() == AleaSimTags.LOG_SCHEDULER) {
                String idd = "";
                if (active_scheduling_queue.size() > 0) {
                    idd += active_scheduling_queue.getLast().getID();
                }
                //System.out.println(">>> " + in_job_counter + " so far arrived, in active_scheduling_queue = " + getQueueSize() + " jobs, at time = " + Math.round(clock())+" running = "+getRunningJobs()+" FREE = "+getFreeCPUs()+" last jobID = "+idd);
                System.out.println(in_job_counter + " arrived, waiting = " + getScheduleSize() + " simtime = " + Math.round(clock()) + " running = " + getRunningJobs() + " FREE = " + getFreeCPUs() + " last jobID = " + last_job_id);
                super.sim_schedule(this.getEntityId(this.getEntityName()), (3 * 3600.0), AleaSimTags.LOG_SCHEDULER);
                continue;
            }

            // if periodic optimization is used, select proper algorithm according to 'algorithm' parameter
            if (ev.get_tag() == AleaSimTags.EVENT_OPTIMIZE) {
                if (ExperimentSetup.opt_alg != null) {
                    // select number of iteration of TS
                    int iteration = this.getScheduleSize() * 2;
                    Date dd = new Date();
                    clock1 = dd.getTime();
                    ExperimentSetup.opt_alg.execute(iteration, ExperimentSetup.time_limit);
                    Date dd2 = new Date();
                    clock2 = dd2.getTime();
                    clock += clock2 - clock1;
                    // send periodical event that will arrive in 300s, i.e., in 5 minutes
                    super.sim_schedule(this.getEntityId(this.getEntityName()), 300.0, AleaSimTags.EVENT_OPTIMIZE);
                }
                super.sim_schedule(this.getEntityId(this.getEntityName()), 0.0, AleaSimTags.EVENT_SCHEDULE);
                continue;
            }

            // on-demand optimization when early job completion is detected
            if (ev.get_tag() == AleaSimTags.SCHEDULER_OPTIMIZE_ONDEMAND && ExperimentSetup.fix_alg != null) {
                // select number of iteration of LS
                int iteration = this.getScheduleSize() * 2;
                Date dd = new Date();
                clock1 = dd.getTime();
                ExperimentSetup.fix_alg.execute(iteration, ExperimentSetup.on_demand_time_limit);
                Date dd2 = new Date();
                clock2 = dd2.getTime();
                clock += clock2 - clock1;
                super.sim_schedule(this.getEntityId(this.getEntityName()), 0.0, AleaSimTags.EVENT_SCHEDULE);
                continue;
            }
            // JobLoader sended all gridlets
            if (ev.get_tag() == AleaSimTags.SUBMISSION_DONE) {
                end_of_submission = true;
                this.submitted = (Integer) ev.get_data();
                System.out.println("End of new job submissions from JOB LOADER recorded by scheduler... " + in_job_counter + " jobs arrived. So far received " + received + " jobs. Submitted jobs = " + submitted);
                continue;
            }

            // periodical result collection - will be replaced by simple ResultCollector calling.
            if (ev.get_tag() == AleaSimTags.SCHEDULER_COLLECT) {
                collectPeriodicalResults();
                // send periodical event that will arrive in 3600s, i.e., in one hour
                super.sim_schedule(this.getEntityId(this.getEntityName()), 3600.0, AleaSimTags.SCHEDULER_COLLECT);
                continue;
            }

            // ack from policy delivered
            if (ev.get_tag() == AleaSimTags.GRIDLET_STARTED) {
                Gridlet gl = (Gridlet) ev.get_data();

                ComplexGridlet cgl = (ComplexGridlet) gl;
                if (cgl.getOnJobStart() != null) {
                    super.sim_schedule(this.getEntityId(cgl.getOnJobStart()), 0, AleaSimTags.AGENT_ONJOBSTART, cgl);
                }

                waiting_size--;
                prev_scheduled--;
                //System.out.println(gl.getGridletID()+ " gridlet started ack. Prevscheduled = "+prev_scheduled);
                // do another scheduling round
                if (prev_scheduled == 0) {
                    scheduleGridlets();
                }

                continue;
            }

            // Gridlet was sent, do another scheduling round if possible
            if (ev.get_tag() == AleaSimTags.GRIDLET_SENT) {

                GridletInfo gi = (GridletInfo) ev.get_data();

                waiting_size--;
                //System.out.println(gi.getID()+ " gridlet WAS SENT - error . Prevscheduled = "+prev_scheduled);
                prev_scheduled--;

                // do another scheduling round
                if (prev_scheduled == 0) {
                    scheduleGridlets();
                }
                continue;
            }
            if (ev.get_tag() == AleaSimTags.EVENT_SCHEDULE) {
                /*if (ev.get_data() != null) {
                    System.out.println(GridSim.clock() + ": SCHEDULER: scheduling due to: " + ev.get_data()+" prev_sch:"+prev_scheduled+" waiting:"+waiting_for_preempted_job_to_arrive);
                }*/

                // do another scheduling round
                if (prev_scheduled == 0) {
                    scheduleGridlets();
                }

                // periodic scheduling attempt (PBS verif only)
                //super.sim_schedule(this.getEntityId(this.getEntityName()), (3600), AleaSimTags.EVENT_SCHEDULE);
                continue;
            }
            // Failure appeared
            if (ev.get_tag() == AleaSimTags.FAILURE_START) {
                String data = (String) ev.get_data();
                String[] d = data.split("x");
                int resId = Integer.parseInt(d[0]);
                int killed_cpus = Integer.parseInt(d[1]);
                updateResourceInfoAfterFailureOrRestart(resId);
                System.out.println(Math.round(clock()) + ": Failure of: " + super.getEntityName(resId) + ", running " + printRunningPEsOnResource(resId) + " CPUs, killed = " + killed_cpus + " resID = " + resId);
                continue;
            }
            // Restart appeared
            if (ev.get_tag() == AleaSimTags.FAILURE_RESTART) {
                int resId = (Integer) ev.get_data();
                updateResourceInfoAfterFailureOrRestart(resId);
                System.out.println(Math.round(clock()) + ": Restart of: " + super.getEntityName(resId) + ", running " + printRunningPEsOnResource(resId) + " CPUs,  resID = " + resId);
                continue;
            }
            // gridlet was finished. Get it, record results and do another scheduling run
            if (ev.get_tag() == GridSimTags.GRIDLET_RETURN) {
                ComplexGridlet gridlet_received = (ComplexGridlet) ev.get_data();
                //System.out.println("Gridlet: " + gridlet_received.getGridletID() + " returned to Scheduler, status: " + gridlet_received.getGridletStatusString() + " at time: " + GridSim.clock());
                if (gridlet_received.getGridletStatus() == Gridlet.PAUSED) {
                    this.waiting_for_preempted_job_to_arrive = false;
                }
                // remove job from schedule on resource
                for (int j = 0; j < resourceInfoList.size(); j++) {
                    ResourceInfo ri = (ResourceInfo) resourceInfoList.get(j);
                    if (gridlet_received.getResourceID() == ri.resource.getResourceID()) {
                        // we lower the load of resource, update info about overall tardiness and exit cycle

                        ri.lowerResInExec(gridlet_received.getGridletID(), gridlet_received.getUserID());
                        double g_tard = Math.max(0, gridlet_received.getFinishTime() - gridlet_received.getDue_date());
                        ri.prev_tard += g_tard;
                        if (g_tard <= 0.0) {
                            ri.prev_score++;
                        }
                        break;
                    }
                }

                //System.out.println("[SCHEDULER] Job " + gridlet_received.getGridletID() + " from " + gridlet_received.getArchRequired() + " returned completed at sim. time = " + GridSim.clock());
                Date dd = new Date();
                clock1 = dd.getTime();

                // update and possibly reschedule jobs that were waiting for their predecessors to be finished first
                if (!DAG_queue.isEmpty()) {
                    boolean returned = updateDAGqueueUponJobComplete(gridlet_received);
                    if (returned && prev_scheduled == 0) {
                        super.sim_schedule(this.getEntityId(this.getEntityName()), 0.0, AleaSimTags.EVENT_SCHEDULE);
                    }
                }

                // return jobs that were previously unable to run to the normal active_scheduling_queue
                if (tried_queue.size() > 0) {
                    int return_counter = 0;
                    while (tried_queue.size() > 0 && return_counter < 100) {
                        GridletInfo gi = tried_queue.removeFirst();
                        //System.out.println(gi.getID() + ": is returned to the normal active_scheduling_queue.");
                        return_counter++;
                        ExperimentSetup.policy.addNewJob(gi);
                    }
                    if (prev_scheduled == 0) {
                        super.sim_schedule(this.getEntityId(this.getEntityName()), 0.0, AleaSimTags.EVENT_SCHEDULE);
                        //scheduleGridlets();
                    }
                }

                if (hold_queue.size() > 0 && ExperimentSetup.limit_schedule_size) {
                    //System.out.println("hold active_scheduling_queue, job end: "+GridSim.clock());
                    updateResourceInfos(clock(), "hold queue");
                    //(getScheduleSize() > ExperimentSetup.max_schedule_size || (classic_availPEs * ExperimentSetup.max_schedule_CPU_request_factor) < getScheduleCPUSize())
                    while (hold_queue.size() > 0 && ((classic_availPEs * ExperimentSetup.max_schedule_CPU_request_factor) > getScheduleCPUSize() && !schedule_too_long)) {
                        GridletInfo gi = hold_queue.removeFirst();
                        //System.out.println(gi.getID() + ": is returned. Requested " + getScheduleCPUSize() + " CPUs by " + getScheduleSize() + " jobs in schedule.");
                        ExperimentSetup.policy.addNewJob(gi);
                        if (prev_scheduled == 0) {
                            super.sim_schedule(this.getEntityId(this.getEntityName()), 0.0, AleaSimTags.EVENT_SCHEDULE);
                            //scheduleGridlets();
                        }
                    }
                }
                Date dd2 = new Date();
                clock2 = dd2.getTime();
                clock += clock2 - clock1;

                //System.out.println(gridlet_received.getGridletID() + " finished at: " + GridSim.clock());
                boolean optimize = false;

                if (ExperimentSetup.use_multiple_queues && gridlet_received.getGridletStatus() != Gridlet.CANCELED) {
                    ExperimentSetup.queues.get(gridlet_received.getQueue()).setQueue_used_CPUs(ExperimentSetup.queues.get(gridlet_received.getQueue()).getQueue_used_CPUs() - gridlet_received.getNumPE());
                }

                if (ExperimentSetup.use_user_groups && gridlet_received.getGridletStatus() != Gridlet.CANCELED) {
                    ExperimentSetup.groups.get(gridlet_received.getGroupID()).setUsedQuota(ExperimentSetup.groups.get(gridlet_received.getGroupID()).getUsedQuota() - gridlet_received.getNumPE());
                    ExperimentSetup.users.get(gridlet_received.getUser()).setUsed_quota(ExperimentSetup.users.get(gridlet_received.getUser()).getUsed_quota() - gridlet_received.getNumPE());
                }

                if (ExperimentSetup.use_user_groups && ExperimentSetup.groups.get(gridlet_received.getGroupID()).getFreeQuota() > 0 && ExperimentSetup.groups.get(gridlet_received.getGroupID()).getHeldJobsCount() > 0) {
                    int limit = ExperimentSetup.groups.get(gridlet_received.getGroupID()).getFreeQuota();
                    while (ExperimentSetup.groups.get(gridlet_received.getGroupID()).getHeldJobs().size() > 0 && limit > 0) {
                        GridletInfo gr = ExperimentSetup.groups.get(gridlet_received.getGroupID()).getHeldJobs().removeFirst();
                        ExperimentSetup.groups.get(gridlet_received.getGroupID()).setHeldJobsCount(ExperimentSetup.groups.get(gridlet_received.getGroupID()).getHeldJobsCount() - 1);
                        hold_queue.remove(gr);
                        //System.out.println("RETURN job "+gr.getID()+" group "+ExperimentSetup.groups.get(gridlet_received.getGroup()).getName()+" free= "+ExperimentSetup.groups.get(gridlet_received.getGroup()).getFreeQuota());
                        limit = limit - gr.getNumPE();
                        ExperimentSetup.policy.addNewJob(gr);

                    }
                    if (prev_scheduled == 0) {
                        super.sim_schedule(this.getEntityId(this.getEntityName()), 0.0, AleaSimTags.EVENT_SCHEDULE, "Group");
                        //scheduleGridlets();
                    }
                }

                //rc.addFinishedJobToResults(gridlet_received, resourceInfoList);
                reqc -= gridlet_received.getNumPE();
                received++;
                double cpu_time = 0.0;
                if (gridlet_received.getGridletStatus() == Gridlet.FAILED_RESOURCE_UNAVAILABLE || gridlet_received.getGridletStatus() == Gridlet.FAILED) {
                    cpu_time = Math.max(1.0, gridlet_received.getActualCPUTime());
                    if (gridlet_received.getOnJobFail() != null) {
                        super.sim_schedule(this.getEntityId(gridlet_received.getOnJobFail()), 60, AleaSimTags.AGENT_ONJOBFAIL, gridlet_received);
                    }
                } else if (gridlet_received.getGridletStatus() == Gridlet.CANCELED) {
                    cpu_time = 0.0;
                    if (gridlet_received.getOnJobFail() != null) {
                        super.sim_schedule(this.getEntityId(gridlet_received.getOnJobFail()), 60, AleaSimTags.AGENT_ONJOBFAIL, gridlet_received);
                    }
                } else {

                    if (gridlet_received.getOnJobCompl() != null) {
                        // notify agent of this job that the job was completed
                        super.sim_schedule(this.getEntityId(gridlet_received.getOnJobCompl()), 60, AleaSimTags.AGENT_ONJOBCOMPL, gridlet_received);
                    }

                    cpu_time = gridlet_received.getActualCPUTime();
                    if (ExperimentSetup.fix_alg != null) {
                        if (gridlet_received.getExpectedFinishTime() > gridlet_received.getFinishTime() && gridlet_received.getFinishTime() >= 0) {
                            double diff = gridlet_received.getExpectedFinishTime() - gridlet_received.getFinishTime();
                            //System.out.println("shorter than expected");
                            // job finished earlier than expected - do optimization of schedule if the gap is large enough
                            updateResourceInfos(clock(), "shorter than expected");
                            if (diff > ExperimentSetup.gap_length) {
                                //System.out.println("SHORTER actual time hole...");
                                optimize = true;
                            }
                        }
                        if (!optimize && ExperimentSetup.use_LastLength && gridlet_received.getExpectedFinishTime() < gridlet_received.getFinishTime() && gridlet_received.getFinishTime() >= 0) {
                            double diff = gridlet_received.getFinishTime() - gridlet_received.getExpectedFinishTime();
                            // job finished later than expected - do optimization of schedule if the gap is large enough
                            //System.out.println("longer");
                            updateResourceInfos(clock(), "longer");
                            if (diff > ExperimentSetup.gap_length) {
                                //System.out.println("Later by: "+Math.round(diff)+" seconds.");
                                optimize = true;
                            }
                        }
                    }

                    if (ExperimentSetup.use_compresion && ExperimentSetup.estimates) {
                        if (gridlet_received.getExpectedFinishTime() > gridlet_received.getFinishTime() && gridlet_received.getFinishTime() >= 0) {
                            double diff = gridlet_received.getExpectedFinishTime() - gridlet_received.getFinishTime();
                            // job finished earlier than expected - do compression of schedule
                            if (diff > ExperimentSetup.gap_length) {
                                int id = gridlet_received.getResourceID();
                                if (ExperimentSetup.fast_schedule_compression) {
                                    compressScheduleFast(id);
                                } else {
                                    compressSchedule(id);
                                }
                            }
                        }
                    }

                }

                // update job's allocations when previous job finished earlier (avoid CPU-mapping mismatch)
                if (!ExperimentSetup.use_compresion && ExperimentSetup.estimates) {
                    if (gridlet_received.getExpectedFinishTime() > gridlet_received.getFinishTime() && gridlet_received.getFinishTime() >= 0) {
                        double diff = gridlet_received.getExpectedFinishTime() - gridlet_received.getFinishTime();
                        // job finished earlier than expected - update job's CPU allocations in schedule
                        if (diff > 0.0) {
                            int id = gridlet_received.getResourceID();
                            //System.out.println(GridSim.clock() + ": gi:" + gridlet_received.getGridletID() + ": Updating schedule due to earlier completion. "
                            //       + "Diff: " + diff + " estL:" + gridlet_received.getEstimatedLength() + " actF:" + gridlet_received.getFinishTime() + " estF:" + gridlet_received.getExpectedFinishTime());

                            //System.out.println(GridSim.clock() + ": completes: " + gridlet_received.getGridletID() + " EARLIER than exp. | releasing: " + gridlet_received.getPlannedPEsString());
                            updateScheduleOnRes(id);

                        }
                    }
                }

                rc.addFinishedJobToResults(gridlet_received, resourceInfoList);

                // update of user's resource consuption
                updateLengthStatistics(gridlet_received, cpu_time);

                User u = ExperimentSetup.users.get(gridlet_received.getUser());
                u.getRunning_jobs().remove(gridlet_received);
                long job_wait = Math.round(Math.max(0.0, gridlet_received.getExecStartTime() - gridlet_received.getArrival_time()));
                u.getWait_times().addLast(job_wait);
                u.getArrival_wait_map().put(Math.round(gridlet_received.getArrival_time()), job_wait);

                if (ExperimentSetup.use_fairshare) {
                    //update fairshare factor
                    updateUsageAndFFafterCompletion(gridlet_received);

                }

                if (received % 10 == 0) {
                    /*if ((algorithm >= 9 && algorithm != 12) || algorithm == 4) {
                        System.out.println("<<< " + received + " so far completed, in schedule = " + getScheduleSize() + " jobs, at time = " + Math.round(clock()) + " running = " + getRunningJobs() + " jobs free CPUs = " + getFreeCPUs());
                    } else {*/
                    //String dated = new java.text.SimpleDateFormat("dd-MM-yyyy").format(new java.util.Date(Math.round(clock()) * 1000));
                    String dated = new java.text.SimpleDateFormat("dd-MM-yyyy [HH:mm]").format(new java.util.Date(Math.round(clock() + Scheduler.start_date) * 1000));

                    System.out.println("<<< " + received + " completed so far, in queue/schedule " + getQueueSize() + " jobs, requiring " + getQueueCPUSize()
                            + " CPUs, held jobs " + hold_queue.size() + " running " + getRunningJobs()
                            + " jobs, free CPUs " + getFreeCPUs() + ", free RAM " + Math.round(getFreeRAM() / (1024.0 * 1024)) + " GB, free GPUs " + getFreeGPUs() + ", "
                            + "tried jobs " + tried_queue.size() + ", Day: " + dated + " SimClock: " + Math.round(clock()));

                }

                if (received % 5000 == 0) {
                    // time to time collect the garbage
                    System.gc();
                }

                // optimize schedule if necessary
                if (optimize && ExperimentSetup.fix_alg != null && getScheduleSize() > 0 && ExperimentSetup.useEventOpt) {
                    // use LS
                    event_opt++;
                    super.sim_schedule(this.getEntityId(this.getEntityName()), 0.0, AleaSimTags.SCHEDULER_OPTIMIZE_ONDEMAND);
                    // use Random Search
                    //super.sim_schedule(this.getEntityId(this.getEntityName()), 0.0, 987654322);

                    // skip the rest - scheduling will be called by the optimization procedure
                    continue;

                }

                // some resource is probably available - try send gridlets according to schedule
                if (prev_scheduled == 0) {
                    /*
                     * Date dd = new Date(); clock1 = dd.getTime();
                     * scheduleGridlets(); Date dd2 = new Date(); clock2 =
                     * dd2.getTime(); clock += clock2 - clock1;
                     */
                    super.sim_schedule(this.getEntityId(this.getEntityName()), 0.0, AleaSimTags.EVENT_SCHEDULE, "completed job:" + gridlet_received.getGridletID());
                }

                // null gridlet to allow garbage collection
                gridlet_received = null;
                continue; // with other incoming event

            }
            // New gridlet arrived
            if (ev.get_tag() == AleaSimTags.GRIDLET_INFO) {
                ComplexGridlet gl = (ComplexGridlet) ev.get_data();
                GridletInfo gi = new GridletInfo(gl);
                gi.setDue_date(gl.getDue_date() + GridSim.clock());
                gl.setDue_date(gl.getDue_date() + GridSim.clock());
                last_job_id = gi.getID();
                setLengthStatistics(gi);

                if (restart) {
                    // reset internal variables at the beginning
                    activePEs = 0.0;
                    classic_activePEs = 0.0;
                    requestedPEs = 0.0;
                    last_event = clock();
                    restart = false;
                    in_job_counter = 0;
                    reqc = 0;
                    busyc = 0;
                    schedQueue.clear();
                    schedQueue2.clear();
                } else {
                    // update machine usage
                    load += (activePEs / availPEs) * (GridSim.clock() - last_event);
                    classic_load += (classic_activePEs / classic_availPEs) * (GridSim.clock() - last_event);
                    max_load += 1.0 * (GridSim.clock() - last_event);
                    last_event = GridSim.clock();
                }

                // cancel all jobs that cannot be executed due to missing properties                
                if (!isGridletExecutable(gi) || gi.getLength() < 0) {
                    bad++;
                    System.out.println("Job: " + gi.getID() + " is unexecutable (Error). Requirements: properties[" + gi.getProperties() + "], total CPUs=" + gi.getNumPE() + ", RAM=" + gi.getRam() + ", CPU per node=" + gi.getPpn() + ", #nodes=" + gi.getNumNodes());
                    try {
                        if (gl.getActualCPUTime() > 0.0) {
                            gl.setGridletStatus(Gridlet.FAILED_RESOURCE_UNAVAILABLE);
                        } else {
                            gl.setGridletStatus(Gridlet.CANCELED);
                        }
                    } catch (Exception ex) {
                        ex.printStackTrace();
                    }
                    // due to resource failure some gridlets may not be executable anymore - therefore we have to cancel them
                    // but MUST NOT increase the in_job_counter otherwise the simulation will not finish.
                    if (!gl.isRepeated()) {
                        in_job_counter++;
                        reqc += gi.getNumPE();
                    }
                    super.sim_schedule(this.getEntityId(this.getEntityName()), 0.0, GridSimTags.GRIDLET_RETURN, gl);
                    continue;
                }

                // used for schedule-based methods when resource failure appears
                // (planned jobs from schedule are rescheduled)
                if (!gl.isRepeated()) {
                    //increase requestedPEs value
                    requestedPEs += gi.getNumPE();
                    waiting_size++;
                    in_job_counter++;
                    reqc += gi.getNumPE();
                }

                Date d = new Date();
                clock1 = d.getTime();

                // if job has no predecessors, handle it normally
                if (gi.getPrecedingJobs() == null || gi.getPrecedingJobs().isEmpty()) {
                    // call scheduling algorithm here
                    if (((classic_availPEs * ExperimentSetup.max_schedule_CPU_request_factor) < getScheduleCPUSize() || schedule_too_long) && ExperimentSetup.limit_schedule_size) {
                        //System.out.println(gi.getID() + ": is held at time: " + GridSim.clock() + ". Requested " + getScheduleCPUSize() + " CPUs by " + getScheduleSize() + " jobs in schedule. Too long:" + schedule_too_long);
                        hold_queue.add(gi);
                        //updateResourceInfos(clock());
                        //super.sim_schedule(this.getEntityId(this.getEntityName()), 0.0, AleaSimTags.EVENT_SCHEDULE);
                    } else {
                        // add job normally
                        ExperimentSetup.policy.addNewJob(gi);
                    }
                } else {
                    //check predecessors
                    boolean add_to_DAG_queue = false;
                    for (int pr = 0; pr < gi.getPrecedingJobs().size(); pr++) {
                        int predecessor = gi.getPrecedingJobs().get(pr);
                        if (unfinished_predecessors.contains(predecessor)) {
                            add_to_DAG_queue = true;
                        }
                    }
                    // add job to DAG active_scheduling_queue of jobs waiting for completed predecessors
                    if (add_to_DAG_queue) {
                        System.out.println("[SCHEDULER] Job " + gi.getID() + " from " + gi.getGridlet().getArchRequired() + " added to DAG waiting queue -> waiting for completion of jobs: " + gi.getPrecedingJobs().toString());
                        DAG_queue.add(gi);
                    } else {
                        // add job normally
                        ExperimentSetup.policy.addNewJob(gi);
                    }

                }

                // write on screen info so that the simulation progress can be seen
                if (in_job_counter % 10 == 0) {
                    /*if ((algorithm >= 9 && algorithm != 12) || algorithm == 4) {
                        System.out.println(">>> " + in_job_counter + " so far arrived, in schedule = " + getScheduleSize() + " jobs, at time = " + Math.round(clock()) + " running = " + getRunningJobs() + "jobs,  FREE CPUs = " + getFreeCPUs());
                    } else {*/
                    //String dated = new java.text.SimpleDateFormat("dd-MM-yyyy-hh:mm:ss").format(new java.util.Date(Math.round(clock()) * 1000));
                    String dated = new java.text.SimpleDateFormat("dd-MM-yyyy [HH:mm]").format(new java.util.Date(Math.round(clock() + Scheduler.start_date) * 1000));
                    //System.out.println(">>> " + in_job_counter + " so far arrived, in active_scheduling_queue = " + getQueueSize() + " jobs, at time = " + Math.round(clock()) + " running = " + getRunningJobs() + " jobs, free CPUs = " + getFreeCPUs() + ", #" + active_scheduling_queue.getFirst().getID() + " is the first waiting job in active_scheduling_queue. Day: " + dated);
                    System.out.println(">>> " + in_job_counter + " arrived so far, in queue/schedule " + getQueueSize() + " jobs, requiring " + getQueueCPUSize()
                            + " CPUs, held jobs " + hold_queue.size() + " running " + getRunningJobs()
                            + " jobs, free CPUs " + getFreeCPUs() + ", free RAM " + Math.round(getFreeRAM() / (1024.0 * 1024)) + " GB, free GPUs " + getFreeGPUs() + ", "
                            + "tried jobs " + tried_queue.size() + ", Day: " + dated + " SimClock: " + Math.round(clock()));

                }

                User usr = ExperimentSetup.users.get(gi.getUser());
                usr.setQueued_jobs(usr.getQueued_jobs() + 1);

                // update total sched. generation time
                Date d2 = new Date();
                clock2 = d2.getTime();
                clock += clock2 - clock1;

                // try to schedule according to prepared active_scheduling_queue/schedule
                //System.out.println(GridSim.clock() + ": Try Scheduling gridlets from schedule of size: " + getQueueSize() + " prev_scheduled = " + prev_scheduled);
                if (prev_scheduled == 0) {
                    //System.out.println(GridSim.clock() + ": Scheduling gridlets from schedule of size: " + getQueueSize());
                    super.sim_schedule(this.getEntityId(this.getEntityName()), 0.0, AleaSimTags.EVENT_SCHEDULE, "new job:" + gi.getID());
                    //scheduleGridlets();
                }
                continue; // with other incoming event

            }
        }

        // all jobs were received
        if (end_of_submission && received == in_job_counter) {
            //ExperimentSetup.result_collector.recordSystemThroughput(clock(), ExperimentSetup.users);//started
            //ExperimentSetup.result_collector.recordFairshareFactor(clock(), ExperimentSetup.users);
            Scheduler.final_makespan = GridSim.clock();
            ExperimentSetup.result_collector.recordUserUsage(clock(), ExperimentSetup.users);

            // turn off the JobLoader
            super.send(this.getEntityId(data_set + "_JobLoader"), GridSimTags.SCHEDULE_NOW, GridSimTags.END_OF_SIMULATION, 0.0);
            // turn off the FailureLoader
            super.send(this.getEntityId(data_set + "_FailureLoader"), GridSimTags.SCHEDULE_NOW, GridSimTags.END_OF_SIMULATION, 0.0);
            // turn off the Scheduler (this entity)
            super.send(super.getEntityId(super.getEntityName()), GridSimTags.SCHEDULE_NOW, GridSimTags.END_OF_SIMULATION, 0.0);
        }

        Sim_event ev = new Sim_event();
        while (ev.get_tag() != -1) {
            sim_get_next(ev);
            System.out.println(ev.get_tag() + ": (-1) this is the tag that should quit Scheduler");
        }

        if (ev.get_tag() == GridSimTags.END_OF_SIMULATION) {
            // write out final results
            System.out.println("---------------------------- End Of Simulation at time: "+Math.round(GridSim.clock())+" - CALLING RESULT COLLECTOR ------------------------------------");
            System.out.println("---------------------------- Event optimization performed = " + event_opt + " times. -------------------------");
            System.out.println("---------------------------- Cancelled due miss. property = " + bad + " jobs. -------------------------");
            //rc.computeResults(av_PEs, wav_PEs, failure_time, wfailure_time, clock, runtime, classic_load, max_load, submitted);
            // NEW: created object SchedulerData
            SchedulerData sd = new SchedulerData(av_PEs, wav_PEs, failure_time, wfailure_time, clock, runtime, classic_load, max_load, submitted);
            rc.computeResults(sd);
            drawCharts();
        }
        // shut down I/O ports, turn off this entity
        shutdownUserEntity();
        super.terminateIOEntities();
    }

    /**
     * Starts scheduling according to the applied algorithm and prepared
     * schedule/active_scheduling_queue
     */
    private boolean scheduleGridlets() {
        Date d = new Date();
        clock1 = d.getTime();
        // try to schedule according to prepared active_scheduling_queue/schedule
        /*System.out.println(GridSim.clock() + " OK, scheduling from following schedule:");
        for (int i = 0; i < resourceInfoList.size(); i++) {
            ResourceInfo ri = (ResourceInfo) resourceInfoList.get(i);
            ri.printSchedule();
        }*/
        for (int i = 0; i < resourceInfoList.size(); i++) {
            ResourceInfo ri = (ResourceInfo) resourceInfoList.get(i);
            for (int jobID = 0; jobID < ri.resInExec.size(); jobID++) {
                GridletInfo gi = ri.resInExec.get(jobID);
                // job exceeds its runtime - perform prolongation
                if (gi.getExpectedFinishTime() <= (GridSim.clock() + 0.1)) {
                    ri.forceUpdate(GridSim.clock());
                    break;
                }
            }
        }

        if (!waiting_for_preempted_job_to_arrive) {
            prev_scheduled += ExperimentSetup.policy.selectJob();
        }
        Date d2 = new Date();
        clock2 = d2.getTime();
        clock += clock2 - clock1;

        return true;
    }

    /**
     * Returns the number of jobs waiting (in the schedule).
     */
    public static int getScheduleSize() {
        int size = 0;
        for (int i = 0; i < resourceInfoList.size(); i++) {
            ResourceInfo ri = (ResourceInfo) resourceInfoList.get(i);
            size += ri.resSchedule.size();
        }
        return size;
    }

    public static int getScheduleCPUSize() {
        int size = 0;
        for (int i = 0; i < resourceInfoList.size(); i++) {
            ResourceInfo ri = (ResourceInfo) resourceInfoList.get(i);
            for (int s = 0; s < ri.resSchedule.size(); s++) {
                size += ri.resSchedule.get(s).getNumPE();
            }
        }
        return size;
    }

    /**
     * Returns number of jobs currently in execution.
     */
    private int getRunningJobs() {
        int runningJobs = 0;
        for (int i = 0; i < resourceInfoList.size(); i++) {
            ResourceInfo ri = (ResourceInfo) resourceInfoList.get(i);
            runningJobs += ri.resInExec.size();
        }
        return runningJobs;
    }

    private long getFreeRAM() {
        long freeRAM = 0;
        for (int i = 0; i < resourceInfoList.size(); i++) {
            ResourceInfo ri = (ResourceInfo) resourceInfoList.get(i);
            //MachineList machines = ri.resource.getMachineList();
            ArrayList machines = new ArrayList<>(ri.resource.getMachineList());
            for (int ii = 0; ii < machines.size(); ii++) {
                MachineWithRAMandGPUs machine = (MachineWithRAMandGPUs) machines.get(ii);
                freeRAM += machine.getFreeRam();
            }
        }
        return freeRAM;
    }

    private long getFreeGPUs() {
        long freeGPUs = 0;
        for (int i = 0; i < resourceInfoList.size(); i++) {
            ResourceInfo ri = (ResourceInfo) resourceInfoList.get(i);
            //MachineList machines = ri.resource.getMachineList();
            ArrayList machines = new ArrayList<>(ri.resource.getMachineList());
            
            for (int ii = 0; ii < machines.size(); ii++) {
                MachineWithRAMandGPUs machine = (MachineWithRAMandGPUs) machines.get(ii);
                freeGPUs += machine.getFreeGPUs();
            }
        }
        return freeGPUs;
    }

    private int getFreeCPUs() {
        int runningJobs = 0;
        for (int i = 0; i < resourceInfoList.size(); i++) {
            ResourceInfo ri = (ResourceInfo) resourceInfoList.get(i);
            runningJobs += ri.getNumFreePE();
        }
        return runningJobs;
    }

    /**
     * Calculates number of non failed CPUs
     */
    private int getAvailPEs() {
        int avail = 0;
        for (int i = 0; i < resourceInfoList.size(); i++) {
            ResourceInfo ri = (ResourceInfo) resourceInfoList.get(i);
            avail += ri.getNumRunningPE();
        }
        return avail;
    }

    /**
     * Generates an array of permuted integers from 0 .. N-1.
     */
    public static int[] permute(int N) {

        int[] a = new int[N];

        // insert integers 0..N-1
        for (int i = 0; i < N; i++) {
            a[i] = i;
            // shuffle
        }
        for (int i = 0; i < N; i++) {
            //int r = (int) (Math.random() * (i+1));     // int between 0 and i
            int r = (int) (perm_rnd.nextDouble() * (i + 1));     // int between 0 and i

            int swap = a[r];
            a[r] = a[i];
            a[i] = swap;
        }
        return a;

    }

    /**
     * Returns the number of working (non-failed) PEs on given resource.
     */
    private int printRunningPEsOnResource(int resId) {
        ResourceInfo ri = null;
        for (int i = 0; i < resourceInfoList.size(); i++) {
            ri = (ResourceInfo) resourceInfoList.get(i);
            if (ri.resource.getResourceID() == resId) {
                break;
            }
        }
        //System.out.println(resId+" Resource found "+ri.resource.getResourceName());
        ri.stable_free = false;
        return ri.getNumRunningPE();
    }

    /**
     * Updates internal variables of resource infos objects. Use for
     * schedule-based algorithms only!
     *
     * @param current_time current simulation time used to predict total
     * tardiness of all jobs in this moment
     */
    public static void updateResourceInfos(double current_time, String message) {

        for (int i = 0; i < resourceInfoList.size(); i++) {
            ResourceInfo ri = (ResourceInfo) resourceInfoList.get(i);
            ri.update(current_time, "up res infos from:" + message);

        }
    }

    public static void updateResourceInfos(double current_time) {

        for (int i = 0; i < resourceInfoList.size(); i++) {
            ResourceInfo ri = (ResourceInfo) resourceInfoList.get(i);
            ri.update(current_time, "up res infos from:");

        }
    }

    public static void forceUpdateResourceInfos(double current_time) {

        for (int i = 0; i < resourceInfoList.size(); i++) {
            ResourceInfo ri = (ResourceInfo) resourceInfoList.get(i);
            ri.forceUpdate(current_time);

        }
    }

    /**
     * Updates internal ResourceInfo objects after failure or restart of some
     * machine. Moreover - if schedule is built it is updated wrt. currently
     * running machines
     */
    private void updateResourceInfoAfterFailureOrRestart(int resId) {
        ResourceInfo ri = null;
        for (int i = 0; i < resourceInfoList.size(); i++) {
            ri = (ResourceInfo) resourceInfoList.get(i);
            if (ri.resource.getResourceID() == resId) {
                break;
            }
        }
        int index_id = 0;
        int numPE = 0;
        MachineList list = ri.resource.getMachineList();
        for (int i = 0; i < list.size(); i++) {
            Machine m = (Machine) list.get(i);
            if (!m.getFailed()) {
                numPE += m.getNumPE();
                for (int p = 0; p < m.getNumPE(); p++) {
                    //System.out.print(index_id+".");
                    ri.finishTimeOnPE[index_id] = clock();
                    index_id++;

                }
            } else {
                for (int p = 0; p < m.getNumPE(); p++) {
                    //System.out.print(index_id+".");
                    ri.finishTimeOnPE[index_id] = Double.MAX_VALUE;
                    index_id++;
                }

            }
        }
        ri.numPE = numPE;
        schedQueue.addAll(ri.resSchedule);
        ri.resSchedule.clear();
        ri.forceUpdate(clock());

        // if resource restarts/fails then start a new scheduling round, otherwise the simulation may hang forever
        if (schedQueue.size() == 0 && prev_scheduled == 0) {
            super.sim_schedule(this.getEntityId(this.getEntityName()), 0.0, AleaSimTags.EVENT_SCHEDULE);
            //scheduleGridlets();
        }

        // reschedule jobs that were planned on failed machines
        for (int i = 0; i < schedQueue.size(); i++) {
            removeGridletInfo(((GridletInfo) schedQueue.get(i)));
            ComplexGridlet gl = ((GridletInfo) schedQueue.get(i)).getGridlet();
            gl.setRepeated(true);
            super.sim_schedule(this.getEntityId(this.getEntityName()), 0.0, AleaSimTags.GRIDLET_INFO, gl);
        }

        schedQueue.clear();
    }

    /**
     * Decides whether GridResource is suitable for a job concerning job's
     * requirements.
     */
    public static boolean isSuitable(ResourceInfo ri, GridletInfo gi) {
        if (!ExperimentSetup.failures) {
            HashMap<Integer, Boolean> h = gi.getResourceSuitable();
            boolean suitable = h.get(ri.resource.getResourceID());
            return suitable;
        } else {
            return ri.canExecuteEver(gi);
        }

    }

    /**
     * Decides whether GridResource is suitable for a job concerning job's
     * requirements. If so, all information about such resource are updated.
     * This is used in ESG or CONS when assigning new job into schedule.
     */
    public static boolean isGridletSuitableThenUpdate(ResourceInfo ri, GridletInfo gi, double current_time) {

        if (isSuitable(ri, gi)) {
            ri.update(current_time, "new job arrival:" + gi.getID());
            return true;
        } else {
            return false;
        }

    }

    public static boolean isGridletExecutable(GridletInfo gi) {
        ResourceInfo ri = null;
        boolean executable = false;

        if (ExperimentSetup.use_multiple_queues) {
            int queue_limit = ExperimentSetup.queues.get(gi.getQueue()).getQueue_CPU_limit();
            if (queue_limit < gi.getNumPE()) {
                System.out.println(gi.getID() + " is unexecutable: Queue limit FAIL. Queue Limit=" + queue_limit + " < requested=" + gi.getNumPE());
                return false;
            }
        }

        if (ExperimentSetup.use_user_groups) {
            FairshareGroup g = ExperimentSetup.groups.get(gi.getGroupID());
            User u = ExperimentSetup.users.get(gi.getUser());
            int group_limit = g.getQuota();
            int user_limit = u.getUserQuota();
            if (group_limit < gi.getNumPE()) {
                System.out.println(gi.getID() + " is unexecutable: Group Quota limit FAIL. Group Quota limit=" + group_limit + " < requested=" + gi.getNumPE() + " for group " + g.getName());
                return false;
            } else if (user_limit < gi.getNumPE()) {
                System.out.println(gi.getID() + " is unexecutable: User Quota limit FAIL. User Quota limit=" + user_limit + " < requested=" + gi.getNumPE() + " for user " + u.getName());
                return false;
            } else {
                //System.out.println(gi.getID()+": Quota group_limit OK. Quota group_limit="+group_limit+" >= requested="+gi.getNumPE()+" for group "+g.getName());
            }
        }

        for (int i = 0; i < resourceInfoList.size(); i++) {
            ri = (ResourceInfo) resourceInfoList.get(i);
            if (ri.canExecuteEver(gi)) {
                HashMap h = gi.getResourceSuitable();
                h.put(ri.resource.getResourceID(), true);
                executable = true;
                gi.setExecutable(true);

            } else {
                HashMap h = gi.getResourceSuitable();
                h.put(ri.resource.getResourceID(), false);
                //System.out.println(gi.getID() + ": cannot run on:" + ri.resource.getResourceName() + " CPU=" + isSuitable(ri, gi) + " RAM=" + (gi.getRam() <= ri.getRamOfOneMachine()) + " req: ppn=" + gi.getPpn() + " nodes=" + gi.getNumNodes() + " RAM=" + gi.getRam() + " suit=" + isSuitable(ri, gi)+" req="+gi.getProperties()+" use_reqs="+use_specific_job_requirements);
            }
        }
        return executable;

    }

    /**
     * Returns the number of waiting jobs in active_scheduling_queue(s)
     */
    private int getQueueSize() {
        int size = 0;
        for (int q = 0; q < all_queues.size(); q++) {
            size += all_queues.get(q).size();
        }

        return size + getScheduleSize();
    }

    private int getQueueCPUSize() {
        int size = 0;
        for (int q = 0; q < all_queues.size(); q++) {
            LinkedList queue_c = all_queues.get(q);
            for (int qq = 0; qq < queue_c.size(); qq++) {
                size += ((GridletInfo) queue_c.get(qq)).getNumPE();
            }
        }

        return size + getScheduleCPUSize();
    }

    /**
     * Updates user-related information of job execution time used in fair-share
     * mechanism
     */
    private void updateLengthStatistics(ComplexGridlet gridlet_received, double cpu_time) {
        int user_index = users.indexOf(new String(gridlet_received.getUser()));

        double old_time = users_CPUtime.get(user_index);
        double old_max = users_MAX.get(user_index);
        double old_sqrt = users_SQRT.get(user_index);

        double final_old_time = final_users_CPUtime.get(user_index);
        double old_mult = users_multiplications.get(user_index);
        double prevwt = total_uwt.get(user_index);
        double final_prevwt = final_total_uwt.get(user_index);
        double prevram = total_uram.get(user_index);
        double final_prevram = final_total_uram.get(user_index);

        double relative_RAM = (gridlet_received.getNumNodes() * gridlet_received.getRam()) / ExperimentSetup.avail_RAM;
        double relative_CPUs = gridlet_received.getNumPE() / ExperimentSetup.avail_CPUs;
        old_max += Math.max(relative_CPUs, relative_RAM) * cpu_time;
        old_sqrt += (1.0 - Math.sqrt((Math.abs(1.0 - relative_RAM) * Math.abs(1.0 - relative_CPUs)))) * cpu_time;

        //old_time += cpu_time * relative_CPUs;
        old_time += cpu_time * gridlet_received.getNumPE();

        final_old_time += cpu_time * gridlet_received.getNumPE();

        // rewrite for multinode jobs
        double ram_per_node = gridlet_received.getRam() / ExperimentSetup.avail_RAM;
        double ram_per_cpu_per_node = ram_per_node / gridlet_received.getNumPE();

        old_mult += (cpu_time * relative_CPUs) * (Math.max(0.0, gridlet_received.getNumNodes() * ram_per_cpu_per_node));
        prevwt += Math.max(0.0, gridlet_received.getExecStartTime() - gridlet_received.getArrival_time());//*gridlet_received.getNumPE();
        final_prevwt += Math.max(0.0, gridlet_received.getExecStartTime() - gridlet_received.getArrival_time());//*gridlet_received.getNumPE();;
        prevram += Math.max(0.0, gridlet_received.getNumNodes() * gridlet_received.getRam());
        final_prevram += Math.max(0.0, gridlet_received.getNumNodes() * gridlet_received.getRam());
        //System.out.println(gridlet_received.getGridletID()+" adding vwt ="+Math.round(gridlet_received.getExecStartTime() - gridlet_received.getArrival_time())+ " total="+ Math.round(prevwt));

        if (gridlet_received.getNumPE() > 1) {
            double tot_length = users_P_length.get(user_index);
            tot_length += gridlet_received.getGridletLength();
            int tot_jobs = users_P_jobs.get(user_index);
            tot_jobs++;
            users_P_length.remove(user_index);
            users_P_length.add(user_index, tot_length);
            users_P_jobs.remove(user_index);
            users_P_jobs.add(user_index, tot_jobs);

        } else {
            double tot_length = users_length.get(user_index);
            tot_length += gridlet_received.getGridletLength();
            int tot_jobs = users_jobs.get(user_index);
            tot_jobs++;
            users_length.remove(user_index);
            users_length.add(user_index, tot_length);
            users_jobs.remove(user_index);
            users_jobs.add(user_index, tot_jobs);
        }

        users_CPUtime.remove(user_index);
        users_MAX.remove(user_index);
        users_SQRT.remove(user_index);
        final_users_CPUtime.remove(user_index);
        users_multiplications.remove(user_index);
        total_uwt.remove(user_index);
        final_total_uwt.remove(user_index);
        total_uram.remove(user_index);
        final_total_uram.remove(user_index);
        users_CPUtime.add(user_index, old_time);
        users_MAX.add(user_index, old_max);
        users_SQRT.add(user_index, old_sqrt);
        final_users_CPUtime.add(user_index, final_old_time);
        users_multiplications.add(user_index, old_mult);
        total_uwt.add(user_index, prevwt);
        total_uram.add(user_index, prevram);
        final_total_uwt.add(user_index, final_prevwt);
        final_total_uram.add(user_index, final_prevram);

        users_last_length.remove(user_index);
        users_last_length.add(user_index, gridlet_received.getGridletLength());

        double tot_perc = users_percentage_length.get(user_index);
        tot_perc += gridlet_received.getEstimatedLength() / gridlet_received.getGridletLength();
        users_percentage_length.remove(user_index);
        users_percentage_length.add(user_index, tot_perc);

        User u = ExperimentSetup.users.get(gridlet_received.getUser());
        // record how much of the user-specified walltime was actually used
        u.addPercentage(Math.max(1.0, gridlet_received.getEstimatedLength() / gridlet_received.getGridletLength()));

        //updateFairShareOld();
    }

    /**
     * Sets job priority according to a fair share mechanism
     */
    private void setLengthStatistics(GridletInfo gi) {
        if (users.indexOf(new String(gi.getUser())) == -1) {
            users.addLast(gi.getUser());
            users_CPUtime.addLast(0.0);
            users_MAX.addLast(0.0);
            users_SQRT.addLast(0.0);
            final_users_CPUtime.addLast(0.0);
            users_multiplications.addLast(0.0);
            total_uwt.addLast(0.0);
            total_uram.addLast(0.0);
            final_total_uwt.addLast(0.0);
            final_total_uram.addLast(0.0);
            double walltime_length = gi.getEstimatedLength();
            users_last_length.addLast(walltime_length);

            // sequential jobs
            users_length.addLast(0.0);
            users_jobs.addLast(0);
            // parallel jobs
            users_P_length.addLast(0.0);
            users_P_jobs.addLast(0);

            users_percentage_length.addLast(0.0);

            // store user into a map of all users.
            if (!ExperimentSetup.users.containsKey(gi.getUser())) {
                User u = new User(gi.getUser(), "user_" + gi.getUser());
                System.out.println("Adding user " + gi.getUser());
                ExperimentSetup.users.put(gi.getUser(), u);
            }

        }
        User u = ExperimentSetup.users.get(gi.getUser());
        // set FairShare priority of this job
        updateFairshareFactorPriority(gi);

        updateGridletWalltimeEstimateApproximation(gi);

    }

    /**
     * This method updates the gridlet's estimated walltime using some form of
     * approximation gi.
     *
     * @param gi job that has finished
     */
    public static void updateGridletWalltimeEstimateApproximation(GridletInfo gi) {
        int user_index = users.indexOf(new String(gi.getUser()));
        double avg_length = 0.0;
        if (gi.getNumPE() > 1) {
            avg_length = Math.round(Math.max(1.0, users_P_length.get(user_index)) / Math.max(1.0, users_P_jobs.get(user_index)));
            //System.out.println(gi.getID()+ " ******** of "+users_P_jobs.get(user_index)+" ********** SETTING PARALLEL ESTIMATE est = "+Math.round(avg_length/1)+" MIPS, real = "+Math.round(gi.getLength()/1)+" MIPS | diff = "+Math.round(((gi.getLength()-avg_length)/(gi.getLength()*0.01)))+" % ");
        } else {
            avg_length = Math.round(Math.max(1.0, users_length.get(user_index)) / Math.max(1.0, users_jobs.get(user_index)));
            //System.out.println(gi.getID()+ " ******* of "+users_jobs.get(user_index)+" *********** SETTING SEQUENTIAL ESTIMATE est = "+Math.round(avg_length/1)+" MIPS, real = "+Math.round(gi.getLength()/1)+" MIPS | diff = "+Math.round(((gi.getLength()-avg_length)/(gi.getLength()*0.01)))+" % ");
        }
        double last_length = Math.max(1.0, users_last_length.get(user_index));
        gi.setAvg_length(avg_length);
        gi.setLast_length(last_length);

        double avg_perc_length = gi.getEstimatedLength() / (Math.max(1.0, users_percentage_length.get(user_index)) / Math.max(1.0, users_jobs.get(user_index)));
        //System.out.println(gi.getID()+" estimated lenght = "+gi.getEstimatedLength()+" real length= "+gi.getLength()+ " , perc overkil is: "+(Math.max(1.0,users_percentage_length.get(user_index))/Math.max(1.0, users_jobs.get(user_index))));
        gi.setAvg_perc_length(avg_perc_length);

    }

    /**
     * This method updates the fairshare priority for owner of finished job gi.
     *
     * @param gi job that has finished
     */
    private void updateFairshareFactorPriority(GridletInfo gi) {
        int user_index = users.indexOf(new String(gi.getUser()));
        double user_priority = 1.0;
        if (!ExperimentSetup.use_fairshare_RAM) {
            // priority = total CPU time only
            user_priority = users_CPUtime.get(user_index);
        } else {
            // priority = weird
            if (ExperimentSetup.multiply_sums) {
                user_priority = users_CPUtime.get(user_index) * Math.max(1.0, (total_uram.get(user_index) * 1.0));

                // priority = nasobky RAM a CPUt
            } else if (ExperimentSetup.sum_multiplications) {
                user_priority = users_multiplications.get(user_index);
                //System.out.println(gi.getID()+" new priority RAM = "+Math.round(user_priority));

                // priority = MAX(CPUt, RAM)
            } else if (ExperimentSetup.use_MAX) {
                user_priority = users_MAX.get(user_index);

                // priority = SQRT
            } else if (ExperimentSetup.use_SQRT) {
                user_priority = users_SQRT.get(user_index);
            }
        }

        // zbytek uz jen pro NUWT
        if (ExperimentSetup.use_fairshare_WAIT && !ExperimentSetup.use_fairshare_RAM) {
            // priority = 1/(total wait time / total CPU time) smaller is better (big wait time --> small value ==> 1/big value)
            user_priority = 1.0 / (total_uwt.get(user_index) / Math.max(1.0, (users_CPUtime.get(user_index) * 1.0)));
        }
        if (ExperimentSetup.use_fairshare_WAIT && ExperimentSetup.use_fairshare_RAM) {
            if (ExperimentSetup.sum_multiplications) {
                user_priority = 1.0 / (Math.max(1.0, total_uwt.get(user_index)) / (users_multiplications.get(user_index)));
                //System.out.println(gi.getID()+" new priority NUWT RAM = "+Math.round(user_priority));
            } else {
                user_priority = 1.0 / (total_uwt.get(user_index) / (Math.max(1.0, users_CPUtime.get(user_index)) * Math.max(1.0, (total_uram.get(user_index) * 1.0))));
            }
        }

        gi.setPriority(user_priority);
    }

    /**
     * This method updates the fairshare priority for every waiting job. The
     * method is used to adjust the fairshare weight of jobs according to the
     * newest knowledge.
     */
    private void updateFairShareOld() {
        for (int q = 0; q < Scheduler.all_queues.size(); q++) {
            LinkedList curr_queue = Scheduler.all_queues.get(q);
            for (int i = 0; i < curr_queue.size(); i++) {
                GridletInfo gi = (GridletInfo) curr_queue.get(i);
                //System.out.print(gi.getID()+" has priortity = "+gi.getPriority());
                //double diff = gi.getPriority();
                updateFairshareFactorPriority(gi);
                //diff = diff - gi.getPriority();
                //System.out.println(" > "+gi.getPriority()+" after update of "+gi.getID()+" diff = "+diff);
            }
        }
        //the same stuff for schedule
        ResourceInfo ri = null;
        for (int i = 0; i < resourceInfoList.size(); i++) {
            ri = (ResourceInfo) resourceInfoList.get(i);
            for (int s = 0; s < ri.resSchedule.size(); s++) {
                GridletInfo gi = (GridletInfo) ri.resSchedule.get(s);
                //System.out.print(gi.getID()+" has priortity = "+gi.getPriority());
                updateFairshareFactorPriority(gi);
                //System.out.println(" > "+gi.getPriority()+" after update of "+gi.getID());
            }
        }

    }

    public void updateFairShare() {
        Object[] keys = ExperimentSetup.users.keySet().toArray();
        for (int i = 0; i < ExperimentSetup.users.size(); i++) {
            User u = ExperimentSetup.users.get((String) keys[i]);
            updateUserFairshareFactor(u);
            //System.out.println("Updating u:"+u.getName()+" FF = "+u.getFairshare_factor()+" with running usage:"+u.getRunningUsage()+ " and total usage:"+calculate_total_usage());
        }
    }

    private void applyDecay() {
        Object[] keys = ExperimentSetup.users.keySet().toArray();
        for (int i = 0; i < ExperimentSetup.users.size(); i++) {
            User u = ExperimentSetup.users.get((String) keys[i]);
            long prev_usage = u.getCumul_usage();
            long usage_decay = Math.round(prev_usage * ExperimentSetup.decay_factor);
            u.setCumul_usage(usage_decay);

            double prev_ff = u.getFairshare_factor();
            long time = Math.round(GridSim.clock());

            //always reset temporary usage when decaying
            u.setTemporary_usage(0);
            u.setLast_temp_timestamp(time);

            updateUserFairshareFactor(u);
            //System.out.println("Doing decay for user: " + u.getName() + " usage: " + prev_usage + " decayed: " + usage_decay + ", FF(old): " + prev_ff + " FF(new): " + u.getFairshare_factor());
        }
    }

    public void updateTemporaryUsageAndFFuponJobStart(GridletInfo gi) {
        User u = ExperimentSetup.users.get(gi.getUser());
        long prev_usage = u.getTemporary_usage();
        // estimated job usage
        long temp = Math.round(gi.getNumPE() * gi.getJobLimit());
        u.setTemporary_usage(prev_usage + temp);
        long time = Math.round(GridSim.clock());
        u.setLast_temp_timestamp(time);
        updateUserFairshareFactor(u);
    }

    public void resetTemporaryUsageAndUpdateFF() {
        Object[] keys = ExperimentSetup.users.keySet().toArray();
        for (int i = 0; i < ExperimentSetup.users.size(); i++) {
            User u = ExperimentSetup.users.get((String) keys[i]);
            long time = Math.round(GridSim.clock());
            if (time > u.getLast_temp_timestamp() + (1)) { //20*60 - 20 minutes originaly, (1): pbs verif strict 
                u.setTemporary_usage(0);
                u.setLast_temp_timestamp(time);
            } else {
                //System.out.println("Same timestamp:"+time+" = "+u.getLast_temp_timestamp());
            }
            updateUserFairshareFactor(u);
        }
    }

    public void updateUsageAndFFafterCompletion(ComplexGridlet gl) {
        User u = ExperimentSetup.users.get(gl.getUser());
        long prev_usage = u.getCumul_usage();
        // real job usage
        long usage = Math.round(gl.getNumPE() * gl.getActualCPUTime());
        u.setCumul_usage(prev_usage + usage);
        long time = Math.round(GridSim.clock());
        u.setLast_temp_timestamp(time);
        u.setTemporary_usage(0);
        updateUserFairshareFactor(u);
    }

    private void updateUserFairshareFactor(User u) {
        double ff = 0.5;
        double tree_usage = (u.getCumul_usage() + u.getTemporary_usage() + u.getRunningUsage()) / calculate_total_usage();
        double target_usage = u.getUser_share() / calculate_total_shares();
        ff = Math.pow(2, -(tree_usage / target_usage));
        u.setFairshare_factor(ff);
    }

    public static double calculate_total_usage() {
        double tu = 0.0;
        Object[] keys = ExperimentSetup.users.keySet().toArray();
        for (int i = 0; i < ExperimentSetup.users.size(); i++) {
            User u = ExperimentSetup.users.get((String) keys[i]);
            tu += u.getCumul_usage() + u.getTemporary_usage() + u.getRunningUsage();
        }
        return Math.max(tu, 0.00000001);
    }

    public static double calculate_total_shares() {
        double ts = 0.0;
        Object[] keys = ExperimentSetup.users.keySet().toArray();
        for (int i = 0; i < ExperimentSetup.users.size(); i++) {
            User u = ExperimentSetup.users.get((String) keys[i]);
            ts += u.getUser_share();
        }
        return Math.max(ts, 1.0);
    }

    public void drawCharts() {
        if (ExperimentSetup.draw_chart == false) {
            return;
        }
        TimeSeriesCollection dataset_ff = new TimeSeriesCollection();
        TimeSeriesCollection dataset_usage = new TimeSeriesCollection();
        TimeSeriesCollection dataset_cumul_usage = new TimeSeriesCollection();
        TimeSeriesCollection dataset_waiting_jobs = new TimeSeriesCollection();

        Object[] keys = ExperimentSetup.users.keySet().toArray();
        for (int u = 0; u < ExperimentSetup.users.size(); u++) {
            // use this hack to keep them always ordered
            User us = ExperimentSetup.users.get(ExperimentSetup.user_logins.get(u));
            //User us = ExperimentSetup.users.get((String)keys[u]);
            dataset_usage.addSeries(us.series_u);
            dataset_waiting_jobs.addSeries(us.series_w);
            dataset_ff.addSeries(us.series_ff);
            dataset_cumul_usage.addSeries(us.series_c_u);
        }

        TimeSeriesCollection dataset_cluster_usage = new TimeSeriesCollection();
        TimeSeriesCollection dataset_cluster_used = new TimeSeriesCollection();
        TimeSeriesCollection dataset_system_usage = new TimeSeriesCollection();
        dataset_system_usage.addSeries(series_system_usage);
        dataset_system_usage.addSeries(series_alloc_usage);

        for (int i = 0; i < resourceInfoList.size(); i++) {
            ResourceInfo ri = (ResourceInfo) resourceInfoList.get(i);
            dataset_cluster_usage.addSeries(ri.series_usage);
            dataset_cluster_used.addSeries(ri.series_used);
        }

        int width = ExperimentSetup.chart_width;
        int height = ExperimentSetup.chart_height;

        String yaxis = "used quota (# of CPUs)";
        if (ExperimentSetup.all_jobs_allocate_whole_nodes) {
            yaxis = "used quota (# of nodes)";
        }

        TimeSeriesChart user_usage = new TimeSeriesChart("User Used Quota (sample based)", Scheduler.scheduling_algorithm + " (sampling period: " + ExperimentSetup.sample_tick + "s)", dataset_usage, false, width, height, yaxis, 0);
        TimeSeriesChart user_waiting = new TimeSeriesChart("User Waiting Jobs (sample based)", Scheduler.scheduling_algorithm + " (sampling period: " + ExperimentSetup.sample_tick + "s)", dataset_waiting_jobs, false, width, height, "waiting jobs", 1);
        TimeSeriesChart cluster_usage = new TimeSeriesChart("Cluster Usage % (sample based)", Scheduler.scheduling_algorithm + " (sampling period: " + ExperimentSetup.sample_tick + "s)", dataset_cluster_usage, false, width, height, "CPU usage (%)", 2);
        TimeSeriesAreaChart system_usage = new TimeSeriesAreaChart("Overal System Usage % (sample based)", Scheduler.scheduling_algorithm + " (sampling period: " + ExperimentSetup.sample_tick + "s)", dataset_system_usage, true, width, height, "CPU usage (%)", 3);
        ScatterChart wait_scatter = new ScatterChart("Wait times (minutes) wrt. arrivals", Scheduler.scheduling_algorithm, false, width, height, "wait time (m)", "arrival time (s)", 4);
        BoxPlotChart wait_boxplot = new BoxPlotChart("Distr. of wait time (minutes) wrt. users ", Scheduler.scheduling_algorithm, "users", "wait time (minutes)", width, height, 4);

        yaxis = "# of used CPUs";
        if (ExperimentSetup.all_jobs_allocate_whole_nodes) {
            yaxis = "# of used nodes";
        }
        if (ExperimentSetup.basic_charts == false) {
            TimeSeriesChart cluster_used = new TimeSeriesChart("Cluster Used CPUs (sample based)", Scheduler.scheduling_algorithm + " (sampling period: " + ExperimentSetup.sample_tick + "s)", dataset_cluster_used, false, width, height, yaxis, 4);
            if (ExperimentSetup.use_fairshare) {
                if (ExperimentSetup.use_decay) {
                    TimeSeriesChart example = new TimeSeriesChart("User Fairshare Factor", Scheduler.scheduling_algorithm + " - Decay applied", dataset_ff, true, width, height, "Fairshare Factor", 5);
                } else {
                    TimeSeriesChart example = new TimeSeriesChart("User Fairshare Factor", Scheduler.scheduling_algorithm + " - No Decaying applied", dataset_ff, true, width, height, "Fairshare Factor", 5);
                }

                if (ExperimentSetup.use_decay) {

                    TimeSeriesChart examplecu = new TimeSeriesChart("User Cumulative Usage", Scheduler.scheduling_algorithm + " - Decay applied", dataset_cumul_usage, false, width, height, "usage (CPU hours)", 6);
                } else {
                    TimeSeriesChart examplecu = new TimeSeriesChart("User Cumulative Usage", Scheduler.scheduling_algorithm + " - No Decaying applied", dataset_cumul_usage, false, width, height, "usage (CPU hours)", 6);
                }
            }
        }
    }

    /**
     * This method decrease the fairshare priority for every waiting job by
     * factor of 2.0. The method is used to lower the fairshare weight of old
     * jobs.
     */
    private void decreaseFairShare() {
        for (int i = 0; i < users_CPUtime.size(); i++) {
            double old_v = users_CPUtime.get(i);
            users_CPUtime.remove(i);
            users_CPUtime.add(i, (old_v / 2.0));

            old_v = total_uram.get(i);
            total_uram.remove(i);
            total_uram.add(i, (old_v / 2.0));

            old_v = users_multiplications.get(i);
            users_multiplications.remove(i);
            users_multiplications.add(i, (old_v / 2.0));

            old_v = users_MAX.get(i);
            users_MAX.remove(i);
            users_MAX.add(i, (old_v / 2.0));

            old_v = total_uwt.get(i);
            total_uwt.remove(i);
            total_uwt.add(i, (old_v / 2.0));
        }
        for (int q = 0; q < Scheduler.all_queues.size(); q++) {
            LinkedList curr_queue = Scheduler.all_queues.get(q);
            //System.out.println(q+" Current active_scheduling_queue has "+curr_queue.size());
            for (int i = 0; i < curr_queue.size(); i++) {
                GridletInfo gi = (GridletInfo) curr_queue.get(i);
                //System.out.print(gi.getID()+" has priortity = "+gi.getPriority());
                updateFairshareFactorPriority(gi);
                //System.out.println(" > "+gi.getPriority()+" after update of "+gi.getID());
            }
        }
        //the same stuff for schedule
        ResourceInfo ri = null;
        for (int i = 0; i < resourceInfoList.size(); i++) {
            ri = (ResourceInfo) resourceInfoList.get(i);
            for (int s = 0; s < ri.resSchedule.size(); s++) {
                GridletInfo gi = (GridletInfo) ri.resSchedule.get(s);
                //System.out.print("DEC: "+gi.getID()+" has priortity = "+gi.getPriority());
                updateFairshareFactorPriority(gi);
                //System.out.println(" > "+gi.getPriority()+" after update of "+gi.getID());
            }
        }
    }

    /**
     * Removes job from a temporary active_scheduling_queue - used during
     * rescheduling after machine restart or machine failure.
     */
    private boolean removeGridletInfo(GridletInfo gi) {
        for (int j = 0; j < job_queue.size(); j++) {
            GridletInfo gs = (GridletInfo) job_queue.get(j);
            if (gs.getID() == gi.getID() && gs.getUser().equals(gi.getUser())) {
                job_queue.remove(j);
                return true;
            }
        }
        return false;
    }

    /**
     * Algorithm that compresses the schedule when early job completion is
     * detected. Typically used by Conservative backfilling.
     */
    private void compressSchedule(int resid) {
        if (!ExperimentSetup.use_fairshare || ExperimentSetup.algID == 4) {
            ResourceInfo ri = null;
            double runtime1 = new Date().getTime();

            for (int i = 0; i < resourceInfoList.size(); i++) {
                ri = (ResourceInfo) resourceInfoList.get(i);
                if (ri.resource.getResourceID() == resid) {
                    break;
                }
            }
            //System.out.println("Starting compression of "+ri.resSchedule.size()+" jobs.");
            schedQueue2.addAll(ri.resSchedule);
            ri.resSchedule.clear();
            ri.stable = false;
            ri.holes.clear();

            double current_time = clock();
            for (int i = 0; i < schedQueue2.size(); i++) {
                ri.update(current_time);
                ri.findHoleForGridlet((GridletInfo) schedQueue2.get(i));
                ri.update(current_time);
            }
            schedQueue2.clear();
            //System.out.println("Compression completed ...");
            runtime += (new Date().getTime() - runtime1);
            clock += new Date().getTime() - runtime1;
        } else {
            // do nothing as the schedule will be compressed via fair-share mechanism
        }
    }

    /**
     * Algorithm that quickly compresses the schedule when early job completion
     * is detected. typically used by Conservative backfilling. Jobs are not
     * reinserted, just compressed.
     */
    private void compressScheduleFast(int resid) {
        if (!ExperimentSetup.use_fairshare || ExperimentSetup.algID == 4) {
            ResourceInfo ri = null;
            double runtime1 = new Date().getTime();

            for (int i = 0; i < resourceInfoList.size(); i++) {
                ri = (ResourceInfo) resourceInfoList.get(i);
                if (ri.resource.getResourceID() == resid) {
                    break;
                }
            }
            ri.stable = false;
            ri.holes.clear();
            double current_time = clock();
            ri.update(current_time);

            runtime += (new Date().getTime() - runtime1);
            clock += new Date().getTime() - runtime1;
        } else {
            // do nothing as the schedule will be compressed via fair-share mechanism
        }
    }

    private void updateScheduleOnRes(int resid) {
        if (!ExperimentSetup.use_fairshare || ExperimentSetup.algID == 100) {
            ResourceInfo ri = null;
            double runtime1 = new Date().getTime();

            for (int i = 0; i < resourceInfoList.size(); i++) {
                ri = (ResourceInfo) resourceInfoList.get(i);
                if (ri.resource.getResourceID() == resid) {
                    break;
                }
            }
            ri.stable = false;
            ri.holes.clear();
            double current_time = clock();
            ri.update(current_time, "early compl.");

            runtime += (new Date().getTime() - runtime1);
            clock += new Date().getTime() - runtime1;
        } else {
            // do nothing as the schedule will be compressed via fair-share mechanism
        }
    }

    /**
     * This method submits job to a specified resource
     */
    public void submitJob(ComplexGridlet gl, int resID) {
        if (ExperimentSetup.use_multiple_queues) {
            ExperimentSetup.queues.get(gl.getQueue()).setQueue_used_CPUs(ExperimentSetup.queues.get(gl.getQueue()).getQueue_used_CPUs() + gl.getNumPE());
        }
        if (ExperimentSetup.use_user_groups) {
            ExperimentSetup.groups.get(gl.getGroupID()).setUsedQuota(ExperimentSetup.groups.get(gl.getGroupID()).getUsedQuota() + gl.getNumPE());
            ExperimentSetup.users.get(gl.getUser()).setUsed_quota(ExperimentSetup.users.get(gl.getUser()).getUsed_quota() + gl.getNumPE());
        }

        gridletSubmit(gl, resID);
        started++;
        User u = ExperimentSetup.users.get(gl.getUser());
        u.setStarted_jobs(u.getStarted_jobs() + 1);
        u.getRunning_jobs().add(gl);
        //System.out.println(gl.getUser()+" starting:"+gl.getGridletID()+", new ff: "+ExperimentSetup.users.get(gl.getUser()).getFairshare_factor()+" time: "+GridSim.clock()+" FREE CPUs left = "+ getFreeCPUs()+", Total shares="+calculate_total_shares());
        //System.out.println("------EoS-----");
        //System.out.println("[SCHEDULER] Job " + gl.getGridletID() + " from " + gl.getArchRequired() + " is submitted to cluster " + GridSim.getEntityName(resID) + ", FREE CPUs left = " + getFreeCPUs() + " at sim. time = " + GridSim.clock());
    }

    /**
     * This method periodically collects results used for graph generation.
     */
    private void collectPeriodicalResults() {

        if (hour_count == 0) {
            // 24 hours have passed - increase day counter
            day_count++;
        }
        // 3600 seconds have passed - increase hour counter
        hour_count++;
        if (hour_count == 24) {
            hours.clear();
            day_util.clear();
            hour_count = 0;
            week_count++;
            day_usage = Math.round((day_usage / 24.0) * 100) / 100.0;
            try {
                out.writeString(folder_prefix + "/day_usage_" + suff + ".csv", day_count + "\t" + day_usage);
                days.add(day_count);
                util.add(day_usage);

                int waitj = 0;
                int reqj = 0;
                if (algorithm < 10) {
                    waitj = getQueueSize();
                } else {
                    waitj = getScheduleSize();
                }
                waiting.add(waitj);
                running.add(getRunningJobs());

                CommonObjectives.getActualUsage();
                requested.add(reqc);
                used.add(busyc);
                availCPUs.add(getAvailPEs());
                boolean names = cl_names.size() > 0;
                for (int c = 0; c < totalResource; c++) {
                    cl_util.add(CommonObjectives.getClusterUsage(c));
                    cl_status.add(CommonObjectives.getClusterStatus(c));
                    if (!names) {
                        cl_names.add(((ResourceInfo) resourceInfoList.get(c)).resource.getResourceName());
                        cl_CPUs.add(((ResourceInfo) resourceInfoList.get(c)).resource.getNumPE());
                    }
                }

            } catch (IOException ex) {
                ex.printStackTrace();
            }
            anim = windows.get(0);
            anim.reDrawUsage(days, util, Math.round(clock()));

            anim = windows.get(1);
            anim.reDrawClusterUsage(days, cl_util, Math.round(clock()), totalResource, cl_names);

            anim = windows.get(2);
            anim.reDrawRW(days, waiting, running, Math.round(clock()));

            anim = windows.get(3);

            anim.reDrawRU(days, requested, used, Math.round(clock()), getAvailPEs(), availCPUs);

            anim = windows.get(6);
            anim.reDrawClusterUsageCol(days, cl_util, Math.round(clock()), totalResource, cl_names, cl_CPUs);

            //anim = windows.get(7);
            //anim.reDrawClusterStatusCol(days, cl_status, Math.round(clock()), totalResource, cl_names);
            anim = windows.get(8);
            anim.reDrawClusterWeightedUsageCol(days, cl_util, Math.round(clock()), totalResource, cl_names, cl_CPUs);

            day_usage = 0.0;
        }
        if (week_count == 7) {
            week_usage = Math.round((week_usage / 168.0) * 100) / 100.0;
            try {
                out.writeString(folder_prefix + "/week_usage_" + suff + ".csv", Math.round(day_count / 7) + "\t" + week_usage);

            } catch (IOException ex) {
                ex.printStackTrace();
            }
            week_usage = 0.0;
            week_count = 0;
        }

        hours.add(hour_count);
        day_util.add(CommonObjectives.getActualUsage());
        anim = windows.get(4);
        LinkedList<Integer> h = new LinkedList(hours);
        LinkedList<Double> day_u = new LinkedList(day_util);
        anim.reDrawDayUsage(h, day_u, Math.round(clock()));

        boolean names = cl_names.size() > 0;
        hour_cl_util.clear();
        for (int c = 0; c < totalResource; c++) {
            hour_cl_util.add(CommonObjectives.getClusterUsage(c));
            if (!names) {
                cl_names.add(((ResourceInfo) resourceInfoList.get(c)).resource.getResourceName());
                cl_CPUs.add(((ResourceInfo) resourceInfoList.get(c)).resource.getNumPE());
            }
        }
        LinkedList<Double> h_cl_u = new LinkedList(hour_cl_util);

        anim = windows.get(5);
        anim.reDrawDayClusterUsage(h, h_cl_u, Math.round(clock()), totalResource, cl_names);

        // write out current results
        try {
            out.writeString(folder_prefix + "/actual_usage_" + suff + ".csv", day_count + "\t" + CommonObjectives.getActualUsage());
            out.writeString(folder_prefix + "/running_" + suff + ".csv", day_count + "\t" + getRunningJobs());
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        day_usage += CommonObjectives.getActualUsage();
        week_usage += CommonObjectives.getActualUsage();

        // write out active_scheduling_queue length (not to be used for schedule-based algorithms)
        if (algorithm < 10) {
            try {
                out.writeString(folder_prefix + "/waiting_" + suff + ".csv", day_count + "\t" + getQueueSize());
            } catch (IOException ex) {
                ex.printStackTrace();
            }

        } else {
            // write out schedule size (do not use for active_scheduling_queue-based algorithms)
            try {
                out.writeString(folder_prefix + "/waiting_" + suff + ".csv", day_count + "\t" + getScheduleSize());
            } catch (IOException ex) {
                ex.printStackTrace();
            }

        }
    }

    private boolean updateDAGqueueUponJobComplete(ComplexGridlet gl) {
        boolean scheduleNow = false;
        //first, remove this job from unfinished predecessors
        int id = gl.getGridletID();
        if (unfinished_predecessors.contains(id)) {
            int index = unfinished_predecessors.indexOf(id);
            System.out.println("[SCHEDULER] Job " + id + " from " + gl.getArchRequired() + " is completed and removed from the list of unfinished predecessors [from position: " + index + "] at sim. time = " + GridSim.clock());
            unfinished_predecessors.remove(index);
        }
        // next, check if waiting jobs have been waiting for it (then remove it from their list)
        for (int i = 0; i < DAG_queue.size(); i++) {
            GridletInfo gi = DAG_queue.get(i);
            if (gi.getPrecedingJobs().contains(id)) {
                int index = gi.getPrecedingJobs().indexOf(id);
                gi.getPrecedingJobs().remove(index);
                if (gi.getPrecedingJobs().size() == 0) {
                    DAG_queue.remove(gi);
                    System.out.println("[SCHEDULER] Job " + gi.getID() + " from " + gi.getGridlet().getArchRequired() + " is now added back to normal queue since all predecessors are completed. Sim. time = " + GridSim.clock());
                    ExperimentSetup.policy.addNewJob(gi);
                    scheduleNow = true;
                    i--;
                }
            }
        }
        return scheduleNow;
    }
}
