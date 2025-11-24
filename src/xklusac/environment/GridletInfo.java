package xklusac.environment;

import gridsim.GridSim;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import xklusac.extensions.ProcessorComparator;
//import gridsim.*;

/**
 * Class GridletInfo<p>
 * This class creates an object handling information about gridlets. It use set
 * / get methods to set / get information about gridlet. It stores various
 * information of real gridlet. If more information required, this is the right
 * place to store them (see set/getTardiness(gridletInfo) method) rather then
 * changing ComplexGridlet class (if possible).
 *
 * @author Dalibor Klusacek
 */
public class GridletInfo {

    public String machines_used = "";
    /**
     * owner id
     */
    private int ownerID;
    /**
     * gridlet id
     */
    private int ID;
    /**
     * selected resource id
     */
    private int resourceID;
    /**
     * gridlet status
     */
    private int status;
    /**
     * computational length
     */
    private double length;
    /**
     * not used
     */
    private double finishedSoFar;
    /**
     * not used
     */
    private double cost;
    /**
     * not used
     */
    private double completitionFactor;
    /**
     * link to original gridlet
     */
    private ComplexGridlet gl;
    /**
     * architecture required by the gridlet
     */
    private String archRequired;
    /**
     * OS required by the gridlet
     */
    private String osRequired;
    /**
     * release date (start time)
     */
    private double release_date;
    /**
     * due date (deadline)
     */
    private double due_date;
    /**
     * Expected tardiness calculated by the scheduler
     */
    private double tardiness;
    /**
     * It denotes this dynamicaly changing information: time_to_release =
     * max(0.0, (arrival_time + release_date) - current_time)
     */
    private double time_to_release;
    /**
     * gridlet priority
     */
    private double priority;
    /**
     * number of PEs to run this gridlet
     */
    private int numPE;
    /**
     * expected finish time - for schedule only
     */
    private double expectedFinishTime;
    /**
     * expected start time - for schedule only
     */
    private double expectedStartTime;

    /**
     * expected wait time - for schedule only
     */
    private double expectedWaitTime;
    /**
     * estimatedLength length - not used
     */
    private double estimatedLength;
    /**
     * machine's PE rating used to estimate the gridlet lenght -not used
     */
    private double estimatedMachine;
    /**
     * queue where this gridlet was submitted
     */
    private String queue;
    private String properties;
    private LinkedList<Integer> PEs = new LinkedList();
    private List<Integer> plannedPEs = Collections.synchronizedList(new ArrayList());
    private List<Integer> lastPlannedPEs = new ArrayList();
    double last_alloc_time = -1.0;
    double last_node_time = -1.0;
    double last_predicted_start_time = -1.0;
    private String user = "";
    private double avg_length = 0.0;
    private double last_length = 0.0;
    private double avg_perc_length = 0.0;
    private long jobLimit = 0;
    private double percentage;
    private boolean init;
    private long ram;
    private int numNodes;
    private int ppn;
    private boolean pinned;
    private ArrayList<Integer> precedingJobs = null;

    private HashMap<Integer, Boolean> resourceSuitable;

    private boolean executable;
    private int gpus_per_node = 0;
    private int groupID = 0;
    private boolean checkpoint_limit_eligible = true;
    private boolean preempted = true;

    /**
     * Creates a new instance of GridletInfo object based on the "real" gridlet
     *
     * @param gl - Gridlet - the constructor gets the important informations
     * about gridlet and sets them to inner variables
     */
    public GridletInfo(ComplexGridlet gl) {

        this.setOwnerID(gl.getUserID());
        this.setID(gl.getGridletID());
        this.setResourceID(gl.getResourceID());
        this.setStatus(gl.getGridletStatus());
        this.setLength(gl.getGridletLength());
        this.setFinishedSoFar(gl.getGridletFinishedSoFar());
        this.setCompletitionFactor(gl.getGridletFinishedSoFar() / gl.getGridletLength());
        this.setGridlet(gl);
        this.setOsRequired(gl.getOpSystemRequired());
        this.setArchRequired(gl.getArchRequired());
        this.setRelease_date(gl.getRelease_date());
        this.setDue_date(gl.getDue_date());
        this.setTardiness(0.0);
        this.setTime_to_release(0.0);
        this.setPriority(gl.getPriority());
        this.setNumPE(gl.getNumPE());
        this.setExpectedFinishTime(GridSim.clock() + gl.getJobLimit());
        this.setEstimatedLength(gl.getEstimatedLength());
        this.setEstimatedMachine(gl.getEstimatedMachine());
        this.setQueue(gl.getQueue());
        this.setExpectedStartTime(-1.0);
        this.setProperties(gl.getProperties());
        this.setUser(gl.getUser());
        this.setAvg_length(0.0);
        this.setJobLimit(gl.getJobLimit());
        this.setPercentage(gl.getPercentage());
        this.setInit(true);
        this.setRam(gl.getRam());
        this.setNumNodes(gl.getNumNodes());
        this.setPpn(gl.getPpn());
        this.setResourceSuitable(new HashMap());
        this.setExpectedWaitTime(this.getExpectedWaitTime());
        this.setPinned(false);
        this.setPrecedingJobs(gl.getPrecedingJobs());
        this.setExecutable(false);
        this.setGpus_per_node(gl.getGpus_per_node());
        this.setGroupID(gl.getGroupID());
        this.setCheckpoint_limit_eligible(true);
        this.setPreempted(gl.isPreempted());

    }

    /**
     * Getter method
     */
    public int getOwnerID() {
        return ownerID;
    }

    /**
     * Setter method
     */
    public void setOwnerID(int ownerID) {
        this.ownerID = ownerID;
    }

    /**
     * Getter method
     */
    public int getID() {
        return ID;
    }

    /**
     * Setter method
     */
    public void setID(int ID) {
        this.ID = ID;
    }

    /**
     * Getter method
     */
    public int getResourceID() {
        return resourceID;
    }

    /**
     * Setter method
     */
    public void setResourceID(int resourceID) {
        this.resourceID = resourceID;
    }

    /**
     * Getter method
     */
    public int getStatus() {
        this.status = getGridlet().getGridletStatus(); // essential for fresh information
        return status;
    }

    /**
     * Setter method
     */
    public void setStatus(int status) {
        this.status = status;
    }

    /**
     * Getter method
     */
    public double getLength() {
        return length;
    }

    /**
     * Setter method
     */
    public void setLength(double length) {
        this.length = length;
    }

    /**
     * Getter method
     */
    public double getFinishedSoFar() {
        this.finishedSoFar = getGridlet().getGridletFinishedSoFar(); // essential for fresh information
        return finishedSoFar;
    }

    /**
     * Setter method
     */
    public void setFinishedSoFar(double finishedSoFar) {
        this.finishedSoFar = finishedSoFar;
    }

    /**
     * Getter method
     */
    public double getCompletitionFactor() {
        return completitionFactor;
    }

    /**
     * Setter method
     */
    public void setCompletitionFactor(double completitionFactor) {
        this.completitionFactor = completitionFactor;
    }

    /**
     * Getter method
     */
    public String getArchRequired() {
        return archRequired;
    }

    /**
     * Setter method
     */
    public void setArchRequired(String osRequired) {
        this.archRequired = osRequired;
    }

    /**
     * Getter method
     */
    public String getOsRequired() {
        return osRequired;
    }

    /**
     * Setter method
     */
    public void setOsRequired(String osRequired) {
        this.osRequired = osRequired;
    }

    /**
     * Getter method
     */
    public ComplexGridlet getGridlet() {
        return gl;
    }

    /**
     * Setter method
     */
    public void setGridlet(ComplexGridlet gl) {
        this.gl = gl;
    }

    /**
     * Getter method
     */
    public double getRelease_date() {
        return release_date;
    }

    /**
     * Setter method
     */
    public void setRelease_date(double release_date) {
        this.release_date = release_date;
    }

    /**
     * Getter method
     */
    public double getDue_date() {
        return due_date;
    }

    /**
     * Setter method
     */
    public void setDue_date(double due_date) {
        this.due_date = due_date;
    }

    /**
     * Getter method
     */
    public double getTardiness() {
        return tardiness;
    }

    /**
     * Setter method
     */
    public void setTardiness(double tardiness) {
        this.tardiness = tardiness;
    }

    /**
     * Getter method
     */
    public double getTime_to_release() {
        return time_to_release;
    }

    /**
     * Setter method
     */
    public void setTime_to_release(double time_to_release) {
        this.time_to_release = time_to_release;
    }

    /**
     * Getter method
     */
    public double getPriority() {
        double ff = ExperimentSetup.users.get(this.getUser()).getFairshare_factor();
        return ff;
    }

    /**
     * Setter method
     */
    public void setPriority(double priority) {
        this.priority = priority;
    }

    /**
     * Getter method
     */
    public int getNumPE() {
        return numPE;
    }

    /**
     * Setter method
     */
    public void setNumPE(int numPE) {
        this.numPE = numPE;
    }

    /**
     * Getter method
     */
    public double getExpectedFinishTime() {
        return expectedFinishTime;
    }

    /**
     * Setter method
     */
    public void setExpectedFinishTime(double expectedFinishTime) {
        this.expectedFinishTime = expectedFinishTime;
        this.getGridlet().setExpectedFinishTime(expectedFinishTime);
    }

    /**
     * Getter method
     */
    public double getEstimatedLength() {
        return estimatedLength;
    }

    /**
     * Setter method
     */
    public void setEstimatedLength(double estimated) {
        this.estimatedLength = estimated;
    }

    /**
     * Setter method
     */
    public double getEstimatedMachine() {
        return estimatedMachine;
    }

    /**
     * Setter method
     */
    public void setEstimatedMachine(double estimatedMachine) {
        this.estimatedMachine = estimatedMachine;
    }

    /**
     * Getter method
     */
    public String getQueue() {
        return queue;
    }

    /**
     * Setter method
     */
    public void setQueue(String queue) {
        this.queue = queue;
    }

    /**
     * Getter method
     */
    public double getExpectedStartTime() {
        return expectedStartTime;
    }

    /**
     * Setter method
     */
    public void setExpectedStartTime(double expectedStartTime) {
        //System.out.println(this.getID() + " setting start to: " + expectedStartTime + " at time: " + GridSim.clock() + " plan: " + this.getPlannedPEsString());
        this.expectedStartTime = expectedStartTime;
    }

    public String getProperties() {
        return properties;
    }

    public void setProperties(String properties) {
        this.properties = properties;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public double getAvg_length() {
        return avg_length;
    }

    public void setAvg_length(double avg_length) {
        this.avg_length = avg_length;
    }

    public ArrayList<Integer> getPEs() {
        return this.getGridlet().getPEs();
    }

    public void setPEs(ArrayList<Integer> PEs) {
        //System.out.println(this.getID()+" setting not on a resource but from info...");
        Collections.sort(PEs, new ProcessorComparator());
        this.getGridlet().setPEs(PEs);
    }

    public String getPEsString() {
        String pes = "";
        for (int i = 0; i < this.getGridlet().getPEs().size(); i++) {
            if (i < this.getGridlet().getPEs().size() - 1) {
                pes += this.getGridlet().getPEs().get(i) + ",";
            } else {
                pes += this.getGridlet().getPEs().get(i) + "";
            }
        }
        return pes;
    }

    /**
     * @return the plannedPEs
     */
    public List<Integer> getPlannedPEs() {
        return plannedPEs;
    }

    /**
     * @param plannedPEs the plannedPEs to set
     */
    public void setPlannedPEs(ArrayList<Integer> planPEs, String who, String sch) {
        Collections.sort(planPEs, new ProcessorComparator());

        for (int i = 0; i < planPEs.size(); i++) {
            this.plannedPEs.add(planPEs.get(i));
        }
        /*if (this.getID() == 911) {
            System.out.println("job " + this.getID() + ": simtime=" + Math.round(GridSim.clock()) + " setting plan: " + this.getPlannedPEsString() + " | start/finish= " + Math.round(this.getExpectedStartTime()) + " / " + Math.round(this.getExpectedFinishTime()) + " schedule: " + sch);
        }*/
        checkChangeInPlan(who);
    }

    /**
     * @param plannedPEs the plannedPEs to set
     */
    public void setPlannedPEs(ArrayList<Integer> planPEs, String who) {
        Collections.sort(planPEs, new ProcessorComparator());

        for (int i = 0; i < planPEs.size(); i++) {
            this.plannedPEs.add(planPEs.get(i));
        }
        /*if (this.getID() == 911) {
            System.out.println("job " + this.getID() + ": simtime=" + Math.round(GridSim.clock()) + " setting plan: " + this.getPlannedPEsString() + " | start/finish= " + Math.round(this.getExpectedStartTime()) + " / " + Math.round(this.getExpectedFinishTime()) + " schedule: " + sch);
        }*/
        checkChangeInPlan(who);
    }

    public String getPlannedPEsString() {
        String pes = "";
        for (int i = 0; i < plannedPEs.size(); i++) {
            if (i < this.plannedPEs.size() - 1) {
                pes += plannedPEs.get(i) + ",";
            } else {
                pes += plannedPEs.get(i) + "";
            }
        }
        return pes;
    }

    /**
     * Returns (estimated) job runtime. The actual method for calculating the
     * rutime is chosen via experiment setup. Generally, it is either the exact
     * runtime or the runtime estimate. Several methods for estimate calculation
     * are supported.
     */
    public double getJobRuntime(int peRating) {
        if (ExperimentSetup.estimates) {
            if (ExperimentSetup.use_PercentageLength) {
                ExperimentSetup.scheduler.updateGridletWalltimeEstimateApproximation(this);
                return Math.min(jobLimit, Math.max(0.0, (this.getAvg_perc_length() / peRating)));

            } else if (ExperimentSetup.use_MaxPercentageLength) {
                // estimate is calculated using 5 recent jobs of that user - for each a relative usage (percentage) of requested time is calculated. 
                // Then, the max usage among those 5 jobs is used as a factor to multiply the new user estimate (conservative strategy).

                // if the estimate is known - do not change it except for making it longer
                if (this.getGridlet().getPredicted_runtime() > 0 && this.getGridlet().getExecStartTime() > 0.0) {
                    // job is underestimated so prolong
                    if ((this.getExpectedFinishTime() + 1.0) <= GridSim.clock()) {
                        this.getGridlet().setProlonged(this.getGridlet().getProlonged() + 1);
                        double minPestimate = this.getGridlet().getPredicted_runtime();
                        double minPestimate_old = minPestimate;
                        double overtime = GridSim.clock() - this.getExpectedFinishTime();
                        int est_multiplier = 1;
                        while (overtime > est_multiplier * minPestimate) {
                            est_multiplier++;
                        }
                        minPestimate += minPestimate * est_multiplier;
                        this.getGridlet().setPredicted_runtime(Math.round(Math.min(jobLimit, minPestimate)));
                        //if(this.getID()==94704)
                        //        System.out.println(this.getID()+" is extended pred: "+minPestimate_old+" new: "+(Math.round(this.getGridlet().getPredicted_runtime())));
                        double run_time = GridSim.clock() - this.getGridlet().getExecStartTime();
                        double time_remaining = Math.max(0.0, (this.getGridlet().getPredicted_runtime() - run_time));
                        this.setExpectedFinishTime((GridSim.clock() + time_remaining));
                        //System.out.println(this.getID()+" started at: "+this.getGridlet().getExecStartTime()+" running for: "+run_time+" extended from: "+minPestimate_old+" to new est: "+minPestimate+" till "+this.getExpectedFinishTime()+" by multiplier: "+est_multiplier+
                        //        " time remain: "+time_remaining+" at clock: "+GridSim.clock()+" due to overtime: "+overtime);
                    }
                } else {
                    User u = ExperimentSetup.users.get(this.getUser());
                    // we check previous 5 jobs and choose the one most using the walltime using this "percentage" value (not a real percentage). 
                    // Then we apply this ratio on this new job walltime limit and call it the expected runtime. 
                    double avg_perc = u.getMinPercentage();
                    double avg_l = (this.getEstimatedLength() / avg_perc) * ExperimentSetup.predictor_increase;
                    this.getGridlet().setPredicted_runtime(Math.round(Math.min(jobLimit, Math.max(0.0, (avg_l / peRating)))));
                    // if the first prediction is underestimated - record the difference
                    if (this.getGridlet().getUnderestimated_by() < 1.0) {
                        double real_run = Math.min(jobLimit, Math.max(0.0, (this.getLength() / peRating)));
                        double pred = this.getGridlet().getPredicted_runtime();
                        if (real_run > pred) {
                            //if(this.getID()==94704)
                            //    System.out.println(this.getID()+" is short. real: "+real_run+" pred: "+pred+" diff: "+(Math.round(real_run - pred)));
                            this.getGridlet().setUnderestimated_by(Math.round(real_run - pred));
                        }
                    }

                }
                return Math.round(Math.min(jobLimit, this.getGridlet().getPredicted_runtime()));

            } else if (ExperimentSetup.use_AvgLength) {
                // estimate = average runtime of previous jobs
                ExperimentSetup.scheduler.updateGridletWalltimeEstimateApproximation(this);
                //System.out.println("avg length ===== "+Math.round(this.getAvg_length() / peRating)+" ? "+Math.round(this.getLast_length() / peRating));
                return Math.min(jobLimit, Math.max(0.0, (this.getAvg_length() / peRating)));
            } else if (ExperimentSetup.use_LastLength) {
                ExperimentSetup.scheduler.updateGridletWalltimeEstimateApproximation(this);
                //System.out.println(this.getID()+" last length = "+Math.min(jobLimit, Math.max(0.0, (this.getLast_length() / peRating)))+" / job limit = "+jobLimit+" user = "+this.getUser());
                return Math.min(jobLimit, Math.max(0.0, ((this.getLast_length() / peRating))));
            } else {
                // return original runtime estimate                
                return jobLimit;
            }
        } else {
            // return real (exact) runtime
            return Math.min(jobLimit, Math.max(0.0, (this.getLength() / peRating)));
        }

    }

    public double getJobLimit() {
        return this.jobLimit;
    }

    public void setJobLimit(long jobLimit) {
        this.jobLimit = jobLimit;
    }

    public double getLast_length() {
        return last_length;
    }

    public void setLast_length(double last_length) {
        this.last_length = last_length;
    }

    public double getPercentage() {
        return percentage;
    }

    public void setPercentage(double percentage) {
        this.percentage = percentage;
    }

    public boolean isInit() {
        return init;
    }

    public void setInit(boolean init) {
        this.init = init;
    }

    /**
     * @return the ram
     */
    public long getRam() {
        return ram;
    }

    /**
     * @param ram the ram to set
     */
    public void setRam(long ram) {
        this.ram = ram;
    }

    /**
     * @return the numNodes
     */
    public int getNumNodes() {
        return numNodes;
    }

    /**
     * @param numNodes the numNodes to set
     */
    public void setNumNodes(int numNodes) {
        this.numNodes = numNodes;
    }

    /**
     * @return the ppn
     */
    public int getPpn() {
        return ppn;
    }

    /**
     * @param ppn the ppn to set
     */
    public void setPpn(int ppn) {
        this.ppn = ppn;
    }

    /**
     * @return the resourceSuitable
     */
    public HashMap getResourceSuitable() {
        return resourceSuitable;
    }

    /**
     * @param resourceSuitable the resourceSuitable to set
     */
    public void setResourceSuitable(HashMap resourceSuitable) {
        this.resourceSuitable = resourceSuitable;
    }

    /**
     * @return the avg_perc_length
     */
    public double getAvg_perc_length() {
        return avg_perc_length;
    }

    /**
     * @param avg_perc_length the avg_perc_length to set
     */
    public void setAvg_perc_length(double avg_perc_length) {
        this.avg_perc_length = avg_perc_length;
    }

    /**
     * @return the expectedWaitTime
     */
    public double getExpectedWaitTime() {
        return Math.max(0.0, this.getExpectedStartTime() - this.getGridlet().getArrival_time());
    }

    /**
     * @param expectedWaitTime the expectedWaitTime to set
     */
    public void setExpectedWaitTime(double expectedWaitTime) {
        this.expectedWaitTime = expectedWaitTime;
    }

    public void checkChangeInPlan(String who) {
        Collections.sort(plannedPEs, new ProcessorComparator());
        Collections.sort(lastPlannedPEs, new ProcessorComparator());
        if (plannedPEs.size() == lastPlannedPEs.size()) {

            double abs_diff = Math.abs(Math.round(this.getExpectedStartTime()) - Math.round(last_predicted_start_time));
            if (abs_diff > 3.0) {
                //System.out.println(this.getID() + ": Start time is not equal since last planning: " + this.getExpectedStartTime() + "<>" + last_predicted_start_time + " time diff= " + (this.getExpectedStartTime() - last_predicted_start_time)+" at time: "+GridSim.clock());
                last_alloc_time = GridSim.clock();
            }
            for (int i = 0; i < plannedPEs.size(); i++) {
                if (!plannedPEs.get(i).equals(lastPlannedPEs.get(i))) {
                    //System.out.println(this.getID() + ": CPU IDs are not equal since last planning: " + plannedPEs.get(i) + "<>" + lastPlannedPEs.get(i) + " time diff= " + (GridSim.clock() - last_alloc_time)+" start time diff = "+Math.round(this.getExpectedStartTime()-last_predicted_start_time)+" sec. at simtime = "+GridSim.clock());
                    last_alloc_time = GridSim.clock();
                    last_node_time = GridSim.clock();
                    break;
                }
            }

        } else {
            last_alloc_time = GridSim.clock();
            last_node_time = GridSim.clock();
        }
        lastPlannedPEs.clear();
        for (int i = 0; i < plannedPEs.size(); i++) {
            lastPlannedPEs.add(plannedPEs.get(i));
        }
        last_predicted_start_time = this.getExpectedStartTime();
        this.getGridlet().setLast_alloc_time(last_alloc_time);
        this.getGridlet().setLast_node_time(last_node_time);
        //System.out.println(this.getID() + "--------------- "+who+" ---------------: "+GridSim.clock()+" CPUs="+this.getPEsString()+" start="+this.getExpectedStartTime()); 

    }

    /**
     * @return the pinned
     */
    public boolean isPinned() {
        return pinned;
    }

    /**
     * @param pinned the pinned to set
     */
    public void setPinned(boolean pinned) {
        this.pinned = pinned;
    }

    /**
     * @return the precedingJobs
     */
    public ArrayList<Integer> getPrecedingJobs() {
        return precedingJobs;
    }

    /**
     * @param precedingJobs the precedingJobs to set
     */
    public void setPrecedingJobs(ArrayList<Integer> precedingJobs) {
        this.precedingJobs = precedingJobs;
    }

    /**
     * @return the executable
     */
    public boolean isExecutable() {
        return executable;
    }

    /**
     * @param executable the executable to set
     */
    public void setExecutable(boolean executable) {
        this.executable = executable;
    }

    /**
     * @return the gpus_per_node
     */
    public int getGpus_per_node() {
        return gpus_per_node;
    }

    /**
     * @param gpus_per_node the gpus_per_node to set
     */
    public void setGpus_per_node(int gpus_per_node) {
        this.gpus_per_node = gpus_per_node;
    }

    /**
     * @return the groupID
     */
    public int getGroupID() {
        return groupID;
    }

    /**
     * @param groupID the groupID to set
     */
    public void setGroupID(int groupID) {
        this.groupID = groupID;
    }

    /**
     * This method checks whether this job can checkpoint other jobs wrt. to its queue-limits, group and user quota (if it can run right now)
     * @return the checkpoint_limit_eligible
     */
    public boolean isCheckpoint_limit_eligible() {
        return checkpoint_limit_eligible;
    }

    /**
     * @param checkpoint_limit_eligible the checkpoint_limit_eligible to set
     */
    public void setCheckpoint_limit_eligible(boolean checkpoint_limit_eligible) {
        this.checkpoint_limit_eligible = checkpoint_limit_eligible;
    }

    /**
     * @return the preempted
     */
    public boolean isPreempted() {
        return preempted;
    }

    /**
     * @param preempted the preempted to set
     */
    public void setPreempted(boolean preempted) {
        this.preempted = preempted;
    }

}
