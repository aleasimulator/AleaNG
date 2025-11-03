/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package xklusac.environment;

/**
 *
 * @author Dalibor
 */
import alea.core.AleaSimTags;
import eduni.simjava.Sim_event;
import gridsim.*;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.util.Random;
import xklusac.extensions.*;
import eduni.simjava.distributions.Sim_normal_obj;
import java.util.ArrayList;

/**
 * Class SWFLoader<p>
 * Loads jobs dynamically over time from the file. Then sends these gridlets to
 * the scheduler. SWF stands for Standard Workloads Format (SWF).
 *
 * @author Dalibor Klusacek
 */
public class SWFLoader extends GridSim {

    /**
     * input
     */
    Input r = new Input();
    /**
     * current folder
     */
    String folder_prefix = "";
    /**
     * buffered reader
     */
    BufferedReader br = null;
    /**
     * total number of jobs in experiment
     */
    int total_jobs = 0;
    /**
     * start time (for UNIX epoch converting)
     */
    long start_time = -1;
    long time_diff = -1;
    /**
     * number of PEs in the "biggest" resource
     */
    int maxPE = 1;
    /**
     * minimal PE rating of the slowest resource
     */
    int minPErating = 1;
    int maxPErating = 1;
    /**
     * gridlet counter
     */
    int current_gl = 0;
    /**
     * data set name
     */
    String data_set = "";
    /**
     * counter of failed jobs (as stored in the GWF file)
     */
    int fail = 0;
    int help_j = 0;
    Random rander = new Random(4567);
    double last_delay = 0.0;
    Sim_normal_obj norm;
    double prevl = -1.0;
    double preve = -1.0;
    int prevc = -1;
    long prevram = -1;
    long prev_job_limit = -1;
    int count = 1;

    /**
     * Creates a new instance of JobLoader
     */
    public SWFLoader(String name, double baudRate, int total_jobs, String data_set, int maxPE, int minPErating, int maxPErating) throws Exception {
        super(name, baudRate);
        System.out.println(name + ": openning all jobs from " + data_set);

        folder_prefix = System.getProperty("user.dir");

        System.out.println("Opening job file at: " + folder_prefix + ExperimentSetup.data_sets_dir + data_set );
        br = r.openFile(new File(folder_prefix + ExperimentSetup.data_sets_dir + data_set));
        this.total_jobs = total_jobs;
        this.maxPE = maxPE;
        this.minPErating = minPErating;
        this.maxPErating = maxPErating;
        this.data_set = data_set;
        this.norm = new Sim_normal_obj("normal distr", 0.0, 5.0, (121 + ExperimentSetup.rnd_seed));

    }

    /**
     * Reads jobs from data_set file and sends them to the Scheduler entity
     * dynamically over time.
     */
    public void body() {
        super.gridSimHold(10.0);    // hold by 10 second

        while (current_gl < total_jobs) {

            Sim_event ev = new Sim_event();
            sim_get_next(ev);

            if (ev.get_tag() == AleaSimTags.EVENT_WAKE) {

                ComplexGridlet gl = readGridlet(current_gl);
                current_gl++;
                if (gl == null && current_gl < total_jobs) {
                    super.sim_schedule(this.getEntityId(this.getEntityName()), 0.0, AleaSimTags.EVENT_WAKE);
                    continue;
                } else if (gl == null && current_gl >= total_jobs) {
                    continue;
                }
                // to synchronize job arrival wrt. the data set.
                double delay = Math.max(0.0, (gl.getArrival_time() - super.clock()));
                // some time is needed to transfer this job to the scheduler, i.e., delay should be delay = delay - transfer_time. Fix this in the future.
                //System.out.println("Sending: "+gl.getGridletID());
                last_delay = delay;
                super.sim_schedule(this.getEntityId("Alea_Job_Scheduler"), delay, AleaSimTags.GRIDLET_INFO, gl);

                delay = Math.max(0.0, (gl.getArrival_time() - super.clock()));
                if (current_gl < total_jobs) {
                    // use delay - next job will be loaded after the simulation time is equal to the previous job arrival.
                    super.sim_schedule(this.getEntityId(this.getEntityName()), delay, AleaSimTags.EVENT_WAKE);
                }

                continue;
            }
        }
        System.out.println("Shuting down JOB LOADER - last job loaded = " + current_gl + " of " + total_jobs + " expected jobs.");
        super.sim_schedule(this.getEntityId("Alea_Job_Scheduler"), Math.round(last_delay + 2), AleaSimTags.SUBMISSION_DONE, (current_gl));
        Sim_event ev = new Sim_event();
        sim_get_next(ev);

        if (ev.get_tag() == GridSimTags.END_OF_SIMULATION) {
            System.out.println("Shuting down the " + data_set + " JOB LOADER ... with: " + fail + " unparsable or otherwise skipped jobs");
        }
        shutdownUserEntity();
        super.terminateIOEntities();

    }

    /**
     * Reads one job from file.
     */
    private ComplexGridlet readGridlet(int j) {
        String[] values = null;
        String line = "";

        //System.out.println("Read job "+j);
        if (j == 0) {
            while (true) {
                try {
                    //System.out.println(j+":"+line+"");
                    line = br.readLine();
                    values = line.split("\t");
                } catch (IOException ex) {
                    ex.printStackTrace();
                }
                if (!values[0].contains(";") && !values[0].contains("id")) {
                    //System.out.println(j+":"+line+": error --- "+values[0]);
                    if (line.charAt(0) == ' ') {
                        line = line.substring(1);
                    }
                    if (line.charAt(0) == ' ') {
                        line = line.substring(1);
                    }
                    if (line.charAt(0) == ' ') {
                        line = line.substring(1);
                    }
                    if (line.charAt(0) == ' ') {
                        line = line.substring(1);
                    }
                    values = line.split("\\s+");
                    break;
                } else {
                    if (Scheduler.start_date < 0 && line.contains("; UnixStartTime: ")) {
                        Scheduler.start_date = Long.parseLong(line.replace("; UnixStartTime: ", ""));
                        String dated = new java.text.SimpleDateFormat("HH:mm dd-MM-yyyy").format(new java.util.Date(Scheduler.start_date * 1000));
                        System.out.println("Start time of workload is UnixStartTime: " + Scheduler.start_date + ", [" + dated + "]");
                    }
                    if (line.contains("; number of jobs: ")) {
                        int jobs = Integer.parseInt(line.replace("; number of jobs: ", ""));
                        if(total_jobs > jobs){
                            System.out.println("\n================== !!! WARNING !!! ==================");
                            System.out.println("AleaNG will read all " + jobs+" jobs from workload file. \n(Shortening the number specified in config: "+total_jobs+" jobs)");
                            System.out.println("================== !!! WARNING !!! ==================\n");
                            total_jobs = jobs;
                        }else{
                            System.out.println("\n================== !!! WARNING !!! ==================");
                            System.out.println("AleaNG will read only " + total_jobs+" jobs from workload file. \n(Shortening the number specified in file: "+jobs+" jobs)");
                            System.out.println("================== !!! WARNING !!! ==================\n");
                        }
                    }
                    //System.out.println("comment--- "+values[0]);
                }
            }
        } else {
            try {
                line = br.readLine();
                //System.out.println(">"+line+"<");
                if (line.charAt(0) == ' ') {
                    line = line.substring(1);
                }
                if (line.charAt(0) == ' ') {
                    line = line.substring(1);
                }
                if (line.charAt(0) == ' ') {
                    line = line.substring(1);
                }
                if (line.charAt(0) == ' ') {
                    line = line.substring(1);
                }
                //System.out.println("error1 = "+line+" at gi = "+j);
                values = line.split("\\s+");

            } catch (IOException ex) {
                System.out.println("error = " + values[0] + " at gi = " + j);
                ex.printStackTrace();
            }
        }

        // such line is not a job description - it is a typo in the SWF file
        if (values.length < 5 || values[1].equals("-1")) {
            fail++;
            System.out.println(j + " returning: null " + values[0]);
            return null;
        }

        // such job failed or was cancelled and no info about runtime or numCPU is available therefore we skip it
        if (values[3].equals("-1") || values[4].equals("-1")) {
            fail++;
            //System.out.println("returning: null2 ");
            return null;
        }
        //System.out.println(values[0]+"+"+values[1]+"+"+values[2] + ": Number parsing error: " + values[4]);
        int id = Integer.parseInt(values[0]);
        int numCPU;
        try {
            numCPU = Integer.parseInt(values[4]);
            if (data_set.equals("thunder.swf")) {
                numCPU = Math.max(1, (numCPU / 4));
                //System.out.println(values[0] + ": wants: " + values[4]+" gets "+numCPU);
            }
        } catch (NumberFormatException ex) {
            System.out.println(values[0] + ": Number parsing error: " + values[4]);
            //ex.printStackTrace();
            numCPU = 1;
        }

        // we do not allow more PEs for one job than there is on the "biggest" machine.
        // Co-allocation is only supported over one cluster (GridResource) by now.
        /*if (numCPU > maxPE) {
            numCPU = maxPE;

        }*/
        long arrival = 0;
        // synchronize GridSim's arrivals with the UNIX epoch format as given in GWF
        if (start_time < 0) {
            //System.out.println("prvni: "+j+" start at:"+values[1]+" line="+line);
            start_time = Integer.parseInt(values[1]);
            if (start_time < Math.round(GridSim.clock())) {
                arrival = Math.round(GridSim.clock());
                time_diff = start_time - Math.round(GridSim.clock());
            } else {
                arrival = Math.round(GridSim.clock());
                time_diff = Math.round(Integer.parseInt(values[1]) - GridSim.clock());
            }
            //System.out.println(id+": serizujeme..."+arrival+" diff "+time_diff);
        } else {
            arrival = ((Integer.parseInt(values[1]) - time_diff));
            //System.out.println(id+": pokracujeme..."+arrival+" diff "+time_diff);

        }
        arrival = Math.round((arrival * 1.0) / ExperimentSetup.arrival_rate_multiplier);

        // minPErating is the default speed of the slowest machine in the data set        
        double length = Math.round((Integer.parseInt(values[3])) * maxPErating);

        // active_scheduling_queue name
        int queue = Integer.parseInt(values[14]);

        // requested RAM = KB per node (not CPU)
        long ram = Long.parseLong(values[9]);
        if (ram == -1) {

            ram = 1;

        }
        if (!ExperimentSetup.use_RAM) {
            ram = 0;
        }

        long job_limit = 0;
        if (values[8].contains(".")) {
            //System.out.println("old="+values[8]);
            values[8] = values[8].substring(0, values[8].indexOf("."));
            //System.out.println("new="+values[8]);
        }
        job_limit = Integer.parseInt(values[8]);
        if (job_limit < 0) {
            // atlas = 432000
            // thunder = 432000
            if (data_set.equals("thunder.swf")) {
                job_limit = 360000; //~100 hours
                // System.out.println(values[0] + ": limit: " + job_limit);
                ExperimentSetup.max_estim++;
            } else if (data_set.equals("atlas.swf")) {
                job_limit = 73200; //20 hours 20 minutes
                ExperimentSetup.max_estim++;
            } else if (data_set.equals("star.swf")) {
                job_limit = 64800; //18 hours
                ExperimentSetup.max_estim++;
            } else if (data_set.equals("ctc-sp2.swf")) {
                job_limit = 64800; //18 hours
                ExperimentSetup.max_estim++;
            } else if (data_set.equals("blue.swf")) {
                job_limit = 36 * 3600; //2 hours
                ExperimentSetup.max_estim++;
            } else if (data_set.equals("kth-sp2.swf")) {
                job_limit = 14400; //4 hours
                ExperimentSetup.max_estim++;
            } else if (data_set.equals("sandia.swf")) {
                job_limit = 18000; //5 hours
                ExperimentSetup.max_estim++;
            } else {
                job_limit = Integer.parseInt(values[3]);
            }
        }

        double estimatedLength = 0.0;
        if (ExperimentSetup.estimates) {
            //roughest estimate that can be done = active_scheduling_queue limit        
            estimatedLength = Math.round(Math.max((job_limit * maxPErating), length));
            //System.out.println(id+" Estimates "+estimatedLength+" real = "+length);
        } else {
            // exact estimates
            estimatedLength = length;
            //System.out.println(id+" Exact "+estimatedLength);
        }

        String user = values[11];

        //System.out.println(id + " requests " + ram + " KB RAM per " + numCPU + " CPUs, user: " + user + ", length: " + length + " estimatedLength: " + estimatedLength);
        int numNodes = -1;
        int ppn = -1;
        int gpus = 0;
        String properties = "" + values[15];
        if (values.length > 20) {
            //System.out.println(line+" len:"+values[0]);
            properties = values[20];
            gpus = Integer.parseInt(values[18]);

            // PBS-Pro compatible variant
            if (data_set.contains("hpc2n")) {
                ppn = 2;
                if (numCPU < ppn) {
                    ppn = numCPU;
                    numNodes = 1;
                } else if (numCPU % 2 == 1) {
                    ppn = 1;
                    numNodes = numCPU;
                } else {
                    Long nn = Math.round(Math.ceil(numCPU / ppn));
                    numNodes = nn.intValue();
                }
                if (ppn * numNodes != numCPU) {
                    System.out.println(id + ": numNodes value is wrong, CPUs = " + numCPU + " ppn = " + ppn);
                }
            }
            if (ExperimentSetup.allocate_whole_nodes) {
                numNodes = numCPU;
                ppn = 1;
            }

            if (ppn == -1 || numNodes == -1) {
                String[] spec = properties.split("x");
                numNodes = Integer.parseInt(spec[0]);
                ppn = Integer.parseInt(spec[1]);
            }

            if (ppn * numNodes != numCPU) {
                System.out.println(id + ": still CPUs mismatch CPUs = " + numCPU + " ppn = " + ppn + " nodes = " + numNodes);
                numCPU = ppn * numNodes;
            }

        }

        // obsolete and useless
        double perc = norm.sample();

        job_limit = Math.max(1, Math.round(job_limit / ExperimentSetup.runtime_minimizer));
        length = Math.max(1.0, Math.round(length / ExperimentSetup.runtime_minimizer));
        estimatedLength = Math.max(1, Math.round(estimatedLength / ExperimentSetup.runtime_minimizer));

        // manually established - fix it according to your needs
        double deadline = job_limit * 2;

        // no DAGS 
        values[16] = "-1";
        String[] pjobs = values[16].split("&");
        ArrayList<Integer> precedingJobs = new ArrayList();
        for (int pj = 0; pj < pjobs.length; pj++) {
            if (!pjobs[pj].equals("-1")) {
                //System.out.println(id + " has predecessor " + pjobs[pj]);
                int predecessor = Integer.parseInt(pjobs[pj]);
                precedingJobs.add(predecessor);
                if (!ExperimentSetup.scheduler.unfinished_predecessors.contains(predecessor)) {
                    ExperimentSetup.scheduler.unfinished_predecessors.add(predecessor);
                }
            }
        }

        String arch = "RISC";

        int gpus_per_node;
        if (ExperimentSetup.allocate_whole_nodes) {
            gpus_per_node = gpus / numNodes;
        } else {
            gpus_per_node = gpus / numNodes;
            if ((numNodes * gpus_per_node) != gpus) {
                System.out.println(id + " ERROR: gpu and gpu_per_node mismatch: " + numNodes + " numNodes, " + gpus + " gpus, " + gpus_per_node + " gpus_per_node.");
            }
        }

        // DATA for synthetic experiment
        /*
        // big user
        if(id < 72){
            ppn = 28;
            numCPU = 28;
            numNodes = 1;
            length = 3600;
            job_limit = Math.round(length);
            user = "User_B";
            arrival = 100+id;
        }else{
            ppn = 2;
            numCPU = 2;
            numNodes = 1;
            length = 3600;
            job_limit = Math.round(length);
            user = "User_A";
            arrival = 100+id;
        }
        ram = 100;
        gpus_per_node = Math.min(0, gpus_per_node);
         */
        // END of synth experiment
        // DATA for PBS comparison experiment
        /*gpus_per_node = Math.min(0, gpus_per_node);
        length = Math.min(length, 3600*168);
        
        if(numCPU<=4){
            user = "tiny";
        }else if (numCPU>4 && numCPU<=8){
            user = "small";
        
        }else if (numCPU>8 && numCPU<=16){
            user = "medium";
        }else{
            user = "large";
        }
         */
        //System.out.println("su "+user+" -c \"cd; qsub -l walltime=2:00:00 -m n -o /dev/null -e /dev/null -l select=${"+numCPU+"}:ncpus=1:mem=300mb -- /usr/bin/sleep $"+(length)+"\"");
        int group = Integer.parseInt(values[12]);

        /*if (id % 5 == 0) {
            properties = "all";
            queue = 0;
            group = 0;
            user = "0";
            
        } else {
            properties = "normal";
            queue = 2;
            group = 1;
            user = "1";
            if (id % 2 == 0) {
                user = "2";
                queue = 1;
            }

        }*/
        if (!Scheduler.all_queues_names.contains(ExperimentSetup.queues_id_to_name_mapping.get(queue)) && ExperimentSetup.use_multiple_queues) {
            fail++;
            System.out.println("Unknown queue " + queue + " - skipping job " + id);
            return null;
        }

        ComplexGridlet gl = new ComplexGridlet(id, user, job_limit, (length), estimatedLength, 0, 0,
                "Linux", arch, arrival, deadline, 1, numCPU, 0.0, queue, properties, perc, ram, numNodes, ppn, gpus_per_node, precedingJobs, group);

        // and set user id to the Scheduler entity - otherwise it would be returned to the JobLoader when completed.
        //System.out.println("[JOB LOADER] Sending job "+id+" from "+gl.getArchRequired()+" to scheduler. Job has limit = "+job_limit+" seconds,  requires "+numNodes+" nodes each with "+ppn+" CPUs [total "+numCPU+" CPUs]. RAM required per node = "+(ram/(1024.0*1024))+" GB. Sim. time = "+GridSim.clock());
        gl.setUserID(super.getEntityId("Alea_Job_Scheduler"));
        return gl;
    }
}
