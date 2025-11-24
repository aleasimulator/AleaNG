/*
 * Title:        GridSim Toolkit
 * Description:  GridSim (Grid Simulation) Toolkit for Modeling and Simulation
 *               of Parallel and Distributed Systems such as Clusters and Grids
 * Licence:      GPL - http://www.gnu.org/copyleft/gpl.html
 *
 * $Id: Machine.java,v 1.14 2007/08/20 02:13:29 anthony Exp $
 */
package xklusac.environment;

import gridsim.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Hashtable;
import xklusac.extensions.JobCompletionTimeComparator;

/**
 * GridSim Machine class represents an uniprocessor or shared memory
 * multiprocessor machine. It can contain one or more Processing Elements (PEs).
 *
 * @author Manzur Murshed and Rajkumar Buyya
 * @since GridSim Toolkit 1.0 @invariant $none
 */
public class MachineWithRAMandGPUs extends Machine {

    // |PEs| > 1 is SMP (Shared Memory Multiprocessors)    
    private int cpus;
    private long ram;
    private long used_ram;
    private int gpus;
    private int used_gpus;
    private int freeVirtualPE;
    private double[] firstFreeTime;
    //public Hashtable<Integer, Integer> job_gpu_usage = new Hashtable<Integer, Integer>();
    //public Hashtable<Integer, ResGridlet> PE_to_job_mapping = new Hashtable<Integer, ResGridlet>();
    // tells whether this machine is working properly or has failed.
    private boolean failed_;
    public ArrayList<ResGridlet> running_jobs = new ArrayList();
    public ArrayList<Capacity> capacity_list = new ArrayList();
    private double est = 0;
    public int banned_PEs = 0;
    public int banned_GPUs = 0;

    /**
     * Allocates a new Machine object
     *
     * @param id the machine ID
     * @param list list of PEs @pre id > 0 @pre list != null @post $none
     */
    public MachineWithRAMandGPUs(int id, PEList list, long ram, int gpus_per_node) {
        super(id, list);
        this.cpus = list.size();
        this.ram = ram;
        this.used_ram = 0;
        this.gpus = gpus_per_node;
        this.used_gpus = 0;
        this.firstFreeTime = new double[list.size()];
        for (int i = 0; i < firstFreeTime.length; i++) {
            firstFreeTime[i] = 0.0;
            //PE_to_job_mapping.put(i, null);
        }
    }

    public boolean containsJob(ComplexGridlet gr) {
        for (int i = 0; i < running_jobs.size(); i++) {
            ComplexGridlet g = (ComplexGridlet) running_jobs.get(i).getGridlet();
            if(g.equals(gr)){
            //if (g.getGridletID() == gr.getGridletID()) {
                return true;
            }
        }
        return false;
    }
    
      public String getRunningJobsString() {
          String jobs = "";
        for (int i = 0; i < running_jobs.size(); i++) {
            ComplexGridlet g = (ComplexGridlet) running_jobs.get(i).getGridlet();
            jobs+=g.getGridletID()+" ";
        }
        return jobs;
    }


    public boolean calculateMachineCapacityInTime(double time) {
        capacity_list.clear();
        System.setProperty("java.util.Arrays.useLegacyMergeSort", "true");
        Collections.sort(running_jobs, new JobCompletionTimeComparator());

        //System.out.println("machine "+this.getMachineID()+" with "+this.getNumPE()+" CPUs --------------- time:" + time);
        for (int i = 0; i < running_jobs.size(); i++) {
            ComplexGridlet g = (ComplexGridlet) running_jobs.get(i).getGridlet();
            //System.out.println("job:" + running_jobs.get(i).getGridletID() + " ends at " + g.getExpectedFinishTime());
        }

        ComplexGridlet g = null;
        int cpu_init = this.getNumFreePE();
        long ram_init = this.getFreeRam();
        int gpu_init = this.getFreeGPUs();
        double start = time;

        // add increasing capcities as less jobs are executed
        for (int i = 0; i < running_jobs.size(); i++) {
            g = (ComplexGridlet) running_jobs.get(i).getGridlet();
            double duration = Math.round(Math.max(0, g.getExpectedFinishTime() - start));
            Capacity capacity = new Capacity(cpu_init, ram_init, gpu_init, start, duration);
            capacity_list.addLast(capacity);
            // System.out.println("c:" + running_jobs.get(i).getGridletID() + " capacity " + capacity.getCpus()+" CPUs at start: "+start);
            start = g.getExpectedFinishTime();
            cpu_init = cpu_init + g.getPpn();
            ram_init = ram_init + g.getRam();
            gpu_init = gpu_init + g.getGpus_per_node();
        }
        // this is the full capacity after last job finishes
        Capacity full_c = new Capacity(cpus, ram, gpus, start, Double.MAX_VALUE);
        capacity_list.addLast(full_c);
        //System.out.println("end capacity bigger by:" +g.getPpn()+" == "+full_c.getCpus()+" cap_length="+capacity_list.size());

        return true;
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
     * @return the used_ram
     */
    public long getUsedRam() {
        return used_ram;
    }

    /**
     * @param used_ram the used_ram to set
     */
    public void setUsedRam(long used_ram) {
        this.used_ram = used_ram;
    }

    /**
     * @return the free ram memory
     */
    public long getFreeRam() {
        return Math.max(0, (this.getRam() - this.getUsedRam()));
    }

    /**
     * @return the free ram memory
     */
    public double getPercUsedRam() {
        return Math.max(0, (((this.getUsedRam() * 1.0) / this.getRam())) * 100);
    }

    /**
     * @return the gpus
     */
    public int getGpus() {
        return gpus;
    }

    /**
     * @param gpus the gpus to set
     */
    public void setGpus(int gpus) {
        this.gpus = gpus;
    }

    /**
     * @return the used_gpus
     */
    public int getUsed_gpus() {
        return used_gpus;
    }

    /**
     * @param used_gpus the used_gpus to set
     */
    public void setUsed_gpus(int used_gpus) {
        this.used_gpus = used_gpus;
    }

    /**
     * @return the free ram memory
     */
    public int getFreeGPUs() {
        return Math.max(0, (this.getGpus() - this.getUsed_gpus()));
    }

    /**
     * @return the freeVirtualPE
     */
    public int getNumFreeVirtualPE() {
        return freeVirtualPE;
    }

    /**
     * @param freeVirtualPE the freeVirtualPE to set
     */
    public void setNumFreeVirtualPE(int freeVirtualPE) {
        this.freeVirtualPE = freeVirtualPE;
    }

    /**
     * @return the firstFreeTime
     */
    public double getFirstFreeTimeOnPE(int index) {

        return firstFreeTime[index];
    }

    public double[] getFirstFreeTimeArray() {
        return firstFreeTime;
    }

    public void setFirstFreeTimeArray(double[] a) {
        firstFreeTime = a;
    }

    /**
     * @param firstFreeTime the firstFreeTime to set
     */
    public void setFirstFreeTimeOnPE(int index, double firstFreeTime) {
        //System.out.println("EST on machine "+this.getMachineID()+" | "+firstFreeTime);
        this.firstFreeTime[index] = firstFreeTime;
    }

    public void updateFirstFreeTimeAfterNodeJobAllocation(int ppn, double runtime) {
        //System.out.println("EST on machine "+this.getMachineID()+" | "+firstFreeTime);
        double est = -1.0;
        for (int p = 0; p < ppn; p++) {
            double min = Double.MAX_VALUE;
            int index = -1;
            for (int i = 0; i < firstFreeTime.length; i++) {
                if (firstFreeTime[i] <= min && firstFreeTime[i] > -998) {
                    index = i;
                    min = firstFreeTime[i];
                }
            }
            if (p != ppn - 1) {
                firstFreeTime[index] = -999;
            } else {
                firstFreeTime[index] = Double.MAX_VALUE; // TO DO prekopat v budoucnu
                est = firstFreeTime[index];
            }
        }
        // oznacene uzly nastavim na est + time
        for (int i = 0; i < firstFreeTime.length; i++) {
            if (firstFreeTime[i] < -998) {
                firstFreeTime[i] = est;
            }
        }

        /*for (int i = 0; i < firstFreeTime.length; i++) {
            System.out.print(Math.round(firstFreeTime[i])+",");
        }
        System.out.println("| Max updated...:");*/
    }

    public double getEarliestStartTimeForNodeJob(int ppn) {
        double est = Double.MAX_VALUE;
        if (ppn > this.getNumPE()) {
            return est;
        }
        double[] est_times = Arrays.copyOf(firstFreeTime, firstFreeTime.length);
        Arrays.sort(est_times);


        /*System.out.print(Math.round(GridSim.clock()) + " | machine " + this.getMachineID() + " |");
        for (int i = 0; i < est_times.length;
                i++) {
            System.out.print(Math.round(est_times[i]) + ",");
        }
        System.out.println("| EST = " + Math.round(est_times[ppn - 1]) + " for PPN          = " + ppn);
         */
        return est_times[ppn - 1];
    }

    /**
     * @return the est
     */
    public double getEst() {
        return est;
    }

    /**
     * @param est the est to set
     */
    public void setEst(double est) {
        this.est = est;
    }
} // end class

class Capacity {

    private int cpus;
    private long ram;
    private int gpus;
    private double start_time;
    private double duration;

    public Capacity(int cpus, long ram, int gpus, double start, double duration) {
        setCpus(cpus);
        setGpus(gpus);
        setRam(ram);
        setStart_time(start);
        setDuration(duration);
    }

    /**
     * @return the cpus
     */
    public int getCpus() {
        return cpus;
    }

    /**
     * @param cpus the cpus to set
     */
    public void setCpus(int cpus) {
        this.cpus = cpus;
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
     * @return the gpus
     */
    public int getGpus() {
        return gpus;
    }

    /**
     * @param gpus the gpus to set
     */
    public void setGpus(int gpus) {
        this.gpus = gpus;
    }

    /**
     * @return the start_time
     */
    public double getStart_time() {
        return start_time;
    }

    /**
     * @param start_time the start_time to set
     */
    public void setStart_time(double start_time) {
        this.start_time = start_time;
    }

    /**
     * @return the duration
     */
    public double getDuration() {
        return duration;
    }

    /**
     * @param duration the duration to set
     */
    public void setDuration(double duration) {
        this.duration = duration;
    }

}
