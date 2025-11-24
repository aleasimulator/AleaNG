/*
 * MachineLoader.java
 *
 *
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */
package xklusac.environment;

import gridsim.*;
import java.io.BufferedReader;
import java.io.File;
import java.util.Calendar;
import java.util.LinkedList;
import xklusac.extensions.Input;

/**
 * Class MachineLoader<p>
 * Creates GridResources according to specified data set.
 *
 * @author Dalibor Klusacek
 */
public class MachineLoader {

    private double bandwidth;
    private double cost;
    private String data_set;
    public int total_CPUs = 0;

    /**
     * Creates a new instance of MachineLoader
     */
    public MachineLoader(double bandwidth, double cost, String data_set) {
        System.out.println("--------------------------------------");
        System.out.println("Starting Machine Loader ...");
        this.bandwidth = bandwidth;
        this.cost = cost;
        this.data_set = data_set;
        this.total_CPUs = 0;
        init(data_set);
        System.out.println("--------------------------------------");
    }

    /**
     * Based on the type of workload, machines and Grid resources are generated
     * here.
     */
    private void init(String set) {

        if (set.contains(".gwf")) {
            createGridResources(bandwidth, cost, set);
        } else if (set.contains(".swf")) {
            createGridResources(bandwidth, cost, set);
        } else if (set.contains(".dyn")) {
            createGridResources(bandwidth, cost, set);
        } else {
            System.out.println("Wrong machine workload format or file extension (gwf.machines, swf.machines, dyn.machines)");
        }

    }

    /**
     * Creates Grid resources
     */
    protected void createGridResources(double bandwidth, double cost, String data_set) {
        // read data-set from file
        LinkedList lines = new LinkedList();
        Input r = new Input();

        BufferedReader br = null;
        String adresar = System.getProperty("user.dir");
        br = r.openFile(new File(adresar + ExperimentSetup.data_sets_dir + data_set + ".machines"));
        System.out.println("Opening: " + adresar + ExperimentSetup.data_sets_dir + data_set + ".machines");
        r.getLines(lines, br);
        r.closeFile(br);
        int name_id = 0;
        int max = 0;
        int min = Integer.MAX_VALUE;
        // unused now
        String cpu_ids = "0,1,2,3,4,5";

        // create resources and machines from file
        for (int j = 0; j < lines.size(); j++) {

            String[] values = ((String) lines.get(j)).split("\\s+");
            if (values[0].contains(";")) {
                continue;
            }
            //System.out.println(lines.get(j));
            int id = Integer.parseInt(values[0]);
            int gpus_per_node = Integer.parseInt(values[7]);
            long ram = 1024;
            if (values.length > 5) {
                //ram in KB or GB
                ram = Long.parseLong(values[5]);
                // small value indicates that the RAM was stored in GB rather than SWF's KB
                if (ram < 1024 * 10) {
                    ram = ram * 1024 * 1024;
                }
            }
            int totalMachine = Integer.parseInt(values[2]);
            int totalPE = Integer.parseInt(values[3]);

            if (ExperimentSetup.allocate_whole_nodes) {
                totalPE = 1;
            }
            int peRating = Integer.parseInt(values[4]);
            String name = values[1];
            String description;
            if (values.length > 6) {
                //partition
                description = values[6];
            } else {
                description = values[0];
            }

            System.out.println("Creating cluster " + name + " with " + totalMachine + " nodes. Each node has " + totalPE + " CPUs and " + gpus_per_node + " GPUs.");

            // for JobLoader's purposes
            max = totalPE * totalMachine;
            if (max > ExperimentSetup.maxPE) {
                ExperimentSetup.maxPE = max;
            }
            // for JobLoader's purposes
            min = peRating;
            if (min < ExperimentSetup.minPErating) {
                ExperimentSetup.minPErating = min;
            }
            if (peRating > ExperimentSetup.maxPErating) {
                ExperimentSetup.maxPErating = peRating;
            }

            //    A Machine contains one or more PEs or CPUs. Therefore, should
            //    create an object of PEList to store these PEs before creating
            //    a Machine.
            MachineList mList = new MachineList();
            for (int m = 0; m < totalMachine; m++) {

                PEList peList = new PEList();
                for (int k = 0; k < totalPE; k++) {
                    // need to store PE id and MIPS Rating
                    peList.add(new PE(k, peRating));
                }

                mList.add(new MachineWithRAMandGPUs(m, peList, ram, gpus_per_node));

            }

            //    Create a ResourceCharacteristics object that stores the
            //    properties of a Grid resource: architecture, OS, list of
            //    Machines allocation policy: time- or space-shared, time zone
            //    and its price (G$/PE time unit).
            // to-do: read this from file. These values are sharcnet specific
            String arch = "Pentium 4";      // system architecture e.g. Xeon, Opteron, Pentium3

            String os = "Scientific Linux";          // operating system e.g. Linux, Debian

            double time_zone = 0.0;         // time zone this resource located

            String properties = description;

            //name = name;
            name_id++;

            ComplexResourceCharacteristics resConfig = new ComplexResourceCharacteristics(
                    arch, os, mList, ResourceCharacteristics.SPACE_SHARED, time_zone, cost, ram, properties, cpu_ids);

            // Finally, we need to create a ComplexGridResource object.
            long seed = 11L * 13 * 17 * 19 * 23 + 1;
            double peakLoad = 0.0;       // the resource load during peak hour

            double offPeakLoad = 0.0;    // the resource load during off-peak hr

            double holidayLoad = 0.0;    // the resource load during holiday

            // incorporates weekends so the grid resource is on 7 days a week
            LinkedList Weekends = new LinkedList();
            Weekends.add(Calendar.SATURDAY);
            Weekends.add(Calendar.SUNDAY);

            // incorporates holidays. However, no holidays are set in this example
            LinkedList Holidays = new LinkedList();
            AllocPolicy apolicy = null;
            try {
                // this is usefull because we can define resources internal scheduling system (FCFS/RR/BackFilling,FairQueuing...)               
                AdvancedSpaceSharedWithRAM policy = null;
                policy = new AdvancedSpaceSharedWithRAM(name, "AdvancedSpaceSharedPolicyWithRAM", resConfig);
                ExperimentSetup.local_schedulers.add(policy);
                apolicy = policy;

            } catch (Exception ex) {
                ex.printStackTrace();
            }

            try {
                ResourceCalendar resCalendar = new ResourceCalendar(time_zone,
                        peakLoad, offPeakLoad, holidayLoad, Weekends,
                        Holidays, seed);

                ComplexGridResource gridRes = new ComplexGridResource(name, bandwidth, resConfig,
                        resCalendar, apolicy);

                ExperimentSetup.avail_CPUs += gridRes.resource_.getNumPE();

                MachineList machines = gridRes.resource_.getMachineList();
                for (int i = 0; i < machines.size(); i++) {
                    MachineWithRAMandGPUs machine = (MachineWithRAMandGPUs) machines.get(i);
                    ExperimentSetup.avail_RAM += machine.getRam();

                }

            } catch (Exception e) {
                System.out.println("Error in creating GridResource.");
                System.out.println(e.getMessage());
            }

        }
    }

}
