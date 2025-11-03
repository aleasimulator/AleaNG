/*
 * MachineLoader.java
 *
 *
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */
package xklusac.environment;

import java.io.BufferedReader;
import java.io.File;
import java.util.Collections;

import java.util.LinkedList;
import static xklusac.environment.ExperimentSetup.name;
import xklusac.extensions.Input;
import xklusac.extensions.Queue;
import xklusac.extensions.QueuePriorityComparator;

/**
 * Class MachineLoader<p>
 * Creates GridResources according to specified data set.
 *
 * @author Dalibor Klusacek
 */
public class QueueLoader {

    /**
     * Creates a new instance of MachineLoader
     */
    public QueueLoader(String data_set) {
        System.out.println("--------------------------------------");
        System.out.println("Starting Queue Loader ...");
        init(data_set);
        System.out.println("--------------------------------------");
    }

    /**
     * Based on the type of workload, machines and Grid resources are generated
     * here.
     */
    private void init(String set) {
        createQueues(set);
    }

    /**
     * Creates Grid resources
     */
    protected void createQueues(String data_set) {
        // read data-set from file
        LinkedList lines = new LinkedList();
        Input r = new Input();
        String adresar = System.getProperty("user.dir");
        //System.out.println("Starting QUEUE LOADER...");

        BufferedReader br = null;

        br = r.openFile(new File(adresar + ExperimentSetup.data_sets_dir+data_set+ ".queues"));
        System.out.println("Opening: " + adresar + ExperimentSetup.data_sets_dir+data_set+".queues");
        r.getLines(lines, br);
        r.closeFile(br);

        // create queues from file
        LinkedList<Queue> unordered_q = new LinkedList<>();
        for (int j = 0; j < lines.size(); j++) {

            String[] values = ((String) lines.get(j)).split("\\s+");
            if (values[0].contains(";")) {
                continue;
            }

            //System.out.println(lines.get(j));
            int id = Integer.parseInt(values[0]);
            String name = values[1];            
            int limit = Integer.parseInt(values[2]);
            int priority = Integer.parseInt(values[3]);
            Queue q = new Queue(id, name, limit, priority);
            ExperimentSetup.queues.put(name, q);
            ExperimentSetup.queues_id_to_name_mapping.put(id, name);
            unordered_q.add(q);
        }

        Collections.sort(unordered_q, new QueuePriorityComparator());
        for (int j = 0; j < unordered_q.size(); j++) {
            Queue q = unordered_q.get(j);
            Scheduler.all_queues.addLast(new LinkedList<GridletInfo>());
            Scheduler.all_queues_names.addLast(q.getName());
            System.out.println("Adding queue " + q.getName() + " with priority " + q.getPriority()+" to position: "+j);

        }
        unordered_q.clear();

    }
}
