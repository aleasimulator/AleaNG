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
import java.util.LinkedList;
import xklusac.environment.User;
import xklusac.extensions.Input;

/**
 * Class FairshareGroupLoader<p>
 * Creates Fairshare Groups according to specified data set.
 *
 * @author Dalibor Klusacek
 */
public class FairshareUserLoader {

    /**
     * Creates a new instance of MachineLoader
     */
    public FairshareUserLoader(String data_set) {
        System.out.println("--------------------------------------");
        System.out.println("Starting FairshareUser Loader ...");
        init(data_set);
        System.out.println("--------------------------------------");
    }

    /**
     * Based on the type of workload, machines and Grid resources are generated
     * here.
     */
    private void init(String set) {
        createUsers(set);
    }

    /**
     * Creates Grid resources
     */
    protected void createUsers(String data_set) {
        // read data-set from file
        LinkedList lines = new LinkedList();
        Input r = new Input();
        String adresar = System.getProperty("user.dir");

        BufferedReader br = null;

        br = r.openFile(new File(adresar + ExperimentSetup.data_sets_dir + data_set + ".users"));
        System.out.println("Opening: " + adresar + ExperimentSetup.data_sets_dir + data_set + ".users");
        r.getLines(lines, br);
        r.closeFile(br);

        // create groups from file
        for (int j = 0; j < lines.size(); j++) {
            String[] values = ((String) lines.get(j)).split("\\s+");
            if (values[0].contains(";")) {
                continue;
            }
            int id = Integer.parseInt(values[0]);
            String name = values[1];
            int share = Integer.parseInt(values[2]);
            int quota = Integer.parseInt(values[3]);

            User u = new User(id + "", name);
            u.setUser_share(share);
            u.setUserQuota(quota);
            ExperimentSetup.users.put(id + "", u);
            System.out.println("Creating user " + name + " with CPU quota: " + quota);

        }

    }
}
