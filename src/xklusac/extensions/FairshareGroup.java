/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package xklusac.extensions;

import java.util.LinkedList;
import xklusac.environment.ExperimentSetup;
import xklusac.environment.GridletInfo;

/**
 *
 * @author daliborT470
 */
public class FairshareGroup {

    // id corresponding to the job file
    private int id;
    // relative number of shares between different groups
    private int share;
    private String name;
    // number of CPUs that can be used simultaneously
    private int quota;
    // used CPUs currently
    private int usedQuota;
    private int heldJobsCount;
    private LinkedList<GridletInfo> heldJobs;

    public FairshareGroup(int id, String name, int share, int quota) {
        this.setId(id);
        this.setShare(share);
        this.setName(name);
        this.setQuota(quota);
        this.setUsedQuota(0);
        this.setHeldJobsCount(0);
        this.setHeldJobs(new LinkedList());
    }

    /**
     * @return the id
     */
    public int getId() {
        return id;
    }

    /**
     * @param id the id to set
     */
    public void setId(int id) {
        this.id = id;
    }

    /**
     * @return the share
     */
    public int getShare() {
        return share;
    }

    /**
     * @param share the share to set
     */
    public void setShare(int share) {
        this.share = share;
    }

    /**
     * @return the name
     */
    public String getName() {
        return name;
    }

    /**
     * @param name the name to set
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * @return the quota
     */
    public int getQuota() {
        return quota;
    }

    /**
     * @param quota the quota to set
     */
    public void setQuota(int quota) {
        this.quota = quota;
    }

    /**
     * @return the usedQuota
     */
    public int getUsedQuota() {
        return usedQuota;
    }

    /**
     * @param usedQuota the usedQuota to set
     */
    public void setUsedQuota(int usedQuota) {
        this.usedQuota = usedQuota;
    }

    public int getFreeQuota() {
        return (getQuota() - getUsedQuota());
    }

    public int getFreeQuota(int freeCPUs, int partition) {
        int normalFree = getQuota() - getUsedQuota();

       return normalFree;
    }

    /**
     * @return the heldJobsCount
     */
    public int getHeldJobsCount() {
        return heldJobsCount;
    }

    /**
     * @param heldJobsCount the heldJobsCount to set
     */
    public void setHeldJobsCount(int heldJobsCount) {
        this.heldJobsCount = heldJobsCount;
    }

    /**
     * @return the heldJobs
     */
    public LinkedList<GridletInfo> getHeldJobs() {
        return heldJobs;
    }

    /**
     * @param heldJobs the heldJobs to set
     */
    public void setHeldJobs(LinkedList<GridletInfo> heldJobs) {
        this.heldJobs = heldJobs;
    }

}
