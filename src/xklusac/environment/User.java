/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package xklusac.environment;

import gridsim.GridSim;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import org.jfree.data.time.TimeSeries;

/**
 *
 * @author Dalibor
 */
public class User {

    private String name;
    private String login;
    private int jobs;
    private double slowdown;
    private double response;
    private double wait;
    private double runtime;
    private ArrayList<Double> percentages;
    private long cumul_usage;
    private long temporary_usage;
    private double fairshare_factor;
    private int user_share;
    private long last_temp_timestamp;
    private long queued_jobs;
    private long started_jobs;
    private int quota;
    private int used_quota;
    private LinkedList<ComplexGridlet> running_jobs = new LinkedList();
    private LinkedList<Long> wait_times = new LinkedList();
    private HashMap<Long,Long> arrival_wait_map = new HashMap<Long, Long>();
    

    TimeSeries series_u;
    TimeSeries series_ff;
    TimeSeries series_c_u;
    TimeSeries series_w;

    public User(String name, String login) {
        ExperimentSetup.user_logins.add(name);
        this.setLogin(login);
        this.setName(name);
        this.setJobs(0);
        this.setSlowdown(0.0);
        this.setResponse(0.0);
        this.setRuntime(0.0);
        this.setWait(0.0);
        this.setCumul_usage(1);
        this.setTemporary_usage(0);
        this.setFairshare_factor(1.0);
        this.setUser_share(1);
        this.setLast_temp_timestamp(0);
        this.setQueued_jobs(0);
        this.setStarted_jobs(0);
        this.setUserQuota(0);
        this.setUsed_quota(0);
        System.out.println("New USER " + name+" arrived/created at SimClock: "+Math.round(GridSim.clock()));
        

        this.percentages = new ArrayList<Double>();
        series_u = new TimeSeries(login+" (id:"+name+")");
        series_ff = new TimeSeries(login+" (id:"+name+")");
        series_c_u = new TimeSeries(login+" (id:"+name+")");
        series_w = new TimeSeries(login+" (id:"+name+")");
    }

    public int getJobs() {
        return jobs;
    }

    public void setJobs(int jobs) {
        this.jobs = jobs;
    }

    public double getSlowdown() {
        return slowdown;
    }

    public void setSlowdown(double slowdown) {
        this.slowdown = slowdown;
    }

    public double getResponse() {
        return response;
    }

    public void setResponse(double response) {
        this.response = response;
    }

    public double getWait() {
        return wait;
    }

    public void setWait(double wait) {
        this.wait = wait;
    }

    public double getRuntime() {
        return runtime;
    }

    public void setRuntime(double runtime) {
        this.runtime = runtime;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void updateRuntime(double runtime) {
        this.runtime += runtime;
    }

    public void updateJobs(double jobs) {
        this.jobs += jobs;
    }

    public void updateSlowdown(double slowdown) {
        this.slowdown += slowdown;
    }

    public void updateWait(double wait) {
        this.wait += wait;
    }

    public void updateResponse(double response) {
        this.response += response;
    }

    /**
     * @return the percentages
     */
    public ArrayList getPercentages() {
        return percentages;
    }

    /**
     * @param percentages the percentages to set
     */
    public void setPercentages(ArrayList percentages) {
        this.percentages = percentages;
    }

    public void addPercentage(double perc) {
        percentages.add(perc);
        if (percentages.size() > 5) {
            percentages.remove(0);
        }
    }

    public double getAvgPercentage() {
        double avg = 0.0;
        for (int i = 0; i < percentages.size(); i++) {
            avg += percentages.get(i);
        }
        if (percentages.size() > 0) {
            return avg / percentages.size();
        } else {
            return 1.0;
        }
    }

    public double getMinPercentage() {
        double min = Double.MAX_VALUE;
        for (int i = 0; i < percentages.size(); i++) {
            if (min > percentages.get(i)) {
                min = percentages.get(i);
            }
        }
        if (percentages.size() > 0) {
            return min;
        } else {
            return 1.0;
        }
    }

    public String printPercentage() {
        String p = "";
        for (int i = 0; i < percentages.size(); i++) {
            p += Math.round(percentages.get(i) * 10.0) / 10.0 + ", ";
        }
        return p;
    }

    /**
     * @return the cumul_usage
     */
    public long getCumul_usage() {
        return cumul_usage;
    }

    public long getRunningUsage() {
        long running_usage = 0;
        for (int i = 0; i < running_jobs.size(); i++) {
            ComplexGridlet gl = running_jobs.get(i);
            double runtime = GridSim.clock() - gl.getExecStartTime();
            running_usage += Math.round(gl.getNumPE() * runtime * ExperimentSetup.PBS_factor);
            //System.out.println(gl.getGridletID()+ " runtime so far: "+runtime);
        }
        //System.out.println("Running usage is for user " + this.getName() + " is: " + tick_usage + " at time: " + Math.round(GridSim.clock()));
        return running_usage;
    }
    
     public long getLastTickUsage() {
        long tick_usage = 0;
        for (int i = 0; i < running_jobs.size(); i++) {
            ComplexGridlet gl = running_jobs.get(i);
            double tick_runtime = Math.min(ExperimentSetup.sample_tick, GridSim.clock() - gl.getExecStartTime());
            tick_usage += Math.round(gl.getNumPE() * tick_runtime * ExperimentSetup.PBS_factor);
            //System.out.println(gl.getGridletID()+ " runtime so far: "+runtime);
        }
        //System.out.println("Running usage is for user " + this.getName() + " is: " + tick_usage + " at time: " + Math.round(GridSim.clock()));
        return tick_usage;
    }

    /**
     * @param cumul_usage the cumul_usage to set
     */
    public void setCumul_usage(long cumul_usage) {
        this.cumul_usage = cumul_usage;
    }

    /**
     * @return the fairshare_factor
     */
    public double getFairshare_factor() {
        return fairshare_factor;
    }

    /**
     * @param fairshare_factor the fairshare_factor to set
     */
    public void setFairshare_factor(double fairshare_factor) {
        this.fairshare_factor = fairshare_factor;
    }

    /**
     * @return the user_share
     */
    public int getUser_share() {
        //System.out.println(login+" share="+user_share);
        return user_share;
    }

    /**
     * @param user_share the user_share to set
     */
    public void setUser_share(int user_share) {
        this.user_share = user_share;
    }

    /**
     * @return the temporary_usage
     */
    public long getTemporary_usage() {
       return Math.round(temporary_usage * 1.0);        // 0.01=PBS verif | 1.0=synt.exp tiny, small, medium... 
       //return temporary_usage;
    }

    /**
     * @param temporary_usage the temporary_usage to set
     */
    public void setTemporary_usage(long temporary_usage) {
        this.temporary_usage = temporary_usage;
    }

    /**
     * @return the last_temp_timestamp
     */
    public long getLast_temp_timestamp() {
        return last_temp_timestamp;
    }

    /**
     * @param last_temp_timestamp the last_temp_timestamp to set
     */
    public void setLast_temp_timestamp(long last_temp_timestamp) {
        this.last_temp_timestamp = last_temp_timestamp;
    }

    /**
     * @return the queued_jobs
     */
    public long getQueued_jobs() {
        return queued_jobs;
    }

    /**
     * @param queued_jobs the queued_jobs to set
     */
    public void setQueued_jobs(long queued_jobs) {
        this.queued_jobs = queued_jobs;
    }

    /**
     * @return the started_jobs
     */
    public long getStarted_jobs() {
        return started_jobs;
    }

    /**
     * @param started_jobs the started_jobs to set
     */
    public void setStarted_jobs(long started_jobs) {
        this.started_jobs = started_jobs;
    }

    /**
     * @return the running_jobs
     */
    public LinkedList<ComplexGridlet> getRunning_jobs() {
        return running_jobs;
    }

    /**
     * @param running_jobs the running_jobs to set
     */
    public void setRunning_jobs(LinkedList<ComplexGridlet> running_jobs) {
        this.running_jobs = running_jobs;
    }

    /**
     * @return the quota
     */
    public int getUserQuota() {
        return quota;
    }

    /**
     * @param quota the quota to set
     */
    public void setUserQuota(int quota) {
        this.quota = quota;
    }

    /**
     * @return the used_quota
     */
    public int getUsed_quota() {
        return used_quota;
    }

    /**
     * @param used_quota the used_quota to set
     */
    public void setUsed_quota(int used_quota) {
        this.used_quota = used_quota;
    }
    
    public int getFreeQuota() {
        return (getUserQuota() - getUsed_quota());
    }

    /**
     * @return the login
     */
    public String getLogin() {
        return login;
    }

    /**
     * @param login the login to set
     */
    public void setLogin(String login) {
        this.login = login;
    }

    /**
     * @return the wait_times
     */
    public LinkedList<Long> getWait_times() {
        return wait_times;
    }

    /**
     * @param wait_times the wait_times to set
     */
    public void setWait_times(LinkedList<Long> wait_times) {
        this.wait_times = wait_times;
    }

    /**
     * @return the arrival_wait_map
     */
    public HashMap<Long,Long> getArrival_wait_map() {
        return arrival_wait_map;
    }

    /**
     * @param arrival_wait_map the arrival_wait_map to set
     */
    public void setArrival_wait_map(HashMap<Long,Long> arrival_wait_map) {
        this.arrival_wait_map = arrival_wait_map;
    }

}
