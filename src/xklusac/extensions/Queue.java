/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package xklusac.extensions;

/**
 *
 * @author dalibor
 */
public class Queue {
    private String name;
    private int queue_CPU_limit;
    private int priority;
    private int queue_used_CPUs;
    
    public Queue(int id, String name, int limit, int priority){
        this.name = name;
        this.queue_CPU_limit = limit;
        this.priority = priority;
        this.queue_used_CPUs = 0;
        System.out.println("Creating queue: "+name+" with priority: "+priority+" and CPU limit: "+limit);
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
     * @return the queue_CPU_limit
     */
    public int getQueue_CPU_limit() {
        return queue_CPU_limit;
    }

    /**
     * @param queue_CPU_limit the queue_CPU_limit to set
     */
    public void setQueue_CPU_limit(int queue_CPU_limit) {
        this.queue_CPU_limit = queue_CPU_limit;
    }

    /**
     * @return the priority
     */
    public int getPriority() {
        return priority;
    }

    /**
     * @param priority the priority to set
     */
    public void setPriority(int priority) {
        this.priority = priority;
    }

    /**
     * @return the queue_used_CPUs
     */
    public int getQueue_used_CPUs() {
        return queue_used_CPUs;
    }

    /**
     * @param used the queue_used_CPUs to set
     */
    public void setQueue_used_CPUs(int running) {
        //System.out.println(this.name+": Setting used CPUs to: "+running);
        this.queue_used_CPUs = running;
    }
    
    public int getQueueAvailCPUs() {
        //System.out.println("| cpu_limit:"+queue_CPU_limit+" used:"+queue_used_CPUs+" |");
        return (queue_CPU_limit - queue_used_CPUs);
    }
    
}
