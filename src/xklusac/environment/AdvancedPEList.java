/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package xklusac.environment;

import gridsim.PE;
import gridsim.PEList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * GridSim PEList maintains a list of PEs (Processing Elements) that make up a
 * machine.
 *
 * @author Manzur Murshed and Rajkumar Buyya
 * @since GridSim Toolkit 1.0
 * @invariant $none
 */
public class AdvancedPEList extends PEList {

    private Map<Integer, PE> idIndex;

    public AdvancedPEList() {
        super();
        this.idIndex = new HashMap<>();
    }

    /**
     * Override add to keep the index in sync with the list.
     */
    @Override
    public boolean add(PE pe) {
        // Add to the main list
        boolean added = super.add(pe);

        // If successful, add to our lookup index
        if (added) {
            idIndex.put(pe.getID(), pe);
        }
        return added;
    }

    /**
     * Gets MIPS Rating for a specified PE ID
     *
     * @param id the PE ID
     * @return the MIPS rating if exists, otherwise returns -1
     * @pre id >= 0
     * @post $none
     */
    public int getMIPSRating(int id) {
        PE obj = idIndex.get(id);
        if (obj != null) {
            return obj.getMIPSRating();
        }
        /*
        Iterator<PE> it = super.iterator();
        while (it.hasNext()) {
            obj = it.next();
            if (obj.getID() == id) {
                return obj.getMIPSRating();
            }
        }*/

        return -1;  // no PE with given id
    }

    /**
     * Gets a PE ID which is FREE
     *
     * @return a PE ID if it is FREE, otherwise returns -1
     * @pre $none
     * @post $none
     */
    public int getFreePEID() {
        PE obj = null;
        Iterator<PE> it = super.iterator();
        while (it.hasNext()) {
            obj = it.next();
            if (obj.getStatus() == PE.FREE) {
                return obj.getID();
            }
        }

        return -1;
    }

    /**
     * Gets the number of <tt>FREE</tt> or non-busy PE.
     *
     * @return number of PE
     * @pre $none
     * @post $result >= 0
     */
    public int getNumFreePE() {
        int counter = 0;
        PE obj = null;

        // a loop that counts the number of free PE
        Iterator<PE> it = super.iterator();
        while (it.hasNext()) {
            obj = it.next();
            if (obj.getStatus() == PE.FREE) {
                counter++;
            }
        }

        return counter;
    }

    /**
     * Sets the PE status
     *
     * @param status PE status, either <tt>PE.FREE</tt> or <tt>PE.BUSY</tt>
     * @param peID PE id
     * @return <tt>true</tt> if the PE status has changed, <tt>false</tt>
     * otherwise (PE id might not be exist)
     * @pre peID >= 0
     * @post $none
     */
    public boolean setStatusPE(boolean status, int peID) {
        boolean found = false;
        PE obj = idIndex.get(peID);
        if (obj.getID() == peID) {
            obj.setStatus(status);
            found = true;
        }
        //System.out.println("Yes, found "+peID);
        /*
        

        // a loop that counts the number of free PE
        Iterator<PE> it = super.iterator();
        while ( it.hasNext() )
        {
            obj = it.next();
            if (obj.getID() == peID)
            {
                obj.setStatus(status);
                found = true;
                break;
            }
        }*/

        return found;
    }

    /**
     * Gets the number of <tt>BUSY</tt> PE
     *
     * @return number of PE
     * @pre $none
     * @post $result >= 0
     */
    public int getNumBusyPE() {
        return super.size() - getNumFreePE();
    }

    /**
     * Gets the byte size of PEList internal data members
     *
     * @return the byte size
     * @pre $none
     * @post $result >= 0
     */
    public int getByteSize() {
        return super.size() * PE.getByteSize();
    }

    /**
     * Sets the status of PEs of this machine to FAILED. NOTE: <tt>resName</tt>
     * and <tt>machineID</tt> are used for debugging purposes, which is
     * <b>ON</b> by default. Use {@link #setStatusFailed(boolean)} if you do not
     * want this information.
     *
     * @param resName the name of the resource
     * @param machineID the id of this machine
     * @param fail the new value for the "failed" parameter
     */
    public void setStatusFailed(String resName, int machineID, boolean fail) {
        String status = null;
        if (fail) {
            status = "FAILED";
        } else {
            status = "WORKING";
        }

        System.out.println(resName + " - Machine: " + machineID
                + " is " + status);

        this.setStatusFailed(fail);
    }

    /**
     * Sets the status of PEs of this machine to FAILED.
     *
     * @param fail the new value for the "failed" parameter
     */
    public void setStatusFailed(boolean fail) {
        // a loop to set the status of all the PEs in this machine
        Iterator<PE> it = super.iterator();
        while (it.hasNext()) {
            PE obj = it.next();
            if (fail) {
                obj.setStatus(PE.FAILED);
            } else {
                obj.setStatus(PE.FREE);
            }
        }
    }

}
