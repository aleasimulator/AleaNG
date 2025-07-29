/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package xklusac.extensions;

import java.io.File;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.ChartUtils;
import xklusac.environment.ExperimentSetup;

/**
 * This class stores a chart into a PNG graphic file.
 * @author Dalibor Klusacek
 */
public final class SaveChartToFile {

    /**
     * Saves generated charts into a PNG file.
     * @param chart_name name of the file
     * @param chart chart to be saved
     * @param width width of the PNG
     * @param height height of the PNG
     */
    public static synchronized void save(String chart_name, JFreeChart chart, int width, int height) {
        
        
        String suffix = "";
        if(ExperimentSetup.use_decay){
            suffix = "-decaying-"+ExperimentSetup.decay_factor+"-"+Math.round(ExperimentSetup.decay_interval)+"h";
        }
        try {
            File file = new File(chart_name + suffix + ".png");
            System.out.println("Saving to PNG chart: " + chart_name + " with size: " + width + " x " + height + " pixels.");
            ChartUtils.saveChartAsPNG(file, chart, width, height);
        } catch (IOException ex) {
            Logger.getLogger(SaveChartToFile.class.getName()).log(Level.SEVERE, null, ex);
        }

    }

}
