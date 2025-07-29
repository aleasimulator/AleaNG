/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package xklusac.extensions;

import java.awt.Color;
import java.awt.Dimension;
import java.awt.Font;
import java.awt.geom.Ellipse2D;
import java.awt.geom.Rectangle2D;
import java.text.SimpleDateFormat;
import java.util.Locale;
import javax.swing.JFrame;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.DateAxis;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.xy.XYLineAndShapeRenderer;
import org.jfree.chart.title.TextTitle;
import org.jfree.data.xy.XYDataset;

/**
 * This class draws a time series chart.
 * @author Dalibor Klusacek
 */
public class TimeSeriesChart extends JFrame {

    private static final long serialVersionUID = 1L;

    /**
     * This method generates a Time Series chart
     * @param title title of the chart
     * @param subtitle smaller subtitle
     * @param dataset the formatted data to be drawn
     * @param fixed_y_axis boolean variable defining whether y-axis range should be fixed to 0..1 (true) or not and an automatic range is calculated (false)
     * @param width width of the chart in pixels
     * @param height height of the chart in pixels
     */
    public TimeSeriesChart(String title, String subtitle, XYDataset dataset, boolean fixed_y_axis, int width, int height) {
        super(title);
        

        // Create chart  
        JFreeChart chart = ChartFactory.createTimeSeriesChart(
                title, // Chart  
                "Date", // X-Axis Label  
                "Number", // Y-Axis Label  
                dataset);

        
        //Changes background color
        if (!subtitle.isEmpty()) {
            chart.addSubtitle(new TextTitle(subtitle+" [AleaNG Simulator]"));
        }
        chart.getTitle().setFont(new Font("Tahoma", Font.BOLD, 16));
        
        XYPlot plot = (XYPlot) chart.getPlot();
        XYLineAndShapeRenderer r = (XYLineAndShapeRenderer) plot.getRenderer();
        r.setDefaultShapesVisible(true);
        r.setSeriesShape(0, new Ellipse2D.Double(-1d, -1d, 2d, 2d));
        r.setSeriesShape(1, new Rectangle2D.Double(-1d, -1d, 2d, 2d));
        r.setSeriesShape(2, new Ellipse2D.Double(-1d, -1d, 2d, 2d));
        r.setSeriesShape(3, new Rectangle2D.Double(-1d, -1d, 2d, 2d));
        r.setDefaultShapesFilled(false);
        r.setSeriesPaint(0, Color.red);
        r.setSeriesPaint(1, Color.BLUE);
        r.setSeriesPaint(2, Color.darkGray);
        r.setSeriesPaint(3, new Color (0, 176, 19));
        //plot.setBackgroundPaint(new Color (177, 177, 177));
        plot.setBackgroundPaint(new Color (236, 236, 236));
        plot.setDomainGridlinePaint(Color.GRAY);
        plot.setRangeGridlinePaint(Color.LIGHT_GRAY);
        NumberAxis yAxis = new NumberAxis("usage {CPU hours)");
        
        if (fixed_y_axis) {
            yAxis.setRange(0.0, 1.0);
            yAxis.setAttributedLabel("Fairshare Factor");
        }
        DateAxis axis = (DateAxis) plot.getDomainAxis();
        axis.setDateFormatOverride(new SimpleDateFormat("dd-MMM HH:mm", new Locale.Builder().setLanguage("en").setRegion("US").build()));//HH:mm dd-MMM-yy
        axis.setTickLabelFont(new Font("Tahoma", Font.PLAIN, 11));
        axis.setLabelFont(new Font("Tahoma", Font.PLAIN, 12));
        plot.setRangeAxis(yAxis);
        plot.setDomainAxis(axis);
        SaveChartToFile.save(subtitle+"-"+title, chart, width, height);
        
        JFrame fr = new JFrame();
        fr.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        
        ChartPanel panel = new ChartPanel(chart){

            @Override
            public Dimension getPreferredSize() {
                return new Dimension(width, height);
            }
        };
        fr.add(panel);
        fr.pack();
        fr.setLocationRelativeTo(null);
        fr.setTitle(title);
        fr.setVisible(true);
        
    }
}
