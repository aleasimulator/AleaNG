/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package xklusac.extensions;

import com.itextpdf.awt.PdfGraphics2D;
import com.itextpdf.text.Document;
import com.itextpdf.text.pdf.PdfContentByte;
import com.itextpdf.text.pdf.PdfTemplate;
import com.itextpdf.text.pdf.PdfWriter;
import java.awt.Color;
import java.awt.Dimension;
import java.awt.Font;
import java.awt.Graphics2D;
import java.awt.geom.Rectangle2D;
import java.io.FileOutputStream;
import java.text.SimpleDateFormat;
import java.util.Locale;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.swing.JFrame;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.DateAxis;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.xy.XYAreaRenderer;
import org.jfree.chart.title.TextTitle;
import org.jfree.data.xy.XYDataset;
import xklusac.environment.ExperimentSetup;
import xklusac.environment.FileUtil;
import xklusac.environment.Scheduler;

/**
 * This class draws a time series chart.
 *
 * @author Dalibor Klusacek
 */
public class TimeSeriesAreaChart extends JFrame {

    private static final long serialVersionUID = 1L;
    private static int posX = 10;
    private static int posY = 1;

    /**
     * This method generates a Time Series chart
     *
     * @param title title of the chart
     * @param subtitle smaller subtitle
     * @param dataset the formatted data to be drawn
     * @param fixed_y_axis boolean variable defining whether y-axis range should
     * be fixed to 0..1 (true) or not and an automatic range is calculated
     * (false)
     * @param width width of the chart in pixels
     * @param height height of the chart in pixels
     */
    public TimeSeriesAreaChart(String title, String subtitle, XYDataset dataset, boolean fixed_y_axis, int width, int height, String y_label, int position) {
        super(title);

        // Create chart  
        JFreeChart chart = ChartFactory.createTimeSeriesChart(
                title, // Chart  
                "", // X-Axis Label  
                "Number", // Y-Axis Label  
                dataset);

        //Changes background color
        if (!subtitle.isEmpty()) {
            chart.addSubtitle(new TextTitle(subtitle + " ["+ExperimentSetup.workload_file+"]"));
        }
        chart.getTitle().setFont(new Font("Tahoma", Font.BOLD, 14));

        XYPlot plot = (XYPlot) chart.getPlot();
        plot.setRenderer(new XYAreaRenderer());
        //plot.setForegroundAlpha(0.25f);
        XYAreaRenderer r = (XYAreaRenderer) plot.getRenderer();

        //r.setDefaultShapesVisible(false);
        /*r.setSeriesShape(0, new Ellipse2D.Double(-1d, -1d, 2d, 2d));
        r.setSeriesShape(1, new Rectangle2D.Double(-1d, -1d, 2d, 2d));
        r.setSeriesShape(2, new Ellipse2D.Double(-1d, -1d, 2d, 2d));
        r.setSeriesShape(3, new Rectangle2D.Double(-1d, -1d, 2d, 2d));
        r.setDefaultShapesFilled(false);
        r.setSeriesPaint(0, Color.red);
        r.setSeriesPaint(1, Color.BLUE);
        r.setSeriesPaint(2, Color.darkGray);
        r.setSeriesPaint(3, new Color(0, 176, 19));
         */
        //plot.setBackgroundPaint(new Color (177, 177, 177));
        plot.setBackgroundPaint(new Color(236, 236, 236));
        plot.setDomainGridlinePaint(Color.GRAY);
        plot.setRangeGridlinePaint(Color.LIGHT_GRAY);
        NumberAxis yAxis = new NumberAxis(y_label);

        if (fixed_y_axis) {
            yAxis.setRange(0.0, 1.0);
        }
        DateAxis axis = (DateAxis) plot.getDomainAxis();
        axis.setDateFormatOverride(new SimpleDateFormat("dd-MMM-YY HH:mm", new Locale.Builder().setLanguage("en").setRegion("US").build()));//HH:mm dd-MMM-yy
        axis.setTickLabelFont(new Font("Tahoma", Font.PLAIN, 11));
        axis.setLabelFont(new Font("Tahoma", Font.PLAIN, 11));
        chart.getLegend().setItemFont(new Font("Tahoma", Font.PLAIN, 11));
        plot.setRangeAxis(yAxis);
        plot.setDomainAxis(axis);
        String filename = title+"-"+Scheduler.scheduling_algorithm +"-"+ExperimentSetup.workload_file;
        //SaveChartToFile.save(filename, chart, width, height);
        
        //shorten filename to avoid Acrobat Reader problems
        filename = filename.replace("(sample based)", "");
        filename = filename.replace(".swf", "");
        SaveChartToFile.save(FileUtil.getPath(System.getProperty("user.dir")+ "/"+filename), chart, width, height);
        
        //pdf output
        try {

            Document document = new Document(new com.itextpdf.text.Rectangle(width, height));
            //PdfWriter writer = PdfWriter.getInstance(document, new FileOutputStream(filename + ".pdf"));
            PdfWriter writer = PdfWriter.getInstance(document, new FileOutputStream(FileUtil.getPath(System.getProperty("user.dir")+ "/"+filename+".pdf")));
            document.open();

            // Draw chart onto PDF using PdfContentByte
            PdfContentByte cb = writer.getDirectContent();

            PdfTemplate template = cb.createTemplate(width, height);
            Graphics2D g2 = new PdfGraphics2D(template, width, height);
            chart.draw(g2, new Rectangle2D.Double(0, 0, width, height));
            g2.dispose();
            cb.addTemplate(template, 0, 0);

            document.close();

        } catch (Exception ex) {
            Logger.getLogger(TimeSeriesChart.class.getName()).log(Level.SEVERE, null, ex);
        }

        JFrame fr = new JFrame();
        fr.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);

        ChartPanel panel = new ChartPanel(chart) {

            @Override
            public Dimension getPreferredSize() {
                return new Dimension(width, height);
            }
        };
        fr.setTitle(title);
        fr.add(panel);
        fr.pack();
        
            int startX = posX+(ExperimentSetup.simulation_run*width/2)+(ExperimentSetup.simulation_run*20);
            int startY = posY;
            fr.setLocation(startX + position * 0, startY + position * (height-0));
        

        fr.setVisible(true);

    }
}
