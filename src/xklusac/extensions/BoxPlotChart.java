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
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.swing.JFrame;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.CategoryAxis;
import org.jfree.chart.axis.LogarithmicAxis;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.labels.BoxAndWhiskerToolTipGenerator;
import org.jfree.chart.plot.CategoryPlot;
import org.jfree.chart.title.TextTitle;
import org.jfree.data.statistics.BoxAndWhiskerCategoryDataset;
import org.jfree.data.statistics.DefaultBoxAndWhiskerCategoryDataset;
import xklusac.environment.ExperimentSetup;
import xklusac.environment.FileUtil;
import xklusac.environment.Scheduler;
import xklusac.environment.User;

/**
 *
 * @author daliborT470
 */
public class BoxPlotChart extends JFrame {

    private static int posX = 10;
    private static int posY = 1;

    private static final long serialVersionUID = 1L;

    public BoxPlotChart(String title, String subtitle, String xaxis, String yaxis, int width, int height, int position) {

        super(title);

        final CategoryAxis xAxis = new CategoryAxis(xaxis);

        //LogAxis yAxis = new LogAxis(yaxis);
        //yAxis.setBase(2);
        NumberAxis yAxis = new NumberAxis(yaxis);

        //yAxis = new LogarithmicAxis(yaxis);
        yAxis.setAutoRange(true);
        //yAxis.setRange(-100.0, 300.0);

        //final LogarithmicAxis yAxis = new LogarithmicAxis(yaxis);
        yAxis.setAutoRangeIncludesZero(true);

        final BoxAndWhiskerRendererUpgraded renderer = new BoxAndWhiskerRendererUpgraded();
        renderer.setUseOutlinePaintForWhiskers(false);
        renderer.setFillBox(true);
        renderer.setMeanVisible(true);

        //renderer.setSeriesPaint(4, Color.ORANGE);
        /*renderer.setSeriesPaint(0, Color.darkGray);
        renderer.setSeriesPaint(1, Color.green);
        renderer.setSeriesPaint(2, Color.blue);
        renderer.setSeriesPaint(3, Color.red);
        renderer.setSeriesPaint(4, Color.orange);
        renderer.setSeriesPaint(5, Color.cyan);
        renderer.setSeriesPaint(6, Color.gray);
         */
        renderer.setDefaultToolTipGenerator(new BoxAndWhiskerToolTipGenerator());
        BoxAndWhiskerCategoryDataset datasetused = null;

        datasetused = createDatasetFromJobs();

        final BoxAndWhiskerCategoryDataset dataset = datasetused;

        final CategoryPlot plot = new CategoryPlot(dataset, xAxis, yAxis, renderer);

        final JFreeChart chart = new JFreeChart(
                title,
                new Font("Tahoma", Font.BOLD, 20),
                plot,
                true
        );
        chart.setBackgroundPaint(Color.white);

        if (!subtitle.isEmpty()) {
            chart.addSubtitle(new TextTitle(subtitle + " [" + ExperimentSetup.workload_file + "]"));
        }
        chart.getTitle().setFont(new Font("Tahoma", Font.BOLD, 14));

        //chart.addSubtitle(new TextTitle(subtitle));
        plot.setBackgroundPaint(Color.white);
        plot.setDomainGridlinePaint(Color.GRAY);
        plot.setRangeGridlinePaint(Color.LIGHT_GRAY);

        String filename = title + "-" + Scheduler.scheduling_algorithm + "-" + ExperimentSetup.workload_file;
        //shorten filename to avoid Acrobat Reader problems
        filename = filename.replace("(sample based)", "");
        filename = filename.replace(".swf", "");
        filename = filename.replace("Distr. of wait time (minutes) wrt. users", "Boxplot-wait");
        SaveChartToFile.save(FileUtil.getPath(System.getProperty("user.dir") + "/" + filename), chart, width, height);

        //pdf output
        try {

            Document document = new Document(new com.itextpdf.text.Rectangle(width, height));
            PdfWriter writer = PdfWriter.getInstance(document, new FileOutputStream(FileUtil.getPath(System.getProperty("user.dir") + "/" + filename + ".pdf")));
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
            Logger.getLogger(ScatterChart.class.getName()).log(Level.SEVERE, null, ex);
        }

        JFrame fr = new JFrame();
        fr.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);

        ChartPanel panel = new ChartPanel(chart) {

            @Override
            public Dimension getPreferredSize() {
                return new Dimension((int) (Math.round(width / 1.5)), (int) Math.round(height * 1.3));
            }
        };
        fr.setTitle(title);
        fr.add(panel);
        fr.pack();

        int startX = posX + (ExperimentSetup.simulation_run * width / 2) + (ExperimentSetup.simulation_run * 20);
        int startY = posY;
        fr.setLocation(startX + position * 0, startY + position * (height - 0) + 20);

        fr.setVisible(true);

    }

    private BoxAndWhiskerCategoryDataset createDatasetFromJobs() {

        String data_set_name = "users";

        final DefaultBoxAndWhiskerCategoryDataset dataset
                = new DefaultBoxAndWhiskerCategoryDataset();

        Object[] keys = ExperimentSetup.users.keySet().toArray();
        for (int u = 0; u < ExperimentSetup.users.size(); u++) {
            // use this hack to keep them always ordered
            User us = ExperimentSetup.users.get(ExperimentSetup.user_logins.get(u));
            LinkedList<Long> wait_times = us.getWait_times();

            final List list = new ArrayList();
            String user_login = us.getLogin();

            for (int jj = 0; jj < wait_times.size(); jj++) {
                long job_wait = wait_times.get(jj);
                double wait = job_wait / 60.0;
                // wait time distribution
                if (wait < 0.001) {
                    wait = 0.001;
                }
                list.add(wait);

            }
            dataset.add(list, user_login, data_set_name);

        }

        return dataset;
    }

}
