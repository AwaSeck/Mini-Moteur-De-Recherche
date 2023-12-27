package qengine.program;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.data.category.CategoryDataset;
import org.jfree.data.category.DefaultCategoryDataset;

import javax.swing.*;
import java.awt.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SuppressWarnings("serial")
public class Histogramme extends JFrame {

    public Histogramme(String title, List<Integer> data) {
        super(title);

        // Convertir la liste en tableau d'entiers
        int[] dataArray = data.stream().mapToInt(Integer::intValue).toArray();

        // Créer un jeu de données pour l'histogramme
        CategoryDataset dataset = createDataset(dataArray);

        // Créer le graphique à barres
        JFreeChart chart = ChartFactory.createBarChart(
                title,
                "Réponses",              // Axe des X
                "Occurrence",          // Axe des Y
                dataset
        );

        // Ajouter le graphique à une interface graphique Swing
        ChartPanel chartPanel = new ChartPanel(chart);
        chartPanel.setPreferredSize(new Dimension(800,600));
        setContentPane(chartPanel);
    }

    private CategoryDataset createDataset(int[] data) {
        DefaultCategoryDataset dataset = new DefaultCategoryDataset();

        // Calculer les valeurs et occurrences
        Map<Integer, Integer> histogram = calculateHistogram(data);

        // Ajouter les données au jeu de données
        for (int value : histogram.keySet()) {
            dataset.addValue(histogram.get(value), "Occurrence", String.valueOf(value));
        }

        return dataset;
    }

    private Map<Integer, Integer> calculateHistogram(int[] data) {
        Map<Integer, Integer> histogram = new HashMap<>();

        for (int value : data) {
        	//int bin = value * 100;
            histogram.put(value, histogram.getOrDefault(value, 0) + 1);
        }

        return histogram;
    }
    
    public static void main(String[] args) {
    	List<Integer> data = List.of(1,2,1,6,7,3,9,65,3,2,1,0,0,0,6,1,1,1,2,21,21,34,55);
        SwingUtilities.invokeLater(() -> {
        	Histogramme example = new Histogramme("Histogram Example", data);
            example.setSize(800, 600);
            example.setLocationRelativeTo(null);
            example.setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE);
            example.setVisible(true);
        });
    }
    
}
