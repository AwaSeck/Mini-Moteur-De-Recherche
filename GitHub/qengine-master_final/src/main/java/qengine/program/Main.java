package qengine.program;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import javax.swing.SwingUtilities;
import javax.swing.WindowConstants;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.jena.ontology.OntTools.Path;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.util.FileManager;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.helpers.StatementPatternCollector;
import org.eclipse.rdf4j.query.parser.ParsedQuery;
import org.eclipse.rdf4j.query.parser.sparql.SPARQLParser;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFParser;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.rio.ntriples.NTriplesParser;

final class Main {
	
	
	static final String baseURI = null;
	static final String workingDir = "data/";
	static final String outputDir = "output/";

	static final String queryFile = workingDir + "13K.queryset";
	//static final String queryFile = workingDir + "6500.queryset";
	//static final String queryFile = workingDir + "2600.queryset";
	//static final String queryFile = workingDir + "STAR_ALL_workload.queryset";

	static final String dataFile = workingDir + "2M.nt";
	//static final String dataFile = workingDir + "1M.nt";
	//static final String dataFile = workingDir + "500K.nt";
	//static final String dataFile = workingDir + "100K.nt";
	
	//static final String outputFile = outputDir + "outputFileCMD.csv";
	//static final String outputFile = outputDir + "export_query_500K.csv";
	static final String outputFile = outputDir + "export_query_2M.csv";
	static final String outputFileTime = outputDir + "output.csv";
	
	
	private static List<Statement> statementList;
	private static Dictionary dictionary;
	private static Hexastore hexastore;
	private static BufferedWriter writer;
	
	
	// D�clarer une liste pour stocker les requ�tes
	static List<ParsedQuery> queriesList= new ArrayList<>();;
	
	//Calcul du nb de requetes
	private static int countNbReq = 1;
	private static int nbReponse = 0 ;
	private static List<Integer> tabReponse = new ArrayList<>();
	private static int duplicateCount = 0;
	
    private static long startTimeParseData;
    private static long endTimeParseData;
    private static long endTimeDictionary;
    private static long endTimeHexastore;
    private static long endTimeparseQueries; 
    private static long endTimeprocessAQuery;

	// ========================================================================

    public static void processAQuery(ParsedQuery query, Model rdfModel) throws IOException {
        List<StatementPattern> patterns = StatementPatternCollector.process(query.getTupleExpr());

        String predicate = null;
        String object = null;
        int keyPredicate = -1;
        int keyObject = -1;
        List<Integer> result = new ArrayList<>();
        int i = 0;
        for (StatementPattern pattern : patterns) {
            System.out.println("\nPrédicat du pattern :" + pattern.getPredicateVar().getValue());
            System.out.println("Object du pattern :" + pattern.getObjectVar().getValue());
            writer.write("Prédicat du pattern :" + pattern.getPredicateVar().getValue());
            writer.write("\nObject du pattern :" + pattern.getObjectVar().getValue());

            predicate = pattern.getPredicateVar().getValue().toString();
            object = pattern.getObjectVar().getValue().toString();
            keyPredicate = dictionary.getKey(predicate);
            keyObject = dictionary.getKey(object);
            if (keyPredicate == -1 || keyObject == -1) {
                System.out.println("Predicate ou Object inexistante");
                writer.write("Predicate ou Object inexistantes\n");
                return;
            }
            if (i == 0) {
                result.addAll(hexastore.getSubject("ops", keyPredicate, keyObject));
            } else {
                result.retainAll(hexastore.getSubject("ops", keyPredicate, keyObject));
            }
            i += 1;
        }

        writer.write("\nLa liste des réponses:\n");

        if (result.size() == 0) {
            System.out.println("\nPas de résultat trouvé pour cette requête.");
           writer.write("Pas de résultat trouvé pour cette requête.\n");
        }
        nbReponse = result.size();
        for (int cle : result) {
            System.out.println("--> key :" + cle + ", Value :" + dictionary.getValue(cle));
            writer.write("key :" + cle + ", Value :" + dictionary.getValue(cle) + "\n");
        }
        writer.write("le nombre de réponse: " + nbReponse + "\n");

        System.out.println("--------------------------------------------------");
        writer.write("--------------------------------------------------------\n");
    }
	

	public static void main(String[] args) throws Exception {		
		startTimeParseData = System.currentTimeMillis();
		parseData();
		endTimeParseData = System.currentTimeMillis();
		
		dictionary = new Dictionary(statementList);
		dictionary.createDictionary();

		endTimeDictionary = System.currentTimeMillis();
		
		hexastore = new Hexastore(statementList, dictionary);
		hexastore.creationIndexHexastore();
		endTimeHexastore = System.currentTimeMillis();
		
		writer = new BufferedWriter(new FileWriter(outputFile));
		parseQueries();
		endTimeparseQueries = System.currentTimeMillis();
		
		// Charger le modèle RDF à partir du fichier N-Triples
	    Model rdfModel = ModelFactory.createDefaultModel();
	    FileManager.get().readModel(rdfModel, dataFile, null, "N-TRIPLE");

	    for (ParsedQuery query : queriesList) {
	        System.out.println("la requete " + countNbReq + ":");
	        writer.write("la requete " + countNbReq + ":\n");
	        processAQuery(query, rdfModel); // Passez le modèle RDF à la méthode
	        tabReponse.add(nbReponse);
	        countNbReq++;
	    }
		
	    endTimeprocessAQuery = System.currentTimeMillis();
	    
	    // affichage du nb de doublons dans la console
	    System.out.println("Nombre total de doublons dans les requêtes : "+duplicateCount+"\n");
        
	    
	    // Mesurer le temps de lecture des donn�es
        long dataReadTime = endTimeParseData - startTimeParseData;
        // Mesurer le temps de Cr�er le dictionnaire
        long dictionaryCreationTime =  endTimeDictionary - endTimeParseData;
        // Mesurer le temps de Cr�er l'index
        long hexastoreCreationTime =  endTimeHexastore - endTimeDictionary;
        // Mesurer le temps de lecture des requ�tes
        long queryReadTime = endTimeparseQueries - endTimeHexastore;
        // Mesurer le temps total d'�valuation du workload
        long totalEvaluationTime = endTimeprocessAQuery - endTimeparseQueries;
        // Mesurer le temps total du programme (du d�but � la fin)
        long totalTime = System.currentTimeMillis() - startTimeParseData;
        
        // Afficher les r�sultats dans la console 
        System.out.println("Nombre de triplets RDF : " + 2044605);
        System.out.println("Nombre de requetes : " + (countNbReq-1));
        System.out.println("Temps de lecture des donn�es : " + dataReadTime + " ms");
        System.out.println("Temps de lecture des requ�tes : " + queryReadTime + " ms");
        System.out.println("Temps de cr�ation du dictionnaire : " + dictionaryCreationTime + " ms");
        System.out.println("Temps de cr�ation des index : " + hexastoreCreationTime + " ms");
        System.out.println("Temps total d'�valuation du workload : " + totalEvaluationTime + " ms");
        System.out.println("Temps total (du d�but � la fin du programme) : " + totalTime + " ms");
        
        
        //Histogramme
        SwingUtilities.invokeLater(() -> {
            Histogramme example = new Histogramme("Histogramme pour 13K requêtes sur 2M de données", tabReponse);
            example.setSize(800, 600);
            example.setLocationRelativeTo(null);
            example.setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE);
            example.setVisible(true);
        });
        
        
       
		//pour la partie ligne de commande:
        CommandLineParser parser = new DefaultParser();
        Options options = createOptions();        
        try {
            CommandLine cmd = parser.parse(options, args);
            handleOptions(cmd);
            writer.close();
        } catch (ParseException e) {
            System.err.println("Erreur lors de l'analyse des arguments : " + e.getMessage());
            printHelp(options);
        }

	}

	// ========================================================================
	
    private static Options createOptions() {
        Options options = new Options();

        // Ajoutez les options n�cessaires � votre programme
        options.addOption("queries", true, "Chemin vers le dossier des requ�tes");
        options.addOption("data", true, "Chemin vers le fichier de donn�es");
        options.addOption("output", true, "Chemin vers le dossier de sortie");
        options.addOption("Jena", false, "Active la v�rification Jena");
        options.addOption("warm", true, "Utilise un �chantillon des requ�tes pour chauffer le syst�me");
        options.addOption("shuffle", false, "Consid�re une permutation al�atoire des requ�tes");

        return options;
    }

    
    private static void handleOptions(CommandLine cmd) {
        String queriesPath = cmd.getOptionValue("queries");
        String dataPath = cmd.getOptionValue("data");
        String outputPath = cmd.getOptionValue("output");
        boolean useJena = cmd.hasOption("Jena");
        String warmPercentage = cmd.getOptionValue("cold");
        boolean shuffleQueries = cmd.hasOption("shuffle");

        // Exemple d'utilisation des options
        System.out.println("Chemin vers le dossier des requ�tes : " + queriesPath);
        System.out.println("Chemin vers le fichier de donn�es : " + dataPath);
        System.out.println("Chemin vers le dossier de sortie : " + outputPath);
        System.out.println("Utiliser Jena : " + useJena);
        System.out.println("Pourcentage d'�chantillon pour chauffer le syst�me : " + warmPercentage);
        System.out.println("Permuter al�atoirement les requ�tes : " + shuffleQueries);
        
        
        // Mesurer le temps de lecture des donn�es
        long dataReadTime = endTimeParseData - startTimeParseData;
        // Mesurer le temps de Cr�er le dictionnaire
        long dictionaryCreationTime =  endTimeDictionary - endTimeParseData;
        // Mesurer le temps de Cr�er l'index
        long hexastoreCreationTime =  endTimeHexastore - endTimeDictionary;
        // Mesurer le temps de lecture des requ�tes
        long queryReadTime = endTimeparseQueries - endTimeHexastore;
        // Mesurer le temps total d'�valuation du workload
        long totalEvaluationTime = endTimeprocessAQuery - endTimeparseQueries;
        // Mesurer le temps total du programme (du d�but � la fin)
        long totalTime = System.currentTimeMillis() - startTimeParseData;
        
        // Exporter les r�sultats dans un fichier CSV
       // exportResultsToCSV(outputPath, dataPath, queriesPath);
        try (CSVPrinter csvPrinter = new CSVPrinter(new FileWriter(outputPath, true), CSVFormat.DEFAULT)) {
        	// Afficher le r�sultat dans le terminal
			csvPrinter.printRecord("Nom du fichier de donn�es: "+dataPath);
			csvPrinter.printRecord("Nom du dossier des requetes: "+queriesPath);
			csvPrinter.printRecord("Nombre de triplets RDF: "+ 107338);
			csvPrinter.printRecord("Nombre de requetes: "+(countNbReq-1));
			csvPrinter.printRecord("Temps de lecture des donnees: "+dataReadTime);
			csvPrinter.printRecord("Temps de lecture des requetes: "+queryReadTime);
			csvPrinter.printRecord("Temps de cr�ation du dictionnaire: "+dictionaryCreationTime);
			csvPrinter.printRecord("Temps de cr�ation des index: "+hexastoreCreationTime);
			csvPrinter.printRecord("Temps total d'�valuation du workload: "+totalEvaluationTime);
			csvPrinter.printRecord("Temps total (du d�but � la fin du programme): "+totalTime);
			
		} catch (IOException e) {
				e.printStackTrace();
			}
        
    }
    
    private static void printHelp(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("java -jar rdfengine", options);
     
        
    }
    
    
	private static void parseQueries() throws FileNotFoundException, IOException {
		try (Stream<String> lineStream = Files.lines(Paths.get(queryFile))) {
			SPARQLParser sparqlParser = new SPARQLParser();
			Iterator<String> lineIterator = lineStream.iterator();
			StringBuilder queryString = new StringBuilder();
	        Set<String> uniqueQueries = new HashSet<>();
			
			// On stocke plusieurs lignes jusqu'à ce que l'une d'entre elles se termine par un '}'On considère alors que c'est la fin d'une requête 
			while (lineIterator.hasNext()){  
				String line = lineIterator.next();
				queryString.append(line);
				if (line.trim().endsWith("}")) {
	                // Vérifier si la requête est déjà présente dans l'ensemble
	                if (!uniqueQueries.add(queryString.toString())) {
	                    duplicateCount++;
	                }
					ParsedQuery query = sparqlParser.parseQuery(queryString.toString(), baseURI);
	                // Stocker la requ�te dans la liste
	                queriesList.add(query);
					queryString.setLength(0);					
				}
			}
		}
	}

	private static void parseData() throws FileNotFoundException, IOException {

		try (Reader dataReader = new FileReader(dataFile)) {
			MainRDFHandler rdfHandler = new MainRDFHandler();
			// On va parser des données au format ntriples
			NTriplesParser rdfParser = new NTriplesParser();

			// On utilise notre implémentation de handler
			rdfParser.setRDFHandler(rdfHandler);

			// Parsing et traitement de chaque triple par le handler
			rdfParser.parse(dataReader, baseURI);
			statementList = rdfHandler.getStatementList();
		}
	}
}
