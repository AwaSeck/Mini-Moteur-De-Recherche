package qengine.program;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.eclipse.rdf4j.model.Statement;


public class Dictionary {

    private Map<Integer, String> dictionary;
    private Map<String, Integer> reverseDictionary;  
    private List<Statement> statementList;
    int taille ;

    public Dictionary(List<Statement> statementList) {
        this.dictionary = new HashMap<>();
        this.reverseDictionary = new HashMap<>();
        this.statementList = new ArrayList<>(statementList);
    }


    @SuppressWarnings("deprecation")
	public void createDictionary() {
        HashSet<String> existingValues = new HashSet<>(dictionary.values());

        for (Statement st : statementList) {
            // Ajouter les valeurs du sujet, prédicat et objet s'ils n'existent pas dans le dictionnaire
            addValueToDictionary(st.getSubject().toString(), existingValues);
            addValueToDictionary(st.getPredicate().toString(), existingValues);
            addValueToDictionary(st.getObject().toString(), existingValues);
        }
    }

    private void addValueToDictionary(String value, HashSet<String> existingValues) {
        if (!existingValues.contains(value)) {
            int key = dictionary.size();
            dictionary.put(key, value);
            reverseDictionary.put(value, key);  // Ajouter à la map inversée
            existingValues.add(value);
        }
    }

    public Map<Integer, String> getDictionary() {
        return dictionary;
    }

    public int getKey(String value) {
        return reverseDictionary.getOrDefault(value, -1);
    }

    public String getValue(int key) {
        return dictionary.get(key);
    }
}
