package qengine.program;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import org.eclipse.rdf4j.model.Statement;

public class Hexastore {

    private List<Statement> statementList;
    private Dictionary dictionary;
    private HashMap<Integer, HashMap<Integer, List<Integer>>> ops;
    /*
    private HashMap<Integer, HashMap<Integer, List<Integer>>> sop;
    private HashMap<Integer, HashMap<Integer, List<Integer>>> spo;
    private HashMap<Integer, HashMap<Integer, List<Integer>>> osp;
    private HashMap<Integer, HashMap<Integer, List<Integer>>> pso;
    private HashMap<Integer, HashMap<Integer, List<Integer>>> pos;
*/
    
    public Hexastore(List<Statement> statementList, Dictionary dictionary) {
        //initializeHexastore();
        this.ops = new HashMap<>();
        this.statementList = new ArrayList<>(statementList);
        this.dictionary = dictionary;
    }

    /*
    private void initializeHexastore() {
        this.ops = new HashMap<>();
        this.sop = new HashMap<>();
        this.spo = new HashMap<>();
        this.osp = new HashMap<>();
        this.pso = new HashMap<>();
        this.pos = new HashMap<>();
    }
*/

    public void creationIndexHexastore() {
        List<List<Integer>> indexData = extractIndexData();

        for (List<Integer> row : indexData) {
            int key1 = row.get(0);
            int key2 = row.get(1);
            int value = row.get(2);

            updateIndex(ops, key1, key2, value);
            /*
            updateIndex(sop, key1, key2, value);
            updateIndex(spo, key1, key2, value);
            updateIndex(osp, key1, key2, value);
            updateIndex(pso, key1, key2, value);
            updateIndex(pos, key1, key2, value);
            */
        }
    }
    
    private List<List<Integer>> extractIndexData() {
        int expectedSize = 2 * statementList.size();  // ou une autre valeur qui convient
        List<List<Integer>> indexData = new ArrayList<>(expectedSize);

        for (Statement st : statementList) {
            String subject = st.getSubject().toString();
            @SuppressWarnings("deprecation")
            String predicate = st.getPredicate().toString();
            String object = st.getObject().toString();
            int keySub = dictionary.getKey(subject);
            int keyPred = dictionary.getKey(predicate);
            int keyObj = dictionary.getKey(object);

            indexData.add(Arrays.asList(keyObj, keyPred, keySub));
            /*
            indexData.add(Arrays.asList(keySub, keyObj, keyPred));
            indexData.add(Arrays.asList(keySub, keyPred, keyObj));
            indexData.add(Arrays.asList(keyObj, keySub, keyPred));
            indexData.add(Arrays.asList(keyPred, keySub, keyObj));
            indexData.add(Arrays.asList(keyPred, keyObj, keySub));
            */
        }

        return indexData;
    }


    private void updateIndex(HashMap<Integer, HashMap<Integer, List<Integer>>> index, int key1, int key2, int value) {
        HashMap<Integer, List<Integer>> innerMap = index.get(key1);

        if (innerMap == null) {
            innerMap = new HashMap<>();
            index.put(key1, innerMap);
        }

        List<Integer> list = innerMap.get(key2);

        if (list == null) {
            list = new ArrayList<>();
            innerMap.put(key2, list);
        }

        list.add(value);
    }


    public List<Integer> getSubject(String hex, int predicate, int object) {
        List<Integer> res = new ArrayList<>();
        if (hex.equals("ops")) {
            HashMap<Integer, List<Integer>> map1 = ops.get(object);
            if (map1 != null && map1.containsKey(predicate)) {
                res.addAll(map1.get(predicate));
            } else {
                res.add(-1);
            }
        } else {
            res.add(-1);
        }
        return res;
    }
    
}


