package exp;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

public class Utils {
    private static final Trie trie = new Trie();

    public static final HashMap<String,String> hardLabel = new HashMap<String,String>(){{
        put("Harry Potter","Harry Potter");
        put("Voldemort","Voldemort");
		put("Hermione Granger","Hermione Granger");
		put("Albus Dumbledore","Albus Dumbledore");
        put("Dumbledor","Hogwarts");
        put("Professor Severus Snape","Professor Severus Snape");
		put("Sirius Black","Sirius Black");
		put("Draco Malfoy","Draco Malfoy");
		put("Rubeus Hagrid","Rubeus Hagrid");
        put("Ron Weasley","Ron Weasley");
        put("Weasley","Weasley");
		put("Fred Weasley","Weasley");
		put("George Weasley","Weasley");
		put("Ginny Weasley","Weasley");
		put("Godric Gryffindor","Godric Gryffindor");
    }};
    public static final HashMap<String,String> nameMp = new HashMap<String,String>(){{
        put("Harry","Harry Potter");
        put("Potter","Harry Potter");
        put("Ron","Ron Weasley");
        put("Hermione","Hermione Granger");
        put("Granger","Hermione Granger");
        put("Dumbledore","Albus Dumbledore");
        put("Albus","Albus Dumbledore");
        put("Hagrid","Rubeus Hagrid");
        put("Rubeus","Rubeus Hagrid");
        put("Snape","Professor Severus Snape");
        put("Malfoy","Draco Malfoy");
        put("Draco","Draco Malfoy");
        put("Neville","Neville Longbottom");
        put("Longbottom","Neville Longbottom");
        put("Sirius","Sirius Black");
        put("Black","Sirius Black");
        put("Fred","Fred Weasley");
        put("Gryffindor","Godric Gryffindor");
        put("George","George Weasley");
        put("Ginny","Ginny Weasley");
        put("Vernon","Vernon Dursley");
        put("Lupin","Professor Lupin");
        put("Dudley","Dudley Dursley");
        put("Percy","Percy Weasley");
        put("Slytherin","Salazar Slytherin");
        put("Moody","Mad-Eye Moody");
        put("Petunia","Petunia Dursley");
        put("Cedric","Cedric Diggory");
        put("Lockhart","Gibleroy Lockhart");
        put("Crabbe","Vincent Crabbe");
        put("Cho","Cho Chang");
        put("Riddle","Tom Riddle");
        put("Bagman","Ludo Bagman");
        put("Wood","Oliver Wood");
        put("Tom Riddle","Voldemort");
        put("Tom","Voldemort");
    }};

	public static ArrayList<String> tokenize(String value){
        ArrayList<String> list = new ArrayList<>();
        StringTokenizer tokenizer = new StringTokenizer(value,",.?:;'\"! \t\n");
        String tem = ""; Trie current = trie;
        while (tokenizer.hasMoreTokens()){
            String token = tokenizer.nextToken();
            Trie temNode = current.searchNode(token);
            if(temNode==null){
                if(!tem.equals("")&&current.isEnd()){
                    tem = Utils.nameMp.getOrDefault(tem, tem);
                    list.add(tem);
                }
                temNode = trie.searchNode(token);
                current = temNode==null? trie: temNode;
                tem = temNode==null? "": token;
            }else{
                current = temNode;
                tem = tem.equals("") ? token: tem + " " + token;
            }
        }
        if(!tem.equals("")&&current.isEnd()){
            tem = Utils.nameMp.getOrDefault(tem, tem);
			list.add(tem);
        }
        return list;
    }

    public static void insertName(String name){
        trie.insert(name);
    }

    public static class Trie {
        private Map<String, Trie> children;
        private boolean end;

        public Trie() {
            children = new HashMap<>();
            end = false;
        }

        public void insert(String word) {
            String[] parts = word.split(" ");
            Trie current = this;
            for (String part: parts) {
                Trie next = current.children.getOrDefault(part,new Trie());
                current.children.put(part,next);
                current = next;
            }
            current.end = true;
        }

        public boolean search(String word) {
            String[] parts = word.split(" ");
            Trie current = this;
            for (String part: parts) {
                Trie next = current.children.getOrDefault(part,null);
                if(next==null) return false;
                current = next;
            }
            return current.end;
        }

        public boolean startWith(String word) {
            String[] parts = word.split(" ");
            Trie current = this;
            for (String part: parts) {
                Trie next = current.children.getOrDefault(part,null);
                if(next==null) return false;
                current = next;
            }
            return true;
        }
        public Trie searchNode(String word) {
            return children.getOrDefault(word,null);
        }
        public boolean isEnd(){
            return end;
        }
    }
}
