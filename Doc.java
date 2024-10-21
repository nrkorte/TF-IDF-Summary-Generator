package patwo;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.ArrayList;
import java.util.List;
import java.util.Arrays;

public class Doc {
    private Map<String, DocInfo> documentMap;

    public Doc(String rawText) {
        documentMap = parseDocument(rawText);
    }

    private Map<String, DocInfo> parseDocument(String rawText) {
        Map<String, DocInfo> docMap = new HashMap<>();

        String[] lines = rawText.split("\n");

        for (String line : lines) {
            if (line.trim().isEmpty()) {
                continue;
            }

            String[] parts = line.split("<====>");

            if (parts.length == 3 && !parts[1].isEmpty() && !parts[2].isEmpty()) {
                String docId = parts[1].trim();
                String docBody = formatDoc(parts[2].trim());

                DocInfo docInfo = new DocInfo(docBody);
                docMap.put(docId, docInfo);
            }
        }

        return docMap;
    }

    public Map<String, DocInfo> getDocumentMap() {
        return documentMap;
    }

    public String getDocumentBody(String docId) {
        return documentMap.get(docId).getDocumentBody();
    }

    public int getMaxFrequency(String docId) {
        return documentMap.get(docId).getMaxFrequency();
    }

    public List<String> getTop5(String docId) {
        return documentMap.get(docId).getTop5();
    }

    public List<String> parseSentences(String docId) {
        String documentBody = getDocumentBody(docId);

        String[] sentences = documentBody.split("[.!?]");

        List<String> sentenceList = new ArrayList<>(Arrays.asList(sentences));
        for (int i = 0; i < sentences.length; i++) {
            sentences[i] = sentences[i].trim();
        }

        return sentenceList;
    }

	public String formatDoc(String docText) {
        String ret = docText.replaceAll("[\\'\"()$&,*{}\\t\\];0-9%]", "");
        ret = ret.replaceAll("[^\\x00-\\x7F]", "").toLowerCase();
		return ret;
	}

    public class DocInfo {
        private String documentBody;
        private int maxFrequency;
        private List<String> top_5;

        public DocInfo(String docBody) {
            this.documentBody = docBody;
            this.maxFrequency = calculateMaxFrequency();
            this.top_5 = new ArrayList<>();
        }

        public String getDocumentBody() {
            return documentBody;
        }

        public int getMaxFrequency() {
            return maxFrequency;
        }

        public List<String> getTop5() {
            return top_5;
        }

        private int calculateMaxFrequency() {
            Map<String, Integer> frequencyMap = new HashMap<>();
            String[] words = documentBody.split("\\s+");

            for (String word : words) {
                word = word.toLowerCase(); // Normalize to lowercase
                if (frequencyMap.containsKey(word)) {
                    frequencyMap.put(word, frequencyMap.get(word) + 1);
                } else {
                    frequencyMap.put(word, 1);
                }
            }

            int maxFreq = 0;
            for (int frequency : frequencyMap.values()) {
                if (frequency > maxFreq) {
                    maxFreq = frequency;
                }
            }

            return maxFreq;
        }
    }
}


 
