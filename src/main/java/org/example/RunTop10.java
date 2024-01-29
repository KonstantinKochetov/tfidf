package org.example;

import java.io.*;
import java.util.*;
import java.util.regex.*;

public class RunTop10 {
    public static void main(String[] args) {
        String[] fileNames = {"english.txt", "german.txt", "french.txt", "italien.txt", "spanish.txt", "dutch.txt", "russian.txt", "ukrainian.txt"};

        for (String fileName : fileNames) {
            System.out.println("Top 10 words for " + fileName + ":");
            try {
                List<WordEntry> wordEntries = getTopWords(fileName);
                for (int i = 0; i < Math.min(10, wordEntries.size()); i++) {
                    System.out.println(wordEntries.get(i).word);
                }
            } catch (IOException e) {
                System.err.println("Error processing file: " + fileName);
                e.printStackTrace();
            }
            System.out.println();
        }
    }

    private static List<WordEntry> getTopWords(String fileName) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(fileName));
        String line;
        Pattern pattern = Pattern.compile("^(.*?)@.*?\\[(.*?), (.*?), (.*?)\\]$");
        PriorityQueue<WordEntry> queue = new PriorityQueue<>(Comparator.comparingDouble((WordEntry w) -> w.number).reversed());

        while ((line = reader.readLine()) != null) {
            Matcher matcher = pattern.matcher(line);
            if (matcher.find()) {
                String word = matcher.group(1);
                double number = Double.parseDouble(matcher.group(4));
                queue.add(new WordEntry(word, number));
            }
        }
        reader.close();

        List<WordEntry> topWords = new ArrayList<>();
        Set<String> uniqueWords = new HashSet<>();
        while (topWords.size() < 100 && !queue.isEmpty()) {
            WordEntry entry = queue.poll();
            if (uniqueWords.add(entry.word)) {
                topWords.add(entry);
            }
        }

        if (topWords.size() > 10) {
            topWords = topWords.subList(0, 10);
        }

        return topWords;

    }

    private static class WordEntry {
        String word;
        double number;

        WordEntry(String word, double number) {
            this.word = word;
            this.number = number;
        }
    }
}
