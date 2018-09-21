package com.bdsassingment;

public class Pair implements Comparable {
    private String word;
    private int count;

    Pair(String word, int count) {
        this.word = word;
        this.count = count;
    }


    String getWord() {
        return word;
    }

    int getCount() {
        return count;
    }

    String concatinated() {
        return this.word + "\t" + this.count;
    }

    @Override
    public int compareTo(Object o) {
        Pair other = (Pair) o;
        if (other.count == this.count)
            return 0;
        return this.count > ((Pair) o).count ? 1 : -1;
    }
}
