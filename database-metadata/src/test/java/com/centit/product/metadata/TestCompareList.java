package com.centit.product.metadata;

import com.centit.support.algorithm.CollectionsOpt;

import java.util.Arrays;
import java.util.List;

public class TestCompareList {
    public static void main(String[] args) {
        List<Integer> oldList = Arrays.asList(1, 2, 3, 4, 5);
        List<Integer> newList = Arrays.asList(6, 7, 8, 9);

        CollectionsOpt.compareTwoList(newList, oldList, Integer::compare);
    }
}
