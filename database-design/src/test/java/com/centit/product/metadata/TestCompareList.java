package com.centit.product.metadata;

import com.centit.support.algorithm.CollectionsOpt;
import com.centit.support.algorithm.StringBaseOpt;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;

import java.util.Arrays;
import java.util.List;

public class TestCompareList {
    public static void main(String[] args) {
        List<Integer> oldList = Arrays.asList(6, 8, 3, 4, 5);
        List<Integer> newList = Arrays.asList(6, 7, 8, 9);

        Triple<List<Integer>, List<Pair<Integer, Integer>>, List<Integer>>  cmp
            = CollectionsOpt.compareTwoList(newList, oldList, Integer::compare);

        System.out.println(StringBaseOpt.objectToString(cmp));
        System.out.println(StringBaseOpt.objectToString(cmp.getLeft()));
        System.out.println(StringBaseOpt.objectToString(cmp.getMiddle()));
        System.out.println(StringBaseOpt.objectToString(cmp.getRight()));
    }
}
