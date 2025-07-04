package com.csv.application.util;

import java.util.Comparator;

public class SortUtils {

    /**
     * Creates a comparator for comparing string values in descending order.
     * If both values can be parsed as numbers, they are compared numerically in descending order.
     * Otherwise, values are compared as case-insensitive strings.
     * Null values are sorted last.
     *
     * @return a {@link Comparator} for comparing string values in descending order
     */
    public static Comparator<String> createValueComparator() {
        return Comparator.nullsLast((a, b) -> {
            //Try to convert a and b to number and if they are valid number perform sorting otherwise give NumberFormatException
            if (isNumeric(a) && isNumeric(b)) {
                return Double.compare(Double.parseDouble(b), Double.parseDouble(a));
            }
            return b.compareToIgnoreCase(a); // String comparison if a and b are strings
        });
    }

    /**
     * Determines whether the given string represents a valid numeric value.
     *
     * @param value the string to check
     * @return {@code true} if the value can be parsed as a {@code double}; {@code false} otherwise
     */
    private static boolean isNumeric(String value) {
        if (value == null) return false;
        try {
            Double.parseDouble(value);
            return true;
        } catch (NumberFormatException ex) {
            return false;
        }
    }
}
