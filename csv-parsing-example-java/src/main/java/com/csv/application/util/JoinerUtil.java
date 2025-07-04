package com.csv.application.util;

import com.csv.application.domain.exception.EmptyHeaderException;
import com.csv.application.domain.model.ErrorResponse;
import com.csv.application.domain.model.Table;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

public class JoinerUtil {

    /**
     * Validates that the column names and table objects are not null or empty.
     *
     * @param leftColumnName  the column name from the left table
     * @param rightColumnName the column name from the right table
     * @param leftTable       the left table
     * @param rightTable      the right table
     * @return {@code true} if any input is null or empty; {@code false} otherwise
     */
    public static boolean isInvalidColumnNameAndTable(String leftColumnName,
                                                String rightColumnName,
                                                Table leftTable,
                                                Table rightTable) {
        return StringUtils.isBlank(leftColumnName) || StringUtils.isBlank(rightColumnName)
                || null == leftTable || null == rightTable;
    }

    public static ErrorResponse createErrorResponse(int statusCode, String message) {
        return new ErrorResponse(statusCode, message);
    }

    /**
     * Creates the header list for the joined table by combining the headers from the left table
     * with the headers from the right table (excluding the join key column from the right table).
     *
     * @param rightColumnName the column name used for joining from the right table
     * @param leftTable       the left table
     * @param rightTable      the right table
     * @return a combined list of headers for the joined table
     * @throws EmptyHeaderException if either table's headers are null or empty,
     *                              or if any header in the right table is null or empty
     */
    public static List<String> createJoinedHeaders(String rightColumnName, Table leftTable, Table rightTable) {

        if (ObjectUtils.isEmpty(leftTable.headers()) || ObjectUtils.isEmpty(rightTable.headers())) {
            throw new EmptyHeaderException("The left or right table header list is empty or null.");
        }
        return Stream.concat(leftTable.headers().stream(),
                rightTable.headers().stream().filter(header ->
                {
                    if (StringUtils.isEmpty(header)) {
                        throw new EmptyHeaderException("The header from right table is empty.");
                    }
                    return !header.equals(rightColumnName);
                })).collect(toList());
    }
}
