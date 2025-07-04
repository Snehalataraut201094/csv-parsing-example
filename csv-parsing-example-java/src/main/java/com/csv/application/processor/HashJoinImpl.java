package com.csv.application.processor;

import com.csv.application.processor.interfaces.TableJoiner;
import com.csv.application.domain.enums.HttpStatusCode;
import com.csv.application.domain.exception.EmptyHeaderException;
import com.csv.application.domain.model.Result;
import com.csv.application.domain.model.Row;
import com.csv.application.domain.model.Table;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toMap;
import static com.csv.application.util.JoinerUtil.createErrorResponse;
import static com.csv.application.util.JoinerUtil.createJoinedHeaders;
import static com.csv.application.util.JoinerUtil.isInvalidColumnNameAndTable;

public class HashJoinImpl implements TableJoiner {

    private static final Logger log = LoggerFactory.getLogger(HashJoinImpl.class);

    public Result<Table> joinTables(String leftKey, String rightKey, Table left, Table right) {
        if (isInvalidColumnNameAndTable(leftKey, rightKey, left, right)) {
            return Result.failure(createErrorResponse(HttpStatusCode.BAD_REQUEST.getCode(),
                    "The either left or right columnName or table itself is empty or null."));
        }
        try {
            List<String> joinedHeaders = createJoinedHeaders(rightKey, left, right);
            List<Row> joinedRows = performHashJoin(leftKey, rightKey, left, right);

            return Result.success(new Table(joinedHeaders, joinedRows));
        } catch (EmptyHeaderException ex) {
            return Result.failure(createErrorResponse(
                    HttpStatusCode.INTERNAL_SERVER_ERROR.getCode(), ex.getMessage()));
        }
    }

    /**
     * Performs a hash-based inner join between two tables on the specified join columns.
     * This method builds a hash map from the right table based on the join key, and for each
     * row in the left table, finds matching rows in the right table. If a match is found,
     * it creates a new combined {@link Row} and adds it to the result.
     * Rows with null or blank keys in the left table are skipped and logged.
     *
     * @param leftColumnName  the join key column from the left table
     * @param rightColumnName the join key column from the right table
     * @param leftTable       the left table
     * @param rightTable      the right table
     * @return a list of {@link Row} objects that are the result of the join
     */
    private List<Row> performHashJoin(String leftColumnName,
                                      String rightColumnName,
                                      Table leftTable,
                                      Table rightTable) {

        Map<String, List<Row>> rightTableMap = buildRightTableMap(rightColumnName, rightTable);

        List<Row> joinedRows = new ArrayList<>();
        for (Row leftRow : leftTable.rows()) {
            String leftKey = null != leftRow.get(leftColumnName) ? leftRow.get(leftColumnName).trim().toLowerCase() : "";
            if (StringUtils.isEmpty(leftKey)) {
                log.warn("Skipping invalid left row for columnName. Since it's key is empty or null.: {}, {}",
                        leftRow, leftColumnName);
                continue;
            }
            List<Row> matchingRightRows = rightTableMap.get(leftKey);
            if (matchingRightRows == null) {
                log.warn("No matching right rows found for left key: '{}'", leftKey);
                continue;
            }
            for (Row rightRow : matchingRightRows) {
                joinedRows.add(createJoinedRow(leftRow, rightRow, rightColumnName));
            }
        }
        return joinedRows;
    }

    /**
     * Builds a hash map from the right table for efficient lookup during join.
     * The map uses the values from the specified {@code rightColumnName} as keys and groups
     * the corresponding rows in a list.
     * Rows with null or blank join key values are excluded from the map.
     *
     * @param rightColumnName the column name to use as the key
     * @param rightTable      the table to index
     * @return a map of join key to list of rows in the right table
     */
    private static Map<String, List<Row>> buildRightTableMap(String rightColumnName, Table rightTable) {
        return rightTable.rows().stream().filter(row -> {
            String rightKey = row.get(rightColumnName);
            return rightKey != null && !rightKey.isBlank();
        }).collect(Collectors.groupingBy(row -> row.get(rightColumnName).trim().toLowerCase()));
    }

    /**
     * Creates a new {@link Row} by combining the data from a row in the left table and a matching row
     * from the right table.
     * The right table's join key column is excluded to avoid duplication in the joined row.
     * If the same column name exists in both tables (except for the join key), the value from the
     * left table is preserved.
     *
     * @param leftRow         the row from the left table
     * @param rightRow        the matching row from the right table
     * @param rightJoinColumn the join column name from the right table
     * @return a new {@link Row} that combines values from both input rows
     */
    private Row createJoinedRow(Row leftRow, Row rightRow, String rightJoinColumn) {

        Map<String, String> hashMap = Stream.concat(
                        leftRow.getData().entrySet().stream(),
                        rightRow.getData()
                                .entrySet()
                                .stream()
                                .filter(entry -> !entry.getKey().equals(rightJoinColumn)))
                .collect(toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        (v1, v2) -> v1,
                        LinkedHashMap::new));
        return new Row(hashMap);
    }
}
