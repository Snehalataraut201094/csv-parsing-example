package com.csv.application.processor;

import com.csv.application.processor.interfaces.TableJoiner;
import com.csv.application.domain.enums.HttpStatusCode;
import com.csv.application.domain.exception.EmptyHeaderException;
import com.csv.application.domain.model.Result;
import com.csv.application.domain.model.Row;
import com.csv.application.domain.model.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

import static com.csv.application.util.JoinerUtil.createErrorResponse;
import static com.csv.application.util.JoinerUtil.createJoinedHeaders;
import static com.csv.application.util.JoinerUtil.isInvalidColumnNameAndTable;

public class InnerNestedLoopJoinImpl implements TableJoiner {

    private static final Logger log = LoggerFactory.getLogger(InnerNestedLoopJoinImpl.class);

    @Override
    public Result<Table> joinTables(String leftColumnName,
                                    String rightColumnName,
                                    Table leftTable,
                                    Table rightTable) {

        if (isInvalidColumnNameAndTable(leftColumnName, rightColumnName, leftTable, rightTable)) {
            return Result.failure(createErrorResponse(HttpStatusCode.BAD_REQUEST.getCode(),
                    "The either left or right columnName or table itself is empty or null."));
        }
        try {
            List<String> joinedHeaders = createJoinedHeaders(rightColumnName, leftTable, rightTable);
            List<Row> joinedRows = createJoinedRows(leftColumnName, rightColumnName, leftTable, rightTable);

            return Result.success(new Table(joinedHeaders, joinedRows));
        } catch (EmptyHeaderException ex) {
            return Result.failure(createErrorResponse(
                    HttpStatusCode.INTERNAL_SERVER_ERROR.getCode(), ex.getMessage()));
        }
    }

    /**
     * Generates the list of joined {@link Row} objects from two tables using the specified join keys.
     * <p>
     * For each matching pair of rows (based on equality of join key values), a new row is created
     * by merging data from both tables. The right table’s join key column is excluded in the result.
     * </p>
     *
     * @param leftColumnName  the join key from the left table
     * @param rightColumnName the join key from the right table
     * @param leftTable       the left table
     * @param rightTable      the right table
     * @return a list of joined {@link Row} objects
     */
    private List<Row> createJoinedRows(String leftColumnName, String rightColumnName,
                                       Table leftTable, Table rightTable) {
        List<Row> joinedRows = new ArrayList<>();
        for (Row leftRow : leftTable.rows()) {
            if (isRowInvalid(leftRow, leftColumnName)) {
                log.warn("Skipping invalid left row and columnName : {}, {}", leftRow, leftColumnName);
                continue;
            }
            String leftValue = leftRow.get(leftColumnName).toLowerCase().trim();
            for (Row rightRow : rightTable.rows()) {
                if (isRowInvalid(rightRow, rightColumnName)) {
                    log.warn("Skipping invalid right row and columnName : {}, {}", rightRow, rightColumnName);
                    continue;
                }
                String rightValue = rightRow.get(rightColumnName).toLowerCase().trim();
                if (leftValue.equals(rightValue)) {
                    Row eachRow = createEachRow(rightColumnName, rightTable, leftRow, rightRow);
                    joinedRows.add(eachRow);
                }
            }
        }
        return joinedRows;
    }

    /**
     * Checks if a row is invalid for join operations.
     * A row is considered invalid if it is {@code null}, or if the value for the join column
     * is null, empty, or blank.
     *
     * @param row        the {@link Row} to check
     * @param joinColumn the column used for joining
     * @return {@code true} if the row is invalid; {@code false} otherwise
     */
    private boolean isRowInvalid(Row row, String joinColumn) {
        return row == null || row.get(joinColumn) == null || row.get(joinColumn).trim().isEmpty();
    }

    /**
     * Creates a new {@link Row} by combining a row from the left table with a row from the right table,
     * excluding the right table's join key column.
     *
     * @param rightColumnName the join key from the right table
     * @param rightTable      the right table
     * @param leftRow         the row from the left table
     * @param rightRow        the row from the right table
     * @return a new combined {@link Row}
     */
    private Row createEachRow(String rightColumnName, Table rightTable, Row leftRow, Row rightRow) {
        Row eachRow = new Row(new LinkedHashMap<>(leftRow.getData()));
        for (String header : rightTable.headers()) {
            if (!header.equals(rightColumnName)) {
                eachRow.set(header, rightRow.get(header));
            }
        }
        return eachRow;
    }
}
