package com.csv.application.processor;

import com.csv.application.processor.interfaces.TableSorter;
import com.csv.application.domain.enums.HttpStatusCode;
import com.csv.application.domain.model.ErrorResponse;
import com.csv.application.domain.model.Result;
import com.csv.application.domain.model.Row;
import com.csv.application.domain.model.Table;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.Comparator;
import java.util.List;

import static com.csv.application.util.SortUtils.createValueComparator;

public class TableSorterImpl implements TableSorter {
    @Override
    public Result<Table> sortTableByDesc(Table table, String columnName) {

        if (isInvalidColumnName(table, columnName)) {
            return Result.failure(new ErrorResponse(HttpStatusCode.BAD_REQUEST.getCode(),
                    "The specified column does not exist in the table."));
        }
        List<Row> rows = table.rows().stream()
                .sorted(Comparator.comparing(row -> row.get(columnName), createValueComparator()))
                .toList();
        rows.forEach(System.out::println);

        return Result.success(new Table(table.headers(), rows));
    }

    /**
     * Checks if the specified column name is invalid for the given {@link Table}.
     * A column is considered invalid if the name is blank, the table has no headers,
     * or the column does not exist in the header list.
     *
     * @param table      the table to validate against
     * @param columnName the column name to check
     * @return {@code true} if the column name is invalid; {@code false} otherwise
     */
    private boolean isInvalidColumnName(Table table, String columnName) {
        return columnName == null || StringUtils.isBlank(columnName) ||
                CollectionUtils.isEmpty(table.headers()) || !table.headers().contains(columnName);
    }
}
