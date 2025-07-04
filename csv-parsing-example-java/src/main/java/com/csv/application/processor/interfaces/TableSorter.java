package com.csv.application.processor.interfaces;

import com.csv.application.domain.model.Result;
import com.csv.application.domain.model.Table;

public interface TableSorter {

    /**
     * Sorts the rows of the given {@link Table} in descending order based on the specified column name.
     * The method first validates whether the provided column name exists in the table headers.
     * If the column is invalid, it returns a {@link Result} with a failure response and
     * {@code 400 Bad Request} status.
     * The sorting logic prioritizes numerical values if the column data can be parsed as numbers;
     * otherwise, it performs case-insensitive string comparison. {@code null} values are placed last.
     *
     * @param table      the table containing rows to be sorted
     * @param columnName the name of the column to sort by in descending order
     * @return a {@link Result} containing a new {@link Table} with rows sorted in descending order
     * by the specified column, or a failure result if the column is invalid
     */
    Result<Table> sortTableByDesc(Table table, String columnName);
}
