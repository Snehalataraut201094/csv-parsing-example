package com.csv.application.processor.interfaces;

import com.csv.application.domain.model.Result;
import com.csv.application.domain.model.Table;

public interface TableJoiner {

    /**
     * Performs an inner join operation between two {@link Table} objects based on the specified
     * join keys from the left and right tables.
     * This method returns a new {@link Table} containing rows where the value in {@code leftKey}
     * from the left table matches the value in {@code rightKey} from the right table.
     * The result table includes all columns from the left table and all columns from the right table,
     * excluding the duplicate join key column from the right table.
     * If the column names or tables are null/empty, or if a join fails due to header issues,
     * a failure {@link Result} is returned with an appropriate error message.
     *
     * @param leftKey  the column name to join on from the left table
     * @param rightKey the column name to join on from the right table
     * @param left     the left {@link Table}
     * @param right    the right {@link Table}
     * @return a {@link Result} containing the joined {@link Table} on success,
     * or a failure result with an error message if the input is invalid or the join fails
     */
    Result<Table> joinTables(String leftKey,
                             String rightKey,
                             Table left,
                             Table right);
}
