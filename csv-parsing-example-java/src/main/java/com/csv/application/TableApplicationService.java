package com.csv.application;

import com.csv.application.domain.model.Result;
import com.csv.application.domain.model.Table;
import com.csv.application.processor.interfaces.DataReader;
import com.csv.application.processor.interfaces.TableJoiner;
import com.csv.application.processor.interfaces.TableSorter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TableApplicationService {

    public static final String JOIN_COLUMN = "USER_ID";

    private final DataReader dataReader;
    private final TableSorter tableSorter;
    private final TableJoiner tableJoiner;

    public TableApplicationService(DataReader dataReader,
                                   TableSorter tableSorter,
                                   TableJoiner tableJoiner) {
        this.dataReader = dataReader;
        this.tableSorter = tableSorter;
        this.tableJoiner = tableJoiner;
    }

    private static final Logger log = LoggerFactory.getLogger(TableApplicationService.class);

    public Result<Table> process(String leftPath, String rightPath, String sortColumn) {

        Result<Table> leftTableResult = readTable(leftPath);
        if (!leftTableResult.isSuccess()) return leftTableResult;

        Result<Table> rightTableResult = readTable(rightPath);
        if (!rightTableResult.isSuccess()) return rightTableResult;

        Table sortedLeftTable = sortTableDescending(leftTableResult.data(), sortColumn);

        Table sortedRightTable = sortTableDescending(rightTableResult.data(), sortColumn);

        return joinTables(leftTableResult.data(), rightTableResult.data());
    }

    private Result<Table> readTable(String path) {
        Result<Table> result = dataReader.readCSVData(path);

        if (!result.isSuccess()) {
            log.error("Error reading table from path: {}, error: {}", path, result.error());
        } else {
            log.debug("Loaded table from path: {}:\n{}", path, result.data());
        }
        return result;
    }

    private Table sortTableDescending(Table table, String columnName) {
        Result<Table> result = tableSorter.sortTableByDesc(table, columnName);

        if (!result.isSuccess()) {
            log.warn("Sorting failed: {}, using original table", result.error());
            return table;
        } else {
            log.debug("Sorted table data is {}", result.data());
        }
        return result.data();
    }

    private Result<Table> joinTables(Table left, Table right) {

        Result<Table> result = tableJoiner.joinTables(JOIN_COLUMN, JOIN_COLUMN, left, right);

        if (!result.isSuccess()) {
            log.error("Join operation failed: {}", result.error());
        } else {
            log.debug("Joined table data is {}", result.data());
        }
        return result;
    }
}
