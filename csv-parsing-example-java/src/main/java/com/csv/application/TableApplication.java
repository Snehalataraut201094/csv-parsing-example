package com.csv.application;

import com.csv.application.domain.enums.JoinType;
import com.csv.application.domain.model.Result;
import com.csv.application.domain.model.Table;
import com.csv.application.processor.DataReaderImpl;
import com.csv.application.processor.HashJoinImpl;
import com.csv.application.processor.InnerNestedLoopJoinImpl;
import com.csv.application.processor.TableSorterImpl;
import com.csv.application.processor.interfaces.TableJoiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Scanner;

public class TableApplication {

    private static final Logger log = LoggerFactory.getLogger(TableApplication.class);

    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);

        String leftCSVPath = "src/main/resources/users.csv";

        String rightCSVPath = "src/main/resources/purchases.csv";

        System.out.println("Enter column name to sort the left table by DESC: ");
        String columnNameToSort = scanner.nextLine().trim();

        System.out.println("Enter join type (HASH or NESTED): ");
        String joinTypeInput = scanner.nextLine().trim().toUpperCase();

        TableJoiner tableJoiner = getJoinerType(joinTypeInput);

        TableApplicationService service = new TableApplicationService(new DataReaderImpl(), new TableSorterImpl(), tableJoiner);

        Result<Table> result = service.process(leftCSVPath, rightCSVPath, columnNameToSort);

        if (result.isSuccess()) {
            Table finalTable = result.data();
            log.debug("Final joined and sorted table is. : {}", finalTable);
        } else {
            log.error("Application failed: {}", result.error());
        }
    }

    private static TableJoiner getJoinerType(String joinerType) {
        JoinType joinType = JoinType.valueOf(joinerType);
        return switch (joinType) {
            case NESTED -> new InnerNestedLoopJoinImpl();
            default -> new HashJoinImpl();
        };
    }
}
