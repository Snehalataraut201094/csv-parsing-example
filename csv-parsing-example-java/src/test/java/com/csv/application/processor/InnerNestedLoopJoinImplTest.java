package com.csv.application.processor;

import com.csv.application.domain.enums.HttpStatusCode;
import com.csv.application.domain.model.Result;
import com.csv.application.domain.model.Row;
import com.csv.application.domain.model.Table;
import com.csv.application.processor.interfaces.TableJoiner;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class InnerNestedLoopJoinImplTest {

    public static final String INVALID_COLUMN_TABLE_MSG =
            "The either left or right columnName or table itself is empty or null.";
    public static final String USER_ID = "USER_ID";
    private static final String INVALID_HEADERS_MSG = "The left or right table header list is empty or null.";
    public static final String NAME = "NAME";
    public static final String EMAIL = "EMAIL";
    public static final String AD_ID = "AD_ID";
    public static final String TITLE = "TITLE";

    private TableJoiner tableJoiner;

    @BeforeEach
    public void setup() {
        this.tableJoiner = new InnerNestedLoopJoinImpl();
    }

    @ParameterizedTest
    @MethodSource("invalidColumnNameAndTableValue")
    void returnError_whenColumnNameIsInvalidOrTableIsNull(String leftColumnName,
                                                          String rightColumnName,
                                                          Table leftTable,
                                                          Table rightTable) {

        Result<Table> tableResult = tableJoiner.joinTables(leftColumnName, rightColumnName, leftTable, rightTable);
        assertTableResultForError(tableResult, HttpStatusCode.BAD_REQUEST.getCode(), INVALID_COLUMN_TABLE_MSG);
    }

    @ParameterizedTest
    @MethodSource("emptyAndNullHeadersInTables")
    void returnError_whenHeadersInTablesAreEmptyOrNull(String leftColumnName,
                                                       String rightColumnName,
                                                       Table leftTable,
                                                       Table rightTable) {

        Result<Table> tableResult = tableJoiner.joinTables(leftColumnName, rightColumnName, leftTable, rightTable);
        assertTableResultForError(tableResult, HttpStatusCode.INTERNAL_SERVER_ERROR.getCode(), INVALID_HEADERS_MSG);
    }

    @ParameterizedTest
    @MethodSource("provideValidColumnNamesAndTables")
    void returnSuccess_whenValidColumnNamesAndTablesPresent(String leftColumnName,
                                                            String rightColumnName,
                                                            Table leftTable,
                                                            Table rightTable) {

        Result<Table> tableResult = tableJoiner.joinTables(leftColumnName, rightColumnName, leftTable, rightTable);
        assertTableResultSuccess(leftTable, rightTable, tableResult);
    }

    @ParameterizedTest
    @MethodSource("invalidLeftKeyAndNoMatchingRightRows")
    void returnEmptyResult_whenLeftKeyIsInvalidOrNoMatchingRightRow(String leftColumnName,
                                                                    String rightColumnName,
                                                                    Table leftTable,
                                                                    Table rightTable) {

        Result<Table> tableResult = tableJoiner.joinTables(leftColumnName, rightColumnName, leftTable, rightTable);

        assertNotNull(tableResult);
        assertTrue(tableResult.isSuccess());

        assertNotNull(tableResult.data());
        assertNotNull(tableResult.data().rows());
        assertThat(tableResult.data().rows()).hasSize(0);
    }

    static void assertTableResultForError(Result<Table> tableResult, int statusCode, String message) {
        assertNotNull(tableResult);
        assertFalse(tableResult.isSuccess());

        assertNull(tableResult.data());
        assertNotNull(tableResult.error());

        assertEquals(statusCode, tableResult.error().errorCode());
        assertEquals(message, tableResult.error().errorMessage());
    }

    private static void assertTableResultSuccess(Table leftTable, Table rightTable, Result<Table> tableResult) {
        assertNotNull(tableResult);
        assertTrue(tableResult.isSuccess());

        assertNotNull(tableResult.data());
        System.out.println(tableResult.data());
        assertNotNull(tableResult.data().rows());
        assertFalse(tableResult.data().rows().isEmpty(), "Joined table should contain at least one row");

        List<String> actualHeaders = tableResult.data().headers();
        List<String> expectedHeaders = List.of(USER_ID, NAME, EMAIL, AD_ID, TITLE);

        assertNotNull(actualHeaders);
        assertThat(actualHeaders).containsExactlyInAnyOrderElementsOf(expectedHeaders);
        assertThat(actualHeaders).allMatch(Objects::nonNull, "Headers should not contain null values");

        int maxExpectedRows = leftTable.rows().size() * rightTable.rows().size();
        assertThat(tableResult.data().rows().size()).isLessThanOrEqualTo(maxExpectedRows);

        assertAll("No null keys in row columns",
                tableResult.data().rows().stream()
                        .map(row -> () -> assertThat(row.rows().keySet()).noneMatch(Objects::isNull))
        );
    }

    private static Stream<Arguments> invalidColumnNameAndTableValue() {
        return Stream.of(
                Arguments.of(" ", USER_ID, createEmptyTable(), createEmptyTable()),
                Arguments.of(null, USER_ID, createEmptyTable(), createEmptyTable()),
                Arguments.of(USER_ID, " ", createEmptyTable(), createEmptyTable()),
                Arguments.of(USER_ID, null, createEmptyTable(), createEmptyTable()),
                Arguments.of(USER_ID, USER_ID, null, createEmptyTable()),
                Arguments.of(USER_ID, USER_ID, createEmptyTable(), null),
                Arguments.of(USER_ID, USER_ID, null, null),
                Arguments.of(null, null, null, null));
    }

    private static Stream<Arguments> emptyAndNullHeadersInTables() {
        return Stream.of(
                Arguments.of(USER_ID, USER_ID, new Table(null, List.of()), createEmptyTable()),
                Arguments.of(USER_ID, USER_ID, createEmptyTable(), createEmptyTable()),
                Arguments.of(USER_ID, USER_ID, new Table(List.of(USER_ID, NAME, EMAIL), List.of()), new Table(null, List.of())),
                Arguments.of(USER_ID, USER_ID, new Table(List.of(USER_ID, NAME, EMAIL), List.of()), createEmptyTable()));
    }

    private static Stream<Arguments> provideValidColumnNamesAndTables() {
        return Stream.of(Arguments.of(
                USER_ID,
                USER_ID,
                new Table(List.of(USER_ID, NAME, EMAIL), List.of(
                        new Row(Map.of(USER_ID, "1", NAME, "manuel", EMAIL, "manuel@foo.de")),
                        new Row(Map.of(USER_ID, "2", NAME, "andre", EMAIL, "andre@bar.de")),
                        new Row(Map.of(USER_ID, "3", NAME, "swen", EMAIL, "swen@foo.de")))),

                new Table(List.of(AD_ID, TITLE, USER_ID), List.of(
                        new Row(Map.of(AD_ID, "1", TITLE, "guitar-1", USER_ID, "3")),
                        new Row(Map.of(AD_ID, "2", TITLE, "table-2", USER_ID, "2")),
                        new Row(Map.of(AD_ID, "3", TITLE, "chair-1", USER_ID, "1"))))));
    }

    private static Stream<Arguments> invalidLeftKeyAndNoMatchingRightRows() {

        // 1. Case: leftKey is null
        Table leftWithNullKey = new Table(List.of(USER_ID), List.of(
                new Row(new HashMap<>() {{
                    put(USER_ID, null);
                }})));

        // 2. Case: leftKey is empty string
        Table leftWithEmptyKey = new Table(List.of(USER_ID), List.of(
                new Row(Map.of(USER_ID, ""))
        ));

        // 3. Case: leftKey has value not present in rightTable map
        Table leftWithNoMatch = new Table(List.of(USER_ID), List.of(
                new Row(Map.of(USER_ID, "999"))
        ));

        // right table contains valid keys (but not "999")
        Table rightTable = new Table(List.of(USER_ID), List.of(
                new Row(Map.of(USER_ID, "1")),
                new Row(Map.of(USER_ID, "2")),
                new Row(new HashMap<>() {{
                    put(USER_ID, "");
                }}),
                new Row(new HashMap<>() {{
                    put(USER_ID, null);
                }})
        ));

        return Stream.of(
                Arguments.of(USER_ID, USER_ID, leftWithNullKey, rightTable),
                Arguments.of(USER_ID, USER_ID, leftWithEmptyKey, rightTable),
                Arguments.of(USER_ID, USER_ID, leftWithNoMatch, rightTable)
        );
    }

    private static Table createEmptyTable() {
        return new Table(List.of(), List.of());
    }
}

