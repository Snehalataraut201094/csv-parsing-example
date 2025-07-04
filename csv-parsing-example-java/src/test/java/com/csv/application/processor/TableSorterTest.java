package com.csv.application.processor;

import com.csv.application.domain.enums.HttpStatusCode;
import com.csv.application.domain.model.Result;
import com.csv.application.domain.model.Row;
import com.csv.application.domain.model.Table;
import com.csv.application.processor.interfaces.TableSorter;
import com.csv.application.util.SortUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TableSorterTest {

    public static final String INVALID_COLUMN_MSG = "The specified column does not exist in the table.";

    private TableSorter tableSorter;

    @BeforeEach
    public void setup() {
        tableSorter = new TableSorterImpl();
    }

    @ParameterizedTest
    @MethodSource("provideInvalidColumnScenarios")
    void returnsError_whenColumnNameIsInvalid(Table table,
                                              String columnName) {

        Result<Table> tableResult = tableSorter.sortTableByDesc(table, columnName);

        assertNotNull(tableResult);
        assertFalse(tableResult.isSuccess());

        assertNull(tableResult.data());
        assertNotNull(tableResult.error());

        assertEquals(HttpStatusCode.BAD_REQUEST.getCode(), tableResult.error().errorCode());
        assertEquals(INVALID_COLUMN_MSG, tableResult.error().errorMessage());
    }

    @ParameterizedTest
    @MethodSource("provideValidColumnScenarios")
    void returnsSortedTable_whenValidColumnGiven(Table table, String columnName) {

        Result<Table> actualResult = tableSorter.sortTableByDesc(table, columnName);

        assertActualResultData(table, actualResult);

        Result<Table> expectedResult = getExpectedResult(table, columnName);

        assertThat(actualResult.data().rows()).containsExactlyElementsOf(expectedResult.data().rows());
    }

    @ParameterizedTest
    @MethodSource("provideSortByStringTypeColumnName")
    void returnsDESCSortedTable_whenSortByStringTypeColumnName(Table table, String columnName) {

        Result<Table> actualResult = tableSorter.sortTableByDesc(table, columnName);

        assertActualResultData(table, actualResult);

        Result<Table> expectedResult = getExpectedResult(table, columnName);

        assertThat(actualResult.data().rows()).containsExactlyElementsOf(expectedResult.data().rows());
    }

    @Test
    void returnsSameTable_whenTableContainSingleRow() {

        Table table = new Table(List.of("USER_ID", "NAME", "EMAIL"),
                List.of(new Row(Map.of("USER_ID", "1", "NAME", "sunny", "EMAIL", "sunny@foo.de"))));

        Result<Table> actualResult = tableSorter.sortTableByDesc(table, "EMAIL");

        assertActualResultData(table, actualResult);

        assertThat(actualResult.data().rows()).containsExactlyElementsOf(table.rows());
    }

    @ParameterizedTest
    @MethodSource("provideSortByEmptyOrNullColumnData")
    void returnColumnNullValueLast_withNullAndEmptyValuesInColumn(Table table, String columnName) {
        Result<Table> tableResult = tableSorter.sortTableByDesc(table, columnName);

        assertActualResultData(table, tableResult);
        assertThat(tableResult.data().rows().getLast().get(columnName)).isNull();
    }

    @ParameterizedTest
    @MethodSource("provideMixedTypeColumnData")
    void returnDESCOrderTable_withMixedTpeDataValues(Table table, String columnName) {
        Result<Table> tableResult = tableSorter.sortTableByDesc(table, columnName);

        assertActualResultData(table, tableResult);

        List<String> actualResult = tableResult.data().rows().stream().map(row -> row.get(columnName)).toList();

        List<String> expectedResult = table.rows().stream()
                .map(row -> row.get(columnName))
                .sorted(SortUtils.createValueComparator())
                .toList();

        assertThat(actualResult).isEqualTo(expectedResult);
    }

    private static Stream<Arguments> provideInvalidColumnScenarios() {
        return Stream.of(
                Arguments.arguments(new Table(List.of("USER_ID", "NAME", "EMAIL"), List.of()), null),
                Arguments.arguments(new Table(List.of("USER_ID", "NAME", "EMAIL"), List.of()), " "),
                Arguments.arguments(new Table(List.of(), List.of()), "NAME"),
                Arguments.arguments(new Table(List.of("USER_ID", "NAME", "EMAIL"), List.of()), "AGE"));
    }

    private static Stream<Arguments> provideValidColumnScenarios() {
        return Stream.of(
                Arguments.arguments(new Table(List.of("USER_ID", "NAME", "EMAIL"), List.of(
                                new Row(Map.of("USER_ID", "1", "NAME", "manuel", "EMAIL", "manuel@foo.de")),
                                new Row(Map.of("USER_ID", "2", "NAME", "andre", "EMAIL", "andre@bar.de")),
                                new Row(Map.of("USER_ID", "3", "NAME", "swen", "EMAIL", "swen@foo.de")))),
                        "USER_ID"));
    }

    private static Stream<Arguments> provideSortByStringTypeColumnName() {
        return Stream.of(
                Arguments.arguments(new Table(List.of("AD_ID", "TITLE", "USER_ID"), List.of(
                        new Row(Map.of("AD_ID", "1", "TITLE", "guitar-1", "USER_ID", "3")),
                        new Row(Map.of("AD_ID", "2", "TITLE", "table-2", "USER_ID", "2")),
                        new Row(Map.of("AD_ID", "3", "TITLE", "chair-1", "USER_ID", "3")))), "TITLE")
        );
    }

    private static Stream<Arguments> provideSortByEmptyOrNullColumnData() {
        return Stream.of(
                Arguments.arguments(new Table(List.of("AD_ID", "TITLE", "USER_ID"), List.of(
                        new Row(new HashMap<>(Map.of("AD_ID", "1", "TITLE", " ", "USER_ID", "3"))),
                        new Row(new HashMap<>(Map.of("AD_ID", "2", "TITLE", "table-2", "USER_ID", "2"))),
                        new Row(new HashMap<>() {{
                            put("AD_ID", "3");
                            put("TITLE", null);
                            put("USER_ID", "3");
                        }}))), "TITLE"),
                Arguments.arguments(new Table(List.of("USER_ID", "NAME", "EMAIL"), List.of(
                        new Row(Map.of("USER_ID", "1", "NAME", "lena", "EMAIL", "lena@foo.de")),
                        new Row(new HashMap<>() {{
                            put("USER_ID", "2");
                            put("NAME", "andreas");
                            put("EMAIL", null);
                        }}),
                        new Row(new HashMap<>() {{
                            put("USER_ID", "3");
                            put("NAME", "sandy");
                            put("EMAIL", " ");
                        }}))), "EMAIL"));
    }

    private static Stream<Arguments> provideMixedTypeColumnData() {
        return Stream.of(
                Arguments.arguments(new Table(List.of("USER_ID", "NAME", "EMAIL"), List.of(
                                new Row(Map.of("USER_ID", "1", "NAME", "10", "EMAIL", "manuel@foo.de")),
                                new Row(Map.of("USER_ID", "4", "NAME", "andre", "EMAIL", "andre@bar.de")),
                                new Row(Map.of("USER_ID", "3", "NAME", "20", "EMAIL", "swen@foo.de")),
                                new Row(Map.of("USER_ID", "2", "NAME", "lena", "EMAIL", "lena@foo.de")))),
                        "NAME"));
    }

    private Result<Table> getExpectedResult(Table table, String columnName) {
        Table inputCopy = new Table(table.headers(), new ArrayList<>(table.rows()));
        return tableSorter.sortTableByDesc(inputCopy, columnName);
    }

    private static void assertActualResultData(Table table, Result<Table> actualResult) {
        assertAll(
                () -> assertNotNull(actualResult),
                () -> assertTrue(actualResult.isSuccess()),
                () -> assertNotNull(actualResult.data()),
                () -> assertNull(actualResult.error()),
                () -> assertNotNull(actualResult.data().headers()),
                () -> assertThat(actualResult.data().headers()).isNotEmpty(),
                () -> assertThat(actualResult.data().headers()).containsSequence(table.headers()),
                () -> assertNotNull(actualResult.data().rows())
        );
    }
}

