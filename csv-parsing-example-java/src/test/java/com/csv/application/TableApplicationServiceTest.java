package com.csv.application;

import com.csv.application.domain.enums.HttpStatusCode;
import com.csv.application.domain.model.ErrorResponse;
import com.csv.application.domain.model.Result;
import com.csv.application.domain.model.Row;
import com.csv.application.domain.model.Table;
import com.csv.application.processor.interfaces.DataReader;
import com.csv.application.processor.interfaces.TableJoiner;
import com.csv.application.processor.interfaces.TableSorter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class TableApplicationServiceTest {

    public static final String NAME = "NAME";
    public static final String EMAIL = "EMAIL";
    public static final String AD_ID = "AD_ID";
    public static final String TITLE = "TITLE";
    public static final String USER_ID = "USER_ID";
    public static final String INVALID_CSV_PATH = "Either file does not exist at given path or Provided path is not a file.: ";
    public static final String LEFT_CSV_PATH = "src/main/resources/users.csv";
    public static final String RIGHT_CSV_PATH = "src/main/resources/purchases.csv";

    Table leftTable, rightTable, joinedTable, sortedTable;

    @Mock
    private DataReader dataReader;

    @Mock
    TableSorter tableSorter;

    @Mock
    TableJoiner tableJoiner;

    private TableApplicationService service;

    @BeforeEach
    public void setup() {

        service = new TableApplicationService(dataReader, tableSorter, tableJoiner);

        leftTable = new Table(List.of(USER_ID, NAME, EMAIL), List.of(
                new Row(Map.of(USER_ID, "2", NAME, "manuel", EMAIL, "manuel@foo.de")),
                new Row(Map.of(USER_ID, "1", NAME, "andre", EMAIL, "andre@bar.de")),
                new Row(Map.of(USER_ID, "3", NAME, "swen", EMAIL, "swen@foo.de")),
                new Row(Map.of(USER_ID, "4", NAME, "lydia", EMAIL, "lydia@bar"))));

        sortedTable = new Table(List.of(USER_ID, NAME, EMAIL), List.of(
                new Row(Map.of(USER_ID, "4", NAME, "lydia", EMAIL, "lydia@bar")),
                new Row(Map.of(USER_ID, "3", NAME, "swen", EMAIL, "swen@foo.de")),
                new Row(Map.of(USER_ID, "2", NAME, "manuel", EMAIL, "manuel@foo.de")),
                new Row(Map.of(USER_ID, "1", NAME, "andre", EMAIL, "andre@bar.de"))));

        rightTable = new Table(List.of(AD_ID, TITLE, USER_ID), List.of(
                new Row(Map.of(AD_ID, "1", TITLE, "car-1", USER_ID, "1")),
                new Row(Map.of(AD_ID, "3", TITLE, "car-3", USER_ID, "1")),
                new Row(Map.of(AD_ID, "5", TITLE, "guitar-2", USER_ID, "3")),
                new Row(Map.of(AD_ID, "7", TITLE, "table-1", USER_ID, "4")),
                new Row(Map.of(AD_ID, "9", TITLE, "chair-1", USER_ID, "1"))));

        joinedTable = new Table(List.of(USER_ID, NAME, EMAIL, AD_ID, TITLE), List.of(
                new Row(Map.of(USER_ID, "2", NAME, "manuel", EMAIL, "manuel@foo.de", AD_ID, "4", TITLE, "guitar-1")),
                new Row(Map.of(USER_ID, "1", NAME, "andre", EMAIL, "andre@bar.de", AD_ID, "1", TITLE, "car-1")),
                new Row(Map.of(USER_ID, "1", NAME, "andre", EMAIL, "andre@bar.de", AD_ID, "2", TITLE, "car-2")),
                new Row(Map.of(USER_ID, "1", NAME, "andre", EMAIL, "andre@bar.de", AD_ID, "3", TITLE, "car-3")),
                new Row(Map.of(USER_ID, "1", NAME, "andre", EMAIL, "andre@bar.de", AD_ID, "9", TITLE, "chair-1")),
                new Row(Map.of(USER_ID, "3", NAME, "swen", EMAIL, "swen@foo.de", AD_ID, "5", TITLE, "guitar-2")),
                new Row(Map.of(USER_ID, "4", NAME, "lydia", EMAIL, "lydia@bar.de", AD_ID, "6", TITLE, "table-2")),
                new Row(Map.of(USER_ID, "4", NAME, "lydia", EMAIL, "lydia@bar.de", AD_ID, "7", TITLE, "table-1"))
        ));
    }

    @Test
    void shouldReturnJoinedTable_whenAllOperationsSucceed() {
        mockSuccessfulJoinAndSort();

        Result<Table> result = service.process(LEFT_CSV_PATH, RIGHT_CSV_PATH, NAME);

        assertSuccessResult(result);
    }

    @Test
    void shouldReturnError_whenLeftTableFailsToLoad() {

        when(dataReader.readCSVData(anyString()))
                .thenReturn(createResultFailure(HttpStatusCode.BAD_REQUEST.getCode(), INVALID_CSV_PATH + LEFT_CSV_PATH));

        Result<Table> result = service.process(LEFT_CSV_PATH, RIGHT_CSV_PATH, NAME);

        assertFalse(result.isSuccess());
        assertNotNull(result.error());
        assertEquals(HttpStatusCode.BAD_REQUEST.getCode(), result.error().errorCode());
        assertEquals(INVALID_CSV_PATH + LEFT_CSV_PATH, result.error().errorMessage());
    }

    @Test
    void shouldReturnError_whenRightTableFailsToLoad() {

        when(dataReader.readCSVData(anyString())).thenAnswer(invocation -> {
            String path = invocation.getArgument(0);
            if (path.contains("users")) {
                return Result.success(leftTable);
            } else if (path.contains("purchases")) {
                return createResultFailure(HttpStatusCode.BAD_REQUEST.getCode(), INVALID_CSV_PATH + RIGHT_CSV_PATH);
            }
            return createResultFailure(HttpStatusCode.BAD_REQUEST.getCode(), "Failure to read CSV path");
        });

        Result<Table> result = service.process(LEFT_CSV_PATH, RIGHT_CSV_PATH, NAME);

        assertFalse(result.isSuccess());
        assertNotNull(result.error());
        assertEquals(HttpStatusCode.BAD_REQUEST.getCode(), result.error().errorCode());
        assertEquals(INVALID_CSV_PATH + RIGHT_CSV_PATH, result.error().errorMessage());
    }

    @Test
    void shouldSkipSortAndReturnJoin_whenSortFails() {

        mockDataReader();
        when(tableSorter.sortTableByDesc(any(), anyString()))
                .thenReturn(createResultFailure(HttpStatusCode.INTERNAL_SERVER_ERROR.getCode(), "Sorting failed."));
        when(tableJoiner.joinTables(anyString(), anyString(), any(), any())).thenReturn(Result.success(joinedTable));

        Result<Table> result = service.process(LEFT_CSV_PATH, RIGHT_CSV_PATH, "AGE");

        assertTrue(result.isSuccess());
        assertNull(result.error());
        assertEquals(joinedTable, result.data());
    }

    @Test
    void shouldReturnError_whenJoinFails() {

        mockSuccessfulJoinAndSort();
        when(tableJoiner.joinTables(anyString(), anyString(), any(), any()))
                .thenReturn(createResultFailure(HttpStatusCode.INTERNAL_SERVER_ERROR.getCode(), "Failed to perform Joins."));

        Result<Table> result = service.process(LEFT_CSV_PATH, RIGHT_CSV_PATH, "AGE");

        assertFalse(result.isSuccess());
        assertNull(result.data());
        assertNotNull(result.error());
        assertEquals(HttpStatusCode.INTERNAL_SERVER_ERROR.getCode(), result.error().errorCode());
    }


    private void assertSuccessResult(Result<Table> result) {
        assertTrue(result.isSuccess());
        assertNull(result.error());
        assertEquals(joinedTable, result.data());
        assertNotNull(result.data().headers());
        assertThat(result.data().headers()).containsSequence(joinedTable.headers());
        assertNotNull(result.data().rows());
        assertThat(result.data().rows()).containsAll(joinedTable.rows());
    }

    private void mockSuccessfulJoinAndSort() {
        mockDataReader();
        when(tableSorter.sortTableByDesc(any(), anyString()))
                .thenReturn(Result.success(sortedTable));
        when(tableJoiner.joinTables(anyString(), anyString(), any(), any()))
                .thenReturn(Result.success(joinedTable));
    }

    private void mockDataReader() {
        when(dataReader.readCSVData(anyString())).thenAnswer(invocation -> {
            String path = invocation.getArgument(0);
            if (path.contains("users")) {
                return Result.success(leftTable);
            } else if (path.contains("purchases")) {
                return Result.success(rightTable);
            }
            return createResultFailure(HttpStatusCode.BAD_REQUEST.getCode(), "Unable to read CSV path");
        });
    }

    private Result<Table> createResultFailure(int statusCode, String message) {
        return Result.failure(new ErrorResponse(statusCode, message));
    }
}

