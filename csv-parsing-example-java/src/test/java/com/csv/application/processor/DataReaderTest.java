package com.csv.application.processor;

import com.csv.application.domain.enums.HttpStatusCode;
import com.csv.application.domain.model.Result;
import com.csv.application.domain.model.Row;
import com.csv.application.domain.model.Table;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.MockedStatic;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mockStatic;

public class DataReaderTest {

    public static final String INVALID_FILE_PATH_MSG =
            "Either file does not exist at given path or Provided path is not a file.: ";
    public static final String INVALID_PATH_MESSAGE = "Input CSV Path is invalid.: Please provide valid input CSV path.";
    public static final int EXPECTED_ROW_COUNT = 5;

    private DataReaderImpl dataReader;

    @BeforeEach
    public void setup() {
        dataReader = new DataReaderImpl();
    }

    @Test
    void shouldReturnError_whenCsvPathIsNull() {

        Result<Table> tableResult = callReadCSVData(null);

        assertInvalidPathTableResult(tableResult);
    }

    @Test
    void shouldReturnError_whenCsvPathIsEmpty() {

        Result<Table> tableResult = callReadCSVData("");

        assertInvalidPathTableResult(tableResult);
        assertEquals(INVALID_PATH_MESSAGE, tableResult.error().errorMessage());
    }

    @Test
    void shouldReturnError_whenCsvPathIsOnlyWhitespace() {

        Result<Table> tableResult = callReadCSVData(" ");

        assertInvalidPathTableResult(tableResult);
        assertEquals(INVALID_PATH_MESSAGE, tableResult.error().errorMessage());
    }

    @Test
    void shouldReturnError_whenCsvFileIsNotARegularFile() {

        String path = "src/main/resources/employees.xml";
        Path filePath = Path.of(path);

        try (MockedStatic<Files> filesMockedClass = mockStatic(Files.class)) {
            filesMockedClass.when(() -> Files.isRegularFile(filePath)).thenReturn(false);

            Result<Table> tableResult = callReadCSVData(path);

            assertInvalidPathTableResult(tableResult);
            assertEquals(INVALID_FILE_PATH_MSG + path, tableResult.error().errorMessage());
        }
    }

    @ParameterizedTest
    @MethodSource("validCsvFilesProvider")
    void shouldReadCsvAndMatchHeadersAndFirstRow_givenValidCsvFile(String filePath, List<String> expectedHeaders,
                                                                   Map<String, String> expectedFirstRow) {

        Result<Table> tableResult = dataReader.readCSVData(filePath);

        assertTrue(tableResult.isSuccess());

        Table table = tableResult.data();
        assertNotNull(table);

        assertEquals(expectedHeaders, table.headers());

        assertFalse(table.rows().isEmpty());
        Row firstRow = table.rows().getFirst();
        assertNotNull(firstRow);

        assertThat(firstRow.getData()).containsAllEntriesOf(expectedFirstRow);
    }

    @ParameterizedTest
    @MethodSource("csvFilesWithEmptyHeaderProvider")
    void shouldReturnError_whenCsvHeadersAreEmpty(String filePath, Path tempFile) throws IOException {
        Result<Table> tableResult = dataReader.readCSVData(filePath);

        assertFailureDueToEmptyHeadersOrRecords(tableResult);
        deleteFileIfExist(tempFile);
    }

    @ParameterizedTest
    @MethodSource("csvFilesWithEmptyContentProvider")
    void shouldReadCsvAndMarkMissingFieldsAsEmpty_whenSomeValuesAreMissingInRecords(String filePath,
                                                                                    Path tempFile) throws IOException {
        Result<Table> tableResult = dataReader.readCSVData(filePath);

        assertFailureDueToEmptyHeadersOrRecords(tableResult);
        deleteFileIfExist(tempFile);
    }

    @ParameterizedTest
    @MethodSource("csvFilesWithMissingValuesProvider")
    void shouldReadCsvAndMarkMissingFieldsAsEmpty_whenSomeValuesAreMissingInRecordsForHeader(String filePath,
                                                                                             Path tempFile) throws IOException {
        Result<Table> tableResult = dataReader.readCSVData(filePath);

        assertTrue(tableResult.isSuccess());

        Table table = tableResult.data();
        assertNotNull(table);

        List<Row> rows = table.rows();
        assertEquals(EXPECTED_ROW_COUNT, rows.size());

        // Row 1: all present
        assertEquals("car-1", rows.getFirst().getData().get("TITLE"));
        assertThat(rows.get(1).getData().get("TITLE")).isEmpty();
        assertThat(rows.get(2).getData().get("AD_ID")).isEmpty();
        assertThat(rows.get(3).getData().get("AD_ID")).isEmpty();

        deleteFileIfExist(tempFile);
    }

    private static void assertInvalidPathTableResult(Result<Table> tableResult) {
        assertNotNull(tableResult);
        assertNull(tableResult.data());
        assertNotNull(tableResult.error());
        assertEquals(HttpStatusCode.BAD_REQUEST.getCode(), tableResult.error().errorCode());
    }

    private static void assertFailureDueToEmptyHeadersOrRecords(Result<Table> tableResult) {
        assertNotNull(tableResult);
        Table table = tableResult.data();
        assertNull(table);
        assertNotNull(tableResult.error());
        assertEquals(HttpStatusCode.INTERNAL_SERVER_ERROR.getCode(), tableResult.error().errorCode());
        assertThat(tableResult.error().errorMessage()).contains("headers", "records", "empty");
    }

    static Stream<Arguments> validCsvFilesProvider() {
        return Stream.of(
                Arguments.of(
                        "src/main/resources/users.csv",
                        List.of("USER_ID", "NAME", "EMAIL"),
                        Map.of(
                                "USER_ID", "2",
                                "NAME", "manuel",
                                "EMAIL", "manuel@foo.de"
                        )
                ),
                Arguments.of(
                        "src/main/resources/purchases.csv",
                        List.of("AD_ID", "TITLE", "USER_ID"),
                        Map.of(
                                "AD_ID", "1",
                                "TITLE", "car-1",
                                "USER_ID", "1"
                        )
                )
        );
    }

    static Stream<Arguments> csvFilesWithMissingValuesProvider() throws IOException {

        Path tempFile = Files.createTempFile("test-headers-missing", ".csv");
        String content = """
                AD_ID,TITLE,USER_ID
                1,car-1,100
                2,,101
                null,table,102
                " ",table-2,104
                " ",table-3,105,5
                """;
        Files.writeString(tempFile, content);
        return Stream.of(Arguments.of(tempFile.toString(), tempFile));
    }

    static Stream<Arguments> csvFilesWithEmptyContentProvider() throws IOException {

        return Stream.of(createArguments("ID, NAME, EMAIL\n"),
                createArguments(" , , \n1,2,3\n"),
                createArguments(""));
    }

    static Stream<Arguments> csvFilesWithEmptyHeaderProvider() throws IOException {
        return Stream.of(createArguments(",,,\n1,2,3,4"));
    }

    private static Arguments createArguments(String content) throws IOException {
        Path tempFile = Files.createTempFile("test-empty", ".csv");
        Files.writeString(tempFile, content);
        return Arguments.of(tempFile.toString(), tempFile);
    }

    private Result<Table> callReadCSVData(String path) {
        return dataReader.readCSVData(path);
    }

    private static void deleteFileIfExist(Path tempFile) throws IOException {
        Files.deleteIfExists(tempFile);
    }
}

