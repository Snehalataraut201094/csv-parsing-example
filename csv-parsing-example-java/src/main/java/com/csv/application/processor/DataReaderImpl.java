package com.csv.application.processor;

import com.csv.application.domain.enums.HttpStatusCode;
import com.csv.application.domain.exception.CSVParsingException;
import com.csv.application.domain.model.ErrorResponse;
import com.csv.application.domain.model.Result;
import com.csv.application.domain.model.Row;
import com.csv.application.domain.model.Table;
import com.csv.application.processor.interfaces.DataReader;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.toMap;

public class DataReaderImpl implements DataReader {

    private static final Logger log = LoggerFactory.getLogger(DataReaderImpl.class);
    public static final String PATH_IS_INVALID = "Input CSV Path is invalid.";

    @Override
    public Result<Table> readCSVData(String path) {

        Path CSVPath;
        try {
            CSVPath = validateAndGetPath(path);
        } catch (InvalidPathException ex) {
            return Result.failure(createErrorResponse(
                    HttpStatusCode.BAD_REQUEST.getCode(), ex.getMessage()));
        }
        try {
            return parseCSVToTable(CSVPath);
        } catch (IOException | CSVParsingException ex) {
            log.error("Failed to read CSV file: {}, {}", path, ex.getMessage());
            return Result.failure(createErrorResponse(
                    HttpStatusCode.INTERNAL_SERVER_ERROR.getCode(), ex.getMessage()));
        }
    }

    /**
     * Below method validates the input path of type @{@link String}.
     * If provided path is null, empty or blank throw InvalidPathException
     * otherwise return the path of type @{@link Path}.
     *
     * @param path of type @{@link String}
     * @return valid path of type @{@link Path}
     * @throws InvalidPathException if any error occurred while validating path
     */
    private Path validateAndGetPath(String path) throws InvalidPathException {
        if (path == null || path.isBlank()) {
            throw new InvalidPathException("Please provide valid input CSV path.", PATH_IS_INVALID);
        }
        Path filePath = Path.of(path);
        if (!Files.isRegularFile(filePath)) {
            throw new InvalidPathException(path, "Either file does not exist at given path or Provided path is not a file.");
        }
        return filePath;
    }

    /**
     * Parses the CSV file at the given path into a {@link Table} object.
     *
     * @param CSVPath the path to the CSV file
     * @return a {@link Result} containing the parsed {@link Table}
     * @throws IOException         if an I/O error occurs while reading the file
     * @throws CSVParsingException if an error occurs while parsing the CSV content
     */
    private Result<Table> parseCSVToTable(Path CSVPath) throws IOException {
        try (var csvParser = new CSVParser(Files.newBufferedReader(CSVPath), buildCSVFormat())) {

            List<String> headers = csvParser.getHeaderNames();
            List<CSVRecord> records = csvParser.getRecords();
            List<Row> rows = buildRowsFromCSVParser(records, headers);
            return Result.success(new Table(headers, rows));

        } catch (IllegalArgumentException ex) {
            throw new CSVParsingException("List of headers or records from CSV are null or empty.");
        }
    }

    /**
     * The below method build CSV format which will be responsible
     * for skipping the first row and to consider it as a header
     * and return the valid CSVFormat.
     *
     * @return CSVFormat of type @{@link CSVFormat}
     */
    private CSVFormat buildCSVFormat() {
        return CSVFormat.DEFAULT.builder().setHeader().setSkipHeaderRecord(true).build();
    }

    /**
     * Converts a list of {@link CSVRecord} objects into a list of {@link Row} objects
     * using the provided CSV headers.
     * Each record is mapped to a Row based on the headers. Empty records are filtered out.
     * Throws a {@link CSVParsingException} if either the headers or records list is null or empty.
     *
     * @param records the list of parsed CSV records
     * @param headers the list of CSV column headers
     * @return a list of {@link Row} objects constructed from the CSV records
     * @throws CSVParsingException if the headers or records are null or empty
     */
    private List<Row> buildRowsFromCSVParser(List<CSVRecord> records,
                                             List<String> headers) {

        if (CollectionUtils.isEmpty(headers) || headers.stream().allMatch(String::isBlank)
                || CollectionUtils.isEmpty(records)) {
            throw new CSVParsingException("List of headers or records from CSV are null or empty.");
        }
        return records.stream()
                .filter(record -> record.size() != 0)
                .map(record -> mapRecordToRow(headers, record))
                .toList();
    }

    /**
     * Maps a single {@link CSVRecord} to a {@link Row} object using the provided headers.
     * For each header, attempts to retrieve the corresponding value from the record.
     * If the header is missing or an error occurs, an empty string is used.
     *
     * @param headers the list of CSV column headers
     * @param record  the CSV record to convert
     * @return a {@link Row} object containing header-value pairs from the record
     */
    private Row mapRecordToRow(List<String> headers, CSVRecord record) {

        Map<String, String> row = headers.stream()
                .collect(toMap(
                        header -> header,
                        header -> {
                            try {
                                if (isRecordPresentForHeader(record, header)) {
                                    return record.get(header);
                                } else {
                                    throw new CSVParsingException("The record for corresponding header is missing.");
                                }
                            } catch (Exception ex) {
                                log.warn("Failed to get value for header '{}': {}", header, ex.getMessage());
                                return "";
                            }
                        }
                ));
        return new Row(row);
    }

    /**
     * Checks whether the specified header is mapped in the {@link CSVRecord} and has a non-null value.
     *
     * @param record the CSV record
     * @param header the header to check
     * @return true if the record contains a non-null value for the given header; false otherwise
     */
    private boolean isRecordPresentForHeader(CSVRecord record, String header) {
        String value = record.isMapped(header) ? record.get(header) : null;
        return value != null && !value.isBlank() && !"null".equalsIgnoreCase(value.trim());
    }

    /**
     * Creates an {@link ErrorResponse} object with the specified HTTP status code and message.
     *
     * @param statusCode the HTTP status code to set in the error response
     * @param message    the error message
     * @return an {@link ErrorResponse} containing the provided status code and message
     */
    private ErrorResponse createErrorResponse(int statusCode, String message) {
        return new ErrorResponse(statusCode, message);
    }
}