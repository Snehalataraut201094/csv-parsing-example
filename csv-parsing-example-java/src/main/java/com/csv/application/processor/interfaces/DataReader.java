package com.csv.application.processor.interfaces;

import com.csv.application.domain.model.Table;
import com.csv.application.domain.model.Result;

public interface DataReader {

    /**
     * Reads a CSV file from the provided file path and parses it into a {@link Table} object.
     * <p>
     * The method first validates the given file path. If the path is invalid, it returns a
     * {@link Result} containing a failure response with a 400 Bad Request status.
     * If the path is valid, it attempts to read and parse the CSV file using a CSV parser.
     * On successful parsing, it returns a {@link Result} containing the parsed {@link Table}.
     * If any I/O or parsing error occurs during this process, it returns a failure response
     * with a 500 Internal Server Error status.
     * </p>
     *
     * @param path the file system path to the CSV file
     * @return a {@link Result} containing the parsed {@link Table} on success,
     * or a failure response with an appropriate HTTP status code and error message on failure
     */
    Result<Table> readCSVData(String path);
}
