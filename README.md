# CSV Parsing Example Java

## Requirements
- Read and parse data from [`purchases.csv`](/src/main/resources/purchases.csv) and [`users.csv`](/src/main/resources/users.csv) and initialize to [`Table.java`](/com/csv/Table.java)
- [`Table.java`](/com/csv/Table.java) should be a general implementation, not specific to the data. Column names are inside .csv files and parsing should be based on this source.
- Implement following operations:
  - **ORDER BY DESC**  
    **Input**: Column name  
    **Output**: Ordered table
    
  - **INNER JOIN (SQL-style)**  
    **Input**:
      - Column name from the right table  
      - Column name from the left table
        
    **Output**: Joined table

### ⚙️ Design Notes
- Design an appropriate domain model (not tied to `purchases` or `users`).
- You can modify the file/folder structure as needed.
- Maven dependencies are allowed.
  
### Out-of-scope
- NO "real" persistence necessary (do not integrate MySQL, HSQLDB, h2database, or JPA).
- Use your own internal storage model (see also [`Table.java`](/com/csv/Table.java)).
- NO need to implement any SQL parser or anything, Java code interface is sufficient.
- NO Javadoc necessary. Tests and self-explanatory code are sufficient.    

---

## CSV parser with in-memory  table operation

### Description:
- This Java 21 application reads CSV files, stores the data in in-memory tables using Java collections, and performs operations like sorting and joining.
- It's a lightweight demonstration of how to handle tabular data without using a database.

### Application Features:
The application does following operations:
- Reads CSV files (users.csv and purchases.csv)
- Saves data into an in-memory table (such as : Map, List)
- Sorts table based on input column name
- Inner joins tables based on common key

### Technical Stack And Added Dependencies:
- Java21
- Junit5
- AssertJ (v : 3.26)
- Mockito (v : 2.17)
- Lombok(v: 1.18)
- Logback(v : 1.5.18)
- slf4j (v : 2.0.17)
- Maven(3.13)
- Apache common CSV (v : 1.10)
- Apache common collections (v : 4.5)
- Apache common lang3 (v : 3.17)

### Project Structure:
- `src/main/java` – core logic
- `src/test/java` – unit tests
- `src/main/resources` – sample CSV files

---

## Running the Application

### Prerequisites:
- Java 21 installed
- Maven version 3.x
- IDE (like IntelliJ or Eclipse)

### Steps:
- javac TableApplication.java
- java TableApplication

## Sample CSV Format:
- USER_ID,NAME,EMAIL
- 2,manuel,manuel@foo.de
- 1,andre,andre@bar.de

---

# CSV Reading and Parsing class(DataReaderImpl)

### Purpose:
DataReaderImpl is the core component responsible for reading and parsing CSV files into a structured, in-memory Table model using pure Java and the Apache Commons CSV library.

### Responsibilities:
- Validates the input CSV file path 
- Reads and parses CSV data using headers 
- Converts each CSV row into a custom Row object 
- Wraps the result into a Table object 
- Handles error scenarios gracefully with custom error responses 
- Logs meaningful information for debugging

### Key Behaviours:
- File Validation: Checks for path validity, file existence, and file type
- CSV Parsing: Uses Apache Commons CSV to read the file and extract headers and records
- Data Mapping: Transforms each CSV record into a Java Row object using headers
- Error Handling: Catches and wraps I/O and parsing issues into domain-specific exceptions
- In-Memory Table Building: Constructs a Table object using parsed headers and data rows

### Custom Domain Classes:
- Table, Row – In-memory representations
- Result<T> – Wrapper for success/failure
- CSVParsingException, ErrorResponse – Custom error handling
- HttpStatusCode enum – Status code abstraction

### Returned Data Structure:
- Result : Contains the error with message and HttpStatusCode if any exception occur otherwise expected table data.

### Located at:
- `src/main/java/com/csv/application/processor/DataReaderImpl.java`  

---
# Table Sorting class(TableSorterImpl)

### Purpose
- Sort the table descending order based on the input column.

### Responsibilities
- Validates the column name existence in the table
- Sort the table in descending order using input column name and comparator

### Key Behaviour
- Validates the column data is Numeric then compares using number comparator.
- If the data is not numeric then using string comparator.

### Returned Data Structure
- Result : ErrorResponse(statusCode and errorMessage)/ Table in descending order

### Located at:
- `src/main/java/com/csv/application/processor/TableSorterImpl.java`

---

# Table inner Joining class(HashJoinImpl)

### Purpose
 - HashJoinImpl is a fast and efficient version of the TableJoiner interface that uses a hash map to quickly join 
two in-memory CSV tables, making it ideal for handling large datasets.

### Responsibilities
- Validates input tables and join columns
- Iterates over each pair of rows from both tables to find matching join key values
- Merges matching rows into a single Row excluding duplicate keys
- Handles blank or null join keys gracefully 
- Logs each skipped or unmatched row with meaningful context

### Key Behaviours
- Validation: Ensures both tables and join columns are non-null and contain valid data
- Hash Join Strategy: Indexes the right table using a case-insensitive key map for O(1) lookup
- Row Merging: Combines data from matching left and right rows, avoiding duplication of the join key
- Null Handling: Skips rows with empty or invalid keys and logs detailed warnings
- Efficient Matching: Avoids nested loops, improving performance with large right tables

### Custom Domain Classes
- Table, Row – Structured in-memory table and row representations
- Result<T> – Wrapper for successful or failed join results
- EmptyHeaderException, ErrorResponse – Custom exception and error response model
- HttpStatusCode – Enum abstraction for HTTP-like status codes

### Returned Data Structure
- Result – On success, returns a Table containing merged rows. On failure, includes structured error message and status code.

### Located at
- `src/main/java/com/csv/application/processor/HashJoinImpl.java`

---

# Table inner Joining class(InnerNestedLoopJoinImpl)

### Purpose
InnerNestedLoopJoinImpl provides a straightforward nested-loop-based implementation of the TableJoiner interface to perform an inner join between two tables.
It is ideal for smaller datasets or debugging purposes due to its simple and readable logic.

### Responsibilities
- Validates input tables and join columns
- Iterates over each pair of rows from both tables to find matching join key values
- Merges matching rows into a single Row excluding duplicate keys
- Handles blank or null join keys gracefully
- Logs each skipped or unmatched row with meaningful context

### Key Behaviours
- Naive Join Strategy: Performs a double iteration (O(n*m)) to find matching join keys
- Case-Insensitive Matching: Normalizes and trims join key values before comparison
- Row Merging: Creates a combined row from left and right rows, skipping the right table's join key column
- Invalid Row Handling: Skips rows with null/empty join keys and logs issues
- Error Propagation: Catches and wraps runtime issues into domain-specific error structures

### Custom Domain Classes
- Table, Row – Internal structures for table data representation
- Result<T> – Container to wrap success or failure scenarios
- EmptyHeaderException, ErrorResponse – Domain-specific error modeling
- HttpStatusCode – Standardized status enum used in responses

### Returned Data Structure
- Result – Success result contains the joined table; failure result includes error info and status.
                                                                                                                                                                                                                      
### Located at
- `src/main/java/com/csv/application/processor/InnerNestedLoopJoinImpl.java`
