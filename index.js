const mysql = require('mysql');

// Database connection details
const connection = mysql.createConnection({
  host: 'localhost',
  user: 'your_username',
  password: 'your_password',
  database: 'tt-api',
});

connection.connect((err) => {
  if (err) {
    console.error('Error connecting to MySQL:', err);
    return;
  }

  console.log('Connected to MySQL.');

  // Step 1: Create a temporary table for BIGINT columns
  const createTempTableQuery = `
    CREATE TEMPORARY TABLE bigint_columns (
      TableName VARCHAR(255),
      ColumnName VARCHAR(255)
    );
  `;

  connection.query(createTempTableQuery, (err, result) => {
    if (err) {
      console.error('Error creating temporary table:', err);
      return;
    }

    // Step 2: Insert table names and columns of type BIGINT
    const insertBigintColumnsQuery = `
      INSERT INTO bigint_columns (TableName, ColumnName)
      SELECT TABLE_NAME, COLUMN_NAME
      FROM information_schema.columns
      WHERE TABLE_SCHEMA = 'tt-api'  -- Change to your database name
        AND DATA_TYPE = 'bigint';
    `;

    connection.query(insertBigintColumnsQuery, (err, result) => {
      if (err) {
        console.error('Error inserting into bigint_columns:', err);
        return;
      }

      // Step 3: Generate the SQL queries dynamically
      const generateQueriesQuery = `
        SELECT CONCAT(
          'SELECT * FROM ', TableName, 
          ' WHERE ', ColumnName, 
          ' > 2147483647;'
        ) AS GeneratedQuery
        FROM bigint_columns;
      `;

      connection.query(generateQueriesQuery, (err, result) => {
        if (err) {
          console.error('Error generating queries:', err);
          return;
        }

        // Step 4: Execute each generated query
        result.forEach((row) => {
          const generatedQuery = row.GeneratedQuery;

          console.log('Executing:', generatedQuery);  // Log the query for reference

          connection.query(generatedQuery, (err, resultSet) => {
            if (err) {
              console.error('Error executing query:', err);
              return;
            }

            if (resultSet.length > 0) {
              console.log('Results:');
              resultSet.forEach((record) => {
                console.log(JSON.stringify(record, null, 2));  // Output in JSON format for clarity
              });
            } else {
              console.log('No records found exceeding INT capacity for this query.');
            }
          });
        });
      });
    });
  });
});

// Close the connection when done
connection.end(() => {
  console.log('Connection closed.');
});
