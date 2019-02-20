# mygit
This is just a simple project to show that it is possible to create your own CSV ‘importer’. It may not cover ALL (100%) scenarios in CSV for now.\n
I decided that for demonstration purpose I can process the following types: INTEGER, FLOAT, BOOLEAN, DATE, VARCHAR. In a configuration file, you may specify how many rows you’d like to process to evaluate data types. Another challenge took some time to resolve - DATE type. I tried different formats for date and successfully identified them as a DATE. The issue was in inserting it to the table. The user is providing the list of Data Formats in the config file to compare and on a success identification that Data Format will be passed to a Snowflake File Format options (as on the second execute statement…next). Simple logic to evaluate, if the data value is not INTEGER, not FLOAT, not BOOLEAN, not DATE let it be VARCHAR. Here is a lot of room for improvement to add more precise data type.
These specifications for my project: point to CSV file, read the Header, create a destination SQL table schema in Snowflake DB and populate the table.
For demonstration purpose I can process the following types: INTEGER, FLOAT, BOOLEAN, DATE, VARCHAR. 
In a configuration file, you may specify how many rows you’d like to process to evaluate data types. Another challenge took some time to resolve - DATE type. I tried different formats for date and successfully identified them as a DATE. The issue was in inserting it to the table. The user is providing the list of Data Formats in the config file to compare and on a success identification that Data Format will be passed to a Snowflake File Format options (as on the second execute statement…next). Simple logic to evaluate, if the data value is not INTEGER, not FLOAT, not BOOLEAN, not DATE let it be VARCHAR. Here is a lot of room for improvement to add more precise data type.
Now using Snowflake commands it is easy and simple to ingest data into the table. As a prerequisite, you need to have a JDBC driver installed on your machine. The application will connect to your Snowflake account reading all properties from the config file. Then the app will create a table in your selected Database/Schema location with your file name as the table name. Next, it will create a temporary Stage to copy a file to an intermediate location.

(“put file:////”+paramsInCSV.get(“fileNameWithPath”)+” @~/staged”);

If your file has a Date column it will be added as Date Format for a Copy process.

(“Copy into “+paramsInCSV.get(“fileName”)+” from @~/staged file_format = (type = csv field_delimiter = ‘“+paramsInCSV.get(“delimiter”)+”’ skip_header = 1 FIELD_OPTIONALLY_ENCLOSED_BY=’\”’ NULL_IF = (‘’,’NULL’, ‘null’, ‘\\N’) EMPTY_FIELD_AS_NULL = true “+sDateFormatSql+”);”);

This line is very important and helps a lot in the import process. The delimiter is auto applied, skipping the header parameter, if needed the data in quotes are cleared from them, checking for what values use NULL, and specifying the date format.
Currently, it is a problem if you have different DATE types in your file, as two columns with different DATE type. Definitely more testing need to be done to cover all CSV scenarios. After the Copy process is done the app will clear the Stage.
‘dateFormat’ is a line with different date formats separated by ‘#’. ‘processHeaderFirstLine’ is obvious setting. ‘csvDelimiters’ is a line to check what is a delimiter in the file. ‘howManyLines2check’ — lines to loop for data type evaluation. ‘preferedLocation’ may be skipped for this version. ‘verbose’ — open valve for logs chatter. ‘columnDefName’ — name for the column if no header in the CSV file. Other settings are for JDBC Snowflake connections.

Place the downloaded Snowflake JDBC driver into the app folder. Run this command to compile the code in the folder of your app: javac Main.java. Run the cmd to process CSV to Snowflake: java -cp .:snowflake-jdbc-3.6.9.jar Main “/data/sample1.csv”. 

Connect to your Snowflake account, database, schema, pick a warehouse to run your query for the imported table.
