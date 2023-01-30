using System.Data;
using System.Data.SqlClient;
using Npgsql;

const string sqlServerConnectionString = "Server=.;Database=LearningTechnologiesDb;Trusted_Connection=True;Encrypt=False;";
const string postgreConnectionString = "Server=localhost;Port=5432;User Id=postgres;Password=postgres;Database=LearningTechnologiesDb";

IEnumerable<string?> FetchTablesInSqlServerSchema(string schemaName)
{
    // Connect to SQL Server
    SqlConnection sqlServerConnection = new SqlConnection(sqlServerConnectionString);
    sqlServerConnection.Open();
    
    SqlCommand sqlServerCommand = new SqlCommand($"SELECT table_name FROM information_schema.tables WHERE table_schema = '{schemaName}'", sqlServerConnection);
    
    using (SqlDataReader sqlServerDataReader = sqlServerCommand.ExecuteReader())
        while (sqlServerDataReader.Read())
            yield return sqlServerDataReader["table_name"].ToString();
    
    sqlServerConnection.Close();
    
}

bool CreateSchemaInPostgres(string schemaName)
{
    using (NpgsqlConnection postgresqlConnection = new NpgsqlConnection(postgreConnectionString))
    {
        postgresqlConnection.Open();

        using (NpgsqlCommand postgresqlCommand = new NpgsqlCommand())
        {
            postgresqlCommand.Connection = postgresqlConnection;
            postgresqlCommand.CommandText = $"CREATE SCHEMA \"{schemaName}\"";
        
            try
            {
                postgresqlCommand.ExecuteNonQuery();
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                return false;
            }
        }
    }
    
    return true;
}

void CreateTablesToPostgres(string schema, List<string> tables)
{
    
    SqlConnection sqlServerConnection = new SqlConnection(sqlServerConnectionString);
    sqlServerConnection.Open();

    NpgsqlConnection postgresqlConnection = new NpgsqlConnection(postgreConnectionString);
    postgresqlConnection.Open();

    NpgsqlCommand postgresqlCommand = new NpgsqlCommand();

    foreach (string table in tables)
    {
        SqlCommand sqlServerTableCommand = new SqlCommand($"SELECT * FROM {schema}." + table, sqlServerConnection);

        using (SqlDataAdapter sqlServerTableDataAdapter = new SqlDataAdapter(sqlServerTableCommand))
        {
            DataTable dataTable = new DataTable();
            sqlServerTableDataAdapter.Fill(dataTable);
            
            var createPostgreTable = $"CREATE TABLE \"{schema}\".\"{table}\" (";
            
            for (int i = 0; i < dataTable.Columns.Count; i++)
            {
                string postgresType = null;
                
                if (dataTable.Columns[i].DataType == typeof(Int32))
                {
                    postgresType = "INTEGER";
                }
                else if (dataTable.Columns[i].DataType == typeof(String))
                {
                    postgresType = "VARCHAR";
                }
                else if (dataTable.Columns[i].DataType == typeof(DateTime))
                {
                    postgresType = "TIMESTAMP";
                }
                else if (dataTable.Columns[i].DataType == typeof(Byte[]))
                {
                    postgresType = "BYTEA";
                }
                else if (dataTable.Columns[i].DataType == typeof(Double) || dataTable.Columns[i].DataType == typeof(float) || dataTable.Columns[i].DataType == typeof(Decimal))
                {
                    postgresType = "FLOAT";
                }
                else if (dataTable.Columns[i].DataType == typeof(Boolean))
                {
                    postgresType = "BOOLEAN";
                }
                
                createPostgreTable += $"{dataTable.Columns[i].ColumnName} {postgresType}, ";
                
                // remove last comma
                createPostgreTable = createPostgreTable.Remove(createPostgreTable.Length - 2);
                createPostgreTable += ")";
                
                postgresqlCommand.CommandText = createPostgreTable;
                try
                {
                    postgresqlCommand.ExecuteNonQuery();
                }
                catch (Exception e)
                {
                    Console.WriteLine(e.Message);
                    break;
                }
            }
        }
        Console.WriteLine($"{schema}.{table} WAS CREATED");
        
        postgresqlConnection.Close();
        sqlServerConnection.Close();
    }
}

void InsertRecordsIntoPostgres(string sqlSchema, string table, string postgreSchema, bool skipDuplicates = false)
{
    
    SqlConnection sqlServerConnection = new SqlConnection(sqlServerConnectionString);
    sqlServerConnection.Open();

    NpgsqlConnection postgresqlConnection = new NpgsqlConnection(postgreConnectionString);
    postgresqlConnection.Open();

    NpgsqlCommand postgresqlCommand = new NpgsqlCommand();
    postgresqlCommand.Connection = postgresqlConnection;
    
    int rowCounter = 0;
    
    SqlCommand sqlServerTableCommand = new SqlCommand($"SELECT * FROM {sqlSchema}.{table}", sqlServerConnection);
   
    using (SqlDataAdapter sqlServerTableDataAdapter = new SqlDataAdapter(sqlServerTableCommand))
    {
        DataTable dataTable = new DataTable();
        sqlServerTableDataAdapter.Fill(dataTable);
        
        Console.WriteLine($"Found {dataTable.Rows.Count} rows in table {table}");
        
        for (int i = 0; i < dataTable.Rows.Count; i++)
        {
            var columns = string.Empty;
            var values = string.Empty;
            for (int j = 0; j < dataTable.Columns.Count; j++)
            {
                columns += $" \"{dataTable.Columns[j].ColumnName}\",";
                values += "@value" + j + ",";
                postgresqlCommand.Parameters.AddWithValue("@value" + j, dataTable.Rows[i][j]);
            }
            
            // remove last comma
            columns = columns.Remove(columns.Length - 1);
            values = values.Remove(values.Length - 1);
            
            try
            {
                postgresqlCommand.CommandText = $"INSERT INTO \"{postgreSchema}\".\"{table}\" ({columns}) VALUES ({values})";
                postgresqlCommand.ExecuteNonQuery();
                
                rowCounter++;
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                break;
            }
            
            postgresqlCommand.Parameters.Clear();
        }

        Console.WriteLine($"Inserted {rowCounter} Rows  In {sqlSchema}.{table}");
        
        postgresqlCommand.Dispose();
        postgresqlConnection.Close();
        sqlServerConnection.Close();
    }
}

void DisableTriggers(string schema, string table)
{
    NpgsqlConnection postgresqlConnection = new NpgsqlConnection(postgreConnectionString);
    postgresqlConnection.Open();
    
    using (NpgsqlCommand postgresqlCommand = new NpgsqlCommand())
    {
        postgresqlCommand.Connection = postgresqlConnection;
        postgresqlCommand.CommandText = $"ALTER TABLE \"{schema}\".\"{table}\" DISABLE TRIGGER ALL;";
        postgresqlCommand.ExecuteNonQuery();
    }
    
    postgresqlConnection.Close();
}

void EnableTriggers(string schema, string table)
{
    NpgsqlConnection postgresqlConnection = new NpgsqlConnection(postgreConnectionString);
    postgresqlConnection.Open();
    
    using (NpgsqlCommand postgresqlCommand = new NpgsqlCommand())
    {
        postgresqlCommand.Connection = postgresqlConnection;
        postgresqlCommand.CommandText = $"ALTER TABLE \"{schema}\".\"{table}\" ENABLE TRIGGER ALL;";
        postgresqlCommand.ExecuteNonQuery();
    }
    
    postgresqlConnection.Close();
}

var sqlSchemas = new List<string> { "dbo", "Petition", "Archiving", "Survey", /*"Notification",*/ "Advising" };
var postgreSchemas = new List<string> { "public", "Petition", "Archiving", "Survey", /*"Notification",*/ "Advising" };

var schemaTuples = sqlSchemas.Zip(postgreSchemas, (a, b) => (a, b));

foreach (var (sqlSchema, postgresSchema) in schemaTuples)
{
    var tables = FetchTablesInSqlServerSchema(sqlSchema).ToList();
    if(!tables.Any())
        continue;

    if (tables.Any(t => t == "__EFMigrationsHistory"))
    {
        tables.Remove("__EFMigrationsHistory");
    }
        
    foreach (var table in tables)
    {
        DisableTriggers(postgresSchema, table!);
        InsertRecordsIntoPostgres(sqlSchema, table!, postgresSchema);
        EnableTriggers(postgresSchema, table!);
    }
}
