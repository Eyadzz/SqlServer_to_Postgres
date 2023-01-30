// using System.Data;
// using System.Data.SqlClient;
// using Npgsql;
//
//
//
// void Main()
// {
//     List<string> tables = new List<string>();
//     // Connect to SQL Server
//     SqlConnection sqlServerConnection = new SqlConnection("Server=.;Database=LearningTechnologiesDb;Trusted_Connection=True;Encrypt=False;");
//     // Connect to PostgreSQL
//     NpgsqlConnection postgresqlConnection = new NpgsqlConnection("Server=localhost;Port=5432;User Id=postgres;Password=postgres;Database=LearningTechnologiesDb");
//     postgresqlConnection.Open();
//     sqlServerConnection.Open();
//     SqlCommand sqlServerCommand = new SqlCommand("SELECT table_name FROM information_schema.tables WHERE table_schema = 'Archiving'", sqlServerConnection);
//     using (SqlDataReader sqlServerDataReader = sqlServerCommand.ExecuteReader())
//     {
//         while (sqlServerDataReader.Read())
//         {
//             tables.Add(sqlServerDataReader["table_name"].ToString());
//         }
//     }
//     sqlServerConnection.Close();
//
//    
//     
//     NpgsqlCommand postgresqlCommand = new NpgsqlCommand();
//     postgresqlCommand.Connection = postgresqlConnection;
//
//     // Create the schema in PostgreSQL
//     /*postgresqlCommand.CommandText = "CREATE SCHEMA Archiving";
//     postgresqlCommand.ExecuteNonQuery();*/
//
//     // Iterate over the tables in SQL Server
//     foreach (string table in tables)
//     {
//         // Fetch the table's data
//         SqlCommand sqlServerTableCommand = new SqlCommand("SELECT * FROM Archiving." + table, sqlServerConnection);
//         using (SqlDataAdapter sqlServerTableDataAdapter = new SqlDataAdapter(sqlServerTableCommand))
//         {
//             DataTable dataTable = new DataTable();
//             sqlServerTableDataAdapter.Fill(dataTable);
//
//             /*// Create the table in PostgreSQL
//                 string createTableSQL = "CREATE TABLE Archiving." + table + " (";
//                 for (int i = 0; i < dataTable.Columns.Count; i++)
//                 {
//                     
//                     string postgresType = null;
//                     if (dataTable.Columns[i].DataType == typeof(Int32))
//                     {
//                         postgresType = "INTEGER";
//                     }
//                     else if (dataTable.Columns[i].DataType == typeof(String))
//                     {
//                         postgresType = "VARCHAR";
//                     }
//                     else if (dataTable.Columns[i].DataType == typeof(DateTime))
//                     {
//                         postgresType = "TIMESTAMP";
//                     }
//                     else if (dataTable.Columns[i].DataType == typeof(Byte[]))
//                     {
//                         postgresType = "BYTEA";
//                     }
//                     else if (dataTable.Columns[i].DataType == typeof(Double) || dataTable.Columns[i].DataType == typeof(float) || dataTable.Columns[i].DataType == typeof(Decimal))
//                     {
//                         postgresType = "FLOAT";
//                     }
//                     else if (dataTable.Columns[i].DataType == typeof(Boolean))
//                     {
//                         postgresType = "BOOLEAN";
//                     }
//
//                     if (dataTable.Columns[i].ColumnName == "Order")
//                     {
//                         dataTable.Columns[i].ColumnName = "\"Order\"";
//                     }
//                     createTableSQL += dataTable.Columns[i].ColumnName + " " + postgresType + ", ";
//                 }
//                 // remove last comma
//                 
//                 createTableSQL = createTableSQL.Remove(createTableSQL.Length - 2);
//                 createTableSQL += ")";
//                 Console.WriteLine(createTableSQL);
//                 postgresqlCommand.CommandText = createTableSQL;
//                 try
//                 {
//                     postgresqlCommand.ExecuteNonQuery();
//                 }
//                 catch (Exception e)
//                 {
//                     Console.WriteLine(e.Message);
//                     continue;
//                 }*/
//                 
//             if(table == "__EFMigrationsHistory")
//                 continue;
//             // Insert the data into the PostgreSQL table
//             postgresqlCommand.CommandText = $"ALTER TABLE \"Archiving\".\"{table}\" DISABLE TRIGGER ALL;";
//             Console.WriteLine(postgresqlCommand.CommandText);
//             postgresqlCommand.ExecuteNonQuery();
//             for (int i = 0; i < dataTable.Rows.Count; i++)
//             {
//                 string columns = "";
//                 string values = "";
//                 for (int j = 0; j < dataTable.Columns.Count; j++)
//                 {
//                     columns += $" \"{dataTable.Columns[j].ColumnName}\",";
//                     values += "@value" + j + ",";
//                     postgresqlCommand.Parameters.AddWithValue("@value" + j, dataTable.Rows[i][j]);
//                 }
//                 // remove last comma
//                 columns = columns.Remove(columns.Length - 1);
//                 values = values.Remove(values.Length - 1);
//                 try
//                 {
//                     postgresqlCommand.CommandText =
//                         "INSERT INTO \"Archiving\".\"" + table + "\"(" + columns + ") VALUES (" + values + ")";
//                 }
//                 catch (Exception ex)
//                 {
//                     Console.WriteLine(ex.Message);
//                 }
//                     
//                 Console.WriteLine(postgresqlCommand.CommandText);
//                 postgresqlCommand.ExecuteNonQuery();
//                 postgresqlCommand.Parameters.Clear();
//             }
//             // Re-enable constraints
//             postgresqlCommand.CommandText = $"ALTER TABLE \"Archiving\".\"{table}\" ENABLE TRIGGER ALL;";
//             postgresqlCommand.ExecuteNonQuery();
//
//         }
//     }
//     postgresqlConnection.Close();
// }