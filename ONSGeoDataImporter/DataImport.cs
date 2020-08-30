using Microsoft.Extensions.Configuration;
using System;
using System.Data.SqlClient;
using CsvHelper;
using System.IO;
using System.Linq;
using System.Globalization;
using System.Collections.Generic;
using System.Text;
using System.Dynamic;
using System.Collections.Immutable;
using static ONSGeoDataImporter.Helpers;

namespace ONSGeoDataImporter
{
    class DataImport
    {

        public ImportOptions options = new ImportOptions();

        public DateTime startTime = DateTime.Now;

        public List<string> failedFiles = new List<string>();
        public List<string> importedFiles = new List<string>();


        public DataImport(IConfiguration config)
        {
            config.GetSection(ImportOptions.Import).Bind(options);
        }


        public bool mainProcess()
        {
            Console.WriteLine($"Starting Import at {startTime.TimeOfDay}");

            //todo download source file 

            //todo save the source file somewhere sensible and extract it

            Console.WriteLine("Starting to Importing Documents Files");

            importDocumentFile();

            Console.WriteLine("Import of Documents Completed");

            //deal with the data file (bulk insert)

            Console.WriteLine("Stuff has happened");

            return true;
        }


        private void importDocumentFile()
        {

            string docsFullPath = $"{options.DataRootPath}/{options.DocumentsFolder}/";

            //maybe add in a check for csv files that aren't in the settings and output a warning for if they add in new files.

            foreach (string[] fileMapping in options.DocumentsFileList)
            {
                string filename = $"{fileMapping[0]}.csv";
                string dbtableName = fileMapping[1];

                try
                {
                    List<dynamic> rows;
                    List<string> columns;

                    using (StreamReader reader = new StreamReader($"{docsFullPath}/{filename}"))
                    {
                        //todo set up culture stuff properly at some point
                        using (CsvReader csv = new CsvReader(reader, CultureInfo.InvariantCulture))
                        {
                            csv.Configuration.IgnoreBlankLines = true;

                            //Handles the file with the empty field at the end.
                            csv.Read();
                            csv.Context.Record = csv.Context.Record.Reverse().SkipWhile(string.IsNullOrWhiteSpace).Reverse().ToArray();
                            csv.ReadHeader();

                            rows = csv.GetRecords<dynamic>().ToList();
                            columns = csv.Context.HeaderRecord.ToList();
                        }

                    }

                    //Some of the files have a blank column at the end
                    columns = columns.Where(s => !string.IsNullOrWhiteSpace(s)).Distinct().ToList();

                    if (createTableIfItsNotThere(dbtableName, columns))
                    {
                        using (SqlConnection connection = new SqlConnection(options.DBConnectionString))
                        {

                            connection.Open();

                            StringBuilder insertStatements = new StringBuilder();

                            // these are all small files so individual imports statements are fast enough - won't work when I get to the data side.
                            //Should be doing this with parameters but this will do for now.

                            List<string> dbcolumns = columns.Select(x => $"[{x}]").ToList();
                            string baseStatement = $"INSERT INTO dbo.{dbtableName} ({string.Join(",", dbcolumns)}) VALUES ( ";

                            foreach (ExpandoObject item in rows)
                            {
                                string insertRow = baseStatement;
                                ImmutableDictionary<string, object> temp = item.ToImmutableDictionary();

                                foreach (string propName in columns)
                                {
                                    object temp1;
                                    temp.TryGetValue(propName, out temp1);

                                    //just in case we have some empty fields
                                    if (temp1 == null)
                                    {
                                        insertRow += $"'',";
                                    }
                                    else
                                    {
                                        //Need to deal with other characters that will need escaping at some point but this works for now
                                        insertRow += $"'{temp1.ToString().Replace("'","''")}',";
                                    }

                                }

                                insertRow = insertRow.Substring(0, insertRow.Length - 1) + ");";
                                insertStatements.AppendLine(insertRow);

                            }

                            using (SqlTransaction transaction = connection.BeginTransaction())
                            {
                                using (SqlCommand command = new SqlCommand())
                                {
                                    command.Connection = connection;
                                    command.Transaction = transaction;

                                    command.CommandText = $"TRUNCATE TABLE dbo.{dbtableName}";
                                    command.ExecuteNonQuery();

                                    command.CommandText = insertStatements.ToString();
                                    command.ExecuteNonQuery();

                                    transaction.Commit();
                                }
                            }
                        }

                        importedFiles.Add(filename);

                        ConsoleWriteColour($"{filename} imported into {dbtableName}", ConsoleColor.Blue);

                    }
                    else
                    {
                        throw new NotImplementedException($"Table {dbtableName} does not exist and cannot be created");
                    }

                }
                catch (Exception ex)
                {
                    failedFiles.Add(filename);
                    //TODO - add in some sensible error handling
                    ConsoleWriteColour($"{filename} failed", ConsoleColor.Yellow);
                    ConsoleWriteColour(ex.ToString(), ConsoleColor.Red);

                }

            }

        }

        private bool createTableIfItsNotThere(string tableName, List<string> columns)
        {
            bool result = true;
            //create the table if it doens't exist (and hope the data types come out ok becasue I'm being lazy)

            try
            {
                using (SqlConnection connection = new SqlConnection(options.DBConnectionString))
                {

                    connection.Open();

                    //create the table if it doens't exist (and hope the data types come out ok becasue I'm being lazy)
                    //NB will need to have create permisisons on the db to run this if it's ever used anywhere other than my local


                    using (SqlCommand command = new SqlCommand())
                    {
                        command.Connection = connection;

                        //TODO add schema into settings
                        command.CommandText = $@"SELECT count(*) 
                                                    FROM INFORMATION_SCHEMA.TABLES
                                                    WHERE TABLE_SCHEMA = 'dbo'
                                                    AND TABLE_NAME = @tableName";

                        command.Parameters.AddWithValue("@tableName", tableName);

                        int tableCount = (int)command.ExecuteScalar();

                        if (tableCount == 0)
                        {
                            List<string> columnsToCreate = columns.Select(x => $"[{x}] [nvarchar](500) NOT NULL").ToList();

                            string tempCreate = string.Join(",", columnsToCreate);

                            command.CommandText = $@"CREATE TABLE [dbo].[{tableName}](
	                                                        {tempCreate}
                                                        ) ON [PRIMARY]";

                            command.ExecuteNonQuery();
                        }

                    }
                }

            }
            catch (Exception ex)
            {
                result = false;
                //TODO - add in some sensible error handling
                ConsoleWriteColour(ex.ToString(), ConsoleColor.Red);
            }

            return result;
        }

    }
}
