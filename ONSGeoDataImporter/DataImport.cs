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
using System.Net.Http;
using System.Threading.Tasks;
using System.IO.Compression;

namespace ONSGeoDataImporter
{
    class DataImport
    {

        public ImportOptions options = new ImportOptions();

        public DateTime startTime = DateTime.Now;

        public List<string> failedFiles = new List<string>();
        public String dataSetPath { get; set; }
        public string fileName { get; set; }

        public DataImport(IConfiguration config, string dataSetName)
        {
            config.GetSection(ImportOptions.Import).Bind(options);
            dataSetPath = $"{options.DataRootPath}/{dataSetName}";
            fileName = dataSetName;
        }

        public async Task<List<string>> mainProcessAsync()
        {
            Console.WriteLine($"Starting Import at {startTime.TimeOfDay}");

            if (await downloadAndExtractSourceDataFile())
            {
                Console.WriteLine("Starting to Importing Data Files");
                await ImportDataFiles();
                Console.WriteLine("Import of Data Files Completed");

                Console.WriteLine("Starting to Importing Documents Files");
                await ImportDocumentFile();
                Console.WriteLine("Import of Documents Completed");
            };

            return failedFiles;
        }



        private async Task<bool> downloadAndExtractSourceDataFile()
        {

            bool status = true;

            ConsoleWriteColour($"Starting to download Source Data", ConsoleColor.Blue);

            try
            {

                //Get rid of any existing Data

                if (Directory.Exists(dataSetPath))
                {
                    Directory.Delete(dataSetPath, true);
                    Directory.CreateDirectory(dataSetPath);
                }
                else
                {
                    Directory.CreateDirectory(dataSetPath);
                }

                using (HttpClient client = new HttpClient())
                {
                    using (var result = await client.GetAsync(options.DataFileUrl))
                    {
                        if (result.IsSuccessStatusCode)
                        {
                            byte[] fileData = await result.Content.ReadAsByteArrayAsync();

                            File.WriteAllBytes($"{dataSetPath}/{fileName}.zip", fileData);

                            ZipFile.ExtractToDirectory($"{dataSetPath}/{fileName}.zip", dataSetPath);

                            ConsoleWriteColour($"Files downloaded and extracted in {GetElapsedTime(startTime)}", ConsoleColor.Blue);

                        }
                        else
                        {
                            ConsoleWriteColour($"File Could not be downloaded", ConsoleColor.Red);
                            status = false;
                        }

                    }
                }

            }
            catch (Exception ex)
            {
                ConsoleWriteColour($"File Could not be downloaded", ConsoleColor.Red);

                throw ex;

            }

            return status;

        }


        private async Task ImportDocumentFile()
        {

            //Much smaller files but may have data issues that need to be handled

            string docsFullPath = $"{dataSetPath}/{options.DocumentsFolder}/";

            //maybe add in a check for csv files that aren't in the settings and output a warning for if they add in new files.

            foreach (FileToDBMapping fileMapping in options.DocumentsFileList)
            {
                string filename = $"{fileMapping.FileName}.csv";
                DateTime fileStart = DateTime.Now;

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

                    if (await createTableIfItsNotThere(fileMapping.DBTableName, columns))
                    {
                        using (SqlConnection connection = new SqlConnection(options.DBConnectionString))
                        {

                            connection.Open();

                            StringBuilder insertStatements = new StringBuilder();

                            // these are all small files so individual imports statements are fast enough - won't work when I get to the data side.
                            //Should be doing this with parameters but this will do for now.

                            List<string> dbcolumns = columns.Select(x => $"[{x}]").ToList();
                            string baseStatement = $"INSERT INTO dbo.{fileMapping.DBTableName} ({string.Join(",", dbcolumns)}) VALUES ( ";

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
                                        insertRow += $"'{temp1.ToString().Replace("'", "''")}',";
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

                                    command.CommandText = $"TRUNCATE TABLE dbo.{fileMapping.DBTableName}";
                                    await command.ExecuteNonQueryAsync();

                                    command.CommandText = insertStatements.ToString();
                                    await command.ExecuteNonQueryAsync();
                                    transaction.Commit();
                                }
                            }
                        }

                        ConsoleWriteColour($"{filename} imported into {fileMapping.DBTableName} in {GetElapsedTime(fileStart)}", ConsoleColor.Blue);

                    }
                    else
                    {
                        failedFiles.Add(filename);
                        throw new NotImplementedException($"Table {fileMapping.DBTableName} does not exist and cannot be created");
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



        private async Task ImportDataFiles()
        {

            //The REALLY big files, but all the data is in codes that ref the document tables so no data issues to worry about

            ConsoleWriteColour($"Starting Import of Data Files", ConsoleColor.Blue);

            string docsFullPath = $"{dataSetPath}/{options.DataFolder}/";

            //maybe add in a check for csv files that aren't in the settings and output a warning for if they add in new files.

            foreach (FileToDBMapping fileMapping in options.DataFileList)
            {
                DateTime fileStart = DateTime.Now;

                string filename = $"{fileMapping.FileName}.csv";

                try
                {
                    List<string> columns;

                    using (StreamReader reader = new StreamReader($"{docsFullPath}/{filename}"))
                    {
                        //todo set up culture stuff properly at some point

                        //For the really big data files we REALLY don't want to be reading the whole file in at this point, just need the header row to get the table set up
                        using (CsvReader csv = new CsvReader(reader, CultureInfo.InvariantCulture))
                        {
                            csv.Configuration.IgnoreBlankLines = true;

                            //Handles the file with the empty field at the end.
                            csv.Read();
                            csv.Context.Record = csv.Context.Record.Reverse().SkipWhile(string.IsNullOrWhiteSpace).Reverse().ToArray();
                            csv.ReadHeader();

                            columns = csv.Context.HeaderRecord.ToList<string>();
                        }

                    }

                    if (await createTableIfItsNotThere(fileMapping.DBTableName, columns))
                    {
                        using (SqlConnection connection = new SqlConnection(options.DBConnectionString))
                        {

                            connection.Open();

                            using (SqlTransaction transaction = connection.BeginTransaction())
                            {

                                using (SqlCommand command = new SqlCommand())
                                {
                                    command.Connection = connection;
                                    command.Transaction = transaction;

                                    command.CommandText = $"TRUNCATE TABLE dbo.{fileMapping.DBTableName}";
                                    await command.ExecuteNonQueryAsync();
                                }

                                using (StreamReader reader = new StreamReader($"{docsFullPath}/{filename}"))
                                {
                                    using (CsvDataReader csvReader = new CsvDataReader(new CsvReader(reader, CultureInfo.InvariantCulture)))
                                    {
                                        using (SqlBulkCopy copy = new SqlBulkCopy(connection, SqlBulkCopyOptions.KeepNulls, transaction))
                                        {
                                            copy.BatchSize = 10000; //TODO put this in settings at some point

                                            copy.DestinationTableName = fileMapping.DBTableName;
                                            await copy.WriteToServerAsync(csvReader);
                                            transaction.Commit();
                                        }
                                    }
                                }
                            }
                        }

                        ConsoleWriteColour($"{filename} imported into {fileMapping.DBTableName} in {GetElapsedTime(fileStart)}", ConsoleColor.Blue);
                    }
                    else
                    {
                        failedFiles.Add(filename);
                        throw new NotImplementedException($"Table {fileMapping.DBTableName} does not exist and cannot be created");
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

        private async Task<bool> createTableIfItsNotThere(string tableName, List<string> columns)
        {
            bool result = true;
            //create the table if it doens't exist (and hope the data types come out ok becasue I'm being lazy)

            try
            {
                using (SqlConnection connection = new SqlConnection(options.DBConnectionString))
                {

                    connection.Open();

                    using (SqlCommand command = new SqlCommand())
                    {
                        command.Connection = connection;

                        //TODO add schema into settings
                        command.CommandText = $@"SELECT count(*) 
                                                    FROM INFORMATION_SCHEMA.TABLES
                                                    WHERE TABLE_SCHEMA = 'dbo'
                                                    AND TABLE_NAME = @tableName";

                        command.Parameters.AddWithValue("@tableName", tableName);

                        int tableCount = (int)await command.ExecuteScalarAsync();

                        if (tableCount == 0)
                        {

                            ConsoleWriteColour($"{tableName} does not exist", ConsoleColor.Yellow);

                            List<string> columnsToCreate = columns.Select(x => $"[{x}] [nvarchar](500) NOT NULL").ToList();

                            string tempCreate = string.Join(",", columnsToCreate);

                            command.CommandText = $@"CREATE TABLE [dbo].[{tableName}](
	                                                        {tempCreate}
                                                        ) ON [PRIMARY]";

                            await command.ExecuteNonQueryAsync();

                            ConsoleWriteColour($"{tableName} created using default data types - may need modification", ConsoleColor.Yellow);

                        }

                    }
                }

            }
            catch (Exception ex)
            {
                result = false;
                //TODO - add in some sensible error handling
                ConsoleWriteColour($"Table {tableName} does not exist and cannot be created", ConsoleColor.Red);
                ConsoleWriteColour(ex.ToString(), ConsoleColor.Red);
            }

            return result;
        }

    }
}
