using System;
using System.Threading;
using Microsoft.Extensions.Configuration;

namespace ONSGeoDataImporter
{
    class Program
    {

        private static IConfiguration Configuration;


        static void Main(string[] args)
        {

            //Set up access to config file

            Configuration = new ConfigurationBuilder()
                                      .AddJsonFile("appsettings.json", false, true)
                                      .Build();

            Console.WriteLine($"ONS GeoData Importer");

            Console.WriteLine("Start Data Import Y/N");

            string input = Console.ReadLine().ToUpper();

            int status = 0;

            while (status == 0)
            {

                switch (input)
                {
                    case "Y":

                        DataImport doStuff = new DataImport(Configuration);
                        bool success = doStuff.mainProcess();

                        if (success)
                        {
                            Console.WriteLine("Import completed type 'Exit' to close");
                            input = Console.ReadLine().ToUpper();
                        }
                        else
                        {
                            Console.WriteLine("Import failed type 'Exit' to close or 'Y' to re-run");
                            input = Console.ReadLine().ToUpper();
                        }
                        break;
                    case "N":
                        Console.WriteLine("Enter 'EXIT' to quit or Y to run");
                        input = Console.ReadLine().ToUpper();
                        break;
                    case "EXIT":
                        Console.WriteLine("Closing application");
                        status = 1;
                        break;
                    default:
                        Console.WriteLine("Command not recognised: Start Data Import Y/N");
                        input = Console.ReadLine().ToUpper();
                        break;
                }
            }

            Thread.Sleep(4000);
            Environment.Exit(0);

        }






    }

}
