using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using System.Collections.Generic;

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

                        DataImport doStuff = new DataImport(Configuration, "NSUL_data");
                        //One day I may need to get more than one Data set and this will become a option you can select
                        //Settings will need a rethink if this is ever needed.
                        List<string> success =  doStuff.mainProcessAsync().Result;

                        if (success.Count == 0)
                        {
                            Console.WriteLine("Import completed type 'Exit' to close");
                            input = Console.ReadLine().ToUpper();
                        }
                        else
                        {
                            Console.WriteLine("Some files failed to import. 'Exit' to close or 'Y' to re-run");
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

            Thread.Sleep(1000);
            Environment.Exit(0);

        }

    }

}
