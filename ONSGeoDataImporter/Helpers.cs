using System;
using System.Collections.Generic;
using System.Text;

namespace ONSGeoDataImporter
{
    static class Helpers
    {

        public  static void ConsoleWriteColour( string text, ConsoleColor color)
        {
            Console.ForegroundColor = color;
            Console.WriteLine(text);
            Console.ForegroundColor = ConsoleColor.Red;

        }

        public static string GetElapsedTime(DateTime start, DateTime end)
        {

            return $"{(end - start).TotalMinutes} mins";

        }

        public static string GetElapsedTime(DateTime start)
        {

            return $"{(DateTime.Now - start).TotalSeconds} s";

        }

    }
}
