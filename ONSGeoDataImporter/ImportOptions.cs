using System;
using System.Collections.Generic;
using System.Text;

namespace ONSGeoDataImporter
{
    class ImportOptions
    {

        public string DBConnectionString { get; set; }
        public const string Import = "ImportOptions";
        public string DataRootPath { get; set; }
        public string DocumentsFolder { get; set; }
        public string DataFolder { get; set; }
        public List<string[]> DocumentsFileList { get; set; }

    }
}
