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
        public string DataFileUrl { get; set; }
        public List<FileToDBMapping> DocumentsFileList { get; set; }
        public List<FileToDBMapping> DataFileList { get; set; }

    }


    class FileToDBMapping
    {

        public string FileName { get; set; }
        public string DBTableName { get; set; }

    }

}
