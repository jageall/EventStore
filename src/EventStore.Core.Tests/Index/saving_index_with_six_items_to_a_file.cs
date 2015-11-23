using System;
using System.IO;
using System.Linq;
using EventStore.Core.Index;
using EventStore.Core.Util;
using Xunit;

namespace EventStore.Core.Tests.Index
{
    public class saving_index_with_six_items_to_a_file: SpecificationWithDirectory
    {
        private string _filename;
        private string _tablename;
        private string _mergeFile;
        private IndexMap _map;
        private MergeResult _result;

        public saving_index_with_six_items_to_a_file()
        {
 
            _filename = GetFilePathFor("indexfile");
            _tablename = GetTempFilePath();
            _mergeFile = GetFilePathFor("outfile");

            _map = IndexMap.FromFile(_filename, maxTablesPerLevel: 4);
            var memtable = new HashListMemTable(maxSize: 10);
            memtable.Add(0, 2, 123);
            var table = PTable.FromMemtable(memtable, _tablename);
            _result = _map.AddPTable(table, 0, 0, _ => true, new FakeFilenameProvider(_mergeFile));
            _result = _result.MergedMap.AddPTable(table, 0, 0, _ => true, new FakeFilenameProvider(_mergeFile));
            _result = _result.MergedMap.AddPTable(table, 0, 0, _ => true, new FakeFilenameProvider(_mergeFile));
            var merged = _result.MergedMap.AddPTable(table, 0, 0, _ => true, new FakeFilenameProvider(_mergeFile));
            _result = merged.MergedMap.AddPTable(table, 0, 0, _ => true, new FakeFilenameProvider(_mergeFile));
            _result = _result.MergedMap.AddPTable(table, 7, 11, _ => true, new FakeFilenameProvider(_mergeFile));
            _result.MergedMap.SaveToFile(_filename);

            table.Dispose();
        
            merged.MergedMap.InOrder().ToList().ForEach(x => x.Dispose());
            merged.ToDelete.ForEach(x => x.Dispose());

            _result.MergedMap.InOrder().ToList().ForEach(x => x.Dispose());
            _result.ToDelete.ForEach(x => x.Dispose());
        }

        [Fact]
        public void the_file_exists()
        {
            Assert.True(File.Exists(_filename));
        }

        [Fact]
        public void the_file_contains_correct_data()
        {
            using (var fs = File.OpenRead(_filename))
            using (var reader = new StreamReader(fs))
            {
                var text = reader.ReadToEnd();
                var lines = text.Replace("\r", "").Split('\n');

                fs.Position = 32;
                var md5 = MD5Hash.GetHashFor(fs);
                var md5String = BitConverter.ToString(md5).Replace("-", "");

                Assert.Equal(7, lines.Count());
                Assert.Equal(md5String, lines[0]);
                Assert.Equal(PTable.Version.ToString(), lines[1]);
                Assert.Equal("7/11", lines[2]);
                var name = new FileInfo(_tablename).Name;
                Assert.Equal("0,0," + name, lines[3]);
                Assert.Equal("0,1," + name, lines[4]);
                Assert.Equal("1,0," + Path.GetFileName(_mergeFile), lines[5]);
                Assert.Equal("", lines[6]);
            }
        }

        [Fact]
        public void saved_file_could_be_read_correctly_and_without_errors()
        {
            var map = IndexMap.FromFile(_filename);
            map.InOrder().ToList().ForEach(x => x.Dispose());

            Assert.Equal(7, map.PrepareCheckpoint);
            Assert.Equal(11, map.CommitCheckpoint);
        }
    }
}