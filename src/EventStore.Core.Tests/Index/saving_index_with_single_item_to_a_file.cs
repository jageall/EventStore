using System;
using System.IO;
using System.Linq;
using EventStore.Core.Index;
using EventStore.Core.Util;
using Xunit;

namespace EventStore.Core.Tests.Index
{
    public class saving_index_with_single_item_to_a_fileFixture : SpecificationWithDirectoryPerTestFixture
    {
        public readonly string _filename;
        public readonly IndexMap _map;
        public readonly string _tablename;
        public readonly string _mergeFile;
        public readonly MergeResult _result;

        public saving_index_with_single_item_to_a_fileFixture()
        {
            _filename = GetFilePathFor("indexfile");
            _tablename = GetTempFilePath();
            _mergeFile = GetFilePathFor("outputfile");

            _map = IndexMap.FromFile(_filename);
            var memtable = new HashListMemTable(maxSize: 10);
            memtable.Add(0, 2, 7);
            var table = PTable.FromMemtable(memtable, _tablename);
            _result = _map.AddPTable(table, 7, 11, _ => true, new FakeFilenameProvider(_mergeFile));
            _result.MergedMap.SaveToFile(_filename);
            _result.ToDelete.ForEach(x => x.Dispose());
            _result.MergedMap.InOrder().ToList().ForEach(x => x.Dispose());
            table.Dispose();
        }
        public override void Dispose()
        {
            _result.ToDelete.ForEach(x => x.MarkForDestruction());
            _result.MergedMap.InOrder().ToList().ForEach(x => x.MarkForDestruction());
            _result.MergedMap.InOrder().ToList().ForEach(x => x.WaitForDisposal(1000));
            base.Dispose();
        }
    }
    public class saving_index_with_single_item_to_a_file : IUseFixture<saving_index_with_single_item_to_a_fileFixture>
    {
        private string _filename;
        private IndexMap _map;
        private string _tablename;
        private string _mergeFile;
        private MergeResult _result;

        public void SetFixture(saving_index_with_single_item_to_a_fileFixture data)
        {
            _filename = data._filename;
            _map = data._map;
            _tablename = data._tablename;
            _mergeFile = data._mergeFile;
            _result = data._result;
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

                Assert.Equal(5, lines.Count());
                Assert.Equal(md5String, lines[0]);
                Assert.Equal(PTable.Version.ToString(), lines[1]);
                Assert.Equal("7/11", lines[2]);
                Assert.Equal("0,0," + Path.GetFileName(_tablename), lines[3]);
                Assert.Equal("", lines[4]);
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