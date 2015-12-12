using System;
using System.IO;
using System.Linq;
using EventStore.Core.Index;
using EventStore.Core.Tests.Fakes;
using EventStore.Core.TransactionLog;
using Xunit;

namespace EventStore.Core.Tests.Index
{
    public class adding_item_to_empty_index_map: IClassFixture<MergeFileFixture>
    {
        private string _filename;
        private IndexMap _map;
        private string _tablename;
        private string _mergeFile;
        private MergeResult _result;

        public void SetFixture(MergeFileFixture data)
        {
            _filename = data.Filename;
            _tablename = data.GetTempFilePath();
            _mergeFile = data.GetFilePathFor("mergefile");

            _map = IndexMap.FromFile(_filename);
            var memtable = new HashListMemTable(maxSize: 10);
            memtable.Add(0, 1, 0);
            var table = PTable.FromMemtable(memtable, _tablename);
            _result = _map.AddPTable(table, 7, 11, _ => true, new FakeFilenameProvider(_mergeFile));
            table.MarkForDestruction();
        }

        [Fact]
        public void the_prepare_checkpoint_is_taken_from_the_latest_added_table()
        {
            Assert.Equal(7, _result.MergedMap.PrepareCheckpoint);
        }

        [Fact]
        public void the_commit_checkpoint_is_taken_from_the_latest_added_table()
        {
            Assert.Equal(11, _result.MergedMap.CommitCheckpoint);
        }

        [Fact]
        public void there_are_no_items_to_delete()
        {
            Assert.Equal(0, _result.ToDelete.Count);
        }

        [Fact]
        public void the_merged_map_has_a_single_file()
        {
            Assert.Equal(1, _result.MergedMap.GetAllFilenames().Count());
            Assert.Equal(_tablename, _result.MergedMap.GetAllFilenames().ToList()[0]);
        }

        [Fact]
        public void the_original_map_did_not_change()
        {
            Assert.Equal(0, _map.InOrder().Count());
            Assert.Equal(0, _map.GetAllFilenames().Count());
        }

        [Fact]
        public void a_merged_file_was_not_created()
        {
            Assert.False(File.Exists(_mergeFile));
        }
    }
}