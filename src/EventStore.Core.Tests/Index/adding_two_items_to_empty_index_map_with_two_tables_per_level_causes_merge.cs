using System.IO;
using System.Linq;
using EventStore.Core.Index;
using EventStore.Core.Tests.Fakes;
using EventStore.Core.TransactionLog;
using Xunit;

namespace EventStore.Core.Tests.Index
{
    public class adding_two_items_to_empty_index_map_with_two_tables_per_level_causes_merge: IUseFixture<MergeFileFixture>
    {
        private string _filename;
        private IndexMap _map;
        private string _mergeFile;
        private MergeResult _result;

        public void SetFixture(MergeFileFixture data)
        {
         
            _filename = data.Filename;
            _mergeFile = data.MergeFile;

            _map = data.Map(maxTablesPerLevel: 2);

            _result = data.Result(() =>
            {
                var memtable = new HashListMemTable(maxSize: 10);
                memtable.Add(0, 1, 0);

                var result = _map.AddPTable(PTable.FromMemtable(memtable, data.GetTempFilePath()),
                    123, 321, _ => true, new GuidFilenameProvider(data.PathName));
                result.ToDelete.ForEach(x => x.MarkForDestruction());
                result = result.MergedMap.AddPTable(PTable.FromMemtable(memtable, data.GetTempFilePath()),
                    100, 400, _ => true, new FakeFilenameProvider(_mergeFile));
                result.ToDelete.ForEach(x => x.MarkForDestruction());
                return result;
            });
        }

        [Fact]
        public void the_prepare_checkpoint_is_taken_from_the_latest_added_table()
        {
            Assert.Equal(100, _result.MergedMap.PrepareCheckpoint);
        }

        [Fact]
        public void the_commit_checkpoint_is_taken_from_the_latest_added_table()
        {
            Assert.Equal(400, _result.MergedMap.CommitCheckpoint);
        }

        [Fact]
        public void there_are_two_items_to_delete()
        {
            Assert.Equal(2, _result.ToDelete.Count);
        }

        [Fact]
        public void the_merged_map_has_a_single_file()
        {
            Assert.Equal(1, _result.MergedMap.GetAllFilenames().Count());
            Assert.Equal(_mergeFile, _result.MergedMap.GetAllFilenames().ToList()[0]);
        }

        [Fact]
        public void the_original_map_did_not_change()
        {
            Assert.Equal(0, _map.InOrder().Count());
            Assert.Equal(0, _map.GetAllFilenames().Count());
        }

        [Fact]
        public void a_merged_file_was_created()
        {
            Assert.True(File.Exists(_mergeFile));
        }
    }
}