using System;
using System.IO;
using System.Linq;
using EventStore.Core.Index;
using Xunit;

namespace EventStore.Core.Tests.Index
{
    public class MergeFileFixture : SpecificationWithDirectoryPerTestFixture
    {
        //TODO JAG unless there is some pressing performance reason, this should really be done per test to simplify the test code
        private readonly string _filename;
        private IndexMap _map;
        private readonly string _mergeFile;
        private MergeResult _result;

        public MergeFileFixture()
        {
            _mergeFile = GetTempFilePath();
            _filename = GetTempFilePath();
        }

        public string Filename
        {
            get { return _filename; }
        }

        public IndexMap Map(int maxTablesPerLevel)
        {
            return _map ?? (_map = IndexMap.FromFile(Filename, maxTablesPerLevel)); 
        }

        public string MergeFile
        {
            get { return _mergeFile; }
        }

        public MergeResult Result(Func<MergeResult> resultFactory)
        {
            return _result ?? (_result = resultFactory());
        }

        public override void Dispose()
        {
            if(_result != null)
            _result.MergedMap.InOrder().ToList().ForEach(x => x.MarkForDestruction());
            File.Delete(_filename);
            File.Delete(_mergeFile);

            base.Dispose();
        }
    }

    public class adding_four_items_to_empty_index_map_with_four_tables_per_level_causes_merge : IUseFixture<MergeFileFixture>
    {
        private string _filename;
        private IndexMap _map;
        private string _mergeFile;
        private MergeResult _result;

        public void SetFixture(MergeFileFixture data)
        {
            _filename = data.Filename;
            _map = data.Map(4);
            _mergeFile = data.MergeFile;
            _result = data.Result(() =>
            {
                var memtable = new HashListMemTable(maxSize: 10);
                memtable.Add(0, 1, 0);
                var result = _map.AddPTable(PTable.FromMemtable(memtable, data.GetTempFilePath()), 1, 2,
                                     _ => true, new GuidFilenameProvider(data.PathName));
                result.ToDelete.ForEach(x => x.MarkForDestruction());

                result = result.MergedMap.AddPTable(PTable.FromMemtable(memtable, data.GetTempFilePath()), 3, 4,
                                                      _ => true, new GuidFilenameProvider(data.PathName));
                result.ToDelete.ForEach(x => x.MarkForDestruction());

                result = result.MergedMap.AddPTable(PTable.FromMemtable(memtable, data.GetTempFilePath()), 4, 5,
                                                      _ => true, new GuidFilenameProvider(data.PathName));
                result.ToDelete.ForEach(x => x.MarkForDestruction());

                result = result.MergedMap.AddPTable(PTable.FromMemtable(memtable, data.GetTempFilePath()), 0, 1,
                                                      _ => true, new FakeFilenameProvider(_mergeFile));
                result.ToDelete.ForEach(x => x.MarkForDestruction());
                return result;
            });
        }

        [Fact]
        public void the_prepare_checkpoint_is_taken_from_the_latest_added_table()
        {
            Assert.Equal(0, _result.MergedMap.PrepareCheckpoint);
        }

        [Fact]
        public void the_commit_checkpoint_is_taken_from_the_latest_added_table()
        {
            Assert.Equal(1, _result.MergedMap.CommitCheckpoint);
        }

        [Fact]
        public void there_are_four_items_to_delete()
        {
            Assert.Equal(4, _result.ToDelete.Count);
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