using System;
using System.IO;
using System.Linq;
using EventStore.Core.Index;
using Xunit;

namespace EventStore.Core.Tests.Index
{
    public class adding_sixteen_items_to_empty_index_map_with_four_tables_per_level_causes_double_merge :
        IUseFixture<adding_sixteen_items_to_empty_index_map_with_four_tables_per_level_causes_double_merge.FixtureData>
    {
        private MergeResult _result;
        private string _finalmergefile2;
        private IndexMap _map;

        public class FixtureData : MergeFileFixture
        {
            private string _filename;
            public IndexMap _map;
            private string _finalmergefile;
            public string _finalmergefile2;

            public MergeResult _result;

            public FixtureData()
            {
                _filename = Filename;
                _finalmergefile = GetTempFilePath();
                _finalmergefile2 = GetTempFilePath();

                _map = IndexMap.FromFile(_filename);

                var memtable = new HashListMemTable(maxSize: 10);
                memtable.Add(0, 1, 0);
                var guidFilename = new GuidFilenameProvider(PathName);
                var result = _map.AddPTable(PTable.FromMemtable(memtable, GetTempFilePath()), 0, 0, _ => true,
                    guidFilename);
                result.ToDelete.ForEach(x => x.MarkForDestruction());
                result = result.MergedMap.AddPTable(PTable.FromMemtable(memtable, GetTempFilePath()), 0, 0,
                    _ => true, guidFilename);
                result.ToDelete.ForEach(x => x.MarkForDestruction());
                result = result.MergedMap.AddPTable(PTable.FromMemtable(memtable, GetTempFilePath()), 0, 0,
                    _ => true, guidFilename);
                result.ToDelete.ForEach(x => x.MarkForDestruction());
                result = result.MergedMap.AddPTable(PTable.FromMemtable(memtable, GetTempFilePath()), 0, 0,
                    _ => true, guidFilename);
                result.ToDelete.ForEach(x => x.MarkForDestruction());
                result = result.MergedMap.AddPTable(PTable.FromMemtable(memtable, GetTempFilePath()), 0, 0,
                    _ => true, guidFilename);
                result.ToDelete.ForEach(x => x.MarkForDestruction());
                result = result.MergedMap.AddPTable(PTable.FromMemtable(memtable, GetTempFilePath()), 0, 0,
                    _ => true, guidFilename);
                result.ToDelete.ForEach(x => x.MarkForDestruction());
                result = result.MergedMap.AddPTable(PTable.FromMemtable(memtable, GetTempFilePath()), 0, 0,
                    _ => true, guidFilename);
                result.ToDelete.ForEach(x => x.MarkForDestruction());
                result = result.MergedMap.AddPTable(PTable.FromMemtable(memtable, GetTempFilePath()), 0, 0,
                    _ => true, guidFilename);
                result.ToDelete.ForEach(x => x.MarkForDestruction());
                result = result.MergedMap.AddPTable(PTable.FromMemtable(memtable, GetTempFilePath()), 0, 0,
                    _ => true, guidFilename);
                result.ToDelete.ForEach(x => x.MarkForDestruction());
                result = result.MergedMap.AddPTable(PTable.FromMemtable(memtable, GetTempFilePath()), 0, 0,
                    _ => true, guidFilename);
                result.ToDelete.ForEach(x => x.MarkForDestruction());
                result = result.MergedMap.AddPTable(PTable.FromMemtable(memtable, GetTempFilePath()), 0, 0,
                    _ => true, guidFilename);
                result.ToDelete.ForEach(x => x.MarkForDestruction());
                result = result.MergedMap.AddPTable(PTable.FromMemtable(memtable, GetTempFilePath()), 0, 0,
                    _ => true, guidFilename);
                result.ToDelete.ForEach(x => x.MarkForDestruction());
                result = result.MergedMap.AddPTable(PTable.FromMemtable(memtable, GetTempFilePath()), 0, 0,
                    _ => true, guidFilename);
                result.ToDelete.ForEach(x => x.MarkForDestruction());
                result = result.MergedMap.AddPTable(PTable.FromMemtable(memtable, GetTempFilePath()), 0, 0,
                    _ => true, guidFilename);
                result.ToDelete.ForEach(x => x.MarkForDestruction());
                result = result.MergedMap.AddPTable(PTable.FromMemtable(memtable, GetTempFilePath()), 0, 0,
                    _ => true, guidFilename);
                result.ToDelete.ForEach(x => x.MarkForDestruction());
                result = result.MergedMap.AddPTable(PTable.FromMemtable(memtable, GetTempFilePath()), 1, 2,
                    _ => true, new FakeFilenameProvider(_finalmergefile, _finalmergefile2));
                result.ToDelete.ForEach(x => x.MarkForDestruction());
                _result = result;
            }

            public override void Dispose()
            {
                _map.Dispose(TimeSpan.FromMilliseconds(1000));
                _result.ToDelete.ForEach(x=>x.Dispose());
                _result.MergedMap.Dispose(TimeSpan.FromMilliseconds(1000));
                base.Dispose();
            }
        }

        public void SetFixture(FixtureData data)
        {
            _result = data._result;
            _map = data._map;
            _finalmergefile2 = data._finalmergefile2;
        }

        [Fact]
        public void the_prepare_checkpoint_is_taken_from_the_latest_added_table()
        {
            Assert.Equal(1, _result.MergedMap.PrepareCheckpoint);
        }

        [Fact]
        public void the_commit_checkpoint_is_taken_from_the_latest_added_table()
        {
            Assert.Equal(2, _result.MergedMap.CommitCheckpoint);
        }

        [Fact]
        public void there_are_eight_items_to_delete()
        {
            Assert.Equal(8, _result.ToDelete.Count);
        }

        [Fact]
        public void the_merged_map_has_a_single_file()
        {
            Assert.Equal(1, _result.MergedMap.GetAllFilenames().Count());
            Assert.Equal(_finalmergefile2, _result.MergedMap.GetAllFilenames().ToList()[0]);
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
            Assert.True(File.Exists(_finalmergefile2));
        }
    }
}