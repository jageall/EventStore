using System;
using EventStore.Common.Utils;
using EventStore.Core.DataStructures;
using EventStore.Core.Index;
using EventStore.Core.Services.Storage.ReaderIndex;
using EventStore.Core.Tests.Fakes;
using EventStore.Core.Tests.TransactionLog.Scavenging.Helpers;
using EventStore.Core.TransactionLog;
using EventStore.Core.TransactionLog.Checkpoint;
using EventStore.Core.TransactionLog.Chunks;
using EventStore.Core.TransactionLog.FileNamingStrategy;
using Xunit;

namespace EventStore.Core.Tests.Services.Storage
{
    public abstract class SimpleDbTestScenario : IClassFixture<SimpleDbTestScenario.FixtureData>
    {
        private readonly int _metastreamMaxCount;
        protected readonly int MaxEntriesInMemTable;
        public IReadIndex ReadIndex;

        public class FixtureData : SpecificationWithDirectoryPerTestFixture
        {
            protected TableIndex TableIndex;
            public IReadIndex ReadIndex;

            protected DbResult DbRes;

            private Action<int, int, Func<TFChunkDbCreationHelper, DbResult>> _initialize;

            public FixtureData()
            {
                _initialize = (maxEntriesInMemTable, metastreamMaxCount, createDb) =>
                {
                    _initialize = (_, __, ___) => { };

                    var dbConfig = new TFChunkDbConfig(PathName,
                        new VersionedPatternFileNamingStrategy(PathName, "chunk-"),
                        1024*1024,
                        0,
                        new InMemoryCheckpoint(0),
                        new InMemoryCheckpoint(0),
                        new InMemoryCheckpoint(-1),
                        new InMemoryCheckpoint(-1));
                    var dbCreationHelper = new TFChunkDbCreationHelper(dbConfig);

                    DbRes = createDb(dbCreationHelper);

                    DbRes.Db.Config.WriterCheckpoint.Flush();
                    DbRes.Db.Config.ChaserCheckpoint.Write(DbRes.Db.Config.WriterCheckpoint.Read());
                    DbRes.Db.Config.ChaserCheckpoint.Flush();

                    var readers = new ObjectPool<ITransactionFileReader>(
                        "Readers", 2, 2, () => new TFChunkReader(DbRes.Db, DbRes.Db.Config.WriterCheckpoint));

                    TableIndex = new TableIndex(GetFilePathFor("index"),
                        () => new HashListMemTable(maxEntriesInMemTable*2),
                        () => new TFReaderLease(readers),
                        maxEntriesInMemTable);

                    ReadIndex = new ReadIndex(new NoopPublisher(),
                        readers,
                        TableIndex,
                        new ByLengthHasher(),
                        0,
                        additionalCommitChecks: true,
                        metastreamMaxCount: metastreamMaxCount);

                    ReadIndex.Init(DbRes.Db.Config.ChaserCheckpoint.Read());
                };
            }

            public override void Dispose()
            {
                DbRes.Db.Close();

                base.Dispose();
            }

            public void EnsureInitialized(int maxEntriesInMemTable, int metastreamMaxCount, Func<TFChunkDbCreationHelper, DbResult> createDb)
            {
                _initialize(maxEntriesInMemTable, metastreamMaxCount, createDb);
            }
        }

        public SimpleDbTestScenario(FixtureData fixture, int maxEntriesInMemTable = 20, int metastreamMaxCount = 1)
        {
            Ensure.Positive(maxEntriesInMemTable, "maxEntriesInMemTable");
            Ensure.Positive(maxEntriesInMemTable, "metastreamMaxCount");
            MaxEntriesInMemTable = maxEntriesInMemTable;
            _metastreamMaxCount = metastreamMaxCount;

            fixture.EnsureInitialized(MaxEntriesInMemTable, _metastreamMaxCount, CreateDb);
            ReadIndex = fixture.ReadIndex;

            
        }

        protected abstract DbResult CreateDb(TFChunkDbCreationHelper dbCreator);
    }
}
