using System;
using System.Collections.Generic;
using System.IO;
using EventStore.Core.DataStructures;
using EventStore.Core.Index;
using EventStore.Core.Index.Hashes;
using EventStore.Core.Services.Storage.ReaderIndex;
using EventStore.Core.Settings;
using EventStore.Core.Tests.Fakes;
using EventStore.Core.TransactionLog;
using EventStore.Core.TransactionLog.Checkpoint;
using EventStore.Core.TransactionLog.Chunks;
using EventStore.Core.TransactionLog.FileNamingStrategy;
using EventStore.Core.TransactionLog.LogRecords;
using Xunit;

namespace EventStore.Core.Tests.TransactionLog.Scavenging.Helpers
{
    public abstract class ScavengeTestScenario: IClassFixture<ScavengeTestScenario.Fixture>
    {
        private readonly int _metastreamMaxCount;
        protected IReadIndex ReadIndex{get { return _fixture.ReadIndex; }}
        private Fixture _fixture;
        private TFChunkDb _db;
        protected TFChunkDb Db
        {
            get { return _db; }
        }
    
        public class Fixture : SpecificationWithDirectoryPerTestFixture
        {
            public IReadIndex ReadIndex;

            public TFChunkDb Db
            {
                get { return _dbResult.Db; }
            }

           
            private DbResult _dbResult;
            private LogRecord[][] _keptRecords;
            private bool _checked;
            private Action<int, Func<TFChunkDbCreationHelper, DbResult>, Func<DbResult, LogRecord[][]>> _initialize;

            public Fixture()
            {
                _initialize = (metastreamMaxCount, createDb, keptRecords) =>
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
                    _dbResult = createDb(dbCreationHelper);
                    _keptRecords = keptRecords(_dbResult);

                    _dbResult.Db.Config.WriterCheckpoint.Flush();
                    _dbResult.Db.Config.ChaserCheckpoint.Write(_dbResult.Db.Config.WriterCheckpoint.Read());
                    _dbResult.Db.Config.ChaserCheckpoint.Flush();

                    var indexPath = Path.Combine(PathName, "index");
                    var readerPool = new ObjectPool<ITransactionFileReader>(
                        "ReadIndex readers pool", ESConsts.PTableInitialReaderCount, ESConsts.PTableMaxReaderCount,
                        () => new TFChunkReader(_dbResult.Db, _dbResult.Db.Config.WriterCheckpoint));
                    var tableIndex = new TableIndex(indexPath,
                        () => new HashListMemTable(maxSize: 200),
                        () => new TFReaderLease(readerPool),
                        maxSizeForMemory: 100,
                        maxTablesPerLevel: 2);
                    var hasher = new XXHashUnsafe();
                    ReadIndex = new ReadIndex(new NoopPublisher(), readerPool, tableIndex, hasher, 100, true,
                        metastreamMaxCount);
                    ReadIndex.Init(_dbResult.Db.Config.WriterCheckpoint.Read());

                    //var scavengeReadIndex = new ScavengeReadIndex(_dbResult.Streams, _metastreamMaxCount);
                    var scavenger = new TFChunkScavenger(_dbResult.Db, tableIndex, hasher, ReadIndex);
                    scavenger.Scavenge(alwaysKeepScavenged: true, mergeChunks: false);
                };
            }

            public override void Dispose()
            {
                ReadIndex.Close();
                _dbResult.Db.Close();

                base.Dispose();

                if (!_checked)
                    throw new Exception("Records were not checked. Probably you forgot to call CheckRecords() method.");
            }

            public void CheckRecords()
            {
                _checked = true;
                Assert.Equal(_keptRecords.Length, _dbResult.Db.Manager.ChunksCount);

                for (int i = 0; i < _keptRecords.Length; ++i)
                {
                    var chunk = _dbResult.Db.Manager.GetChunk(i);

                    var chunkRecords = new List<LogRecord>();
                    RecordReadResult result = chunk.TryReadFirst();
                    while (result.Success)
                    {
                        chunkRecords.Add(result.LogRecord);
                        result = chunk.TryReadClosestForward((int)result.NextPosition);
                    }

                    Assert.True(_keptRecords[i].Length == chunkRecords.Count, "Wrong number of records in chunk #{0}" + i);

                    for (int j = 0; j < _keptRecords[i].Length; ++j)
                    {
                        Assert.Equal(_keptRecords[i][j] , chunkRecords[j]);
                    }
                }
            }

            public void EnsureInitialized(int metastreamMaxCount, Func<TFChunkDbCreationHelper, DbResult> createDb,
                Func<DbResult, LogRecord[][]> keptRecords)
            {
                _initialize(metastreamMaxCount, createDb, keptRecords);
            }
        }

        protected ScavengeTestScenario(int metastreamMaxCount = 1)
        {
            _metastreamMaxCount = metastreamMaxCount;
        }

        public void SetFixture(Fixture fixture)
        {
            fixture.EnsureInitialized(_metastreamMaxCount, CreateDb, KeptRecords);
            _fixture = fixture;
            _db = fixture.Db;
        }

        protected abstract DbResult CreateDb(TFChunkDbCreationHelper dbCreator);

        protected abstract LogRecord[][] KeptRecords(DbResult dbResult);

        protected void CheckRecords()
        {
            _fixture.CheckRecords();
        }
    }
}
