using System;
using System.Threading;
using EventStore.Common.Utils;
using EventStore.Core.Data;
using EventStore.Core.DataStructures;
using EventStore.Core.Index;
using EventStore.Core.Services;
using EventStore.Core.Services.Storage.ReaderIndex;
using EventStore.Core.Tests.Fakes;
using EventStore.Core.TransactionLog;
using EventStore.Core.TransactionLog.Checkpoint;
using EventStore.Core.TransactionLog.Chunks;
using EventStore.Core.TransactionLog.FileNamingStrategy;
using EventStore.Core.TransactionLog.LogRecords;
using Xunit;

namespace EventStore.Core.Tests.Services.Storage
{
    public abstract class ReadIndexTestScenario : IUseFixture<ReadIndexTestScenario.FixtureData> 
    {
        protected readonly int MaxEntriesInMemTable;
        protected readonly int MetastreamMaxCount;
        protected IReadIndex ReadIndex{get { return _fixture.ReadIndex; }}

        protected TFChunkDb Db{get { return _fixture.Db; }}
        protected ICheckpoint WriterCheckpoint { get { return _fixture.WriterCheckpoint; }}
        protected ICheckpoint ChaserCheckpoint{get { return _fixture.ChaserCheckpoint; }}
        private FixtureData _fixture;
        protected FixtureData Fixture{get{return _fixture;}}


        public sealed class FixtureData : SpecificationWithDirectoryPerTestFixture
        {
            
            public TableIndex TableIndex;
            public IReadIndex ReadIndex;
            
            public TFChunkDb Db;
            public TFChunkWriter Writer;
            public ICheckpoint WriterCheckpoint;
            public ICheckpoint ChaserCheckpoint;

            private TFChunkScavenger _scavenger;

            private bool _scavenge;
            private bool _completeLastChunkOnScavenge;
            private bool _mergeChunks;

            private Action<int, int,Action, Action, Action> _initialize;
            private Action _additionalTeardown;
            private Action<ReadIndexTestScenario> _assignStashedValues;
            private Exception _setupError;

            public FixtureData()
            {
                _assignStashedValues = _ => { };
                _initialize = (maxEntriesInMemTable, metastreamMaxCount, writeTestScenario, additionalSetup, additionalTeardown) =>
                {
                    try
                    {

                        _initialize = (_, __, ___, ____, _____) => { if (_setupError != null) throw new ApplicationException("Error setting up fixture", _setupError); };
                        WriterCheckpoint = new InMemoryCheckpoint(0);
                        ChaserCheckpoint = new InMemoryCheckpoint(0);

                        Db = new TFChunkDb(new TFChunkDbConfig(PathName,
                            new VersionedPatternFileNamingStrategy(PathName, "chunk-"),
                            10000,
                            0,
                            WriterCheckpoint,
                            ChaserCheckpoint,
                            new InMemoryCheckpoint(-1),
                            new InMemoryCheckpoint(-1)));

                        Db.Open();
                        // create db
                        Writer = new TFChunkWriter(Db);
                        Writer.Open();
                        writeTestScenario();
                        Writer.Close();
                        Writer = null;

                        WriterCheckpoint.Flush();
                        ChaserCheckpoint.Write(WriterCheckpoint.Read());
                        ChaserCheckpoint.Flush();

                        var readers = new ObjectPool<ITransactionFileReader>("Readers", 2, 2,
                            () => new TFChunkReader(Db, Db.Config.WriterCheckpoint));
                        TableIndex = new TableIndex(GetFilePathFor("index"),
                            () => new HashListMemTable(maxEntriesInMemTable*2),
                            () => new TFReaderLease(readers),
                            maxEntriesInMemTable);

                        var hasher = new ByLengthHasher();
                        ReadIndex = new ReadIndex(new NoopPublisher(),
                            readers,
                            TableIndex,
                            hasher,
                            0,
                            additionalCommitChecks: true,
                            metastreamMaxCount: metastreamMaxCount);

                        ReadIndex.Init(ChaserCheckpoint.Read());

                        // scavenge must run after readIndex is built
                        if (_scavenge)
                        {
                            if (_completeLastChunkOnScavenge)
                                Db.Manager.GetChunk(Db.Manager.ChunksCount - 1).Complete();
                            _scavenger = new TFChunkScavenger(Db, TableIndex, hasher, ReadIndex);
                            _scavenger.Scavenge(alwaysKeepScavenged: true, mergeChunks: _mergeChunks);
                        }

                        additionalSetup();
                        _additionalTeardown = additionalTeardown;


                    }
                    catch (Exception ex)
                    {
                        _setupError = ex;
                        throw new ApplicationException("Error setting up fixture", ex);
                    }
                };
            }

            public void EnsureInitialized(int maxEntriesInMemTable, int metastreamMaxCount, Action writeTestScenario, Action additionalSetup, Action additionalTeardown)
            {
                _initialize(maxEntriesInMemTable, metastreamMaxCount, writeTestScenario, additionalSetup,
                    additionalTeardown);
            }

            public override void Dispose()
            {
                if(_additionalTeardown != null)
                    _additionalTeardown();
                if (ReadIndex != null)
                {
                    ReadIndex.Close();
                    ReadIndex.Dispose();
                }
                if (TableIndex != null)
                {
                    TableIndex.Close();
                }
                if (Db != null)
                {
                    Db.Close();
                    Db.Dispose();
                }
                base.Dispose();
            }

            public void Scavenge(bool completeLast, bool mergeChunks)
            {
                if (_scavenge)
                    throw new InvalidOperationException("Scavenge can be executed only once in ReadIndexTestScenario");
                _scavenge = true;
                _completeLastChunkOnScavenge = completeLast;
                _mergeChunks = mergeChunks;
            }

            public EventRecord WriteSingleEvent(string eventStreamId,
                                       int eventNumber,
                                       string data,
                                       DateTime? timestamp = null,
                                       Guid eventId = default(Guid),
                                       bool retryOnFail = false)
            {
                var prepare = LogRecord.SingleWrite(WriterCheckpoint.ReadNonFlushed(),
                                                    eventId == default(Guid) ? Guid.NewGuid() : eventId,
                                                    Guid.NewGuid(),
                                                    eventStreamId,
                                                    eventNumber - 1,
                                                    "some-type",
                                                    Helper.UTF8NoBom.GetBytes(data),
                                                    null,
                                                    timestamp);
                long pos;

                if (!retryOnFail)
                {
                    Assert.True(Writer.Write(prepare, out pos));
                } else
                {
                    long firstPos = prepare.LogPosition;
                    if (!Writer.Write(prepare, out pos))
                    {
                        prepare = LogRecord.SingleWrite(pos,
                                                        prepare.CorrelationId,
                                                        prepare.EventId,
                                                        prepare.EventStreamId,
                                                        prepare.ExpectedVersion,
                                                        prepare.EventType,
                                                        prepare.Data,
                                                        prepare.Metadata,
                                                        prepare.TimeStamp);
                        if (!Writer.Write(prepare, out pos))
                            Assert.True(false, string.Format("Second write try failed when first writing prepare at {0}, then at {1}.", firstPos, prepare.LogPosition));
                    }
                }

                var commit = LogRecord.Commit(WriterCheckpoint.ReadNonFlushed(), prepare.CorrelationId, prepare.LogPosition, eventNumber);
                Assert.True(Writer.Write(commit, out pos));

                var eventRecord = new EventRecord(eventNumber, prepare);
                return eventRecord;
            }

            public EventRecord WriteStreamMetadata(string eventStreamId, int eventNumber, string metadata, DateTime? timestamp = null)
            {
                var prepare = LogRecord.SingleWrite(WriterCheckpoint.ReadNonFlushed(),
                                                    Guid.NewGuid(),
                                                    Guid.NewGuid(),
                                                    SystemStreams.MetastreamOf(eventStreamId),
                                                    eventNumber - 1,
                                                    SystemEventTypes.StreamMetadata,
                                                    Helper.UTF8NoBom.GetBytes(metadata),
                                                    null,
                                                    timestamp ?? DateTime.UtcNow,
                                                    PrepareFlags.IsJson);
                long pos;
                Assert.True(Writer.Write(prepare, out pos));

                var commit = LogRecord.Commit(WriterCheckpoint.ReadNonFlushed(), prepare.CorrelationId, prepare.LogPosition, eventNumber);
                Assert.True(Writer.Write(commit, out pos));

                var eventRecord = new EventRecord(eventNumber, prepare);
                return eventRecord;
            }

            public EventRecord WriteTransactionBegin(string eventStreamId, int expectedVersion, int eventNumber, string eventData)
            {
                var prepare = LogRecord.Prepare(WriterCheckpoint.ReadNonFlushed(),
                                                Guid.NewGuid(),
                                                Guid.NewGuid(),
                                                WriterCheckpoint.ReadNonFlushed(),
                                                0,
                                                eventStreamId,
                                                expectedVersion,
                                                PrepareFlags.Data | PrepareFlags.TransactionBegin,
                                                "some-type",
                                                Helper.UTF8NoBom.GetBytes(eventData),
                                                null);
                long pos;
                Assert.True(Writer.Write(prepare, out pos));
                return new EventRecord(eventNumber, prepare);
            }

            public PrepareLogRecord WriteTransactionBegin(string eventStreamId, int expectedVersion)
            {
                var prepare = LogRecord.TransactionBegin(WriterCheckpoint.ReadNonFlushed(), Guid.NewGuid(), eventStreamId, expectedVersion);
                long pos;
                Assert.True(Writer.Write(prepare, out pos));
                return prepare;
            }

            public EventRecord WriteTransactionEvent(Guid correlationId,
                                                        long transactionPos,
                                                        int transactionOffset,
                                                        string eventStreamId,
                                                        int eventNumber,
                                                        string eventData,
                                                        PrepareFlags flags,
                                                        bool retryOnFail = false)
            {
                var prepare = LogRecord.Prepare(WriterCheckpoint.ReadNonFlushed(),
                                                correlationId,
                                                Guid.NewGuid(),
                                                transactionPos,
                                                transactionOffset,
                                                eventStreamId,
                                                ExpectedVersion.Any,
                                                flags,
                                                "some-type",
                                                Helper.UTF8NoBom.GetBytes(eventData),
                                                null);

                if (retryOnFail)
                {
                    long firstPos = prepare.LogPosition;
                    long newPos;
                    if (!Writer.Write(prepare, out newPos))
                    {
                        var tPos = prepare.TransactionPosition == prepare.LogPosition ? newPos : prepare.TransactionPosition;
                        prepare = new PrepareLogRecord(newPos,
                                                       prepare.CorrelationId,
                                                       prepare.EventId,
                                                       tPos,
                                                       prepare.TransactionOffset,
                                                       prepare.EventStreamId,
                                                       prepare.ExpectedVersion,
                                                       prepare.TimeStamp,
                                                       prepare.Flags,
                                                       prepare.EventType,
                                                       prepare.Data,
                                                       prepare.Metadata);
                        if (!Writer.Write(prepare, out newPos))
                            Assert.True(false, string.Format("Second write try failed when first writing prepare at {0}, then at {1}.", firstPos, prepare.LogPosition));
                    }
                    return new EventRecord(eventNumber, prepare);
                }

                long pos;
                Assert.True(Writer.Write(prepare, out pos));
                return new EventRecord(eventNumber, prepare);
            }

            public PrepareLogRecord WriteTransactionEnd(Guid correlationId, long transactionId, string eventStreamId)
            {
                var prepare = LogRecord.TransactionEnd(WriterCheckpoint.ReadNonFlushed(),
                                                       correlationId,
                                                       Guid.NewGuid(),
                                                       transactionId,
                                                       eventStreamId);
                long pos;
                Assert.True(Writer.Write(prepare, out pos));
                return prepare;
            }

            public PrepareLogRecord WritePrepare(string streamId,
                                                    int expectedVersion,
                                                    Guid eventId = default(Guid),
                                                    string eventType = null,
                                                    string data = null)
            {
                long pos;
                var prepare = LogRecord.SingleWrite(WriterCheckpoint.ReadNonFlushed(),
                                                    Guid.NewGuid(),
                                                    eventId == default(Guid) ? Guid.NewGuid() : eventId,
                                                    streamId,
                                                    expectedVersion,
                                                    eventType.IsEmptyString() ? "some-type" : eventType,
                                                    data.IsEmptyString() ? LogRecord.NoData : Helper.UTF8NoBom.GetBytes(data),
                                                    LogRecord.NoData,
                                                    DateTime.UtcNow);
                Assert.True(Writer.Write(prepare, out pos));

                return prepare;
            }

            public CommitLogRecord WriteCommit(long preparePos, string eventStreamId, int eventNumber)
            {
                var commit = LogRecord.Commit(WriterCheckpoint.ReadNonFlushed(), Guid.NewGuid(), preparePos, eventNumber);
                long pos;
                Assert.True(Writer.Write(commit, out pos));
                return commit;
            }

            public long WriteCommit(Guid correlationId, long transactionId, string eventStreamId, int eventNumber)
            {
                var commit = LogRecord.Commit(WriterCheckpoint.ReadNonFlushed(), correlationId, transactionId, eventNumber);
                long pos;
                Assert.True(Writer.Write(commit, out pos));
                return commit.LogPosition;
            }

            public EventRecord WriteDelete(string eventStreamId)
            {
                var prepare = LogRecord.DeleteTombstone(WriterCheckpoint.ReadNonFlushed(),
                                                        Guid.NewGuid(), Guid.NewGuid(), eventStreamId, ExpectedVersion.Any);
                long pos;
                Assert.True(Writer.Write(prepare, out pos));
                var commit = LogRecord.Commit(WriterCheckpoint.ReadNonFlushed(),
                                              prepare.CorrelationId,
                                              prepare.LogPosition,
                                              EventNumber.DeletedStream);
                Assert.True(Writer.Write(commit, out pos));

                return new EventRecord(EventNumber.DeletedStream, prepare);
            }

            public PrepareLogRecord WriteDeletePrepare(string eventStreamId)
            {
                var prepare = LogRecord.DeleteTombstone(WriterCheckpoint.ReadNonFlushed(),
                                                        Guid.NewGuid(), Guid.NewGuid(), eventStreamId, ExpectedVersion.Any);
                long pos;
                Assert.True(Writer.Write(prepare, out pos));

                return prepare;
            }

            public CommitLogRecord WriteDeleteCommit(PrepareLogRecord prepare)
            {
                long pos;
                var commit = LogRecord.Commit(WriterCheckpoint.ReadNonFlushed(),
                                              prepare.CorrelationId,
                                              prepare.LogPosition,
                                              EventNumber.DeletedStream);
                Assert.True(Writer.Write(commit, out pos));

                return commit;
            }

            public void AddStashedValueAssignment<T>(T ignored, Action<T> assignStashedValues) where T : ReadIndexTestScenario
            {
                _assignStashedValues += instance => assignStashedValues((T)instance);
            }

            public void AssignStashedValues(ReadIndexTestScenario scenario)
            {
                _assignStashedValues(scenario);
            }
        }

        protected ReadIndexTestScenario(int maxEntriesInMemTable = 20, int metastreamMaxCount = 1)
        {
            Ensure.Positive(maxEntriesInMemTable, "maxEntriesInMemTable");
            MaxEntriesInMemTable = maxEntriesInMemTable;
            MetastreamMaxCount = metastreamMaxCount;
        }

        public void SetFixture(FixtureData fixture)
        {
            _fixture = fixture;
            fixture.EnsureInitialized(MaxEntriesInMemTable, MetastreamMaxCount, WriteTestScenario, AdditionalFixtureSetup, AdditionalFixtureTeardown);
            fixture.AssignStashedValues(this);
        }

        protected virtual void AdditionalFixtureSetup() { }
        protected virtual void AdditionalFixtureTeardown() { }
        protected abstract void WriteTestScenario();



        protected TFPos GetBackwardReadPos()
        {
            var pos = new TFPos(WriterCheckpoint.ReadNonFlushed(), WriterCheckpoint.ReadNonFlushed());
            return pos;
        }
    }
}