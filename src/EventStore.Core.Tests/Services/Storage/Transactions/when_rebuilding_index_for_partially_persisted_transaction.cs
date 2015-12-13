using System;
using System.IO;
using System.Text;
using System.Threading;
using EventStore.Common.Utils;
using EventStore.Core.Data;
using EventStore.Core.DataStructures;
using EventStore.Core.Index;
using EventStore.Core.Services.Storage.ReaderIndex;
using EventStore.Core.Tests.Fakes;
using EventStore.Core.TransactionLog;
using EventStore.Core.TransactionLog.Chunks;
using EventStore.Core.TransactionLog.LogRecords;
using Xunit;

namespace EventStore.Core.Tests.Services.Storage.Transactions
{
    public class when_rebuilding_index_for_partially_persisted_transaction : ReadIndexTestScenario
    {
        public when_rebuilding_index_for_partially_persisted_transaction(FixtureData fixture): base(fixture,maxEntriesInMemTable: 10, metastreamMaxCount:1)
        {
        }

        protected override void AdditionalFixtureSetup()
        {
            Fixture.ReadIndex.Close();
            Fixture.ReadIndex.Dispose();
            Fixture.TableIndex.Close(removeFiles: false);

            var readers = new ObjectPool<ITransactionFileReader>("Readers", 2, 2, () => new TFChunkReader(Fixture.Db, Fixture.WriterCheckpoint));
            Fixture.TableIndex = new TableIndex(Fixture.GetFilePathFor("index"),
                                        () => new HashListMemTable(maxSize: MaxEntriesInMemTable*2),
                                        () => new TFReaderLease(readers),
                                        maxSizeForMemory: MaxEntriesInMemTable);
            Fixture.ReadIndex = new ReadIndex(new NoopPublisher(),
                                      readers,
                                      Fixture.TableIndex,
                                      new ByLengthHasher(),
                                      0,
                                      additionalCommitChecks: true, 
                                      metastreamMaxCount: 1);
            Fixture.ReadIndex.Init(Fixture.ChaserCheckpoint.Read());
        }

        protected override void WriteTestScenario()
        {
            var begin = Fixture.WriteTransactionBegin("ES", ExpectedVersion.Any);
            for (int i = 0; i < 15; ++i)
            {
                Fixture.WriteTransactionEvent(Guid.NewGuid(), begin.LogPosition, i, "ES", i, "data" + i, PrepareFlags.Data);
            }
            Fixture.WriteTransactionEnd(Guid.NewGuid(), begin.LogPosition, "ES");
            Fixture.WriteCommit(Guid.NewGuid(), begin.LogPosition, "ES", 0);
        }

        [Fact]
        public void sequence_numbers_are_not_broken()
        {
            for (int i = 0; i < 15; ++i)
            {
                var result = ReadIndex.ReadEvent("ES", i);
                Assert.Equal(ReadEventResult.Success, result.Result);
                Assert.Equal(Helper.UTF8NoBom.GetBytes("data" + i), result.Record.Data);
            }
        }
    }
}