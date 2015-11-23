using System;
using System.Threading;
using EventStore.Core.Data;
using EventStore.Core.Messages;
using EventStore.Core.Messaging;
using EventStore.Core.Tests.Helpers;
using Xunit;

namespace EventStore.Core.Tests.TransactionLog.Truncation
{
    public class when_truncating_database : IUseFixture<SpecificationWithDirectoryPerTestFixture>, IDisposable
    {
        private MiniNode _miniNode;

        public void SetFixture(SpecificationWithDirectoryPerTestFixture fixture)
        {
            PathName = fixture.PathName;
        }

        public string PathName { get; set; }

        [Fact][Trait("Category", "LongRunning")]
        public void everything_should_go_fine()
        {
            _miniNode = new MiniNode(PathName, inMemDb: false);
            _miniNode.Start();

            var tcpPort = _miniNode.TcpEndPoint.Port;
            var tcpSecPort = _miniNode.TcpSecEndPoint.Port;
            var httpPort = _miniNode.ExtHttpEndPoint.Port;
            const int cnt = 50;
            var countdown = new CountdownEvent(cnt);

            // --- first part of events
            WriteEvents(cnt, _miniNode, countdown);
            Assert.True(countdown.Wait(TimeSpan.FromSeconds(10)), "Took too long writing first part of events.");
            countdown.Reset();

            // -- set up truncation
            var truncatePosition = _miniNode.Db.Config.WriterCheckpoint.ReadNonFlushed();
            _miniNode.Db.Config.TruncateCheckpoint.Write(truncatePosition);
            _miniNode.Db.Config.TruncateCheckpoint.Flush();

            // --- second part of events
            WriteEvents(cnt, _miniNode, countdown);
            Assert.True(countdown.Wait(TimeSpan.FromSeconds(10)), "Took too long writing second part of events.");
            countdown.Reset();

            _miniNode.Shutdown(keepDb: true, keepPorts: true);

            // --- first restart and truncation
            _miniNode = new MiniNode(PathName, tcpPort, tcpSecPort, httpPort, inMemDb: false);
            
            _miniNode.Start();
            Assert.Equal(-1, _miniNode.Db.Config.TruncateCheckpoint.Read());
            Assert.True(_miniNode.Db.Config.WriterCheckpoint.Read() >= truncatePosition);

            // -- third part of events
            WriteEvents(cnt, _miniNode, countdown);
            Assert.True(countdown.Wait(TimeSpan.FromSeconds(10)), "Took too long writing third part of events.");
            countdown.Reset();

            _miniNode.Shutdown(keepDb: true, keepPorts: true);

            // -- second restart
            _miniNode = new MiniNode(PathName, tcpPort, tcpSecPort, httpPort, inMemDb: false);
            Assert.Equal(-1, _miniNode.Db.Config.TruncateCheckpoint.Read());
            _miniNode.Start();

            // -- if we get here -- then everything is ok
            _miniNode.Shutdown();
        }

        [Fact][Trait("Category", "LongRunning")][Trait("Category", "Network")]
        public void with_truncate_position_in_completed_chunk_everything_should_go_fine()
        {
            const int chunkSize = 1024*1024;
            const int cachedSize = chunkSize*3;

            _miniNode = new MiniNode(PathName, chunkSize: chunkSize, cachedChunkSize: cachedSize, inMemDb: false);
            _miniNode.Start();

            var tcpPort = _miniNode.TcpEndPoint.Port;
            var tcpSecPort = _miniNode.TcpSecEndPoint.Port;
            var httpPort = _miniNode.ExtHttpEndPoint.Port;
            const int cnt = 1;
            var countdown = new CountdownEvent(cnt);

            // --- first part of events
            WriteEvents(cnt, _miniNode, countdown, MiniNode.ChunkSize / 5 * 3);
            Assert.True(countdown.Wait(TimeSpan.FromSeconds(10)), "Took too long writing first part of events.");
            countdown.Reset();

            // -- set up truncation
            var truncatePosition = _miniNode.Db.Config.WriterCheckpoint.ReadNonFlushed();
            _miniNode.Db.Config.TruncateCheckpoint.Write(truncatePosition);
            _miniNode.Db.Config.TruncateCheckpoint.Flush();

            // --- second part of events
            WriteEvents(cnt, _miniNode, countdown, MiniNode.ChunkSize / 2);
            Assert.True(countdown.Wait(TimeSpan.FromSeconds(10)), "Took too long writing second part of events.");
            countdown.Reset();

            _miniNode.Shutdown(keepDb: true, keepPorts: true);

            // --- first restart and truncation
            _miniNode = new MiniNode(PathName, tcpPort, tcpSecPort, httpPort, chunkSize: chunkSize, cachedChunkSize: cachedSize, inMemDb: false);

            _miniNode.Start();
            Assert.Equal(-1, _miniNode.Db.Config.TruncateCheckpoint.Read());
            Assert.True(_miniNode.Db.Config.WriterCheckpoint.Read() >= truncatePosition);

            // -- third part of events
            WriteEvents(cnt, _miniNode, countdown, MiniNode.ChunkSize / 5);
            Assert.True(countdown.Wait(TimeSpan.FromSeconds(10)), "Took too long writing third part of events.");
            countdown.Reset();

            // -- if we get here -- then everything is ok
            _miniNode.Shutdown();
        }

        private static void WriteEvents(int cnt, MiniNode miniNode, CountdownEvent countdown, int dataSize = 4000)
        {
            for (int i = 0; i < cnt; ++i)
            {
                miniNode.Node.MainQueue.Publish(
                    new ClientMessage.WriteEvents(Guid.NewGuid(), Guid.NewGuid(),
                                                  new CallbackEnvelope(m =>
                                                  {
                                                      Assert.IsType<ClientMessage.WriteEventsCompleted>(m);
                                                      var msg = (ClientMessage.WriteEventsCompleted) m;
                                                      Assert.Equal(OperationResult.Success, msg.Result);
                                                      countdown.Signal();
                                                  }),
                                                  true,
                                                  "test-stream",
                                                  ExpectedVersion.Any,
                                                  new[]
                                                  {
                                                      new Event(Guid.NewGuid(), "test-event-type", false, new byte[dataSize], null)
                                                  },
                                                  null));
            }
        }

        public void Dispose()
        {
            if(_miniNode != null)
                _miniNode.Shutdown();
        }
    }
}
