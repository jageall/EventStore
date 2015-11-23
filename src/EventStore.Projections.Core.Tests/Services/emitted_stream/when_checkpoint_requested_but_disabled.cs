using System;
using EventStore.Projections.Core.Services.Processing;
using EventStore.Projections.Core.Tests.Services.core_projection;
using Xunit;

namespace EventStore.Projections.Core.Tests.Services.emitted_stream
{
    
    public class when_checkpoint_requested_but_disabled : TestFixtureWithReadWriteDispatchers
    {
        private EmittedStream _stream;
        private TestCheckpointManagerMessageHandler _readyHandler;

        public when_checkpoint_requested_but_disabled()
        {
            _readyHandler = new TestCheckpointManagerMessageHandler();
            _stream = new EmittedStream(
                "test", new EmittedStream.WriterConfiguration(new EmittedStream.WriterConfiguration.StreamMetadata(), null, 50), new ProjectionVersion(1, 0, 0),
                new TransactionFilePositionTagger(0), CheckpointTag.FromPosition(0, 0, -1), _ioDispatcher, _readyHandler,
                noCheckpoints: true);
            _stream.Start();
        }

        [Fact]
        public void invalid_operation_exceptioon_is_thrown()
        {
            Assert.Throws<InvalidOperationException>(()=>_stream.Checkpoint());
        }
    }
}
