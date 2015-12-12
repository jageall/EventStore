using System;
using EventStore.Projections.Core.Services.Processing;
using EventStore.Projections.Core.Tests.Services.core_projection;
using Xunit;

namespace EventStore.Projections.Core.Tests.Services.emitted_stream
{
    
    public class a_checkpoint_requested_on_a_non_started_stream : TestFixtureWithReadWriteDispatchers
    {
        private EmittedStream _stream;

        private TestCheckpointManagerMessageHandler _readyHandler;
        
        public a_checkpoint_requested_on_a_non_started_stream()
        {
            _readyHandler = new TestCheckpointManagerMessageHandler();
            _stream = new EmittedStream(
                "test", new EmittedStream.WriterConfiguration(new EmittedStream.WriterConfiguration.StreamMetadata(), null, 50), new ProjectionVersion(1, 0, 0),
                new TransactionFilePositionTagger(0), CheckpointTag.FromPosition(0, 0, -1), _ioDispatcher, _readyHandler);
        }
        
        [Fact]
        public void throws_invalid_operation_exception()
        {
            Assert.Throws<InvalidOperationException>(() => _stream.Checkpoint());
        }
    }
}
