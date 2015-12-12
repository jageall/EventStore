using System;
using EventStore.ClientAPI;
using EventStore.ClientAPI.Exceptions;
using EventStore.Common.Utils;
using EventStore.Core.Data;
using EventStore.Core.Tests.ClientAPI.Helpers;
using EventStore.Core.Tests.Helpers;
using Xunit;
using ExpectedVersion = EventStore.ClientAPI.ExpectedVersion;
using StreamMetadata = EventStore.ClientAPI.StreamMetadata;
using Newtonsoft.Json.Linq;

namespace EventStore.Core.Tests.ClientAPI
{
    public class when_working_with_metadata : IClassFixture<ConnectedMiniNodeFixture>
    {
        private MiniNode _node;
        private IEventStoreConnection _connection;

        public void SetFixture(ConnectedMiniNodeFixture data)
        {
            _node = data.Node;
            _connection = data.Connection;
        }

        [Fact]
        [Trait("Category", "LongRunning")]
        public void when_getting_metadata_for_an_existing_stream_and_no_metadata_exists()
        {
            const string stream = "when_getting_metadata_for_an_existing_stream_and_no_metadata_exists";

            _connection.AppendToStreamAsync(stream, ExpectedVersion.EmptyStream, TestEvent.NewTestEvent()).Wait();

            var meta = _connection.GetStreamMetadataAsRawBytesAsync(stream).Result;
            Assert.Equal(stream, meta.Stream);
            Assert.Equal(false, meta.IsStreamDeleted);
            Assert.Equal(-1, meta.MetastreamVersion);
            Assert.Equal(Helper.UTF8NoBom.GetBytes(""), meta.StreamMetadata);
        }
    }
}
