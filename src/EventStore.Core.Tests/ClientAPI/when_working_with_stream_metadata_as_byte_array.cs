using System;
using EventStore.ClientAPI;
using EventStore.ClientAPI.Exceptions;
using EventStore.Core.Data;
using EventStore.Core.Tests.ClientAPI.Helpers;
using EventStore.Core.Tests.Helpers;
using Xunit;
using ExpectedVersion = EventStore.ClientAPI.ExpectedVersion;

namespace EventStore.Core.Tests.ClientAPI
{
    public class when_working_with_stream_metadata_as_byte_array : IClassFixture<ConnectedMiniNodeFixture>
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
        public void setting_empty_metadata_works()
        {
            const string stream = "setting_empty_metadata_works";

            _connection.SetStreamMetadataAsync(stream, ExpectedVersion.EmptyStream, (byte[])null).Wait();
            
            var meta = _connection.GetStreamMetadataAsRawBytesAsync(stream).Result;
            Assert.Equal(stream, meta.Stream);
            Assert.Equal(false, meta.IsStreamDeleted);
            Assert.Equal(0, meta.MetastreamVersion);
            Assert.Equal(new byte[0], meta.StreamMetadata);
        }

        [Fact]
        [Trait("Category", "LongRunning")]
        public void setting_metadata_few_times_returns_last_metadata()
        {
            const string stream = "setting_metadata_few_times_returns_last_metadata";

            var metadataBytes = Guid.NewGuid().ToByteArray();
            _connection.SetStreamMetadataAsync(stream, ExpectedVersion.EmptyStream, metadataBytes).Wait();
            var meta = _connection.GetStreamMetadataAsRawBytesAsync(stream).Result;
            Assert.Equal(stream, meta.Stream);
            Assert.Equal(false, meta.IsStreamDeleted);
            Assert.Equal(0, meta.MetastreamVersion);
            Assert.Equal(metadataBytes, meta.StreamMetadata);

            metadataBytes = Guid.NewGuid().ToByteArray();
            _connection.SetStreamMetadataAsync(stream, 0, metadataBytes).Wait();
            meta = _connection.GetStreamMetadataAsRawBytesAsync(stream).Result;
            Assert.Equal(stream, meta.Stream);
            Assert.Equal(false, meta.IsStreamDeleted);
            Assert.Equal(1, meta.MetastreamVersion);
            Assert.Equal(metadataBytes, meta.StreamMetadata);
        }

        [Fact]
        [Trait("Category", "LongRunning")]
        public void trying_to_set_metadata_with_wrong_expected_version_fails()
        {
            const string stream = "trying_to_set_metadata_with_wrong_expected_version_fails";
            var thrown =
                Assert.Throws<AggregateException>(
                    () => _connection.SetStreamMetadataAsync(stream, 5, new byte[100]).Wait());
            Assert.IsType<WrongExpectedVersionException>(thrown.InnerException);
        }

        [Fact]
        [Trait("Category", "LongRunning")]
        public void setting_metadata_with_expected_version_any_works()
        {
            const string stream = "setting_metadata_with_expected_version_any_works";

            var metadataBytes = Guid.NewGuid().ToByteArray();
            _connection.SetStreamMetadataAsync(stream, ExpectedVersion.Any, metadataBytes).Wait();
            var meta = _connection.GetStreamMetadataAsRawBytesAsync(stream).Result;
            Assert.Equal(stream, meta.Stream);
            Assert.Equal(false, meta.IsStreamDeleted);
            Assert.Equal(0, meta.MetastreamVersion);
            Assert.Equal(metadataBytes, meta.StreamMetadata);

            metadataBytes = Guid.NewGuid().ToByteArray();
            _connection.SetStreamMetadataAsync(stream, ExpectedVersion.Any, metadataBytes).Wait();
            meta = _connection.GetStreamMetadataAsRawBytesAsync(stream).Result;
            Assert.Equal(stream, meta.Stream);
            Assert.Equal(false, meta.IsStreamDeleted);
            Assert.Equal(1, meta.MetastreamVersion);
            Assert.Equal(metadataBytes, meta.StreamMetadata);
        }

        [Fact]
        [Trait("Category", "LongRunning")]
        public void setting_metadata_for_not_existing_stream_works()
        {
            const string stream = "setting_metadata_for_not_existing_stream_works";
            var metadataBytes = Guid.NewGuid().ToByteArray();
            _connection.SetStreamMetadataAsync(stream, ExpectedVersion.EmptyStream, metadataBytes).Wait();

            var meta = _connection.GetStreamMetadataAsRawBytesAsync(stream).Result;
            Assert.Equal(stream, meta.Stream);
            Assert.Equal(false, meta.IsStreamDeleted);
            Assert.Equal(0, meta.MetastreamVersion);
            Assert.Equal(metadataBytes, meta.StreamMetadata);
        }

        [Fact]
        [Trait("Category", "LongRunning")]
        public void setting_metadata_for_existing_stream_works()
        {
            const string stream = "setting_metadata_for_existing_stream_works";

            _connection.AppendToStreamAsync(stream, ExpectedVersion.NoStream, TestEvent.NewTestEvent(), TestEvent.NewTestEvent()).Wait();

            var metadataBytes = Guid.NewGuid().ToByteArray();
            _connection.SetStreamMetadataAsync(stream, ExpectedVersion.EmptyStream, metadataBytes).Wait();

            var meta = _connection.GetStreamMetadataAsRawBytesAsync(stream).Result;
            Assert.Equal(stream, meta.Stream);
            Assert.Equal(false, meta.IsStreamDeleted);
            Assert.Equal(0, meta.MetastreamVersion);
            Assert.Equal(metadataBytes, meta.StreamMetadata);
        }

        [Fact]
        [Trait("Category", "LongRunning")]
        public void setting_metadata_for_deleted_stream_throws_stream_deleted_exception()
        {
            const string stream = "setting_metadata_for_deleted_stream_throws_stream_deleted_exception";

            _connection.DeleteStreamAsync(stream, ExpectedVersion.NoStream, hardDelete: true).Wait();

            var metadataBytes = Guid.NewGuid().ToByteArray();
            var thrown =
                Assert.Throws<AggregateException>(
                    () => _connection.SetStreamMetadataAsync(stream, ExpectedVersion.EmptyStream, metadataBytes).Wait());
            Assert.IsType<StreamDeletedException>(thrown.InnerException);
        }

        [Fact]
        [Trait("Category", "LongRunning")]
        public void getting_metadata_for_nonexisting_stream_returns_empty_byte_array()
        {
            const string stream = "getting_metadata_for_nonexisting_stream_returns_empty_byte_array";

            var meta = _connection.GetStreamMetadataAsRawBytesAsync(stream).Result;
            Assert.Equal(stream, meta.Stream);
            Assert.Equal(false, meta.IsStreamDeleted);
            Assert.Equal(-1, meta.MetastreamVersion);
            Assert.Equal(new byte[0], meta.StreamMetadata);
        }

        [Fact]
        [Trait("Category", "LongRunning")]
        public void getting_metadata_for_deleted_stream_returns_empty_byte_array_and_signals_stream_deletion()
        {
            const string stream = "getting_metadata_for_deleted_stream_returns_empty_byte_array_and_signals_stream_deletion";

            var metadataBytes = Guid.NewGuid().ToByteArray();
            _connection.SetStreamMetadataAsync(stream, ExpectedVersion.EmptyStream, metadataBytes).Wait();

            _connection.DeleteStreamAsync(stream, ExpectedVersion.NoStream, hardDelete: true).Wait();

            var meta = _connection.GetStreamMetadataAsRawBytesAsync(stream).Result;
            Assert.Equal(stream, meta.Stream);
            Assert.Equal(true, meta.IsStreamDeleted);
            Assert.Equal(EventNumber.DeletedStream, meta.MetastreamVersion);
            Assert.Equal(new byte[0], meta.StreamMetadata);
        }
    }
}
