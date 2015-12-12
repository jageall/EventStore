using System;
using System.Linq;
using System.Threading;
using EventStore.ClientAPI;
using EventStore.ClientAPI.Internal;
using EventStore.ClientAPI.Exceptions;
using EventStore.Core.Tests.ClientAPI.Helpers;
using EventStore.Core.Tests.Helpers;
using Xunit;

namespace EventStore.Core.Tests.ClientAPI
{
    public class soft_delete : IClassFixture<MiniNodeFixture>, IDisposable
    {
        private MiniNode _node;
        private IEventStoreConnection _conn;

        public void SetFixture(MiniNodeFixture data)
        {
            _node = data.Node;
            _conn = BuildConnection(_node);
            _conn.ConnectAsync().Wait();
        }

        public void Dispose()
        {
            _conn.Close();
        }

        protected virtual IEventStoreConnection BuildConnection(MiniNode node)
        {
            return EventStoreConnection.Create(node.TcpEndPoint.ToESTcpUri());
        }

        [Fact][Trait("Category", "LongRunning")][Trait("Category", "Network")]
        public void soft_deleted_stream_returns_no_stream_and_no_events_on_read()
        {
            const string stream = "soft_deleted_stream_returns_no_stream_and_no_events_on_read";

            Assert.Equal(1, _conn.AppendToStreamAsync(stream, ExpectedVersion.NoStream, TestEvent.NewTestEvent(), TestEvent.NewTestEvent()).Result.NextExpectedVersion);
            _conn.DeleteStreamAsync(stream, 1).Wait();

            var res = _conn.ReadStreamEventsForwardAsync(stream, 0, 100, false).Result;
            Assert.Equal(SliceReadStatus.StreamNotFound, res.Status);
            Assert.Equal(0, res.Events.Length);
            Assert.Equal(1, res.LastEventNumber);
        }

        [Fact][Trait("Category", "LongRunning")][Trait("Category", "Network")]
        public void soft_deleted_stream_allows_recreation_when_expver_any()
        {
            const string stream = "soft_deleted_stream_allows_recreation_when_expver_any";

            Assert.Equal(1, _conn.AppendToStreamAsync(stream, ExpectedVersion.NoStream, TestEvent.NewTestEvent(), TestEvent.NewTestEvent()).Result.NextExpectedVersion);
            _conn.DeleteStreamAsync(stream, 1).Wait();

            var events = new[] {TestEvent.NewTestEvent(), TestEvent.NewTestEvent(), TestEvent.NewTestEvent()};
            Assert.Equal(4, _conn.AppendToStreamAsync(stream, ExpectedVersion.Any, events).Result.NextExpectedVersion);

            var res = _conn.ReadStreamEventsForwardAsync(stream, 0, 100, false).Result;
            Assert.Equal(SliceReadStatus.Success, res.Status);
            Assert.Equal(4, res.LastEventNumber);
            Assert.Equal(3, res.Events.Length);
            Assert.Equal(events.Select(x => x.EventId), res.Events.Select(x => x.OriginalEvent.EventId));
            Assert.Equal(new[]{2, 3, 4}, res.Events.Select(x => x.OriginalEvent.EventNumber));

            var meta = _conn.GetStreamMetadataAsync(stream).Result;
            Assert.Equal(2, meta.StreamMetadata.TruncateBefore);
            Assert.Equal(1, meta.MetastreamVersion);
        }

        [Fact][Trait("Category", "LongRunning")][Trait("Category", "Network")]
        public void soft_deleted_stream_allows_recreation_when_expver_no_stream()
        {
            const string stream = "soft_deleted_stream_allows_recreation_when_expver_no_stream";

            Assert.Equal(1, _conn.AppendToStreamAsync(stream, ExpectedVersion.NoStream, TestEvent.NewTestEvent(), TestEvent.NewTestEvent()).Result.NextExpectedVersion);
            _conn.DeleteStreamAsync(stream, 1).Wait();

            var events = new[] { TestEvent.NewTestEvent(), TestEvent.NewTestEvent(), TestEvent.NewTestEvent() };
            Assert.Equal(4, _conn.AppendToStreamAsync(stream, ExpectedVersion.NoStream, events).Result.NextExpectedVersion);

            var res = _conn.ReadStreamEventsForwardAsync(stream, 0, 100, false).Result;
            Assert.Equal(SliceReadStatus.Success, res.Status);
            Assert.Equal(4, res.LastEventNumber);
            Assert.Equal(3, res.Events.Length);
            Assert.Equal(events.Select(x => x.EventId), res.Events.Select(x => x.OriginalEvent.EventId));
            Assert.Equal(new[] { 2, 3, 4 }, res.Events.Select(x => x.OriginalEvent.EventNumber));

            var meta = _conn.GetStreamMetadataAsync(stream).Result;
            Assert.Equal(2, meta.StreamMetadata.TruncateBefore);
            Assert.Equal(1, meta.MetastreamVersion);
        }

        [Fact][Trait("Category", "LongRunning")][Trait("Category", "Network")]
        public void soft_deleted_stream_allows_recreation_when_expver_is_exact()
        {
            const string stream = "soft_deleted_stream_allows_recreation_when_expver_is_exact";

            Assert.Equal(1, _conn.AppendToStreamAsync(stream, ExpectedVersion.NoStream, TestEvent.NewTestEvent(), TestEvent.NewTestEvent()).Result.NextExpectedVersion);
            _conn.DeleteStreamAsync(stream, 1).Wait();

            var events = new[] { TestEvent.NewTestEvent(), TestEvent.NewTestEvent(), TestEvent.NewTestEvent() };
            Assert.Equal(4, _conn.AppendToStreamAsync(stream, 1, events).Result.NextExpectedVersion);

            var res = _conn.ReadStreamEventsForwardAsync(stream, 0, 100, false).Result;
            Assert.Equal(SliceReadStatus.Success, res.Status);
            Assert.Equal(4, res.LastEventNumber);
            Assert.Equal(3, res.Events.Length);
            Assert.Equal(events.Select(x => x.EventId), res.Events.Select(x => x.OriginalEvent.EventId));
            Assert.Equal(new[] { 2, 3, 4 }, res.Events.Select(x => x.OriginalEvent.EventNumber));

            var meta = _conn.GetStreamMetadataAsync(stream).Result;
            Assert.Equal(2, meta.StreamMetadata.TruncateBefore);
            Assert.Equal(1, meta.MetastreamVersion);
        }

        [Fact][Trait("Category", "LongRunning")][Trait("Category", "Network")]
        public void soft_deleted_stream_when_recreated_preserves_metadata_except_truncatebefore()
        {
            const string stream = "soft_deleted_stream_when_recreated_preserves_metadata_except_truncatebefore";

            Assert.Equal(1, _conn.AppendToStreamAsync(stream, ExpectedVersion.NoStream, TestEvent.NewTestEvent(), TestEvent.NewTestEvent()).Result.NextExpectedVersion);

            Assert.Equal(0, _conn.SetStreamMetadataAsync(stream, ExpectedVersion.NoStream,
                                    StreamMetadata.Build().SetTruncateBefore(int.MaxValue)
                                                          .SetMaxCount(100)
                                                          .SetDeleteRole("some-role")
                                                          .SetCustomProperty("key1", true)
                                                          .SetCustomProperty("key2", 17)
                                                          .SetCustomProperty("key3", "some value")).Result.NextExpectedVersion);

            var events = new[] { TestEvent.NewTestEvent(), TestEvent.NewTestEvent(), TestEvent.NewTestEvent() };
            Assert.Equal(4, _conn.AppendToStreamAsync(stream, 1, events).Result.NextExpectedVersion);

            var res = _conn.ReadStreamEventsForwardAsync(stream, 0, 100, false).Result;
            Assert.Equal(SliceReadStatus.Success, res.Status);
            Assert.Equal(4, res.LastEventNumber);
            Assert.Equal(3, res.Events.Length);
            Assert.Equal(events.Select(x => x.EventId), res.Events.Select(x => x.OriginalEvent.EventId));
            Assert.Equal(new[] { 2, 3, 4 }, res.Events.Select(x => x.OriginalEvent.EventNumber));

            var meta = _conn.GetStreamMetadataAsync(stream).Result;
            Assert.Equal(1, meta.MetastreamVersion);
            Assert.Equal(2, meta.StreamMetadata.TruncateBefore);
            Assert.Equal(100, meta.StreamMetadata.MaxCount);
            Assert.Equal("some-role", meta.StreamMetadata.Acl.DeleteRole);
            Assert.Equal(true, meta.StreamMetadata.GetValue<bool>("key1"));
            Assert.Equal(17, meta.StreamMetadata.GetValue<int>("key2"));
            Assert.Equal("some value", meta.StreamMetadata.GetValue<string>("key3"));
        }

        [Fact][Trait("Category", "LongRunning")][Trait("Category", "Network")]
        public void soft_deleted_stream_can_be_hard_deleted()
        {
            const string stream = "soft_deleted_stream_can_be_deleted";

            Assert.Equal(1, _conn.AppendToStreamAsync(stream, ExpectedVersion.NoStream, TestEvent.NewTestEvent(), TestEvent.NewTestEvent()).Result.NextExpectedVersion);
            _conn.DeleteStreamAsync(stream, 1).Wait();
            _conn.DeleteStreamAsync(stream, ExpectedVersion.Any, hardDelete: true).Wait();

            var res = _conn.ReadStreamEventsForwardAsync(stream, 0, 100, false).Result;
            Assert.Equal(SliceReadStatus.StreamDeleted, res.Status);
            var meta = _conn.GetStreamMetadataAsync(stream).Result;
            Assert.Equal(true, meta.IsStreamDeleted);

            var thrown = Assert.Throws<AggregateException>(() => _conn.AppendToStreamAsync(stream, ExpectedVersion.Any, TestEvent.NewTestEvent()).Wait());
            Assert.IsType<StreamDeletedException>(thrown.InnerException);
        }

        [Fact][Trait("Category", "LongRunning")][Trait("Category", "Network")]
        public void soft_deleted_stream_allows_recreation_only_for_first_write()
        {
            const string stream = "soft_deleted_stream_allows_recreation_only_for_first_write";

            Assert.Equal(1, _conn.AppendToStreamAsync(stream, ExpectedVersion.NoStream, TestEvent.NewTestEvent(), TestEvent.NewTestEvent()).Result.NextExpectedVersion);
            _conn.DeleteStreamAsync(stream, 1).Wait();

            var events = new[] { TestEvent.NewTestEvent(), TestEvent.NewTestEvent(), TestEvent.NewTestEvent() };
            Assert.Equal(4, _conn.AppendToStreamAsync(stream, ExpectedVersion.NoStream, events).Result.NextExpectedVersion);
            
            var thrown = Assert.Throws<AggregateException>(() => _conn.AppendToStreamAsync(stream, ExpectedVersion.NoStream, TestEvent.NewTestEvent()).Wait());
            Assert.IsType<WrongExpectedVersionException>(thrown.InnerException);
            var res = _conn.ReadStreamEventsForwardAsync(stream, 0, 100, false).Result;
            Assert.Equal(SliceReadStatus.Success, res.Status);
            Assert.Equal(4, res.LastEventNumber);
            Assert.Equal(3, res.Events.Length);
            Assert.Equal(events.Select(x => x.EventId), res.Events.Select(x => x.OriginalEvent.EventId));
            Assert.Equal(new[] { 2, 3, 4 }, res.Events.Select(x => x.OriginalEvent.EventNumber));

            var meta = _conn.GetStreamMetadataAsync(stream).Result;
            Assert.Equal(2, meta.StreamMetadata.TruncateBefore);
            Assert.Equal(1, meta.MetastreamVersion);
        }

        [Fact][Trait("Category", "LongRunning")][Trait("Category", "Network")]
        public void soft_deleted_stream_appends_both_writes_when_expver_any()
        {
            const string stream = "soft_deleted_stream_appends_both_concurrent_writes_when_expver_any";

            Assert.Equal(1, _conn.AppendToStreamAsync(stream, ExpectedVersion.NoStream, TestEvent.NewTestEvent(), TestEvent.NewTestEvent()).Result.NextExpectedVersion);
            _conn.DeleteStreamAsync(stream, 1).Wait();

            var events1 = new[] { TestEvent.NewTestEvent(), TestEvent.NewTestEvent(), TestEvent.NewTestEvent() };
            var events2 = new[] { TestEvent.NewTestEvent(), TestEvent.NewTestEvent() };
            Assert.Equal(4, _conn.AppendToStreamAsync(stream, ExpectedVersion.Any, events1).Result.NextExpectedVersion);
            Assert.Equal(6, _conn.AppendToStreamAsync(stream, ExpectedVersion.Any, events2).Result.NextExpectedVersion);

            var res = _conn.ReadStreamEventsForwardAsync(stream, 0, 100, false).Result;
            Assert.Equal(SliceReadStatus.Success, res.Status);
            Assert.Equal(6, res.LastEventNumber);
            Assert.Equal(5, res.Events.Length);
            Assert.Equal(events1.Concat(events2).Select(x => x.EventId), res.Events.Select(x => x.OriginalEvent.EventId));
            Assert.Equal(new[] { 2, 3, 4, 5, 6 }, res.Events.Select(x => x.OriginalEvent.EventNumber));

            var meta = _conn.GetStreamMetadataAsync(stream).Result;
            Assert.Equal(2, meta.StreamMetadata.TruncateBefore);
            Assert.Equal(1, meta.MetastreamVersion);
        }

        [Fact][Trait("Category", "LongRunning")][Trait("Category", "Network")]
        public void setting_json_metadata_on_empty_soft_deleted_stream_recreates_stream_not_overriding_metadata()
        {
            const string stream = "setting_json_metadata_on_empty_soft_deleted_stream_recreates_stream_not_overriding_metadata";

            _conn.DeleteStreamAsync(stream, ExpectedVersion.NoStream, hardDelete: false).Wait();

            Assert.Equal(1, _conn.SetStreamMetadataAsync(stream, 0,
                                    StreamMetadata.Build().SetMaxCount(100)
                                                          .SetDeleteRole("some-role")
                                                          .SetCustomProperty("key1", true)
                                                          .SetCustomProperty("key2", 17)
                                                          .SetCustomProperty("key3", "some value")).Result.NextExpectedVersion);

            var res = _conn.ReadStreamEventsForwardAsync(stream, 0, 100, false).Result;
            Assert.Equal(SliceReadStatus.StreamNotFound, res.Status);
            Assert.Equal(-1, res.LastEventNumber);
            Assert.Equal(0, res.Events.Length);

            var meta = _conn.GetStreamMetadataAsync(stream).Result;
            Assert.Equal(2, meta.MetastreamVersion);
            Assert.Equal(0, meta.StreamMetadata.TruncateBefore);
            Assert.Equal(100, meta.StreamMetadata.MaxCount);
            Assert.Equal("some-role", meta.StreamMetadata.Acl.DeleteRole);
            Assert.Equal(true, meta.StreamMetadata.GetValue<bool>("key1"));
            Assert.Equal(17, meta.StreamMetadata.GetValue<int>("key2"));
            Assert.Equal("some value", meta.StreamMetadata.GetValue<string>("key3"));
        }

        [Fact][Trait("Category", "LongRunning")][Trait("Category", "Network")]
        public void setting_json_metadata_on_nonempty_soft_deleted_stream_recreates_stream_not_overriding_metadata()
        {
            const string stream = "setting_json_metadata_on_nonempty_soft_deleted_stream_recreates_stream_not_overriding_metadata";

            Assert.Equal(1, _conn.AppendToStreamAsync(stream, ExpectedVersion.NoStream, TestEvent.NewTestEvent(), TestEvent.NewTestEvent()).Result.NextExpectedVersion);
            _conn.DeleteStreamAsync(stream, 1, hardDelete: false).Wait();

            Assert.Equal(1, _conn.SetStreamMetadataAsync(stream, 0,
                                                       StreamMetadata.Build().SetMaxCount(100)
                                                                     .SetDeleteRole("some-role")
                                                                     .SetCustomProperty("key1", true)
                                                                     .SetCustomProperty("key2", 17)
                                                                     .SetCustomProperty("key3", "some value")).Result.NextExpectedVersion);

            var res = _conn.ReadStreamEventsForwardAsync(stream, 0, 100, false).Result;
            Assert.Equal(SliceReadStatus.Success, res.Status);
            Assert.Equal(1, res.LastEventNumber);
            Assert.Equal(0, res.Events.Length);

            var meta = _conn.GetStreamMetadataAsync(stream).Result;
            Assert.Equal(2, meta.MetastreamVersion);
            Assert.Equal(2, meta.StreamMetadata.TruncateBefore);
            Assert.Equal(100, meta.StreamMetadata.MaxCount);
            Assert.Equal("some-role", meta.StreamMetadata.Acl.DeleteRole);
            Assert.Equal(true, meta.StreamMetadata.GetValue<bool>("key1"));
            Assert.Equal(17, meta.StreamMetadata.GetValue<int>("key2"));
            Assert.Equal("some value", meta.StreamMetadata.GetValue<string>("key3"));
        }

        [Fact][Trait("Category", "LongRunning")][Trait("Category", "Network")]
        public void setting_nonjson_metadata_on_empty_soft_deleted_stream_recreates_stream_keeping_original_metadata()
        {
            const string stream = "setting_nonjson_metadata_on_empty_soft_deleted_stream_recreates_stream_overriding_metadata";

            _conn.DeleteStreamAsync(stream, ExpectedVersion.NoStream, hardDelete: false).Wait();

            Assert.Equal(1, _conn.SetStreamMetadataAsync(stream, 0, new byte[256]).Result.NextExpectedVersion);

            var res = _conn.ReadStreamEventsForwardAsync(stream, 0, 100, false).Result;
            Assert.Equal(SliceReadStatus.StreamNotFound, res.Status);
            Assert.Equal(-1, res.LastEventNumber);
            Assert.Equal(0, res.Events.Length);

            var meta = _conn.GetStreamMetadataAsRawBytesAsync(stream).Result;
            Assert.Equal(1, meta.MetastreamVersion);
            Assert.Equal(new byte[256], meta.StreamMetadata);
        }

        [Fact][Trait("Category", "LongRunning")][Trait("Category", "Network")]
        public void setting_nonjson_metadata_on_nonempty_soft_deleted_stream_recreates_stream_keeping_original_metadata()
        {
            const string stream = "setting_nonjson_metadata_on_nonempty_soft_deleted_stream_recreates_stream_overriding_metadata";

            Assert.Equal(1, _conn.AppendToStreamAsync(stream, ExpectedVersion.NoStream, TestEvent.NewTestEvent(), TestEvent.NewTestEvent()).Result.NextExpectedVersion);
            _conn.DeleteStreamAsync(stream, 1, hardDelete: false).Wait();

            Assert.Equal(1, _conn.SetStreamMetadataAsync(stream, 0, new byte[256]).Result.NextExpectedVersion);

            var res = _conn.ReadStreamEventsForwardAsync(stream, 0, 100, false).Result;
            Assert.Equal(SliceReadStatus.Success, res.Status);
            Assert.Equal(1, res.LastEventNumber);
            Assert.Equal(2, res.Events.Length);
            Assert.Equal(new[] {0, 1}, res.Events.Select(x => x.OriginalEventNumber).ToArray());

            var meta = _conn.GetStreamMetadataAsRawBytesAsync(stream).Result;
            Assert.Equal(1, meta.MetastreamVersion);
            Assert.Equal(new byte[256], meta.StreamMetadata);
        }
    }
}
