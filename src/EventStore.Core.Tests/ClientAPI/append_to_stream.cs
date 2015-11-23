using System;
using System.Linq;
using EventStore.ClientAPI;
using EventStore.ClientAPI.Exceptions;
using EventStore.Core.Messages;
using EventStore.Core.Tests.ClientAPI.Helpers;
using EventStore.Core.Tests.Helpers;
using Xunit;

namespace EventStore.Core.Tests.ClientAPI
{
    public class MiniNodeFixture : SpecificationWithDirectoryPerTestFixture
    {
        public MiniNodeFixture()
        {
            Node = new MiniNode(PathName);
            Node.Start();
        }

        public MiniNode Node { get; private set; }

        public override void Dispose()
        {
            if(Node != null)
                Node.Shutdown();
            base.Dispose();
        }
    }

    public class ConnectedMiniNodeFixture : MiniNodeFixture
    {
        public ConnectedMiniNodeFixture()
        {
            Connection = TestConnection.Create(Node.TcpEndPoint);
            Connection.ConnectAsync().Wait();
        }

        public override void Dispose()
        {
            Connection.Close();
            base.Dispose();
        }

        public IEventStoreConnection Connection { get; private set; }
    }

    public class append_to_stream : IUseFixture<MiniNodeFixture>
    {
        private readonly TcpType _tcpType = TcpType.Normal;
        private MiniNode _node;

        protected virtual IEventStoreConnection BuildConnection(MiniNode node)
        {
            return TestConnection.To(node, _tcpType);
        }

        [Fact, Trait("Category", "Network")]
        public void should_allow_appending_zero_events_to_stream_with_no_problems()
        {
            const string stream1 = "should_allow_appending_zero_events_to_stream_with_no_problems1";
            const string stream2 = "should_allow_appending_zero_events_to_stream_with_no_problems2";
            using (var store = BuildConnection(_node))
            {
                store.ConnectAsync().Wait();

                Assert.Equal(-1, store.AppendToStreamAsync(stream1, ExpectedVersion.Any).Result.NextExpectedVersion);
                Assert.Equal(-1, store.AppendToStreamAsync(stream1, ExpectedVersion.NoStream).Result.NextExpectedVersion);
                Assert.Equal(-1, store.AppendToStreamAsync(stream1, ExpectedVersion.Any).Result.NextExpectedVersion);
                Assert.Equal(-1, store.AppendToStreamAsync(stream1, ExpectedVersion.NoStream).Result.NextExpectedVersion);

                var read1 = store.ReadStreamEventsForwardAsync(stream1, 0, 2, resolveLinkTos: false).Result;
                Assert.Equal(0, read1.Events.Length);

                Assert.Equal(-1, store.AppendToStreamAsync(stream2, ExpectedVersion.NoStream).Result.NextExpectedVersion);
                Assert.Equal(-1, store.AppendToStreamAsync(stream2, ExpectedVersion.Any).Result.NextExpectedVersion);
                Assert.Equal(-1, store.AppendToStreamAsync(stream2, ExpectedVersion.NoStream).Result.NextExpectedVersion);
                Assert.Equal(-1, store.AppendToStreamAsync(stream2, ExpectedVersion.Any).Result.NextExpectedVersion);

                var read2 = store.ReadStreamEventsForwardAsync(stream2, 0, 2, resolveLinkTos: false).Result;
                Assert.Equal(0, read2.Events.Length);
            }
        }

        [Fact, Trait("Category", "Network")]
        public void should_create_stream_with_no_stream_exp_ver_on_first_write_if_does_not_exist()
        {
            const string stream = "should_create_stream_with_no_stream_exp_ver_on_first_write_if_does_not_exist";
            using (var store = BuildConnection(_node))
            {
                store.ConnectAsync().Wait();

                Assert.Equal(0, store.AppendToStreamAsync(stream, ExpectedVersion.NoStream, TestEvent.NewTestEvent()).Result.NextExpectedVersion);

                var read = store.ReadStreamEventsForwardAsync(stream, 0, 2, resolveLinkTos: false);
                Assert.DoesNotThrow(read.Wait);
                Assert.Equal(1, read.Result.Events.Length);
            }
        }

        [Fact]
        [Trait("Category", "Network")]
        public void should_create_stream_with_any_exp_ver_on_first_write_if_does_not_exist()
        {
            const string stream = "should_create_stream_with_any_exp_ver_on_first_write_if_does_not_exist";
            using (var store = BuildConnection(_node))
            {
                store.ConnectAsync().Wait();
                Assert.Equal(0, store.AppendToStreamAsync(stream, ExpectedVersion.Any, TestEvent.NewTestEvent()).Result.NextExpectedVersion);

                var read = store.ReadStreamEventsForwardAsync(stream, 0, 2, resolveLinkTos: false);
                Assert.DoesNotThrow(read.Wait);
                Assert.Equal(1, read.Result.Events.Length);
            }
        }

        [Fact]
        [Trait("Category", "Network")]
        public void multiple_idempotent_writes()
        {
            const string stream = "multiple_idempotent_writes";
            using (var store = BuildConnection(_node))
            {
                store.ConnectAsync().Wait();

                var events = new[] { TestEvent.NewTestEvent(), TestEvent.NewTestEvent(), TestEvent.NewTestEvent(), TestEvent.NewTestEvent() };
                Assert.Equal(3, store.AppendToStreamAsync(stream, ExpectedVersion.Any, events).Result.NextExpectedVersion);
                Assert.Equal(3, store.AppendToStreamAsync(stream, ExpectedVersion.Any, events).Result.NextExpectedVersion);
            }
        }

        [Fact]
        [Trait("Category", "Network")]
        public void multiple_idempotent_writes_with_same_id_bug_case()
        {
            const string stream = "multiple_idempotent_writes_with_same_id_bug_case";
            using (var store = BuildConnection(_node))
            {
                store.ConnectAsync().Wait();
                var x = TestEvent.NewTestEvent();
                var events = new[] { x,x,x,x,x,x};
                Assert.Equal(5,store.AppendToStreamAsync(stream, ExpectedVersion.Any, events).Result.NextExpectedVersion);
            }
        }

        [Fact]
        [Trait("Category", "Network")]
        public void in_wtf_multiple_case_of_multiple_writes_expected_version_any_per_all_same_id()
        {
            const string stream = "in_wtf_multiple_case_of_multiple_writes_expected_version_any_per_all_same_id";
            using (var store = BuildConnection(_node))
            {
                store.ConnectAsync().Wait();
                var x = TestEvent.NewTestEvent();
                var events = new[] { x, x, x, x, x, x };
                Assert.Equal(5, store.AppendToStreamAsync(stream, ExpectedVersion.Any, events).Result.NextExpectedVersion);
                var f = store.AppendToStreamAsync(stream, ExpectedVersion.Any, events).Result;
                Assert.Equal(0, f.NextExpectedVersion);
            }
        }

        [Fact]
        [Trait("Category", "Network")]
        public void in_slightly_reasonable_multiple_case_of_multiple_writes_with_expected_version_per_all_same_id()
        {
            const string stream = "in_slightly_reasonable_multiple_case_of_multiple_writes_with_expected_version_per_all_same_id";
            using (var store = BuildConnection(_node))
            {
                store.ConnectAsync().Wait();
                var x = TestEvent.NewTestEvent();
                var events = new[] { x, x, x, x, x, x };
                Assert.Equal(5, store.AppendToStreamAsync(stream, ExpectedVersion.EmptyStream, events).Result.NextExpectedVersion);
                var f = store.AppendToStreamAsync(stream, ExpectedVersion.EmptyStream, events).Result;
                Assert.Equal(5, f.NextExpectedVersion);
            }
        }

        [Fact]
        [Trait("Category", "Network")]
        public void should_fail_writing_with_correct_exp_ver_to_deleted_stream()
        {
            const string stream = "should_fail_writing_with_correct_exp_ver_to_deleted_stream";
            using (var store = BuildConnection(_node))
            {
                store.ConnectAsync().Wait();

                var delete = store.DeleteStreamAsync(stream, ExpectedVersion.EmptyStream, hardDelete: true);
                Assert.DoesNotThrow(delete.Wait);

                var append = store.AppendToStreamAsync(stream, ExpectedVersion.NoStream, new[] { TestEvent.NewTestEvent() });

                var thrown = Assert.Throws<AggregateException>(() => append.Wait());
                Assert.IsType<StreamDeletedException>(thrown.InnerException);
            }
        }

        [Fact]
        [Trait("Category", "Network")]
        public void should_return_log_position_when_writing()
        {
            const string stream = "should_return_log_position_when_writing";
            using (var store = BuildConnection(_node))
            {
                store.ConnectAsync().Wait();
                var result = store.AppendToStreamAsync(stream, ExpectedVersion.EmptyStream, TestEvent.NewTestEvent()).Result;
                Assert.True(0 < result.LogPosition.PreparePosition);
                Assert.True(0 < result.LogPosition.CommitPosition);
            }
        }

        [Fact]
        [Trait("Category", "Network")]
        public void should_fail_writing_with_any_exp_ver_to_deleted_stream()
        {
            const string stream = "should_fail_writing_with_any_exp_ver_to_deleted_stream";
            using (var store = BuildConnection(_node))
            {
                store.ConnectAsync().Wait();

                store.DeleteStreamAsync(stream, ExpectedVersion.EmptyStream, hardDelete: true).Wait();

                var append = store.AppendToStreamAsync(stream, ExpectedVersion.Any, new[] { TestEvent.NewTestEvent() });
                var thrown = Assert.Throws<AggregateException>(() => append.Wait());
                Assert.IsType<StreamDeletedException>(thrown.InnerException);
            }
        }

        [Fact]
        [Trait("Category", "Network")]
        public void should_fail_writing_with_invalid_exp_ver_to_deleted_stream()
        {
            const string stream = "should_fail_writing_with_invalid_exp_ver_to_deleted_stream";
            using (var store = BuildConnection(_node))
            {
                store.ConnectAsync().Wait();

                var delete = store.DeleteStreamAsync(stream, ExpectedVersion.EmptyStream, hardDelete: true);
                Assert.DoesNotThrow(delete.Wait);

                var append = store.AppendToStreamAsync(stream, 5, new[] { TestEvent.NewTestEvent() });
                var thrown = Assert.Throws<AggregateException>(() => append.Wait());
                Assert.IsType<StreamDeletedException>(thrown.InnerException);
            }
        }

        [Fact]
        [Trait("Category", "Network")]
        public void should_append_with_correct_exp_ver_to_existing_stream()
        {
            const string stream = "should_append_with_correct_exp_ver_to_existing_stream";
            using (var store = BuildConnection(_node))
            {
                store.ConnectAsync().Wait();
                store.AppendToStreamAsync(stream, ExpectedVersion.EmptyStream, TestEvent.NewTestEvent()).Wait();

                var append = store.AppendToStreamAsync(stream, 0, new[] { TestEvent.NewTestEvent() });
                Assert.DoesNotThrow(append.Wait);
            }
        }

        [Fact]
        [Trait("Category", "Network")]
        public void should_append_with_any_exp_ver_to_existing_stream()
        {
            const string stream = "should_append_with_any_exp_ver_to_existing_stream";
            using (var store = BuildConnection(_node))
            {
                store.ConnectAsync().Wait();
                Assert.Equal(0, store.AppendToStreamAsync(stream, ExpectedVersion.EmptyStream, TestEvent.NewTestEvent()).Result.NextExpectedVersion);
                Assert.Equal(1, store.AppendToStreamAsync(stream, ExpectedVersion.Any, TestEvent.NewTestEvent()).Result.NextExpectedVersion);
            }
        }

        [Fact]
        [Trait("Category", "Network")]
        public void should_fail_appending_with_wrong_exp_ver_to_existing_stream()
        {
            const string stream = "should_fail_appending_with_wrong_exp_ver_to_existing_stream";
            using (var store = BuildConnection(_node))
            {
                store.ConnectAsync().Wait();

                var append = store.AppendToStreamAsync(stream, 1, new[] { TestEvent.NewTestEvent() });
                var thrown = Assert.Throws<AggregateException>(() => append.Wait());
                Assert.IsType<WrongExpectedVersionException>(thrown.InnerException);
            }
        }

        [Fact, Trait("Category", "Network")]
        public void can_append_multiple_events_at_once()
        {
            const string stream = "can_append_multiple_events_at_once";
            using (var store = BuildConnection(_node))
            {
                store.ConnectAsync().Wait();

                var events = Enumerable.Range(0, 100).Select(i => TestEvent.NewTestEvent(i.ToString(), i.ToString()));
                Assert.Equal(99, store.AppendToStreamAsync(stream, ExpectedVersion.EmptyStream, events).Result.NextExpectedVersion);
            }
        }

        public void SetFixture(MiniNodeFixture data)
        {
            _node = data.Node;
        }
    }

    public class ssl_append_to_stream : SpecificationWithDirectoryPerTestFixture, IUseFixture<MiniNodeFixture>
    {
        private readonly TcpType _tcpType = TcpType.Ssl;
        protected MiniNode _node;

        protected virtual IEventStoreConnection BuildConnection(MiniNode node)
        {
            return TestConnection.To(node, _tcpType);
        }

        [Fact, Trait("Category", "Network")]
        public void should_allow_appending_zero_events_to_stream_with_no_problems()
        {
            const string stream = "should_allow_appending_zero_events_to_stream_with_no_problems";
            using (var store = BuildConnection(_node))
            {
                store.ConnectAsync().Wait();
                Assert.Equal(-1, store.AppendToStreamAsync(stream, ExpectedVersion.NoStream).Result.NextExpectedVersion);

                var read = store.ReadStreamEventsForwardAsync(stream, 0, 2, resolveLinkTos: false).Result;
                Assert.Equal(0, read.Events.Length);
            }
        }

        [Fact, Trait("Category", "Network")]
        public void should_create_stream_with_no_stream_exp_ver_on_first_write_if_does_not_exist()
        {
            const string stream = "should_create_stream_with_no_stream_exp_ver_on_first_write_if_does_not_exist";
            using (var store = BuildConnection(_node))
            {
                store.ConnectAsync().Wait();
                Assert.Equal(0, store.AppendToStreamAsync(stream, ExpectedVersion.NoStream, TestEvent.NewTestEvent()).Result.NextExpectedVersion);

                var read = store.ReadStreamEventsForwardAsync(stream, 0, 2, resolveLinkTos: false);
                Assert.Equal(1, read.Result.Events.Length);
            }
        }

        [Fact]
        [Trait("Category", "Network")]
        public void should_create_stream_with_any_exp_ver_on_first_write_if_does_not_exist()
        {
            const string stream = "should_create_stream_with_any_exp_ver_on_first_write_if_does_not_exist";
            using (var store = BuildConnection(_node))
            {
                store.ConnectAsync().Wait();
                Assert.Equal(0, store.AppendToStreamAsync(stream, ExpectedVersion.Any, TestEvent.NewTestEvent()).Result.NextExpectedVersion);

                var read = store.ReadStreamEventsForwardAsync(stream, 0, 2, resolveLinkTos: false);
                Assert.Equal(1, read.Result.Events.Length);
            }
        }

        [Fact]
        [Trait("Category", "Network")]
        public void should_fail_writing_with_correct_exp_ver_to_deleted_stream()
        {
            const string stream = "should_fail_writing_with_correct_exp_ver_to_deleted_stream";
            using (var store = BuildConnection(_node))
            {
                store.ConnectAsync().Wait();

                var delete = store.DeleteStreamAsync(stream, ExpectedVersion.EmptyStream, hardDelete: true);
                Assert.DoesNotThrow(delete.Wait);

                var append = store.AppendToStreamAsync(stream, ExpectedVersion.NoStream, new[] { TestEvent.NewTestEvent() });
                var thrown = Assert.Throws<AggregateException>(() => append.Wait());
                Assert.IsType<StreamDeletedException>(thrown.InnerException);
            }
        }

        [Fact]
        [Trait("Category", "Network")]
        public void should_fail_writing_with_any_exp_ver_to_deleted_stream()
        {
            const string stream = "should_fail_writing_with_any_exp_ver_to_deleted_stream";
            using (var store = BuildConnection(_node))
            {
                store.ConnectAsync().Wait();

                var delete = store.DeleteStreamAsync(stream, ExpectedVersion.EmptyStream, hardDelete: true);
                Assert.DoesNotThrow(delete.Wait);

                var append = store.AppendToStreamAsync(stream, ExpectedVersion.Any, new[] { TestEvent.NewTestEvent() });
                var thrown = Assert.Throws<AggregateException>(() => append.Wait());
                Assert.IsType<StreamDeletedException>(thrown.InnerException);
            }
        }

        [Fact]
        [Trait("Category", "Network")]
        public void should_fail_writing_with_invalid_exp_ver_to_deleted_stream()
        {
            const string stream = "should_fail_writing_with_invalid_exp_ver_to_deleted_stream";
            using (var store = BuildConnection(_node))
            {
                store.ConnectAsync().Wait();

                var delete = store.DeleteStreamAsync(stream, ExpectedVersion.EmptyStream, hardDelete: true);
                Assert.DoesNotThrow(delete.Wait);

                var append = store.AppendToStreamAsync(stream, 5, new[] { TestEvent.NewTestEvent() });
                var thrown = Assert.Throws<AggregateException>(() => append.Wait());
                Assert.IsType<StreamDeletedException>(thrown.InnerException);
            }
        }

        [Fact]
        [Trait("Category", "Network")]
        public void should_append_with_correct_exp_ver_to_existing_stream()
        {
            const string stream = "should_append_with_correct_exp_ver_to_existing_stream";
            using (var store = BuildConnection(_node))
            {
                store.ConnectAsync().Wait();
                Assert.Equal(0, store.AppendToStreamAsync(stream, ExpectedVersion.EmptyStream, TestEvent.NewTestEvent()).Result.NextExpectedVersion);
                Assert.Equal(1, store.AppendToStreamAsync(stream, 0, TestEvent.NewTestEvent()).Result.NextExpectedVersion);
            }
        }

        [Fact]
        [Trait("Category", "Network")]
        public void should_append_with_any_exp_ver_to_existing_stream()
        {
            const string stream = "should_append_with_any_exp_ver_to_existing_stream";
            using (var store = BuildConnection(_node))
            {
                store.ConnectAsync().Wait();
                Assert.Equal(0, store.AppendToStreamAsync(stream, ExpectedVersion.EmptyStream, TestEvent.NewTestEvent()).Result.NextExpectedVersion);
                Assert.Equal(1, store.AppendToStreamAsync(stream, ExpectedVersion.Any, TestEvent.NewTestEvent()).Result.NextExpectedVersion);
            }
        }

        [Fact]
        [Trait("Category", "Network")]
        public void should_return_log_position_when_writing()
        {
            const string stream = "should_return_log_position_when_writing";
            using (var store = BuildConnection(_node))
            {
                store.ConnectAsync().Wait();
                var result = store.AppendToStreamAsync(stream, ExpectedVersion.EmptyStream, TestEvent.NewTestEvent()).Result;
                Assert.True(0 < result.LogPosition.PreparePosition);
                Assert.True(0 < result.LogPosition.CommitPosition);
            }
        }

        [Fact]
        [Trait("Category", "Network")]
        public void should_fail_appending_with_wrong_exp_ver_to_existing_stream()
        {
            const string stream = "should_fail_appending_with_wrong_exp_ver_to_existing_stream";
            using (var store = BuildConnection(_node))
            {
                store.ConnectAsync().Wait();
                Assert.Equal(0, store.AppendToStreamAsync(stream, ExpectedVersion.EmptyStream, TestEvent.NewTestEvent()).Result.NextExpectedVersion);

                var append = store.AppendToStreamAsync(stream, 1, new[] { TestEvent.NewTestEvent() });
                var thrown = Assert.Throws<AggregateException>(() => append.Wait());
                Assert.IsType<WrongExpectedVersionException>(thrown.InnerException);
            }
        }

        [Fact]
        [Trait("Category", "Network")]
        public void can_append_multiple_events_at_once()
        {
            const string stream = "can_append_multiple_events_at_once";
            using (var store = BuildConnection(_node))
            {
                store.ConnectAsync().Wait();

                var events = Enumerable.Range(0, 100).Select(i => TestEvent.NewTestEvent(i.ToString(), i.ToString()));
                Assert.Equal(99, store.AppendToStreamAsync(stream, ExpectedVersion.EmptyStream, events).Result.NextExpectedVersion);
            }
        }

        public void SetFixture(MiniNodeFixture data)
        {
            _node = data.Node;
        }
    }

}
