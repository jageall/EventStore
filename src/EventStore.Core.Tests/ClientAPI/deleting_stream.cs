using System;
using EventStore.ClientAPI;
using EventStore.ClientAPI.Exceptions;
using EventStore.Core.Tests.ClientAPI.Helpers;
using EventStore.Core.Tests.Helpers;
using Xunit;

namespace EventStore.Core.Tests.ClientAPI
{
    public class deleting_stream : IClassFixture<MiniNodeFixture>
    {
        private MiniNode _node;

        public void SetFixture(MiniNodeFixture data)
        {
            _node = data.Node;
        }

        virtual protected IEventStoreConnection BuildConnection(MiniNode node)
        {
            return TestConnection.Create(node.TcpEndPoint);
        }

        [Fact]
        [Trait("Category", "Network")]
        [Trait("Category", "LongRunning")]
        public void which_doesnt_exists_should_success_when_passed_empty_stream_expected_version()
        {
            const string stream = "which_already_exists_should_success_when_passed_empty_stream_expected_version";
            using (var connection = BuildConnection(_node))
            {
                connection.ConnectAsync().Wait();
                var delete = connection.DeleteStreamAsync(stream, ExpectedVersion.EmptyStream, hardDelete: true);
                var ex = Record.Exception(() => delete.Wait());
                Assert.Null(ex);
            }
        }

        [Fact]
        [Trait("Category", "Network")]
        [Trait("Category", "LongRunning")]
        public void which_doesnt_exists_should_success_when_passed_any_for_expected_version()
        {
            const string stream = "which_already_exists_should_success_when_passed_any_for_expected_version";
                        using (var connection = BuildConnection(_node))
            {
                connection.ConnectAsync().Wait();

                var delete = connection.DeleteStreamAsync(stream, ExpectedVersion.Any, hardDelete: true);
                var ex = Record.Exception(() => delete.Wait());
                Assert.Null(ex);
            }
        }

        [Fact]
        [Trait("Category", "Network")]
        [Trait("Category", "LongRunning")]
        public void with_invalid_expected_version_should_fail()
        {
            const string stream = "with_invalid_expected_version_should_fail";
            using (var connection = BuildConnection(_node))
            {
                connection.ConnectAsync().Wait();

                var delete = connection.DeleteStreamAsync(stream, 1, hardDelete: true);
                var thrown = Assert.Throws<AggregateException>(() => delete.Wait());
                Assert.IsType<WrongExpectedVersionException>(thrown.InnerException);
            }
        }

        [Fact]
        [Trait("Category", "Network")]
        [Trait("Category", "LongRunning")]
        public void should_return_log_position_when_writing()
        {
            const string stream = "delete_should_return_log_position_when_writing";
                        using (var connection = BuildConnection(_node))
            {
                connection.ConnectAsync().Wait();

                var result = connection.AppendToStreamAsync(stream, ExpectedVersion.EmptyStream, TestEvent.NewTestEvent()).Result;
                var delete = connection.DeleteStreamAsync(stream, 0, hardDelete: true).Result;
                
                Assert.True(0 < result.LogPosition.PreparePosition);
                Assert.True(0 < result.LogPosition.CommitPosition);
            }
        }

        [Fact]
        [Trait("Category", "Network")]
        [Trait("Category", "LongRunning")]
        public void which_was_already_deleted_should_fail()
        {
            const string stream = "which_was_allready_deleted_should_fail";
                        using (var connection = BuildConnection(_node))
            {
                connection.ConnectAsync().Wait();

                var delete = connection.DeleteStreamAsync(stream, ExpectedVersion.EmptyStream, hardDelete: true);
                delete.Wait();

                var secondDelete = connection.DeleteStreamAsync(stream, ExpectedVersion.Any, hardDelete: true);
                var thrown = Assert.Throws<AggregateException>(() => secondDelete.Wait());
                Assert.IsType<StreamDeletedException>(thrown.InnerException);
            }
        }
    }
}
