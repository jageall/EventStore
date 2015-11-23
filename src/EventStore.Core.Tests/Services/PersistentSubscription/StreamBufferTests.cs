using System;
using EventStore.Core.Data;
using EventStore.Core.Services.PersistentSubscription;
using Xunit;

namespace EventStore.Core.Tests.Services.PersistentSubscription
{
    public class StreamBufferTests
    {
        [Fact]
        public void adding_read_message_in_correct_order()
        {
            var buffer = new StreamBuffer(10, 10, -1, true);
            var id = Guid.NewGuid();
            buffer.AddReadMessage(BuildMessageAt(id, 0));
            Assert.Equal(1, buffer.BufferCount);
            OutstandingMessage message;
            Assert.True(buffer.TryDequeue(out message));
            Assert.Equal(id, message.EventId);
            Assert.False(buffer.Live);
        }

        [Fact]
        public void adding_multiple_read_message_in_correct_order()
        {
            var buffer = new StreamBuffer(10, 10, -1, true);
            var id1 = Guid.NewGuid();
            var id2 = Guid.NewGuid();
            buffer.AddReadMessage(BuildMessageAt(id1, 0));
            buffer.AddReadMessage(BuildMessageAt(id2, 1));
            Assert.Equal(2, buffer.BufferCount);
            OutstandingMessage message;
            Assert.True(buffer.TryDequeue(out message));
            Assert.Equal(id1, message.EventId);
            Assert.True(buffer.TryDequeue(out message));
            Assert.Equal(id2, message.EventId);
            Assert.False(buffer.Live);
        }


        [Fact]
        public void adding_multiple_read_message_in_wrong_order()
        {
            var buffer = new StreamBuffer(10, 10, -1, true);
            var id1 = Guid.NewGuid();
            var id2 = Guid.NewGuid();
            buffer.AddReadMessage(BuildMessageAt(id1, 1));
            buffer.AddReadMessage(BuildMessageAt(id2, 0));
            Assert.Equal(2, buffer.BufferCount);
            OutstandingMessage message;
            Assert.True(buffer.TryDequeue(out message));
            Assert.Equal(id1, message.EventId);
            Assert.True(buffer.TryDequeue(out message));
            Assert.Equal(id2, message.EventId);
            Assert.False(buffer.Live);
        }

        [Fact]
        public void adding_multiple_same_read_message()
        {
            var buffer = new StreamBuffer(10, 10, -1, true);
            var id1 = Guid.NewGuid();
            buffer.AddReadMessage(BuildMessageAt(id1, 0));
            buffer.AddReadMessage(BuildMessageAt(id1, 0));
            Assert.Equal(2, buffer.BufferCount);
            OutstandingMessage message;
            Assert.True(buffer.TryDequeue(out message));
            Assert.Equal(id1, message.EventId);
            Assert.True(buffer.TryDequeue(out message));
            Assert.Equal(id1, message.EventId);
            Assert.False(buffer.Live);
        }

        [Fact]
        public void adding_messages_to_read_after_same_on_live_switches_to_live()
        {
            var buffer = new StreamBuffer(10, 10, -1, true);
            var id1 = Guid.NewGuid();
            buffer.AddLiveMessage(BuildMessageAt(id1, 0));
            buffer.AddReadMessage(BuildMessageAt(id1, 0));
            Assert.True(buffer.Live);
            Assert.Equal(1, buffer.BufferCount);
            OutstandingMessage message;
            Assert.True(buffer.TryDequeue(out message));
            Assert.Equal(id1, message.EventId);
        }

        [Fact]
        public void adding_messages_to_read_after_later_live_does_not_switch()
        {
            var buffer = new StreamBuffer(10, 10, -1, true);
            var id1 = Guid.NewGuid();
            var id2 = Guid.NewGuid();
            buffer.AddLiveMessage(BuildMessageAt(id1, 5));
            buffer.AddReadMessage(BuildMessageAt(id2, 0));
            Assert.False(buffer.Live);
            Assert.Equal(1, buffer.BufferCount);
            OutstandingMessage message;
            Assert.True(buffer.TryDequeue(out message));
            Assert.Equal(id2, message.EventId);
        }

        [Fact]
        public void adding_messages_to_live_without_start_from_beginning()
        {
            var buffer = new StreamBuffer(10, 10, -1, false);
            var id1 = Guid.NewGuid();
            var id2 = Guid.NewGuid();
            buffer.AddLiveMessage(BuildMessageAt(id1, 6));
            buffer.AddLiveMessage(BuildMessageAt(id2, 7));
            Assert.True(buffer.Live);
            Assert.Equal(2, buffer.BufferCount);
            OutstandingMessage message;
            Assert.True(buffer.TryDequeue(out message));
            Assert.Equal(id1, message.EventId);
            Assert.True(buffer.TryDequeue(out message));
            Assert.Equal(id2, message.EventId);
        }

        [Fact]
        public void adding_messages_with_lower_in_live()
        {
            var buffer = new StreamBuffer(10, 10, -1, true);
            var id1 = Guid.NewGuid();
            var id2 = Guid.NewGuid();
            buffer.AddLiveMessage(BuildMessageAt(id1, 5));
            buffer.AddLiveMessage(BuildMessageAt(id1, 6));
            buffer.AddLiveMessage(BuildMessageAt(id2, 7));
            buffer.AddReadMessage(BuildMessageAt(id1, 7));
            Assert.True(buffer.Live);
            Assert.Equal(1, buffer.BufferCount);
            OutstandingMessage message;
            Assert.True(buffer.TryDequeue(out message));
            Assert.Equal(id2, message.EventId);
        }

        private OutstandingMessage BuildMessageAt(Guid id,int version)
        {
            return new OutstandingMessage(id, null, BuildEventAt(id,version), 0);
        }
        private ResolvedEvent BuildEventAt(Guid id, int version)
        {
            return Helper.BuildFakeEvent(id, "foo", "bar", version);
        }
    }
}