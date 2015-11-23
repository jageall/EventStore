using System;
using EventStore.Core.Bus;
using EventStore.Core.Messaging;
using EventStore.Core.Tests.Bus.Helpers;
using EventStore.Core.Tests.Helpers;
using Xunit;

namespace EventStore.Core.Tests.Bus
{
    public abstract class when_publishing_to_queued_handler_before_starting : QueuedHandlerTestWithWaitingConsumer
    {
        protected when_publishing_to_queued_handler_before_starting(Func<IHandle<Message>, string, TimeSpan, IQueuedHandler> queuedHandlerFactory)
                : base(queuedHandlerFactory)
        {
        }

        [Fact]
        public void should_not_throw()
        {
            Assert.DoesNotThrow(() => Queue.Publish(new TestMessage()));
        }

        [Fact]
        public void should_not_forward_message_to_bus()
        {
            Consumer.SetWaitingCount(1);

            Queue.Publish(new TestMessage());

            Consumer.Wait(10);

            Assert.True(Consumer.HandledMessages.ContainsNo<TestMessage>());
        }

        [Fact]
        public void and_then_starting_message_should_be_forwarded_to_bus()
        {
            Consumer.SetWaitingCount(1);

            Queue.Publish(new TestMessage());
            try
            {
                Queue.Start();
                Consumer.Wait();
            }
            finally
            {
                Queue.Stop();
            }

            Assert.True(Consumer.HandledMessages.ContainsSingle<TestMessage>());
        }

        [Fact]
        public void multiple_messages_and_then_starting_messages_should_be_forwarded_to_bus()
        {
            Consumer.SetWaitingCount(3);

            Queue.Publish(new TestMessage());
            Queue.Publish(new TestMessage2());
            Queue.Publish(new TestMessage3());

            try
            {
                Queue.Start();
                Consumer.Wait();
            }
            finally
            {
                Queue.Stop();
            }

            Assert.True(Consumer.HandledMessages.ContainsSingle<TestMessage>() &&
                        Consumer.HandledMessages.ContainsSingle<TestMessage2>() &&
                        Consumer.HandledMessages.ContainsSingle<TestMessage3>());
        }
    }

    public class when_publishing_to_queued_handler_mres_before_starting : when_publishing_to_queued_handler_before_starting
    {
        public when_publishing_to_queued_handler_mres_before_starting()
            : base((consumer, name, timeout) => new QueuedHandlerMRES(consumer, name, false, null, timeout))
        {
        }
    }

    public class when_publishing_to_queued_handler_autoreset_before_starting : when_publishing_to_queued_handler_before_starting
    {
        public when_publishing_to_queued_handler_autoreset_before_starting()
            : base((consumer, name, timeout) => new QueuedHandlerAutoReset(consumer, name, false, null, timeout))
        {
        }
    }

    public class when_publishing_to_queued_handler_sleep_before_starting : when_publishing_to_queued_handler_before_starting
    {
        public when_publishing_to_queued_handler_sleep_before_starting()
            : base((consumer, name, timeout) => new QueuedHandlerSleep(consumer, name, false, null, timeout))
        {
        }
    }

    public class when_publishing_to_queued_handler_pulse_before_starting : when_publishing_to_queued_handler_before_starting
    {
        public when_publishing_to_queued_handler_pulse_before_starting()
            : base((consumer, name, timeout) => new QueuedHandlerPulse(consumer, name, false, null, timeout))
        {
        }
    }
}