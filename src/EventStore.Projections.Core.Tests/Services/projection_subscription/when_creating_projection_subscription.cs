﻿using System;
using EventStore.Core.Services.TimerService;
using EventStore.Core.Tests.Fakes;
using EventStore.Core.Tests.Services.TimeService;
using EventStore.Projections.Core.Services.Processing;
using Xunit;

namespace EventStore.Projections.Core.Tests.Services.projection_subscription
{
    
    public class when_creating_projection_subscription
    {
        [Fact]
        public void it_can_be_created()
        {
            new ReaderSubscription(
                "Test Subscription",
                new FakePublisher(),
                Guid.NewGuid(),
                CheckpointTag.FromPosition(0, 0, -1),
                CreateReaderStrategy(),
                new FakeTimeProvider(),
                1000,
                2000);
        }

        [Fact]
        public void null_publisher_throws_argument_null_exception()
        {
            Assert.Throws<ArgumentNullException>(() =>
            {
                new ReaderSubscription(
                    "Test Subscription",
                    null,
                    Guid.NewGuid(),
                    CheckpointTag.FromPosition(0, 0, -1),
                    CreateReaderStrategy(),
                    new FakeTimeProvider(),
                    1000,
                    2000);
            });
        }

        [Fact]
        public void null_checkpoint_strategy_throws_argument_null_exception()
        {
            Assert.Throws<ArgumentNullException>(() =>
            {
                new ReaderSubscription(
                    "Test Subscription",
                    new FakePublisher(),
                    Guid.NewGuid(),
                    CheckpointTag.FromPosition(0, 0, -1),
                    null,
                    new FakeTimeProvider(),
                    1000,
                    2000);
            });
        }

        [Fact]
        public void null_time_provider_throws_argument_null_exception()
        {
            Assert.Throws<ArgumentNullException>(() =>
            {
                new ReaderSubscription(
                    "Test Subscription",
                    new FakePublisher(),
                    Guid.NewGuid(),
                    CheckpointTag.FromPosition(0, 0, -1),
                    CreateReaderStrategy(),
                    null,
                    1000,
                    2000);
            });
        }

        private IReaderStrategy CreateReaderStrategy()
        {
            var result = new SourceDefinitionBuilder();
            result.FromAll();
            result.AllEvents();
            return ReaderStrategy.Create(
                "test",
                0,
                result.Build(),
                new RealTimeProvider(),
                stopOnEof: false,
                runAs: null);
        }
    }
}
