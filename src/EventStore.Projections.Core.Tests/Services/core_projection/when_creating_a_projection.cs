using System;
using EventStore.Core.Helpers;
using EventStore.Core.Messaging;
using EventStore.Core.Services.TimerService;
using EventStore.Core.Services.UserManagement;
using EventStore.Core.Tests.Fakes;
using EventStore.Projections.Core.Messages;
using EventStore.Projections.Core.Services;
using EventStore.Projections.Core.Services.Processing;
using Xunit;

namespace EventStore.Projections.Core.Tests.Services.core_projection
{
    
    public class when_creating_a_projection
    {
        public when_creating_a_projection()
        {
            var fakePublisher = new FakePublisher();
            _ioDispatcher = new IODispatcher(fakePublisher, new PublishEnvelope(fakePublisher));

            _subscriptionDispatcher =
                new ReaderSubscriptionDispatcher(new FakePublisher());
        }

        private readonly ProjectionConfig _defaultProjectionConfig = new ProjectionConfig(
            null, 5, 10, 1000, 250, true, true, true, true, false);

        private IODispatcher _ioDispatcher;


        private
            ReaderSubscriptionDispatcher
            _subscriptionDispatcher;
        
        [Fact]
        public void a_checkpoint_threshold_less_tan_checkpoint_handled_threshold_throws_argument_out_of_range_exception(
            )
        {
            Assert.Throws<ArgumentException>(
                () => new ProjectionConfig(null, 10, 5, 1000, 250, true, true, false, false, false));
        }
        
        [Fact]
        public void a_negative_checkpoint_handled_interval_throws_argument_out_of_range_exception()
        {
            Assert.Throws<ArgumentOutOfRangeException>(() => new ProjectionConfig(null, -1, 10, 1000, 250, true, true, false, false, false));
        }
        
        [Fact]
        public void a_null_io_dispatcher__throws_argument_null_exception()
        {
            IProjectionStateHandler projectionStateHandler = new FakeProjectionStateHandler();
            var version = new ProjectionVersion(1, 0, 0);
            Assert.Throws<ArgumentNullException>(() => new ContinuousProjectionProcessingStrategy(
                "projection",
                version,
                projectionStateHandler,
                _defaultProjectionConfig,
                projectionStateHandler.GetSourceDefinition(),
                null,
                _subscriptionDispatcher).Create(
                    Guid.NewGuid(),
                    new FakePublisher(),
                    Guid.NewGuid(),
                    SystemAccount.Principal,
                    new FakePublisher(),
                    null,
                    _subscriptionDispatcher,
                    new RealTimeProvider()));
        }
        
        [Fact]
        public void a_null_name_throws_argument_null_excveption()
        {
            IProjectionStateHandler projectionStateHandler = new FakeProjectionStateHandler();
            var version = new ProjectionVersion(1, 0, 0);
            Assert.Throws<ArgumentNullException>(() =>
                new ContinuousProjectionProcessingStrategy(
                    null,
                    version,
                    projectionStateHandler,
                    _defaultProjectionConfig,
                    projectionStateHandler.GetSourceDefinition(),
                    null,
                    _subscriptionDispatcher).Create(
                        Guid.NewGuid(),
                        new FakePublisher(),
                        Guid.NewGuid(),
                        SystemAccount.Principal,
                        new FakePublisher(),
                        _ioDispatcher,
                        _subscriptionDispatcher,
                        new RealTimeProvider()));
        }
        
        [Fact]
        public void a_null_publisher_throws_exception()
        {
            IProjectionStateHandler projectionStateHandler = new FakeProjectionStateHandler();
            var version = new ProjectionVersion(1, 0, 0);
            Assert.Throws<ArgumentNullException>(() =>
            new ContinuousProjectionProcessingStrategy(
                "projection",
                version,
                projectionStateHandler,
                _defaultProjectionConfig,
                projectionStateHandler.GetSourceDefinition(),
                null,
                _subscriptionDispatcher).Create(
                    Guid.NewGuid(),
                    new FakePublisher(),
                    Guid.NewGuid(),
                    SystemAccount.Principal,
                    null,
                    _ioDispatcher,
                    _subscriptionDispatcher,
                    new RealTimeProvider()));
        }
        
        [Fact]
        public void a_null_input_queue_throws_exception()
        {
            IProjectionStateHandler projectionStateHandler = new FakeProjectionStateHandler();
            var version = new ProjectionVersion(1, 0, 0);
            Assert.Throws<ArgumentNullException>(() =>
            new ContinuousProjectionProcessingStrategy(
                "projection",
                version,
                projectionStateHandler,
                _defaultProjectionConfig,
                projectionStateHandler.GetSourceDefinition(),
                null,
                _subscriptionDispatcher).Create(
                    Guid.NewGuid(),
                    null,
                    Guid.NewGuid(),
                    SystemAccount.Principal,
                    new FakePublisher(),
                    _ioDispatcher,
                    _subscriptionDispatcher,
                    new RealTimeProvider()));
        }

        [Fact]
        public void a_null_run_as_does_not_throw_exception()
        {
            IProjectionStateHandler projectionStateHandler = new FakeProjectionStateHandler();
            var version = new ProjectionVersion(1, 0, 0);
            new ContinuousProjectionProcessingStrategy(
                "projection",
                version,
                projectionStateHandler,
                _defaultProjectionConfig,
                projectionStateHandler.GetSourceDefinition(),
                null,
                _subscriptionDispatcher).Create(
                    Guid.NewGuid(),
                    new FakePublisher(),
                    Guid.NewGuid(),
                    null,
                    new FakePublisher(),
                    _ioDispatcher,
                    _subscriptionDispatcher,
                    new RealTimeProvider());
        }
        
        [Fact]
        public void a_null_subscription_dispatcher__throws_argument_null_exception()
        {
            IProjectionStateHandler projectionStateHandler = new FakeProjectionStateHandler();
            var version = new ProjectionVersion(1, 0, 0);
            Assert.Throws<ArgumentNullException>(() =>
            new ContinuousProjectionProcessingStrategy(
                "projection",
                version,
                projectionStateHandler,
                _defaultProjectionConfig,
                projectionStateHandler.GetSourceDefinition(),
                null,
                _subscriptionDispatcher).Create(
                    Guid.NewGuid(),
                    new FakePublisher(),
                    Guid.NewGuid(),
                    SystemAccount.Principal,
                    new FakePublisher(),
                    _ioDispatcher,
                    null,
                    new RealTimeProvider()));
        }
        
        [Fact]
        public void a_null_time_provider__throws_argument_null_exception()
        {
            IProjectionStateHandler projectionStateHandler = new FakeProjectionStateHandler();
            var version = new ProjectionVersion(1, 0, 0);
            Assert.Throws<ArgumentNullException>(() =>
            new ContinuousProjectionProcessingStrategy(
                "projection",
                version,
                projectionStateHandler,
                _defaultProjectionConfig,
                projectionStateHandler.GetSourceDefinition(),
                null,
                _subscriptionDispatcher).Create(
                    Guid.NewGuid(),
                    new FakePublisher(),
                    Guid.NewGuid(),
                    SystemAccount.Principal,
                    new FakePublisher(),
                    _ioDispatcher,
                    _subscriptionDispatcher,
                    null));
        }
        
        [Fact]
        public void a_zero_checkpoint_handled_threshold_throws_argument_out_of_range_exception()
        {
            Assert.Throws<ArgumentOutOfRangeException>(() => new ProjectionConfig(null, 0, 10, 1000, 250, true, true, false, false, false));
        }
        
        [Fact]
        public void an_empty_name_throws_argument_exception()
        {
            IProjectionStateHandler projectionStateHandler = new FakeProjectionStateHandler();
            var version = new ProjectionVersion(1, 0, 0);
            Assert.Throws<ArgumentException>(() =>
            new ContinuousProjectionProcessingStrategy(
                "",
                version,
                projectionStateHandler,
                _defaultProjectionConfig,
                projectionStateHandler.GetSourceDefinition(),
                null,
                _subscriptionDispatcher).Create(
                    Guid.NewGuid(),
                    new FakePublisher(),
                    Guid.NewGuid(),
                    SystemAccount.Principal,
                    new FakePublisher(),
                    _ioDispatcher,
                    _subscriptionDispatcher,
                    new RealTimeProvider()));
        }
    }
}
