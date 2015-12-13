using System;
using System.Text;
using System.Threading;
using EventStore.ClientAPI;
using EventStore.ClientAPI.Exceptions;
using Xunit;

namespace EventStore.Core.Tests.ClientAPI
{
    public class update_existing_persistent_subscription : SpecificationWithMiniNode
    {
        private readonly string _stream = Guid.NewGuid().ToString();
        private readonly PersistentSubscriptionSettings _settings = PersistentSubscriptionSettings.Create()
                                                                .DoNotResolveLinkTos()
                                                                .StartFromCurrent();

        protected override void Given()
        {
            _conn.AppendToStreamAsync(_stream, ExpectedVersion.Any,
                new EventData(Guid.NewGuid(), "whatever", true, Encoding.UTF8.GetBytes("{'foo' : 2}"), new Byte[0]));
            _conn.CreatePersistentSubscriptionAsync(_stream, "existing", _settings, DefaultData.AdminCredentials).Wait();
        }

        protected override void When()
        {
            
        }

        [Fact]
        [Trait("Category", "LongRunning")]
        public void the_completion_succeeds()
        {
            var ex = Record.Exception(() => _conn.UpdatePersistentSubscriptionAsync(_stream, "existing", _settings, DefaultData.AdminCredentials).Wait());
            Assert.Null(ex);
        }

        public update_existing_persistent_subscription(SpecificationFixture fixture) : base(fixture)
        {
        }
    }

    public class update_existing_persistent_subscription_with_subscribers : SpecificationWithMiniNode
    {
        private readonly string _stream = Guid.NewGuid().ToString();
        private readonly PersistentSubscriptionSettings _settings = PersistentSubscriptionSettings.Create()
                                                                .DoNotResolveLinkTos()
                                                                .StartFromCurrent();

        private AutoResetEvent _dropped;
        private SubscriptionDropReason _reason;
        private Exception _exception;
        private Exception _caught = null;

        protected override void Given()
        {
            var dropped = new AutoResetEvent(false);
            Fixture.AddStashedValueAssignment(this, instance => { instance._dropped = dropped; });
            _conn.AppendToStreamAsync(_stream, ExpectedVersion.Any,
                new EventData(Guid.NewGuid(), "whatever", true, Encoding.UTF8.GetBytes("{'foo' : 2}"), new Byte[0]));
            _conn.CreatePersistentSubscriptionAsync(_stream, "existing", _settings, DefaultData.AdminCredentials).Wait();
            _conn.ConnectToPersistentSubscription(_stream, "existing" , (x, y) => { },
                (sub, reason, ex) =>
                {
                    dropped.Set();
                    Fixture.AddStashedValueAssignment(this, instance =>
                    {   
                        instance._reason = reason;
                        instance._exception = ex; 

                    });
                });
        }

        protected override void When()
        {
            try
            {
                _conn.UpdatePersistentSubscriptionAsync(_stream, "existing", _settings, DefaultData.AdminCredentials)
                    .Wait();
            }
            catch (Exception ex)
            {
                Fixture.AddStashedValueAssignment(this, instance =>
                {
                    instance._caught = ex; 
                });
            }
        }

        [Fact]
        [Trait("Category", "LongRunning")]
        public void the_completion_succeeds()
        {
            Assert.Null(_caught);
        }

        [Fact]
        [Trait("Category", "LongRunning")]
        public void existing_subscriptions_are_dropped()
        {
            Assert.True(_dropped.WaitOne(TimeSpan.FromSeconds(5)));
            Assert.Equal(SubscriptionDropReason.UserInitiated, _reason);
            Assert.Null(_exception);
        }

        public update_existing_persistent_subscription_with_subscribers(SpecificationFixture fixture) : base(fixture)
        {
        }
    }

    public class update_non_existing_persistent_subscription : SpecificationWithMiniNode
    {
        private readonly string _stream = Guid.NewGuid().ToString();
        private readonly PersistentSubscriptionSettings _settings = PersistentSubscriptionSettings.Create()
                                                                .DoNotResolveLinkTos()
                                                                .StartFromCurrent();

        protected override void When()
        {
            
        }

        [Fact]
        [Trait("Category", "LongRunning")]
        public void the_completion_fails_with_not_found()
        {
            try
            {
                _conn.UpdatePersistentSubscriptionAsync(_stream, "existing", _settings,
                    DefaultData.AdminCredentials).Wait();
                Assert.True(false, "should have thrown"); // TODO JAG this should be converted to Assert.Throws
            } catch (Exception ex)
            {
                Assert.IsType<AggregateException>(ex);
                Assert.IsType<InvalidOperationException>(ex.InnerException);
            }
        }

        public update_non_existing_persistent_subscription(SpecificationFixture fixture) : base(fixture)
        {
        }
    }

    public class update_existing_persistent_subscription_without_permissions : SpecificationWithMiniNode
    {
        private readonly string _stream = Guid.NewGuid().ToString();
        private readonly PersistentSubscriptionSettings _settings = PersistentSubscriptionSettings.Create()
                                                                .DoNotResolveLinkTos()
                                                                .StartFromCurrent();

        protected override void When()
        {
            _conn.AppendToStreamAsync(_stream, ExpectedVersion.Any,
                new EventData(Guid.NewGuid(), "whatever", true, Encoding.UTF8.GetBytes("{'foo' : 2}"), new Byte[0]));
            _conn.CreatePersistentSubscriptionAsync(_stream, "existing", _settings, DefaultData.AdminCredentials).Wait();
        }

        [Fact]
        [Trait("Category", "LongRunning")]
        public void the_completion_fails_with_access_denied()
        {
            try
            {
                _conn.UpdatePersistentSubscriptionAsync(_stream, "existing", _settings, null).Wait();
                Assert.True(false, "should have thrown"); // TODO JAG this should be converted to Assert.Throws
            }
            catch (Exception ex)
            {
                Assert.IsType<AggregateException>(ex);
                Assert.IsType<AccessDeniedException>(ex.InnerException);
            }
        }

        public update_existing_persistent_subscription_without_permissions(SpecificationFixture fixture) : base(fixture)
        {
        }
    }
}