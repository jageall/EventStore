using System;
using System.Threading;
using EventStore.ClientAPI;
using EventStore.ClientAPI.Exceptions;
using EventStore.Core.Tests.Helpers;
using Xunit;

namespace EventStore.Core.Tests.ClientAPI
{
    public class deleting_existing_persistent_subscription_group_with_permissions : SpecificationWithMiniNode
    {
        private readonly PersistentSubscriptionSettings _settings = PersistentSubscriptionSettings.Create()
                                                                        .DoNotResolveLinkTos()
                                                                        .StartFromCurrent();
        private readonly string _stream = Guid.NewGuid().ToString();

        protected override void When()
        {
            _conn.CreatePersistentSubscriptionAsync(_stream, "groupname123", _settings,
                DefaultData.AdminCredentials).Wait();
        }

        [Fact]
        [Trait("Category", "LongRunning")]
        public void the_delete_of_group_succeeds()
        {
             Assert.DoesNotThrow(() => _conn.DeletePersistentSubscriptionAsync(_stream, "groupname123", DefaultData.AdminCredentials).Wait());
        }
    }

    public class deleting_existing_persistent_subscription_with_subscriber : SpecificationWithMiniNode
    {
        private readonly PersistentSubscriptionSettings _settings = PersistentSubscriptionSettings.Create()
                                                                        .DoNotResolveLinkTos()
                                                                        .StartFromCurrent();
        private readonly string _stream = Guid.NewGuid().ToString();
        private readonly ManualResetEvent _called = new ManualResetEvent(false);

        protected override void Given()
        {
            base.Given();
            _conn.CreatePersistentSubscriptionAsync(_stream, "groupname123", _settings,
    DefaultData.AdminCredentials).Wait();
            _conn.ConnectToPersistentSubscription(_stream, "groupname123",
                (s, e) => { },
                (s, r, e) => _called.Set());
        }

        protected override void When()
        {
            _conn.DeletePersistentSubscriptionAsync(_stream, "groupname123", DefaultData.AdminCredentials).Wait();
        }

        [Fact]
        [Trait("Category", "LongRunning")]
        public void the_subscription_is_dropped()
        {
            Assert.True(_called.WaitOne(TimeSpan.FromSeconds(5)));
        }
    }

    public class deleting_persistent_subscription_group_that_doesnt_exist : SpecificationWithMiniNode
    {
        private readonly string _stream = Guid.NewGuid().ToString();

        protected override void When()
        {
        }

        [Fact]
        [Trait("Category", "LongRunning")]
        public void the_delete_fails_with_argument_exception()
        {
            //TODO JAG fix this test to use Assert.Throws
            try
            {
                _conn.DeletePersistentSubscriptionAsync(_stream, Guid.NewGuid().ToString(), DefaultData.AdminCredentials).Wait();
                throw new Exception("expected exception");
            }
            catch (Exception ex)
            {
                Assert.IsType<AggregateException>(ex);
                var inner = ex.InnerException;
                Assert.IsType<InvalidOperationException>(inner);
            }
        }
    }

    public class deleting_persistent_subscription_group_without_permissions : SpecificationWithMiniNode
    {
        private readonly string _stream = Guid.NewGuid().ToString();

        protected override void When()
        {
        }

        [Fact]
        [Trait("Category", "LongRunning")]
        public void the_delete_fails_with_access_denied()
        {
            try
            {
                _conn.DeletePersistentSubscriptionAsync(_stream, Guid.NewGuid().ToString()).Wait();
                throw new Exception("expected exception");
            }
            catch (Exception ex)
            {
                Assert.IsType<AggregateException>(ex);
                var inner = ex.InnerException;
                Assert.IsType<AccessDeniedException>(inner);
            }
        }
    }
//ALL
/*

    [TestFixture, Category("LongRunning")]
    public class deleting_existing_persistent_subscription_group_on_all_with_permissions : SpecificationWithMiniNode
    {
        private readonly PersistentSubscriptionSettings _settings = PersistentSubscriptionSettingsBuilder.Create()
                                                                .DoNotResolveLinkTos()
                                                                .StartFromCurrent();
        protected override void When()
        {
            _conn.CreatePersistentSubscriptionForAllAsync("groupname123", _settings,
                DefaultData.AdminCredentials).Wait();
        }

        [Fact]
        public void the_delete_of_group_succeeds()
        {
            var result = _conn.DeletePersistentSubscriptionForAllAsync("groupname123", DefaultData.AdminCredentials).Result;
            Assert.Equal(PersistentSubscriptionDeleteStatus.Success, result.Status);
        }
    }

    [TestFixture, Category("LongRunning")]
    public class deleting_persistent_subscription_group_on_all_that_doesnt_exist : SpecificationWithMiniNode
    {
        protected override void When()
        {
        }

        [Fact]
        public void the_delete_fails_with_argument_exception()
        {
            try
            {
                _conn.DeletePersistentSubscriptionForAllAsync(Guid.NewGuid().ToString(), DefaultData.AdminCredentials).Wait();
                throw new Exception("expected exception");
            }
            catch (Exception ex)
            {
                Assert.IsType(typeof(AggregateException), ex);
                var inner = ex.InnerException;
                Assert.IsType(typeof(InvalidOperationException), inner);
            }
        }
    }


    [TestFixture, Category("LongRunning")]
    public class deleting_persistent_subscription_group_on_all_without_permissions : SpecificationWithMiniNode
    {
        protected override void When()
        {
        }

        [Fact]
        public void the_delete_fails_with_access_denied()
        {
            try
            {
                _conn.DeletePersistentSubscriptionForAllAsync(Guid.NewGuid().ToString()).Wait();
                throw new Exception("expected exception");
            }
            catch (Exception ex)
            {
                Assert.IsType(typeof(AggregateException), ex);
                var inner = ex.InnerException;
                Assert.IsType(typeof(AccessDeniedException), inner);
            }
        }
    }
*/

}
