﻿using System;
using System.Text;
using EventStore.ClientAPI;
using EventStore.ClientAPI.Exceptions;
using Xunit;

namespace EventStore.Core.Tests.ClientAPI
{
    public class create_persistent_subscription_on_existing_stream : SpecificationWithMiniNode
    {
        private readonly string _stream = Guid.NewGuid().ToString();

        private readonly PersistentSubscriptionSettings _settings = PersistentSubscriptionSettings.Create()
            .DoNotResolveLinkTos()
            .StartFromCurrent();

        protected override void When()
        {
            _conn.AppendToStreamAsync(_stream, ExpectedVersion.Any,
                new EventData(Guid.NewGuid(), "whatever", true, Encoding.UTF8.GetBytes("{'foo' : 2}"), new Byte[0]));
        }

        [Fact]
        [Trait("Category", "LongRunning")]
        public void the_completion_succeeds()
        {
            var ex = Record.Exception(
                () =>
                    _conn.CreatePersistentSubscriptionAsync(_stream, "existing", _settings, DefaultData.AdminCredentials)
                        .Wait());
            Assert.Null(ex);
        }

        public create_persistent_subscription_on_existing_stream(SpecificationFixture fixture) : base(fixture)
        {
        }
    }

    public class create_persistent_subscription_on_non_existing_stream : SpecificationWithMiniNode
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
        public void the_completion_succeeds()
        {
            var ex = Record.Exception(() => _conn.CreatePersistentSubscriptionAsync(_stream, "nonexistinggroup", _settings, DefaultData.AdminCredentials).Wait());
            Assert.Null(ex);
        }

        public create_persistent_subscription_on_non_existing_stream(SpecificationFixture fixture) : base(fixture)
        {
        }
    }

    public class create_duplicate_persistent_subscription_group : SpecificationWithMiniNode
    {
        private readonly string _stream = Guid.NewGuid().ToString();
        private readonly PersistentSubscriptionSettings _settings = PersistentSubscriptionSettings.Create()
                                                                .DoNotResolveLinkTos()
                                                                .StartFromCurrent();
        protected override void When()
        {
            _conn.CreatePersistentSubscriptionAsync(_stream, "group32", _settings, DefaultData.AdminCredentials).Wait();
        }

        [Fact]
        [Trait("Category", "LongRunning")]
        public void the_completion_fails_with_invalid_operation_exception()
        {

            try
            {
                _conn.CreatePersistentSubscriptionAsync(_stream, "group32",_settings, DefaultData.AdminCredentials).Wait();
                throw new Exception("expected exception");
            }
            catch (Exception ex)
            {
                Assert.IsType<AggregateException>(ex);
                var inner = ex.InnerException;
                Assert.IsType<InvalidOperationException>(inner);
            }
        }

        public create_duplicate_persistent_subscription_group(SpecificationFixture fixture) : base(fixture)
        {
        }
    }

    public class can_create_duplicate_persistent_subscription_group_name_on_different_streams : SpecificationWithMiniNode
    {
        private readonly string _stream = Guid.NewGuid().ToString();
        private readonly PersistentSubscriptionSettings _settings = PersistentSubscriptionSettings.Create()
                                                                .DoNotResolveLinkTos()
                                                                .StartFromCurrent();
        protected override void When()
        {
            _conn.CreatePersistentSubscriptionAsync(_stream, "group3211", _settings, DefaultData.AdminCredentials).Wait();
            
        }

        [Fact]
        [Trait("Category", "LongRunning")]
        public void the_completion_succeeds()
        {
            var ex =
                Record.Exception(
                    () =>
                        _conn.CreatePersistentSubscriptionAsync("someother" + _stream, "group3211", _settings,
                            DefaultData.AdminCredentials).Wait());
            Assert.Null(ex);
        }

        public can_create_duplicate_persistent_subscription_group_name_on_different_streams(SpecificationFixture fixture) : base(fixture)
        {
        }
    }

    public class create_persistent_subscription_group_without_permissions : SpecificationWithMiniNode
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
        public void the_completion_succeeds()
        {
            try
            {
                _conn.CreatePersistentSubscriptionAsync(_stream, "group57", _settings, null).Wait();
                throw new Exception("expected exception");
            }
            catch (Exception ex)
            {
                Assert.IsType<AggregateException>(ex);
                var inner = ex.InnerException;
                Assert.IsType<AccessDeniedException>(inner);
            }
        }

        public create_persistent_subscription_group_without_permissions(SpecificationFixture fixture) : base(fixture)
        {
        }
    }


    public class create_persistent_subscription_after_deleting_the_same : SpecificationWithMiniNode
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
            _conn.DeletePersistentSubscriptionAsync(_stream, "existing", DefaultData.AdminCredentials).Wait();
            
        }

        [Fact]
        [Trait("Category", "LongRunning")]
        public void the_completion_succeeds()
        {
            var ex = Record.Exception(() => _conn.CreatePersistentSubscriptionAsync(_stream, "existing", _settings, DefaultData.AdminCredentials).Wait());
            Assert.Null(ex);
        }

        public create_persistent_subscription_after_deleting_the_same(SpecificationFixture fixture) : base(fixture)
        {
        }
    }

//ALL
/*

    [TestFixture, Category("LongRunning")]
    public class create_persistent_subscription_on_all : SpecificationWithMiniNode
    {
        private PersistentSubscriptionCreateResult _result;
        private readonly PersistentSubscriptionSettings _settings = PersistentSubscriptionSettingsBuilder.Create()
                                                                .DoNotResolveLinkTos()
                                                                .StartFromCurrent();

        protected override void When()
        {
            _result = _conn.CreatePersistentSubscriptionForAllAsync("group", _settings, DefaultData.AdminCredentials).Result;
        }

        [Fact]
        public void the_completion_succeeds()
        {
            Assert.Equal(PersistentSubscriptionCreateStatus.Success, _result.Status);
        }
    }


    [TestFixture, Category("LongRunning")]
    public class create_duplicate_persistent_subscription_group_on_all : SpecificationWithMiniNode
    {
        private readonly PersistentSubscriptionSettings _settings = PersistentSubscriptionSettingsBuilder.Create()
                                                                .DoNotResolveLinkTos()
                                                                .StartFromCurrent();
        protected override void When()
        {
            _conn.CreatePersistentSubscriptionForAllAsync("group32", _settings, DefaultData.AdminCredentials).Wait();
        }

        [Fact]
        public void the_completion_fails_with_invalid_operation_exception()
        {
            try
            {
                _conn.CreatePersistentSubscriptionForAllAsync("group32", _settings, DefaultData.AdminCredentials).Wait();
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
    public class create_persistent_subscription_group_on_all_without_permissions : SpecificationWithMiniNode
    {
        private readonly PersistentSubscriptionSettings _settings = PersistentSubscriptionSettingsBuilder.Create()
                                                                .DoNotResolveLinkTos()
                                                                .StartFromCurrent();
        protected override void When()
        {
        }

        [Fact]
        public void the_completion_succeeds()
        {
            try
            {
                _conn.CreatePersistentSubscriptionForAllAsync("group57", _settings, null).Wait();
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
