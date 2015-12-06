﻿using EventStore.ClientAPI.Exceptions;
using EventStore.ClientAPI.SystemData;
using Xunit;

namespace EventStore.Core.Tests.ClientAPI.Security
{
    public class transactional_write_stream_security : AuthenticationTestBase
    {
        [Fact][Trait("Category", "LongRunning")][Trait("Category", "Network")]
        public void starting_transaction_with_not_existing_credentials_is_not_authenticated()
        {
            Expect<NotAuthenticatedException>(() => TransStart("write-stream", "badlogin", "badpass"));
        }

        [Fact][Trait("Category", "LongRunning")][Trait("Category", "Network")]
        public void starting_transaction_to_stream_with_no_credentials_is_denied()
        {
            Expect<AccessDeniedException>(() => TransStart("write-stream", null, null));
        }

        [Fact][Trait("Category", "LongRunning")][Trait("Category", "Network")]
        public void starting_transaction_to_stream_with_not_authorized_user_credentials_is_denied()
        {
            Expect<AccessDeniedException>(() => TransStart("write-stream", "user2", "pa$$2"));
        }

        [Fact][Trait("Category", "LongRunning")][Trait("Category", "Network")]
        public void starting_transaction_to_stream_with_authorized_user_credentials_succeeds()
        {
            ExpectNoException(() => TransStart("write-stream", "user1", "pa$$1"));
        }

        [Fact][Trait("Category", "LongRunning")][Trait("Category", "Network")]
        public void starting_transaction_to_stream_with_admin_user_credentials_succeeds()
        {
            ExpectNoException(() => TransStart("write-stream", "adm", "admpa$$"));
        }


        [Fact][Trait("Category", "LongRunning")][Trait("Category", "Network")]
        public void committing_transaction_with_not_existing_credentials_is_not_authenticated()
        {
            var transId = TransStart("write-stream", "user1", "pa$$1").TransactionId;
            var t2 = Connection.ContinueTransaction(transId, new UserCredentials("badlogin", "badpass"));
            t2.WriteAsync(CreateEvents()).Wait();
            Expect<NotAuthenticatedException>(() => t2.CommitAsync().Wait());
        }

        [Fact][Trait("Category", "LongRunning")][Trait("Category", "Network")]
        public void committing_transaction_to_stream_with_no_credentials_is_denied()
        {
            var transId = TransStart("write-stream", "user1", "pa$$1").TransactionId;
            var t2 = Connection.ContinueTransaction(transId);
            t2.WriteAsync().Wait();
            Expect<AccessDeniedException>(() => t2.CommitAsync().Wait());
        }

        [Fact][Trait("Category", "LongRunning")][Trait("Category", "Network")]
        public void committing_transaction_to_stream_with_not_authorized_user_credentials_is_denied()
        {
            var transId = TransStart("write-stream", "user1", "pa$$1").TransactionId;
            var t2 = Connection.ContinueTransaction(transId, new UserCredentials("user2", "pa$$2"));
            t2.WriteAsync().Wait();
            Expect<AccessDeniedException>(() => t2.CommitAsync().Wait());
        }

        [Fact][Trait("Category", "LongRunning")][Trait("Category", "Network")]
        public void committing_transaction_to_stream_with_authorized_user_credentials_succeeds()
        {
            var transId = TransStart("write-stream", "user1", "pa$$1").TransactionId;
            var t2 = Connection.ContinueTransaction(transId, new UserCredentials("user1", "pa$$1"));
            t2.WriteAsync().Wait();
            ExpectNoException(() => t2.CommitAsync().Wait());
        }

        [Fact][Trait("Category", "LongRunning")][Trait("Category", "Network")]
        public void committing_transaction_to_stream_with_admin_user_credentials_succeeds()
        {
            var transId = TransStart("write-stream", "user1", "pa$$1").TransactionId;
            var t2 = Connection.ContinueTransaction(transId, new UserCredentials("adm", "admpa$$"));
            t2.WriteAsync().Wait();
            ExpectNoException(() => t2.CommitAsync().Wait());
        }


        [Fact][Trait("Category", "LongRunning")][Trait("Category", "Network")]
        public void transaction_to_no_acl_stream_succeeds_when_no_credentials_are_passed()
        {
            ExpectNoException(() =>
            {
                var t = TransStart("noacl-stream", null, null);
                t.WriteAsync(CreateEvents()).Wait();
                t.CommitAsync().Wait();
            });
        }

        [Fact][Trait("Category", "LongRunning")][Trait("Category", "Network")]
        public void transaction_to_no_acl_stream_is_not_authenticated_when_not_existing_credentials_are_passed()
        {
            Expect<NotAuthenticatedException>(() => TransStart("noacl-stream", "badlogin", "badpass"));
        }

        [Fact][Trait("Category", "LongRunning")][Trait("Category", "Network")]
        public void transaction_to_no_acl_stream_succeeds_when_any_existing_user_credentials_are_passed()
        {
            ExpectNoException(() =>
            {
                var t = TransStart("noacl-stream", "user1", "pa$$1");
                t.WriteAsync(CreateEvents()).Wait();
                t.CommitAsync().Wait();
            });
            ExpectNoException(() =>
            {
                var t = TransStart("noacl-stream", "user2", "pa$$2");
                t.WriteAsync(CreateEvents()).Wait();
                t.CommitAsync().Wait();
            });
        }

        [Fact][Trait("Category", "LongRunning")][Trait("Category", "Network")]
        public void transaction_to_no_acl_stream_succeeds_when_admin_user_credentials_are_passed()
        {
            ExpectNoException(() =>
            {
                var t = TransStart("noacl-stream", "adm", "admpa$$");
                t.WriteAsync(CreateEvents()).Wait();
                t.CommitAsync().Wait();
            });
        }


        [Fact][Trait("Category", "LongRunning")][Trait("Category", "Network")]
        public void transaction_to_all_access_normal_stream_succeeds_when_no_credentials_are_passed()
        {
            ExpectNoException(() =>
            {
                var t = TransStart("normal-all", null, null);
                t.WriteAsync(CreateEvents()).Wait();
                t.CommitAsync().Wait();
            });
        }

        [Fact][Trait("Category", "LongRunning")][Trait("Category", "Network")]
        public void transaction_to_all_access_normal_stream_is_not_authenticated_when_not_existing_credentials_are_passed()
        {
            Expect<NotAuthenticatedException>(() => TransStart("normal-all", "badlogin", "badpass"));
        }

        [Fact][Trait("Category", "LongRunning")][Trait("Category", "Network")]
        public void transaction_to_all_access_normal_stream_succeeds_when_any_existing_user_credentials_are_passed()
        {
            ExpectNoException(() =>
            {
                var t = TransStart("normal-all", "user1", "pa$$1");
                t.WriteAsync(CreateEvents()).Wait();
                t.CommitAsync().Wait();
            });
            ExpectNoException(() =>
            {
                var t = TransStart("normal-all", "user2", "pa$$2");
                t.WriteAsync(CreateEvents()).Wait();
                t.CommitAsync().Wait();
            });
        }

        [Fact][Trait("Category", "LongRunning")][Trait("Category", "Network")]
        public void transaction_to_all_access_normal_stream_succeeds_when_admin_user_credentials_are_passed()
        {
            ExpectNoException(() =>
            {
                var t = TransStart("normal-all", "adm", "admpa$$");
                t.WriteAsync(CreateEvents()).Wait();
                t.CommitAsync().Wait();
            });
        }
    }
}