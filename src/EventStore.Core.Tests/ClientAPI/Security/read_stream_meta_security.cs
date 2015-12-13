﻿using EventStore.ClientAPI.Exceptions;
using Xunit;

namespace EventStore.Core.Tests.ClientAPI.Security
{
    public class read_stream_meta_security : AuthenticationTestBase
    {
        public read_stream_meta_security(Fixture fixture): base(fixture)
        {
        }
        [Fact][Trait("Category", "LongRunning")][Trait("Category", "Network")]
        public void reading_stream_meta_with_not_existing_credentials_is_not_authenticated()
        {
            Expect<NotAuthenticatedException>(() => ReadMeta("metaread-stream", "badlogin", "badpass"));
        }

        [Fact][Trait("Category", "LongRunning")][Trait("Category", "Network")]
        public void reading_stream_meta_with_no_credentials_is_denied()
        {
            Expect<AccessDeniedException>(() => ReadMeta("metaread-stream", null, null));
        }

        [Fact][Trait("Category", "LongRunning")][Trait("Category", "Network")]
        public void reading_stream_meta_with_not_authorized_user_credentials_is_denied()
        {
            Expect<AccessDeniedException>(() => ReadMeta("metaread-stream", "user2", "pa$$2"));
        }

        [Fact][Trait("Category", "LongRunning")][Trait("Category", "Network")]
        public void reading_stream_meta_with_authorized_user_credentials_succeeds()
        {
            ExpectNoException(() => ReadMeta("metaread-stream", "user1", "pa$$1"));
        }

        [Fact][Trait("Category", "LongRunning")][Trait("Category", "Network")]
        public void reading_stream_meta_with_admin_user_credentials_succeeds()
        {
            ExpectNoException(() => ReadMeta("metaread-stream", "adm", "admpa$$"));
        }


        [Fact][Trait("Category", "LongRunning")][Trait("Category", "Network")]
        public void reading_no_acl_stream_meta_succeeds_when_no_credentials_are_passed()
        {
            ExpectNoException(() => ReadMeta("noacl-stream", null, null));
        }

        [Fact][Trait("Category", "LongRunning")][Trait("Category", "Network")]
        public void reading_no_acl_stream_meta_is_not_authenticated_when_not_existing_credentials_are_passed()
        {
            Expect<NotAuthenticatedException>(() => ReadMeta("noacl-stream", "badlogin", "badpass"));
        }

        [Fact][Trait("Category", "LongRunning")][Trait("Category", "Network")]
        public void reading_no_acl_stream_meta_succeeds_when_any_existing_user_credentials_are_passed()
        {
            ExpectNoException(() => ReadMeta("noacl-stream", "user1", "pa$$1"));
            ExpectNoException(() => ReadMeta("noacl-stream", "user2", "pa$$2"));
        }

        [Fact][Trait("Category", "LongRunning")][Trait("Category", "Network")]
        public void reading_no_acl_stream_meta_succeeds_when_admin_user_credentials_are_passed()
        {
            ExpectNoException(() => ReadMeta("noacl-stream", "adm", "admpa$$"));
        }


        [Fact][Trait("Category", "LongRunning")][Trait("Category", "Network")]
        public void reading_all_access_normal_stream_meta_succeeds_when_no_credentials_are_passed()
        {
            ExpectNoException(() => ReadMeta("normal-all", null, null));
        }

        [Fact][Trait("Category", "LongRunning")][Trait("Category", "Network")]
        public void reading_all_access_normal_stream_meta_is_not_authenticated_when_not_existing_credentials_are_passed()
        {
            Expect<NotAuthenticatedException>(() => ReadMeta("normal-all", "badlogin", "badpass"));
        }

        [Fact][Trait("Category", "LongRunning")][Trait("Category", "Network")]
        public void reading_all_access_normal_stream_meta_succeeds_when_any_existing_user_credentials_are_passed()
        {
            ExpectNoException(() => ReadMeta("normal-all", "user1", "pa$$1"));
            ExpectNoException(() => ReadMeta("normal-all", "user2", "pa$$2"));
        }

        [Fact][Trait("Category", "LongRunning")][Trait("Category", "Network")]
        public void reading_all_access_normal_stream_meta_succeeds_when_admin_user_credentials_are_passed()
        {
            ExpectNoException(() => ReadMeta("normal-all", "adm", "admpa$$"));
        }
    }
}