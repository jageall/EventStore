using System;
using System.Net;
using EventStore.Core.Services;
using Xunit;
using Newtonsoft.Json.Linq;

namespace EventStore.Core.Tests.Http.BasicAuthentication
{
    namespace basic_authentication
    {
        public abstract class with_admin_user : HttpBehaviorSpecification
        {
            protected readonly ICredentials _admin = new NetworkCredential(
                SystemUsers.Admin, SystemUsers.DefaultAdminPassword);

            public with_admin_user(SpecificationFixture data) : base(data)
            {
            }
        }

        public class when_requesting_an_unprotected_resource : with_admin_user
        {
            protected override void Given()
            {
            }

            protected override void When()
            {
                GetJson<JObject>("/test-anonymous");
            }

            [Fact]
            [Trait("Category", "LongRunning")]
            public void returns_ok_status_code()
            {
                Assert.Equal(HttpStatusCode.OK, LastResponse.StatusCode);
            }

            [Fact]
            [Trait("Category", "LongRunning")]
            public void does_not_return_www_authenticate_header()
            {
                Assert.Null(LastResponse.Headers[HttpResponseHeader.WwwAuthenticate]);
            }

            public when_requesting_an_unprotected_resource(SpecificationFixture data) : base(data)
            {
            }
        }

        public class when_requesting_a_protected_resource : with_admin_user
        {
            protected override void Given()
            {
            }

            protected override void When()
            {
                GetJson<JObject>("/test1");
            }

            [Fact]
            [Trait("Category", "LongRunning")]
            public void returns_unauthorized_status_code()
            {
                Assert.Equal(HttpStatusCode.Unauthorized, LastResponse.StatusCode);
            }

            [Fact]
            [Trait("Category", "LongRunning")]
            public void returns_www_authenticate_header()
            {
                Assert.NotNull(LastResponse.Headers[HttpResponseHeader.WwwAuthenticate]);
            }

            public when_requesting_a_protected_resource(SpecificationFixture data) : base(data)
            {
            }
        }

        public class when_requesting_a_protected_resource_with_credentials_provided : with_admin_user
        {
            protected override void Given()
            {
                var response = MakeJsonPost(
                    "/users/", new {LoginName = "test1", FullName = "User Full Name", Password = "Pa55w0rd!"}, _admin);
                Assert.Equal(HttpStatusCode.Created, response.StatusCode);
            }

            protected override void When()
            {
                GetJson<JObject>("/test1", credentials: new NetworkCredential("test1", "Pa55w0rd!"));
            }

            [Fact]
            [Trait("Category", "LongRunning")]
            public void returns_ok_status_code()
            {
                Assert.Equal(HttpStatusCode.OK, LastResponse.StatusCode);
            }

            public when_requesting_a_protected_resource_with_credentials_provided(SpecificationFixture data) : base(data)
            {
            }
        }

        public class when_requesting_a_protected_resource_with_invalid_credentials_provided : with_admin_user
        {
            protected override void Given()
            {
                var response = MakeJsonPost(
                    "/users/", new {LoginName = "test1", FullName = "User Full Name", Password = "Pa55w0rd!"}, _admin);
                Assert.Equal(HttpStatusCode.Created, response.StatusCode);
            }

            protected override void When()
            {
                GetJson<JObject>("/test1", credentials: new NetworkCredential("test1", "InvalidPassword!"));
            }

            [Fact]
            [Trait("Category", "LongRunning")]
            public void returns_unauthorized_status_code()
            {
                Assert.Equal(HttpStatusCode.Unauthorized, LastResponse.StatusCode);
            }

            public when_requesting_a_protected_resource_with_invalid_credentials_provided(SpecificationFixture data) : base(data)
            {
            }
        }


        public class when_requesting_a_protected_resource_with_credentials_of_disabled_user_account : with_admin_user
        {
            protected override void Given()
            {
                var response = MakeJsonPost(
                    "/users/", new {LoginName = "test1", FullName = "User Full Name", Password = "Pa55w0rd!"}, _admin);
                Assert.Equal(HttpStatusCode.Created, response.StatusCode);
                response = MakePost("/users/test1/command/disable", _admin);
                Assert.Equal(HttpStatusCode.OK, response.StatusCode);
            }

            protected override void When()
            {
                GetJson<JObject>("/test1", credentials: new NetworkCredential("test1", "Pa55w0rd!"));
            }

            [Fact]
            [Trait("Category", "LongRunning")]
            public void returns_unauthorized_status_code()
            {
                Assert.Equal(HttpStatusCode.Unauthorized, LastResponse.StatusCode);
            }

            public when_requesting_a_protected_resource_with_credentials_of_disabled_user_account(SpecificationFixture data) : base(data)
            {
            }
        }

        public class when_requesting_a_protected_resource_with_credentials_of_deleted_user_account : with_admin_user
        {
            protected override void Given()
            {
                var response = MakeRawJsonPost(
                    "/users/", new {LoginName = "test1", FullName = "User Full Name", Password = "Pa55w0rd!"}, _admin);
                Assert.Equal(HttpStatusCode.Created, response.StatusCode);
                Console.WriteLine("done with json post");
                response = MakeDelete("/users/test1", _admin);
                Assert.Equal(HttpStatusCode.OK, response.StatusCode);
            }

            protected override void When()
            {
                GetJson<JObject>("/test1", credentials: new NetworkCredential("test1", "Pa55w0rd!"));
            }

            [Fact]
            [Trait("Category", "LongRunning")]
            public void returns_unauthorized_status_code()
            {
                Assert.Equal(HttpStatusCode.Unauthorized, LastResponse.StatusCode);
            }

            public when_requesting_a_protected_resource_with_credentials_of_deleted_user_account(SpecificationFixture data) : base(data)
            {
            }
        }
    }
}
