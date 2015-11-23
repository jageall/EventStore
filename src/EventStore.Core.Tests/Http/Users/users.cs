using System.Net;
using EventStore.Core.Services;
using EventStore.Core.Services.Transport.Http.Controllers;
using EventStore.Core.Tests.Helpers;
using Xunit;
using Newtonsoft.Json.Linq;

namespace EventStore.Core.Tests.Http.Users
{
    namespace users
    {
        public abstract class with_admin_user : HttpBehaviorSpecification

        {
            protected readonly ICredentials _admin = DefaultData.AdminNetworkCredentials;
        }

        public class when_creating_a_user : with_admin_user
        {
            private HttpWebResponse _response;

            protected override void Given()
            {
            }

            protected override void When()
            {
                _response = MakeJsonPost(
                    "/users/",
                    new
                        {
                            LoginName = "test1",
                            FullName = "User Full Name",
                            Groups = new[] {"admin", "other"},
                            Password = "Pa55w0rd!"
                        }, _admin);
            }


            [Fact]
            [Trait("Category", "LongRunning")]
            public void returns_created_status_code_and_location()
            {
                Assert.Equal(HttpStatusCode.Created, _response.StatusCode);
                Assert.Equal(MakeUrl("/users/test1").AbsoluteUri, _response.Headers[HttpResponseHeader.Location]);
            }
        }

        public class when_retrieving_a_user_details : with_admin_user
        {
            private JObject _response;

            protected override void Given()
            {
                MakeJsonPost(
                    "/users/",
                    new
                        {
                            LoginName = "test1",
                            FullName = "User Full Name",
                            Groups = new[] {"admin", "other"},
                            Password = "Pa55w0rd!"
                        }, _admin);
            }

            protected override void When()
            {
                var response = GetJson<JObject>("/users/test1");
                Fixture.AddStashedValueAssignment(this, instance =>
                {
                    instance._response = response;
                });
            }


            [Fact]
            [Trait("Category", "LongRunning")]
            public void returns_ok_status_code()
            {
                Assert.Equal(HttpStatusCode.OK, LastResponse.StatusCode);
            }

            [Fact]
            [Trait("Category", "LongRunning")]
            public void returns_valid_json_data()
            {
                HelperExtensions.AssertJson(
                    new
                        {
                            Success = true,
                            Error = "Success",
                            Data =
                        new
                            {
                                LoginName = "test1",
                                FullName = "User Full Name",
                                Groups = new[] {"admin", "other"},
                                Disabled = false,
                                Password___ = false,
                                Links = new[]
                                {
                                    new
                                    {
                                        Href = "http://" + Node.ExtHttpEndPoint + "/users/test1/command/reset-password",
                                        Rel = "reset-password"
                                    },
                                    new
                                    {
                                        Href = "http://" + Node.ExtHttpEndPoint + "/users/test1/command/change-password",
                                        Rel = "change-password"
                                    },
                                    new
                                    {
                                        Href = "http://" + Node.ExtHttpEndPoint + "/users/test1",
                                        Rel = "edit"
                                    },
                                    new
                                    {
                                        Href = "http://" + Node.ExtHttpEndPoint + "/users/test1",
                                        Rel = "delete"
                                    },
                                    new
                                    {
                                        Href = "http://" + Node.ExtHttpEndPoint + "/users/test1/command/disable",
                                        Rel = "disable"
                                    }
                                }
                            }
                        }, _response);
            }
        }

        public class when_retrieving_a_disabled_user_details : with_admin_user
        {
            private JObject _response;

            protected override void Given()
            {
                MakeJsonPost(
                    "/users/",
                    new
                    {
                        LoginName = "test2",
                        FullName = "User Full Name",
                        Groups = new[] { "admin", "other" },
                        Password = "Pa55w0rd!"
                    }, _admin);

                MakePost("/users/test2/command/disable",_admin);
            }

            protected override void When()
            {
                var response = GetJson<JObject>("/users/test2");
                Fixture.AddStashedValueAssignment(this, instance =>
                {
                    instance._response = response;
                });
            }

            [Fact]
            [Trait("Category", "LongRunning")]
            public void returns_ok_status_code()
            {
                Assert.Equal(HttpStatusCode.OK, LastResponse.StatusCode);
            }

            [Fact]
            [Trait("Category", "LongRunning")]
            public void returns_valid_json_data_with_enable_link()
            {
                HelperExtensions.AssertJson(
                    new
                    {
                        Success = true,
                        Error = "Success",
                        Data =
                                new
                                {
                                    Links = new[]
                                            {
                                                new
                                                {
                                                    Href = "http://" + Node.ExtHttpEndPoint + "/users/test2/command/reset-password",
                                                    Rel = "reset-password"
                                                },
                                                new
                                                {
                                                    Href = "http://" + Node.ExtHttpEndPoint + "/users/test2/command/change-password",
                                                    Rel = "change-password"
                                                },
                                                new
                                                {
                                                    Href = "http://" + Node.ExtHttpEndPoint + "/users/test2",
                                                    Rel = "edit"
                                                },
                                                new
                                                {
                                                    Href = "http://" + Node.ExtHttpEndPoint + "/users/test2",
                                                    Rel = "delete"
                                                },
                                                new
                                                {
                                                    Href = "http://" + Node.ExtHttpEndPoint + "/users/test2/command/enable",
                                                    Rel = "enable"
                                                }
                                            }
                                }
                    }, _response);
            }
        }

        public class when_creating_an_already_existing_user_account : with_admin_user
        {
            private HttpWebResponse _response;

            protected override void Given()
            {
                var response = MakeJsonPost(
                    "/users/", new {LoginName = "test1", FullName = "User Full Name", Password = "Pa55w0rd!"}, _admin);
                Assert.Equal(HttpStatusCode.Created, response.StatusCode);
            }
       
            protected override void When()
            {
                var response = MakeJsonPost(
                    "/users/", new {LoginName = "test1", FullName = "User Full Name", Password = "Pa55w0rd!"}, _admin);
                Fixture.AddStashedValueAssignment(this, instance =>
                {
                    instance._response = response;
                });
            }

            [Fact]
            [Trait("Category", "LongRunning")]
            public void returns_create_status_code_and_location()
            {
                Assert.Equal(HttpStatusCode.Created, _response.StatusCode);
            }
        }

        public class when_creating_an_already_existing_user_account_with_a_different_password : with_admin_user
        {
            private HttpWebResponse _response;

            protected override void Given()
            {
                var response = MakeJsonPost(
                    "/users/", new { LoginName = "test1", FullName = "User Full Name", Password = "Pa55w0rd!" }, _admin);
                Assert.Equal(HttpStatusCode.Created, response.StatusCode);
            }

            protected override void When()
            {
                var response = MakeJsonPost(
                    "/users/", new { LoginName = "test1", FullName = "User Full Name", Password = "AnotherPa55w0rd!" }, _admin);
                Fixture.AddStashedValueAssignment(this, instance =>
                {
                    instance._response = response;
                });
            }

            [Fact]
            [Trait("Category", "LongRunning")]
            public void returns_conflict_status_code_and_location()
            {
                Assert.Equal(HttpStatusCode.Conflict, _response.StatusCode);
            }
        }

        public class when_disabling_an_enabled_user_account : with_admin_user
        {
            private HttpWebResponse _response;

            protected override void Given()
            {
                MakeJsonPost(
                    "/users/", new {LoginName = "test1", FullName = "User Full Name", Password = "Pa55w0rd!"}, _admin);
            }

            protected override void When()
            {
                var response = MakePost("/users/test1/command/disable", _admin);
                Fixture.AddStashedValueAssignment(this, instance =>
                {
                    instance._response = response;
                });
            }

            [Fact]
            [Trait("Category", "LongRunning")]
            public void returns_ok_status_code()
            {
                Assert.Equal(HttpStatusCode.OK, _response.StatusCode);
            }

            [Fact]
            [Trait("Category", "LongRunning")]
            public void disables_it()
            {
                var jsonResponse = GetJson<JObject>("/users/test1");
                HelperExtensions.AssertJson(
                    new {Success = true, Error = "Success", Data = new {LoginName = "test1", Disabled = true}},
                    jsonResponse);
            }
        }

        public class when_enabling_a_disabled_user_account : with_admin_user
        {
            private HttpWebResponse _response;

            protected override void Given()
            {
                MakeJsonPost(
                    "/users/", new {LoginName = "test1", FullName = "User Full Name", Password = "Pa55w0rd!"}, _admin);
                MakePost("/users/test1/command/disable", _admin);
            }

            protected override void When()
            {
                var response = MakePost("/users/test1/command/enable", _admin);
                Fixture.AddStashedValueAssignment(this, instance =>
                {
                    instance._response = response;
                });
            }

            [Fact]
            [Trait("Category", "LongRunning")]
            public void returns_ok_status_code()
            {
                Assert.Equal(HttpStatusCode.OK, _response.StatusCode);
            }

            [Fact]
            [Trait("Category", "LongRunning")]
            public void disables_it()
            {
                var jsonResponse = GetJson<JObject>("/users/test1");
                HelperExtensions.AssertJson(
                    new {Success = true, Error = "Success", Data = new {LoginName = "test1", Disabled = false}},
                    jsonResponse);
            }
        }

        public class when_updating_user_details : with_admin_user
        {
            private HttpWebResponse _response;

            protected override void Given()
            {
                MakeJsonPost(
                    "/users/", new {LoginName = "test1", FullName = "User Full Name", Password = "Pa55w0rd!"}, _admin);
            }

            protected override void When()
            {
                var response = MakeRawJsonPut("/users/test1", new {FullName = "Updated Full Name"}, _admin);
                Fixture.AddStashedValueAssignment(this, instance =>
                {
                    instance._response = response;
                });
            }

            [Fact]
            [Trait("Category", "LongRunning")]
            public void returns_ok_status_code()
            {
                Assert.Equal(HttpStatusCode.OK, _response.StatusCode);
            }

            [Fact]
            [Trait("Category", "LongRunning")]
            public void updates_full_name()
            {
                var jsonResponse = GetJson<JObject>("/users/test1");
                HelperExtensions.AssertJson(
                    new { Success = true, Error = "Success", Data = new { FullName = "Updated Full Name" } }, jsonResponse);
            }
        }


        public class when_resetting_a_password : with_admin_user
        {
            private HttpWebResponse _response;

            protected override void Given()
            {
                MakeJsonPost(
                    "/users/", new {LoginName = "test1", FullName = "User Full Name", Password = "Pa55w0rd!"}, _admin);
            }

            protected override void When()
            {
                var response = MakeJsonPost(
                    "/users/test1/command/reset-password", new {NewPassword = "NewPassword!"}, _admin);
                Fixture.AddStashedValueAssignment(this, instance =>
                {
                    instance._response = response;
                });
            }

            [Fact]
            [Trait("Category", "LongRunning")]
            public void returns_ok_status_code()
            {
                Assert.Equal(HttpStatusCode.OK, _response.StatusCode);
            }

            [Fact]
            [Trait("Category", "LongRunning")]
            public void can_change_password_using_the_new_password()
            {
                var response = MakeJsonPost(
                    "/users/test1/command/change-password",
                    new {CurrentPassword = "NewPassword!", NewPassword = "TheVeryNewPassword!"});
                Assert.Equal(HttpStatusCode.OK, response.StatusCode);
            }
        }


        public class when_deleting_a_user_account : with_admin_user
        {
            private HttpWebResponse _response;

            protected override void Given()
            {
                MakeJsonPost(
                    "/users/", new {LoginName = "test1", FullName = "User Full Name", Password = "Pa55w0rd!"}, _admin);
            }

            protected override void When()
            {
                var response = MakeDelete("/users/test1", _admin);
                Fixture.AddStashedValueAssignment(this, instance =>
                {
                    instance._response = response;
                });
            }

            [Fact]
            [Trait("Category", "LongRunning")]
            public void returns_ok_status_code()
            {
                Assert.Equal(HttpStatusCode.OK, _response.StatusCode);
            }

            [Fact]
            [Trait("Category", "LongRunning")]
            public void get_returns_not_found()
            {
                GetJson<JObject>("/users/test1");
                Assert.Equal(HttpStatusCode.NotFound, LastResponse.StatusCode);
            }
        }
    }
}
