using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;
using HttpStatusCode = EventStore.ClientAPI.Transport.Http.HttpStatusCode;
using EventStore.ClientAPI.Common.Utils;
using EventStore.ClientAPI.Exceptions;
using EventStore.ClientAPI.SystemData;
using EventStore.ClientAPI.Transport.Http;

namespace EventStore.ClientAPI.UserManagement
{
    internal class UsersClient
    {
        private readonly TimeSpan _operationTimeout;
        private readonly HttpClient _client;

        public UsersClient(ILogger log, TimeSpan operationTimeout)
        {
            _operationTimeout = operationTimeout;
            _client = new HttpClient();
        }

        public Task Enable(IPEndPoint endPoint, string login, UserCredentials userCredentials = null)
        {
            return SendPost(endPoint.ToHttpUrl("/users/{0}/command/enable", login), string.Empty, userCredentials, HttpStatusCode.OK);
        }

        public Task Disable(IPEndPoint endPoint, string login, UserCredentials userCredentials = null)
        {
            return SendPost(endPoint.ToHttpUrl("/users/{0}/command/disable", login), string.Empty, userCredentials, HttpStatusCode.OK);
        }

        public Task Delete(IPEndPoint endPoint, string login, UserCredentials userCredentials = null)
        {
            return SendDelete(endPoint.ToHttpUrl("/users/{0}", login), userCredentials, HttpStatusCode.OK);
        }

        public Task<List<UserDetails>> ListAll(IPEndPoint endPoint, UserCredentials userCredentials = null)
        {
            return SendGet(endPoint.ToHttpUrl("/users/"), userCredentials, HttpStatusCode.OK)
                    .ContinueWith(x =>
                    {
                        if (x.IsFaulted) throw x.Exception;
                        var r = JObject.Parse(x.Result);
                        return r["data"] != null ? r["data"].ToObject<List<UserDetails>>() : null;
                    });
        }

        public Task<UserDetails> GetCurrentUser(IPEndPoint endPoint, UserCredentials userCredentials = null)
        {
            return SendGet(endPoint.ToHttpUrl("/users/$current"), userCredentials, HttpStatusCode.OK)
                .ContinueWith(x =>
                {
                    if (x.IsFaulted) throw x.Exception;
                    var r = JObject.Parse(x.Result);
                    return r["data"] != null ? r["data"].ToObject<UserDetails>() : null;
                });
        }

        public Task<UserDetails> GetUser(IPEndPoint endPoint, string login, UserCredentials userCredentials = null)
        {
            return SendGet(endPoint.ToHttpUrl("/users/{0}", login), userCredentials, HttpStatusCode.OK)
                .ContinueWith(x =>
                {
                    if (x.IsFaulted) throw x.Exception;
                    var r = JObject.Parse(x.Result);
                    return r["data"] != null ? r["data"].ToObject<UserDetails>() : null;
                });
        }

        public Task CreateUser(IPEndPoint endPoint, UserCreationInformation newUser,
            UserCredentials userCredentials = null)
        {
            var userJson = newUser.ToJson();
            return SendPost(endPoint.ToHttpUrl("/users/"), userJson, userCredentials, HttpStatusCode.Created);
        }

        public Task UpdateUser(IPEndPoint endPoint, string login, UserUpdateInformation updatedUser,
            UserCredentials userCredentials)
        {
            return SendPut(endPoint.ToHttpUrl("/users/{0}", login), updatedUser.ToJson(), userCredentials, HttpStatusCode.OK);
        }

        public Task ChangePassword(IPEndPoint endPoint, string login, ChangePasswordDetails changePasswordDetails,
            UserCredentials userCredentials)
        {
            return SendPost(endPoint.ToHttpUrl("/users/{0}/command/change-password", login), changePasswordDetails.ToJson(), userCredentials, HttpStatusCode.OK);
        }

        public Task ResetPassword(IPEndPoint endPoint, string login, ResetPasswordDetails resetPasswordDetails,
            UserCredentials userCredentials = null)
        {
            return SendPost(endPoint.ToHttpUrl("/users/{0}/command/reset-password", login), resetPasswordDetails.ToJson(), userCredentials, HttpStatusCode.OK);
        }

        private Task<string> SendGet(string url, UserCredentials userCredentials, int expectedCode)
        {
            var source = new TaskCompletionSource<string>();
            _client.Get(url,
                userCredentials,
                _operationTimeout,
                response =>
                {
                    if (response.StatusCode == (System.Net.HttpStatusCode)expectedCode)
                        response.Content.ReadAsStringAsync().ContinueWith(x=> source.SetResult(x.Result));
                    else
                        source.SetException(new UserCommandFailedException(
                            (int)response.StatusCode,
                            string.Format("Server returned {0} ({1}) for GET on {2}",
                                response.StatusCode,
                                response.ReasonPhrase,
                                url)));
                },
                source.SetException);

            return source.Task;
        }

        private Task<string> SendDelete(string url, UserCredentials userCredentials, int expectedCode)
        {
            var source = new TaskCompletionSource<string>();
            _client.Delete(url,
                userCredentials,
                _operationTimeout,
                response =>
                {
                    if ((int)response.StatusCode == expectedCode)
                        response.Content.ReadAsStringAsync().ContinueWith(x => source.SetResult(x.Result));
                    else
                        source.SetException(new UserCommandFailedException(
                            (int)response.StatusCode,
                            string.Format("Server returned {0} ({1}) for DELETE on {2}",
                                response.StatusCode,
                                response.ReasonPhrase,
                                url)));
                },
                source.SetException);

            return source.Task;
        }

        private Task SendPut(string url, string content, UserCredentials userCredentials, int expectedCode)
        {
            var source = new TaskCompletionSource<object>();
            _client.Put(url,
                userCredentials,         
                content,
                "application/json",
                _operationTimeout,
                response =>
                {
                    if ((int)response.StatusCode == expectedCode)
                        source.SetResult(null);
                    else
                        source.SetException(new UserCommandFailedException(
                            (int)response.StatusCode,
                            string.Format("Server returned {0} ({1}) for PUT on {2}",
                                response.StatusCode,
                                response.ReasonPhrase,
                                url)));
                },
                source.SetException);

            return source.Task;
        }

        private Task SendPost(string url, string content, UserCredentials userCredentials, int expectedCode)
        {
            var source = new TaskCompletionSource<object>();
            _client.Post(url,
                userCredentials,
                content,
                "application/json",
                _operationTimeout,
                response =>
                {
                    if ((int)response.StatusCode == expectedCode)
                        source.SetResult(null);
                    else if (response.StatusCode == System.Net.HttpStatusCode.Conflict)
                        source.SetException(new UserCommandConflictException((int)response.StatusCode, response.ReasonPhrase));
                    else
                        source.SetException(new UserCommandFailedException(
                            (int)response.StatusCode,
                            string.Format("Server returned {0} ({1}) for POST on {2}",
                                response.StatusCode,
                                response.ReasonPhrase,
                                url)));
                },
                source.SetException);

            return source.Task;
        }
    }
}
