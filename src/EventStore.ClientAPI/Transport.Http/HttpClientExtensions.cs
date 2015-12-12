using System;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading;
using EventStore.ClientAPI.Common.Utils;
using EventStore.ClientAPI.SystemData;

namespace EventStore.ClientAPI.Transport.Http
{
    internal static class HttpClientExtensions
    {
        public static void Get(this HttpClient client, string url, UserCredentials credentials, TimeSpan timeout, Action<HttpResponseMessage> onSuccess, Action<Exception> onException, string hostHeader = null)
        {
            var request = new HttpRequestMessage(System.Net.Http.HttpMethod.Get, url);
            if (!string.IsNullOrEmpty(hostHeader)) request.Headers.Host = hostHeader;
            Send(client, credentials, request, timeout, onSuccess, onException);
        }

        public static void Post(this HttpClient client, string url, UserCredentials credentials, string content, string contentType, TimeSpan timeout, Action<HttpResponseMessage> onSuccess, Action<Exception> onException)
        {
            var request = new HttpRequestMessage(System.Net.Http.HttpMethod.Post, url)
            {
                Content = new StringContent(content, Encoding.UTF8, contentType)
            };
            Send(client, credentials, request, timeout, onSuccess, onException);
        }

        public static void Delete(this HttpClient client, string url, UserCredentials credentials, TimeSpan timeout, Action<HttpResponseMessage> onSuccess, Action<Exception> onException)
        {
            var request = new HttpRequestMessage(System.Net.Http.HttpMethod.Delete, url);
            Send(client, credentials, request, timeout, onSuccess, onException);
        }

        public static void Put(this HttpClient client, string url, UserCredentials credentials, string content, string contentType, TimeSpan timeout, Action<HttpResponseMessage> onSuccess, Action<Exception> onException)
        {
            var request = new HttpRequestMessage(System.Net.Http.HttpMethod.Put, url)
            {
                Content = new StringContent(content, Encoding.UTF8, contentType)
            };
            Send(client, credentials, request, timeout, onSuccess, onException);
        }

        private static void Send(HttpClient client, UserCredentials credentials, HttpRequestMessage request, TimeSpan timeout, Action<HttpResponseMessage> onSuccess,
            Action<Exception> onException)
        {
            if (credentials != null)
            {
                request.Headers.Authorization = new AuthenticationHeaderValue("Basic",
                    Convert.ToBase64String(Helper.UTF8NoBom.GetBytes(credentials.Username + ":" + credentials.Password)));
            }
            var cts = new CancellationTokenSource();
            cts.CancelAfter(timeout);
            client.SendAsync(request, cts.Token).ContinueWith(x =>
            {
                if (x.IsFaulted)
                {
                    onException(x.Exception.InnerException);
                    return;
                }
                if (x.IsCanceled)
                {
                    onException(new TimeoutException()); //todo : timeout exceptiono may be available from 
                    return;
                }

                onSuccess(x.Result);
            });
        }
    }
}