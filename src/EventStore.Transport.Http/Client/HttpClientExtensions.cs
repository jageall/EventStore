using System;
using System.Net.Http;
using System.Text;
using System.Threading;

namespace EventStore.Transport.Http.Client
{
    public static class HttpClientExtensions
    {

        public static void Get(this HttpClient client, string url, TimeSpan timeout, Action<HttpResponseMessage> onSuccess, Action<Exception> onException)
        {

            var request = new HttpRequestMessage(System.Net.Http.HttpMethod.Get, url);
            Send(client, request, timeout, onSuccess, onException);
        }

        public static void Post(this HttpClient client, string url, string content, string contentType, TimeSpan timeout, Action<HttpResponseMessage> onSuccess, Action<Exception> onException)
        {
            var request = new HttpRequestMessage(System.Net.Http.HttpMethod.Post, url)
            {
                Content = new StringContent(content, Encoding.UTF8, contentType)
            };
            Send(client, request, timeout, onSuccess, onException);
        }

        public static void Delete(this HttpClient client, string url, TimeSpan timeout, Action<HttpResponseMessage> onSuccess, Action<Exception> onException)
        {
            var request = new HttpRequestMessage(System.Net.Http.HttpMethod.Delete, url);
            Send(client, request, timeout, onSuccess, onException);
        }

        public static void Put(this HttpClient client, string url, string content, string contentType, TimeSpan timeout, Action<HttpResponseMessage> onSuccess, Action<Exception> onException)
        {
            var request = new HttpRequestMessage(System.Net.Http.HttpMethod.Put, url)
            {
                Content = new StringContent(content, Encoding.UTF8, contentType)
            };
            Send(client, request, timeout, onSuccess, onException);
        }

        private static void Send(HttpClient client, HttpRequestMessage request, TimeSpan timeout, Action<HttpResponseMessage> onSuccess,
            Action<Exception> onException)
        {
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