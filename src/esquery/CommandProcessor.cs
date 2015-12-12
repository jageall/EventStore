using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading;
using Newtonsoft.Json.Linq;

namespace esquery
{
    class CommandProcessor
    {
        static string GetFirst(string s)
        {
            if (string.IsNullOrEmpty(s)) return null;
            var next = s.IndexOf(char.IsWhiteSpace);
            if (next < 0) return null;
            return s.Substring(0, next);
        }

        static List<string> EatFirstN(int n, string s)
        {
            var ret = new List<string>();
            var current = 0;
            for (int i = 0; i < n; i++)
            {
                var next = s.IndexOfAfter(current, char.IsWhiteSpace);
                if (next < 0) { return null; }
                ret.Add(s.Substring(current, next - current));
                current = next + 1;
            }
            //get whatever else is on the line.
            ret.Add(s.Substring(current, s.Length - current));
            return ret;
        }

        public static object Process(string command, State state)
        {
            var c = GetFirst(command);
            if (c == null) return new HelpCommandResult();
            try
            {
                switch (c)
                {
                    case "a":
                    case "append":
                        var append = EatFirstN(3, command);
                        if (append.Count != 4) return new InvalidCommandResult(command);
                        return Append(state.Args.BaseUri, append[1], append[2], append[3]);
                    case "h":
                    case "help":
                        return new HelpCommandResult();
                    case "q":
                    case "query":
                        var query = EatFirstN(1, command);
                        if (query.Count != 2) return new InvalidCommandResult(command);
                        return CreateAndRunQuery(state.Args.BaseUri, query[1], state.Args.Credentials, state.Piped);
                    case "s":
                    case "subscribe":
                        var sub = EatFirstN(2, command);
                        if (sub.Count != 3) return new InvalidCommandResult(command);
                        return Subscribe(state.Args.BaseUri, sub[1], state.Args.Credentials, state.Piped);
                    default:
                        return new InvalidCommandResult(command);
                }
            } catch (Exception ex)
            {
                return new ExceptionResult(command, ex);
            }
        }

        private static Uri PostQuery(Uri baseUri, string query, NetworkCredential credential)
        {
            var request = new HttpRequestMessage(HttpMethod.Post, baseUri.AbsoluteUri + "projections/transient?enabled=yes");
            request.Content = new StringContent(query, Encoding.UTF8, "application/json");

            var client = CreateClient(credential);
            var response = client.SendAsync(request).Result;
            var s = response.Content.ReadAsStringAsync().Result;

            if (response.StatusCode != HttpStatusCode.Created)
            {
                throw new Exception("Query Failed with Status Code: " + response.StatusCode + "\n" + s);
            }
            return response.Headers.Location;
        }

        public static HttpClient CreateClient(NetworkCredential credential)
        {
            var handler = new HttpClientHandler
            {
                PreAuthenticate = true,
                //TODO take from args
                Credentials = credential
            };

            var client = new HttpClient(handler);
            return client;
        }

        private static QueryInformation CheckQueryStatus(Uri toCheck, NetworkCredential credential)
        {
            //TODO take from args
            var client = CreateClient(credential);
            var request = new HttpRequestMessage(HttpMethod.Get, toCheck);
            using (var response = client.SendAsync(request).Result)
            {
                var s = response.Content.ReadAsStringAsync().Result;
                JObject json = JObject.Parse(s);
                if (response.StatusCode != HttpStatusCode.OK)
                {
                    throw new Exception("Query Polling Failed with Status Code: " + response.StatusCode + "\n" + s);
                }
                var faulted = json["status"].Value<string>().StartsWith("Faulted");
                var completed = json["status"].Value<string>().StartsWith("Completed");
                var faultReason = json["stateReason"].Value<string>();
                var streamurl = json["resultStreamUrl"];
                var resulturl = json["resultUrl"];
                var cancelurl = json["disableCommandUrl"];
                Uri resultstreamuri = null;
                if (streamurl != null)
                    resultstreamuri = new Uri(streamurl.Value<string>());
                Uri resulturi = null;
                if (resulturl != null)
                    resulturi = new Uri(resulturl.Value<string>());

                Uri canceluri = null;
                if (cancelurl != null)
                    canceluri = new Uri(cancelurl.Value<string>());
                var progress = json["progress"].Value<decimal>();

                return new QueryInformation()
                {
                    Faulted = faulted,
                    FaultReason = faultReason,
                    IsStreamResult = streamurl != null,
                    ResultStreamUrl = resultstreamuri,
                    ResultUrl = resulturi,
                    Progress = progress,
                    Completed = completed,
                    CancelUri = canceluri
                };
            }
        }

        private static Uri GetNamedLink(JObject feed, string name)
        {
            var links = feed["links"];
            if (links == null) return null;
            return (from item in links
                    where item["relation"].Value<string>() == name
                    select new Uri(item["uri"].Value<string>())).FirstOrDefault();
        }

        private static Uri GetLast(Uri head, NetworkCredential credential)
        {
            var client = CreateClient(credential);
            var request = new HttpRequestMessage(HttpMethod.Get, head);
            request.Headers.Accept.ParseAdd("application/vnd.eventstore.atom+json");

            using (var response = client.SendAsync(request).Result)
            {

                var json = JObject.Parse(response.Content.ReadAsStringAsync().Result);
                var last = GetNamedLink(json, "last");
                return last ?? GetNamedLink(json, "self");
            }
        }

        private static string GetResult(Uri head, NetworkCredential credential)
        {
            var request = new HttpRequestMessage(HttpMethod.Get, head);
            request.Headers.Accept.ParseAdd("application/json");

            var client = CreateClient(credential);

            using (var response = client.SendAsync(request).Result)
            {

                var json = JObject.Parse(response.Content.ReadAsStringAsync().Result);
                Console.WriteLine("Result: \n\n" + json.ToString());
                return json.ToString();
            }
        }

        private static Uri GetPrevFromHead(Uri head, NetworkCredential credential)
        {
            var client = CreateClient(credential);
            var request = new HttpRequestMessage(HttpMethod.Get, head);
            request.Headers.Accept.ParseAdd("application/vnd.eventstore.atom+json");

            using (var response = client.SendAsync(request).Result)
            {

                var json = JObject.Parse(response.Content.ReadAsStringAsync().Result);
                return GetNamedLink(json, "previous");
            }
        }

        static Uri ReadResults(Uri uri, NetworkCredential credential)
        {
            var client = CreateClient(credential);
            var request = new HttpRequestMessage(HttpMethod.Get, new Uri(uri.AbsoluteUri + "?embed=body"));
            request.Headers.Accept.ParseAdd("application/vnd.eventstore.atom+json");
            request.Headers.Add("ES-LongPoll", "1"); //add long polling
            using (var response = client.SendAsync(request).Result)
            {
                var json = JObject.Parse(response.Content.ReadAsStringAsync().Result);
                if (json["entries"] != null)
                {
                    foreach (var item in json["entries"])
                    {
                        Console.WriteLine(item["title"].ToString());
                        if (item["data"] != null)
                        {
                            Console.WriteLine(item["data"].ToString());
                        } else
                        {
                            Console.WriteLine(GetData(item, credential));
                        }
                    }
                    return GetNamedLink(json, "previous") ?? uri;
                }
            }
            return uri;
        }

        private static string GetData(JToken item, NetworkCredential credential)
        {
            var links = item["links"];
            if (links == null) return "unable to get link.";
            foreach (var c in links)
            {
                var rel = c["relation"];
                if (rel == null) continue;
                var r = rel.Value<string>();
                if (r.ToLower() == "alternate")
                {
                    var client = CreateClient(credential);
                    var request = new HttpRequestMessage(HttpMethod.Get, new Uri(c["uri"].Value<string>()));
                    request.Headers.Accept.ParseAdd("application/json");
                    using (var response = client.SendAsync(request).Result)
                    {
                        return response.Content.ReadAsStringAsync().Result;
                    }
                }
            }
            return "relation link not found";
        }

        private static object CreateAndRunQuery(Uri baseUri, string query, NetworkCredential credential, bool piped)
        {
            try
            {
                var watch = new Stopwatch();
                watch.Start();
                var toCheck = PostQuery(baseUri, query, credential);
                var queryInformation = new QueryInformation();
                if (!piped)
                    Console.WriteLine("Query started. Press esc to cancel.");
                while (!queryInformation.Completed)
                {
                    queryInformation = CheckQueryStatus(toCheck, credential);
                    if (queryInformation.Faulted)
                    {
                        throw new Exception("Query Faulted.\n" + queryInformation.FaultReason);
                    }
                    Console.Write("\r{0}", queryInformation.Progress.ToString("f2") + "%");

                    if (!piped && Console.KeyAvailable && Console.ReadKey(true).Key == ConsoleKey.Escape)
                    {
                        Console.WriteLine("\nCancelling query.");
                        Cancel(queryInformation);
                        return new QueryCancelledResult();
                    }
                    Thread.Sleep(500);
                }
                Console.WriteLine("\rQuery Completed in: " + watch.Elapsed);

                if (queryInformation.IsStreamResult)
                {
                    var last = GetLast(queryInformation.ResultStreamUrl, credential);
                    last = new Uri(last.OriginalString + "?embed=body");
                    var next = ReadResults(last, credential);
                    return new QueryResult();
                } else
                {
                    GetResult(queryInformation.ResultUrl, credential);
                    return new QueryResult();

                }
            } catch (Exception ex)
            {
                return new ErrorResult(ex);
            }
        }

        private static object Subscribe(Uri baseUri, string stream, NetworkCredential credential, bool piped)
        {
            try
            {

                var previous = GetPrevFromHead(new Uri(baseUri.AbsoluteUri + "streams/" + stream + "?embed=body"), credential);
                if (!piped)
                    Console.WriteLine("Beginning Subscription. Press esc to cancel.");
                var uri = previous;
                while (true)
                {
                    uri = ReadResults(uri, credential);
                    if (!piped && Console.KeyAvailable && Console.ReadKey(true).Key == ConsoleKey.Escape)
                    {
                        Console.WriteLine("\nCancelling query.");
                        return new SubscriptionCancelledResult();
                    }
                }
            } catch (Exception ex)
            {
                return new ErrorResult(ex);
            }
        }

        private static void Cancel(QueryInformation queryInformation)
        {
            if (queryInformation.CancelUri == null) return;
            var client = new HttpClient();
            var request = new HttpRequestMessage(HttpMethod.Post, queryInformation.CancelUri);
            
            using (var response = client.SendAsync(request).Result)
            { }
        }

        private static AppendResult Append(Uri baseUri, string stream, string eventType, string data)
        {
            var message = "[{'eventType':'" + eventType + "', 'eventId' :'" + Guid.NewGuid() + "', 'data' : " + data + "}]";
            var client = new HttpClient();
            var request = new HttpRequestMessage(HttpMethod.Post, baseUri.AbsoluteUri + "streams/" + stream);
            request.Content = new StringContent(message, Encoding.UTF8, "application/vnd.eventstore.events+json");
            
            using (var response = client.SendAsync(request).Result)
            {
                return new AppendResult() { StatusCode = response.StatusCode };
            }
        }

        private class AppendResult
        {
            public HttpStatusCode StatusCode;

            public override string ToString()
            {
                if (StatusCode == HttpStatusCode.Created)
                {
                    return "Succeeded.";
                }
                return StatusCode.ToString();
            }
        }

        private class QueryResult
        {
            public override string ToString()
            {
                return "Query Completed";
            }
        }
    }

    class ExceptionResult
    {
        private readonly string _command;
        private readonly Exception _exception;

        public ExceptionResult(string command, Exception exception)
        {
            _command = command;
            _exception = exception;
        }

        public override string ToString()
        {
            return "Command " + _command + " failed \n" + _exception.ToString();
        }
    }

    class SubscriptionCancelledResult
    {
        public override string ToString()
        {
            return "Subscription Cancelled";
        }
    }

    class QueryCancelledResult
    {
        public override string ToString()
        {
            return "Query Cancelled";
        }
    }

    class QueryInformation
    {
        public bool Faulted;
        public string FaultReason;
        public decimal Progress;
        public bool Completed;
        public Uri CancelUri;
        public bool IsStreamResult;
        public Uri ResultUrl;
        public Uri ResultStreamUrl;
    }

    class ErrorResult
    {
        private Exception _exception;

        public ErrorResult(Exception exception)
        {
            _exception = exception;
        }

        public override string ToString()
        {
            return "An error has occured\n" + _exception.Message;
        }
    }

    class HelpCommandResult
    {
        public override string ToString()
        {
            return "esquery help:\n" + "\th/help: prints help\n" + "\tq/query {js query} executes a query.\n"
                   + "\ta/append {stream} {type} {js object}: appends to a stream.\n"
                   + "\ts/subscribe {stream}: subscribes to a stream.\n";
        }
    }

    class InvalidCommandResult
    {
        private string _command;

        public InvalidCommandResult(string command)
        {
            _command = command;
        }

        public override string ToString()
        {
            return "Invalid command: '" + _command + "'";
        }
    }

    static class IEnumerableExtensions
    {
        public static int IndexOf<TSource>(this IEnumerable<TSource> source, Func<TSource, bool> predicate)
        {
            int i = 0;

            foreach (var element in source)
            {
                if (predicate(element))
                    return i;

                i++;
            }
            return i;
        }

        public static int IndexOfAfter<TSource>(this IEnumerable<TSource> source, int start, Func<TSource, bool> predicate)
        {
            int i = 0;

            foreach (var element in source)
            {
                if (predicate(element) && i >= start)
                    return i;

                i++;
            }
            return i;
        }
    }
}