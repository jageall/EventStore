﻿using System;
using System.Net;
using EventStore.Common.Utils;
using EventStore.Core.Messages;
using EventStore.Core.Tests.Helpers;
using EventStore.Transport.Http;
using EventStore.Transport.Http.Codecs;
using Xunit;

namespace EventStore.Core.Tests.Services.Transport.Http
{
    [MightyMooseIgnore]
    public class ping_controller_should : IUseFixture<LeasedPort>, IDisposable
    {
        private IPEndPoint _serverEndPoint;
        private PortableServer _portableServer;

        public void SetFixture(LeasedPort data)
        {
            _serverEndPoint = new IPEndPoint(IPAddress.Loopback, data.Port);
            _portableServer = new PortableServer(_serverEndPoint);
            _portableServer.SetUp();
        }

        public void Dispose()
        {
            _portableServer.TearDown();
        }

        [Fact]
        [Trait("Category", "LongRunning")]
        public void respond_with_httpmessage_text_message()
        {
            var url = _serverEndPoint.ToHttpUrl("/ping?format=json");
            Func<HttpResponse, bool> verifier = response => Codec.Json.From<HttpMessage.TextMessage>(response.Body) != null;

            var result = _portableServer.StartServiceAndSendRequest(HttpBootstrap.RegisterPing, url, verifier);
            Assert.True(result.Item1, result.Item2);
        }

        [Fact]
        [Trait("Category", "LongRunning")]
        public void return_response_in_json_if_requested_by_query_param_and_set_content_type_header()
        {
            var url = _serverEndPoint.ToHttpUrl("/ping?format=json");
            Func<HttpResponse, bool> verifier = response => string.Equals(StripAdditionalAttributes(response.Headers[HttpResponseHeader.ContentType]),
                                                            ContentType.Json,
                                                            StringComparison.InvariantCultureIgnoreCase);

            var result = _portableServer.StartServiceAndSendRequest(HttpBootstrap.RegisterPing, url, verifier);
            Assert.True(result.Item1, result.Item2);
        }

        [Fact]
        [Trait("Category", "LongRunning")]
        public void return_response_in_xml_if_requested_by_query_param_and_set_content_type_header()
        {
            var url = _serverEndPoint.ToHttpUrl("/ping?format=xml");
            Func<HttpResponse, bool> verifier = response => string.Equals(StripAdditionalAttributes(response.Headers[HttpResponseHeader.ContentType]),
                                                            ContentType.Xml,
                                                            StringComparison.InvariantCultureIgnoreCase);

            var result = _portableServer.StartServiceAndSendRequest(HttpBootstrap.RegisterPing, url, verifier);
            Assert.True(result.Item1, result.Item2);
        }

        [Fact]
        [Trait("Category", "LongRunning")]
        public void return_response_in_plaintext_if_requested_by_query_param_and_set_content_type_header()
        {
            var url = _serverEndPoint.ToHttpUrl("/ping?format=text");
            Func<HttpResponse, bool> verifier = response => string.Equals(StripAdditionalAttributes(response.Headers[HttpResponseHeader.ContentType]),
                                                            ContentType.PlainText,
                                                            StringComparison.InvariantCultureIgnoreCase);

            var result = _portableServer.StartServiceAndSendRequest(HttpBootstrap.RegisterPing, url, verifier);
            Assert.True(result.Item1, result.Item2);
        }

        private string StripAdditionalAttributes(string value)
        {
            var index = value.IndexOf(';');
            if (index < 0)
                return value;
            else
                return value.Substring(0, index);

        }
    }
}
