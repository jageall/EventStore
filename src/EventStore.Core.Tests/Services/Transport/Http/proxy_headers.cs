using System;
using System.Collections.Specialized;
using EventStore.Transport.Http.EntityManagement;
using Xunit;

namespace EventStore.Core.Tests.Services.Transport.Http
{
    public class proxy_headers
    {
        [Fact]
        public void with_no_headers_uri_is_unchanged()
        {
            var inputUri = new Uri("http://www.example.com:1234/path/?key=value#anchor");
            var requestedUri =
                HttpEntity.BuildRequestedUrl(inputUri,
                                             new NameValueCollection());

            Assert.Equal(inputUri, requestedUri);
        }

        [Fact]
        public void with_port_forward_header_only_port_is_changed()
        {
            var inputUri = new Uri("http://www.example.com:1234/path/?key=value#anchor");
            var headers = new NameValueCollection { { "X-Forwarded-Port", "4321" } };
            var requestedUri =
                HttpEntity.BuildRequestedUrl(inputUri, headers);

            Assert.Equal(new Uri("http://www.example.com:4321/path/?key=value#anchor"), requestedUri);
        }

        [Fact]
        public void non_integer_port_forward_header_is_ignored()
        {
            var inputUri = new Uri("http://www.example.com:1234/path/?key=value#anchor");
            var headers = new NameValueCollection { { "X-Forwarded-Port", "abc" } };
            var requestedUri =
                HttpEntity.BuildRequestedUrl(inputUri, headers);

            Assert.Equal(inputUri, requestedUri);
        }

        [Fact]
        public void with_proto_forward_header_only_scheme_is_changed()
        {
            var inputUri = new Uri("http://www.example.com:1234/path/?key=value#anchor");
            var headers = new NameValueCollection { { "X-Forwarded-Proto", "https" } };
            var requestedUri =
                HttpEntity.BuildRequestedUrl(inputUri, headers);

            Assert.Equal(new Uri("https://www.example.com:1234/path/?key=value#anchor"), requestedUri);
        }
    }
}
