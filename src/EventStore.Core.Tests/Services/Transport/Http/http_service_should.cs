using System;
using System.Net;
using System.Threading;
using EventStore.Core.Messages;
using EventStore.Core.Tests.Helpers;
using EventStore.Transport.Http;
using Xunit;
using EventStore.Common.Utils;
using System.Linq;
using System.Net.Http;
using EventStore.Transport.Http.Client;
using HttpStatusCode = System.Net.HttpStatusCode;

namespace EventStore.Core.Tests.Services.Transport.Http
{
    public class LeasedPort : IDisposable
    {
        public readonly int Port;

        public LeasedPort()
        {
            Port = PortsHelper.GetAvailablePort(IPAddress.Loopback);
        }

        public virtual void Dispose()
        {
            PortsHelper.ReturnPort(Port);
        }
    }

    public class http_service_should : IClassFixture<http_service_should.Fixture>, IDisposable
    {
        private Fixture _fixture;
        public IPEndPoint ServerEndPoint { get { return _fixture.ServerEndPoint; } }
        public PortableServer PortableServer { get { return _fixture.PortableServer; } }
        public class Fixture : LeasedPort
        {
            public readonly IPEndPoint ServerEndPoint;
            public readonly PortableServer PortableServer;
            private readonly Exception _setupException;

            public Fixture()
            {
                try
                {

                    ServerEndPoint = new IPEndPoint(IPAddress.Loopback, Port);
                    PortableServer = new PortableServer(ServerEndPoint);
                    PortableServer.SetUp();
                }
                catch (Exception ex)
                {
                    _setupException = ex;
                }
            }

            public override void Dispose()
            {
                if(PortableServer != null)
                    PortableServer.TearDown();
                base.Dispose();
            }

            public void EnsureInitialized()
            {
                if(_setupException != null)
                    throw new ApplicationException("Fixture setup failed", _setupException);
            }
        }

        public void SetFixture(Fixture fixture)
        {
            _fixture = fixture;
            fixture.EnsureInitialized();
            PortableServer.SetUp();
        }

        public void Dispose()
        {
            PortableServer.TearDown();
        }

        [Fact]
        [Trait("Category", "Network")]
        public void start_after_system_message_system_init_published()
        {
            Assert.False(PortableServer.IsListening);
            PortableServer.Publish(new SystemMessage.SystemInit());
            Assert.True(PortableServer.IsListening);
        }

        [Fact]
        [Trait("Category", "Network")]
        public void ignore_shutdown_message_that_does_not_say_shut_down()
        {
            PortableServer.Publish(new SystemMessage.SystemInit());
            Assert.True(PortableServer.IsListening);

            PortableServer.Publish(new SystemMessage.BecomeShuttingDown(Guid.NewGuid(), exitProcess: false, shutdownHttp: false));
            Assert.True(PortableServer.IsListening);
        }

        [Fact]
        [Trait("Category", "Network")]
        public void react_to_shutdown_message_that_cause_process_exit()
        {
            PortableServer.Publish(new SystemMessage.SystemInit());
            Assert.True(PortableServer.IsListening);

            PortableServer.Publish(new SystemMessage.BecomeShuttingDown(Guid.NewGuid(), exitProcess: true, shutdownHttp:true));
            Assert.False(PortableServer.IsListening);
        }

        [Fact]
        [Trait("Category", "Network")]
        public void reply_with_404_to_every_request_when_there_are_no_registered_controllers()
        {
            var requests = new[] {"/ping", "/streams", "/gossip", "/stuff", "/notfound", "/magic/url.exe"};
            var successes = new bool[requests.Length];
            var errors = new string[requests.Length];
            var signals = new AutoResetEvent[requests.Length];
            for (var i = 0; i < signals.Length; i++)
                signals[i] = new AutoResetEvent(false);

            PortableServer.Publish(new SystemMessage.SystemInit());

            for (var i = 0; i < requests.Length; i++)
            {
                var i1 = i;
                PortableServer.BuiltInClient.Get(ServerEndPoint.ToHttpUrl(requests[i]),
                            TimeSpan.FromMilliseconds(10000),
                            response =>
                                {
                                    successes[i1] = response.StatusCode == HttpStatusCode.NotFound;
                                    signals[i1].Set();
                                },
                            exception =>
                                {
                                    successes[i1] = false;
                                    errors[i1] = exception.Message;
                                    signals[i1].Set();
                                });
            }

            foreach (var signal in signals)
                signal.WaitOne();

            Assert.True(successes.All(x => x), string.Join(";", errors.Where(e => !string.IsNullOrEmpty(e))));
        }

        [Fact]
        [Trait("Category", "Network")]
        public void handle_invalid_characters_in_url()
        {
            var url = ServerEndPoint.ToHttpUrl("/ping^\"");
            
            Func<HttpResponseMessage, bool> verifier = response => response.Content.Headers.ContentLength == 0 &&
                                                            response.StatusCode == HttpStatusCode.NotFound;
            var result = PortableServer.StartServiceAndSendRequest(HttpBootstrap.RegisterPing, url, verifier);
            Assert.True(result.Item1, result.Item2);
        }
    }
}
