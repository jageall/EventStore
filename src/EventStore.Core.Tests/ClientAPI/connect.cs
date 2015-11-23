using System;
using System.Net;
using System.Threading;
using EventStore.ClientAPI;
using EventStore.ClientAPI.Internal;
using EventStore.Core.Tests.ClientAPI.Helpers;
using EventStore.Core.Tests.Helpers;
using Xunit;
using Xunit.Extensions;

namespace EventStore.Core.Tests.ClientAPI
{
    public class connect
    {
        //TODO GFY THESE NEED TO BE LOOKED AT IN LINUX
        [Theory, Trait("Category", "LongRunning"), Trait("Category", "Network"), Trait("Platform", "WIN")]
        [InlineData(TcpType.Normal)]
        [InlineData(TcpType.Ssl)]
        public void should_not_throw_exception_when_server_is_down(TcpType tcpType)
        {
            var ip = IPAddress.Loopback;
            int port = PortsHelper.GetAvailablePort(ip);
            try
            {
                using (var connection = TestConnection.Create(new IPEndPoint(ip, port), tcpType))
                {
                    Assert.DoesNotThrow(() => connection.ConnectAsync().Wait());
                }
            }
            finally
            {
                PortsHelper.ReturnPort(port);
            }
        }
        //TODO GFY THESE NEED TO BE LOOKED AT IN LINUX
        [Theory, Trait("Category", "LongRunning"), Trait("Category", "Network"), Trait("Platform", "WIN")]
        [InlineData(TcpType.Normal)]
        [InlineData(TcpType.Ssl)]
        public void should_throw_exception_when_trying_to_reopen_closed_connection(TcpType tcpType)
        {
            ClientApiLoggerBridge.Default.Info("Starting '{0}' test...", "should_throw_exception_when_trying_to_reopen_closed_connection");

            var closed = new ManualResetEventSlim();
            var settings = ConnectionSettings.Create()
                                             .EnableVerboseLogging()
                                             .UseCustomLogger(ClientApiLoggerBridge.Default)
                                             .LimitReconnectionsTo(0)
                                             .WithConnectionTimeoutOf(TimeSpan.FromSeconds(10))
                                             .SetReconnectionDelayTo(TimeSpan.FromMilliseconds(0))
                                             .FailOnNoServerResponse();
            if (tcpType == TcpType.Ssl)
                settings.UseSslConnection("ES", false);

            var ip = IPAddress.Loopback;
            int port = PortsHelper.GetAvailablePort(ip);
            try
            {
                using (var connection = EventStoreConnection.Create(settings, new IPEndPoint(ip, port).ToESTcpUri()))
                {
                    connection.Closed += (s, e) => closed.Set();

                    connection.ConnectAsync().Wait();
                    Assert.True(closed.Wait(TimeSpan.FromSeconds(120))); // TCP connection timeout might be even 60 seconds
                    var thrown = Assert.Throws<AggregateException>(() => connection.ConnectAsync().Wait());
                    Assert.IsAssignableFrom<InvalidOperationException>(thrown.InnerException);
                }
            }
            finally
            {
                PortsHelper.ReturnPort(port);
            }
        }

        //TODO GFY THIS TEST TIMES OUT IN LINUX.
        [Theory, Trait("Category", "LongRunning"), Trait("Category", "Network"), Trait("Platform", "WIN")]
        [InlineData(TcpType.Normal)]
        [InlineData(TcpType.Ssl)]
        public void should_close_connection_after_configured_amount_of_failed_reconnections(TcpType tcpType)
        {
            var closed = new ManualResetEventSlim();
            var settings =
                ConnectionSettings.Create()
                                  .EnableVerboseLogging()
                                  .UseCustomLogger(ClientApiLoggerBridge.Default)
                                  .LimitReconnectionsTo(1)
                                  .WithConnectionTimeoutOf(TimeSpan.FromSeconds(10))
                                  .SetReconnectionDelayTo(TimeSpan.FromMilliseconds(0))
                                  .FailOnNoServerResponse();
            if (tcpType == TcpType.Ssl)
                settings.UseSslConnection("ES", false);

            var ip = IPAddress.Loopback;
            int port = PortsHelper.GetAvailablePort(ip);
            try
            {
                using (var connection = EventStoreConnection.Create(settings, new IPEndPoint(ip, port).ToESTcpUri()))
                {
                    connection.Closed += (s, e) => closed.Set();
                    connection.Connected += (s, e) => Console.WriteLine("EventStoreConnection '{0}': connected to [{1}]...", e.Connection.ConnectionName, e.RemoteEndPoint);
                    connection.Reconnecting += (s, e) => Console.WriteLine("EventStoreConnection '{0}': reconnecting...", e.Connection.ConnectionName);
                    connection.Disconnected += (s, e) => Console.WriteLine("EventStoreConnection '{0}': disconnected from [{1}]...", e.Connection.ConnectionName, e.RemoteEndPoint);
                    connection.ErrorOccurred += (s, e) => Console.WriteLine("EventStoreConnection '{0}': error = {1}", e.Connection.ConnectionName, e.Exception); 

                    connection.ConnectAsync().Wait();

                    Assert.True(closed.Wait(TimeSpan.FromSeconds(120))); // TCP connection timeout might be even 60 seconds

                    var thrown =
                        Assert.Throws<AggregateException>(
                            () =>
                                connection.AppendToStreamAsync("stream", ExpectedVersion.EmptyStream,
                                    TestEvent.NewTestEvent()).Wait());

                    Assert.IsAssignableFrom<InvalidOperationException>(thrown.InnerException);
                }
            }
            finally
            {
                PortsHelper.ReturnPort(port);
            }
        }

    }
    
    public class not_connected_tests
    {
        private readonly TcpType _tcpType = TcpType.Normal;

        [Fact]
        [Trait("Category", "LongRunning")]
        public void should_timeout_connection_after_configured_amount_time_on_conenct()
        {
            var closed = new ManualResetEventSlim();
            var settings = 
                ConnectionSettings.Create()
                                  .EnableVerboseLogging()
                                  .UseCustomLogger(ClientApiLoggerBridge.Default)
                                  .LimitReconnectionsTo(0)
                                  .SetReconnectionDelayTo(TimeSpan.FromMilliseconds(0))
                                  .FailOnNoServerResponse()
                                  .WithConnectionTimeoutOf(TimeSpan.FromMilliseconds(1000));

            if (_tcpType == TcpType.Ssl)
                settings.UseSslConnection("ES", false);

            var ip = new IPAddress(new byte[] {8, 8, 8, 8}); //NOTE: This relies on Google DNS server being configured to swallow nonsense traffic
            const int port = 4567;
            using (var connection = EventStoreConnection.Create(settings, new IPEndPoint(ip, port).ToESTcpUri()))
            {
                connection.Closed += (s, e) => closed.Set();
                connection.Connected += (s, e) => Console.WriteLine("EventStoreConnection '{0}': connected to [{1}]...", e.Connection.ConnectionName, e.RemoteEndPoint);
                connection.Reconnecting += (s, e) => Console.WriteLine("EventStoreConnection '{0}': reconnecting...", e.Connection.ConnectionName);
                connection.Disconnected += (s, e) => Console.WriteLine("EventStoreConnection '{0}': disconnected from [{1}]...", e.Connection.ConnectionName, e.RemoteEndPoint);
                connection.ErrorOccurred += (s, e) => Console.WriteLine("EventStoreConnection '{0}': error = {1}", e.Connection.ConnectionName, e.Exception);
                connection.ConnectAsync().Wait();

                Assert.True(closed.Wait(TimeSpan.FromSeconds(15)));
            }

        }

    }
}
