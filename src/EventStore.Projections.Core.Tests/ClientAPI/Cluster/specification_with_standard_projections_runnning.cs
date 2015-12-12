﻿using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using EventStore.ClientAPI;
using EventStore.ClientAPI.Common.Log;
using EventStore.ClientAPI.SystemData;
using EventStore.Common.Options;
using EventStore.Core;
using EventStore.Core.Bus;
using EventStore.Core.Tests;
using EventStore.Core.Tests.Helpers;
using EventStore.Projections.Core.Services.Processing;
using Xunit;
using ResolvedEvent = EventStore.ClientAPI.ResolvedEvent;
using EventStore.ClientAPI.Projections;

namespace EventStore.Projections.Core.Tests.ClientAPI.Cluster
{
    public class StandardProjectionsRunning : SpecificationWithDirectoryPerTestFixture
    {
        private readonly MiniClusterNode[] _nodes = new MiniClusterNode[3];
        private readonly Endpoints[] _nodeEndpoints = new Endpoints[3];
        public IEventStoreConnection Conn { get; private set; }
        private ProjectionsSubsystem _projections;
        public ProjectionsManager Manager { get; private set; }
        private Action<Action, Action, Action> _initialize;
        private Action<specification_with_standard_projections_runnning> _assignStashedValues;

        private class Endpoints

        {
            public readonly IPEndPoint InternalTcp;
            public readonly IPEndPoint InternalTcpSec;
            public readonly IPEndPoint InternalHttp;
            public readonly IPEndPoint ExternalTcp;
            public readonly IPEndPoint ExternalTcpSec;
            public readonly IPEndPoint ExternalHttp;

            public Endpoints(
                int internalTcp, int internalTcpSec, int internalHttp, int externalTcp,
                int externalTcpSec, int externalHttp)
            {
                var testIp = Environment.GetEnvironmentVariable("ES-TESTIP");

                var address = string.IsNullOrEmpty(testIp) ? IPAddress.Loopback : IPAddress.Parse(testIp);
                InternalTcp = new IPEndPoint(address, internalTcp);
                InternalTcpSec = new IPEndPoint(address, internalTcpSec);
                InternalHttp = new IPEndPoint(address, internalHttp);
                ExternalTcp = new IPEndPoint(address, externalTcp);
                ExternalTcpSec = new IPEndPoint(address, externalTcpSec);
                ExternalHttp = new IPEndPoint(address, externalHttp);
            }
        }

        public StandardProjectionsRunning()
        {
            _assignStashedValues = _ => { };
            _initialize = (runStandardProjections, given, when) =>
            {
                _initialize = (_, __, ___) => { };
#if DEBUG
                QueueStatsCollector.InitializeIdleDetection();
#endif
                _nodeEndpoints[0] = new Endpoints(
                    PortsHelper.GetAvailablePort(IPAddress.Loopback), PortsHelper.GetAvailablePort(IPAddress.Loopback),
                    PortsHelper.GetAvailablePort(IPAddress.Loopback), PortsHelper.GetAvailablePort(IPAddress.Loopback),
                    PortsHelper.GetAvailablePort(IPAddress.Loopback), PortsHelper.GetAvailablePort(IPAddress.Loopback));
                _nodeEndpoints[1] = new Endpoints(
                    PortsHelper.GetAvailablePort(IPAddress.Loopback), PortsHelper.GetAvailablePort(IPAddress.Loopback),
                    PortsHelper.GetAvailablePort(IPAddress.Loopback), PortsHelper.GetAvailablePort(IPAddress.Loopback),
                    PortsHelper.GetAvailablePort(IPAddress.Loopback), PortsHelper.GetAvailablePort(IPAddress.Loopback));
                _nodeEndpoints[2] = new Endpoints(
                    PortsHelper.GetAvailablePort(IPAddress.Loopback), PortsHelper.GetAvailablePort(IPAddress.Loopback),
                    PortsHelper.GetAvailablePort(IPAddress.Loopback), PortsHelper.GetAvailablePort(IPAddress.Loopback),
                    PortsHelper.GetAvailablePort(IPAddress.Loopback), PortsHelper.GetAvailablePort(IPAddress.Loopback));

                PortsHelper.GetAvailablePort(IPAddress.Loopback);

                _nodes[0] = CreateNode(0,
                    _nodeEndpoints[0], new IPEndPoint[] {_nodeEndpoints[1].InternalHttp, _nodeEndpoints[2].InternalHttp});
                _nodes[1] = CreateNode(1,
                    _nodeEndpoints[1], new IPEndPoint[] {_nodeEndpoints[0].InternalHttp, _nodeEndpoints[2].InternalHttp});

                _nodes[2] = CreateNode(2,
                    _nodeEndpoints[2], new IPEndPoint[] {_nodeEndpoints[0].InternalHttp, _nodeEndpoints[1].InternalHttp});


                _nodes[0].Start();
                _nodes[1].Start();
                _nodes[2].Start();

                WaitHandle.WaitAll(new[] {_nodes[0].StartedEvent, _nodes[1].StartedEvent, _nodes[2].StartedEvent});
                QueueStatsCollector.WaitIdle(waitForNonEmptyTf: true);
                Conn = EventStoreConnection.Create(_nodes[0].ExternalTcpEndPoint);
                Conn.ConnectAsync().Wait();

                Manager = new ProjectionsManager(
                    new ConsoleLogger(),
                    _nodes[0].ExternalHttpEndPoint,
                    TimeSpan.FromMilliseconds(10000));

                runStandardProjections();
                QueueStatsCollector.WaitIdle();
                given();
                when();
            };
        }

        public override void Dispose()
        {
            Conn.Close();
            _nodes[0].Shutdown();
            _nodes[1].Shutdown();
            _nodes[2].Shutdown();
#if DEBUG
            QueueStatsCollector.InitializeIdleDetection(false);
#endif
        }


        private MiniClusterNode CreateNode(int index, Endpoints endpoints, IPEndPoint[] gossipSeeds)
        {
            _projections = new ProjectionsSubsystem(1, runProjections: ProjectionType.All, startStandardProjections: false);
            var node = new MiniClusterNode(
                PathName, index, endpoints.InternalTcp, endpoints.InternalTcpSec, endpoints.InternalHttp, endpoints.ExternalTcp,
                endpoints.ExternalTcpSec, endpoints.ExternalHttp, skipInitializeStandardUsersCheck: false,
                subsystems: new ISubsystem[] { _projections }, gossipSeeds: gossipSeeds);
            QueueStatsCollector.WaitIdle();
            return node;
        }

        public void EnsureInitialized(Action runStandardProjections, Action given, Action when)
        {
            _initialize(runStandardProjections, given, when);
        }

        public void AddStashedValueAssignment<T>(T ignored, Action<T> assignStashedValues)
            where T : specification_with_standard_projections_runnning
        {
            _assignStashedValues += instance => assignStashedValues((T)instance);
        }

        public void AssignStashedValues(specification_with_standard_projections_runnning scenario)
        {
            _assignStashedValues(scenario);
        }
    }
    
    public class specification_with_standard_projections_runnning : IClassFixture<StandardProjectionsRunning>, IDisposable
    {
        protected readonly UserCredentials _admin = DefaultData.AdminCredentials;
        protected ProjectionsManager _manager{get { return _fixture.Manager; }}
        protected IEventStoreConnection _conn{get { return _fixture.Conn; }}
        private StandardProjectionsRunning _fixture;
        protected StandardProjectionsRunning Fixture{get{return _fixture;}}


        public specification_with_standard_projections_runnning(StandardProjectionsRunning fixture)
        {
#if (!DEBUG)
            throw new NotSupportedException("These tests require DEBUG conditional");
#else

#endif
            _fixture = fixture;
            _fixture.EnsureInitialized(
                () => { if (GivenStandardProjectionsRunning()) EnableStandardProjections(); },
                Given,
                When
                );
            _fixture.AssignStashedValues(this);
        }
        public void Dispose()
        {
            var all = _manager.ListAllAsync(_admin).Result;
            Assert.False (all.Any(p => p.Name == "Faulted"),"Projections faulted while running the test" + "\r\n" + all);
        }

        protected void EnableStandardProjections()
        {
            EnableProjection(ProjectionNamesBuilder.StandardProjections.EventByCategoryStandardProjection);
            EnableProjection(ProjectionNamesBuilder.StandardProjections.EventByTypeStandardProjection);
            EnableProjection(ProjectionNamesBuilder.StandardProjections.StreamByCategoryStandardProjection);
            EnableProjection(ProjectionNamesBuilder.StandardProjections.StreamsStandardProjection);
        }

        protected void DisableStandardProjections()
        {
            DisableProjection(ProjectionNamesBuilder.StandardProjections.EventByCategoryStandardProjection);
            DisableProjection(ProjectionNamesBuilder.StandardProjections.EventByTypeStandardProjection);
            DisableProjection(ProjectionNamesBuilder.StandardProjections.StreamByCategoryStandardProjection);
            DisableProjection(ProjectionNamesBuilder.StandardProjections.StreamsStandardProjection);
        }

        protected virtual bool GivenStandardProjectionsRunning()
        {
            return true;
        }

        protected void EnableProjection(string name)
        {
            _manager.EnableAsync(name, _admin).Wait();
        }

        protected void DisableProjection(string name)
        {
            _manager.DisableAsync(name, _admin).Wait();
        }

        protected virtual void When()
        {
        }

        protected virtual void Given()
        {
        }

        protected void PostEvent(string stream, string eventType, string data)
        {
            _conn.AppendToStreamAsync(stream, ExpectedVersion.Any, new[] { CreateEvent(eventType, data) }).Wait();
        }

        protected void HardDeleteStream(string stream)
        {
            _conn.DeleteStreamAsync(stream, ExpectedVersion.Any, true, _admin).Wait();
        }

        protected void SoftDeleteStream(string stream)
        {
            _conn.DeleteStreamAsync(stream, ExpectedVersion.Any, false, _admin).Wait();
        }

        protected static EventData CreateEvent(string type, string data)
        {
            return new EventData(Guid.NewGuid(), type, true, Encoding.UTF8.GetBytes(data), new byte[0]);
        }

        protected static void WaitIdle()
        {
            QueueStatsCollector.WaitIdle();
        }

        [Conditional("DEBUG")]
        public void AssertStreamTail(string streamId, params string[] events)
        {
#if DEBUG
            var result = _conn.ReadStreamEventsBackwardAsync(streamId, -1, events.Length, true, _admin).Result;
            switch (result.Status)
            {
                case SliceReadStatus.StreamDeleted:
                    Assert.True(false, string.Format("Stream '{0}' is deleted", streamId));
                    break;
                case SliceReadStatus.StreamNotFound:
                    Assert.True(false, string.Format("Stream '{0}' does not exist", streamId));
                    break;
                case SliceReadStatus.Success:
                    var resultEventsReversed = result.Events.Reverse().ToArray();
                    if (resultEventsReversed.Length < events.Length)
                        DumpFailed("Stream does not contain enough events", streamId, events, result.Events);
                    else
                    {
                        for (var index = 0; index < events.Length; index++)
                        {
                            var parts = events[index].Split(new char[] { ':' }, 2);
                            var eventType = parts[0];
                            var eventData = parts[1];

                            if (resultEventsReversed[index].Event.EventType != eventType)
                                DumpFailed("Invalid event type", streamId, events, resultEventsReversed);
                            else
                                if (resultEventsReversed[index].Event.DebugDataView != eventData)
                                    DumpFailed("Invalid event body", streamId, events, resultEventsReversed);
                        }
                    }
                    break;
            }
#endif
        }

        [Conditional("DEBUG")]
        protected void DumpStream(string streamId)
        {
#if DEBUG
            var result = _conn.ReadStreamEventsBackwardAsync(streamId, -1, 100, true, _admin).Result;
            switch (result.Status)
            {
                case SliceReadStatus.StreamDeleted:
                    Assert.True(false, string.Format("Stream '{0}' is deleted", streamId));
                    break;
                case SliceReadStatus.StreamNotFound:
                    Assert.True(false, string.Format("Stream '{0}' does not exist", streamId));
                    break;
                case SliceReadStatus.Success:
                    Dump("Dumping..", streamId, result.Events.Reverse().ToArray());
                    break;
            }
#endif
        }

#if DEBUG
        private void DumpFailed(string message, string streamId, string[] events, ResolvedEvent[] resultEvents)
        {
            var expected = events.Aggregate("", (a, v) => a + ", " + v);
            var actual = resultEvents.Aggregate(
                "", (a, v) => a + ", " + v.Event.EventType + ":" + v.Event.DebugDataView);

            var actualMeta = resultEvents.Aggregate(
                "", (a, v) => a + "\r\n" + v.Event.EventType + ":" + v.Event.DebugMetadataView);


            Assert.True(false, string.Format(
                "Stream: '{0}'\r\n{1}\r\n\r\nExisting events: \r\n{2}\r\n Expected events: \r\n{3}\r\n\r\nActual metas:{4}", streamId,
                message, actual, expected, actualMeta));

        }

        private void Dump(string message, string streamId, ResolvedEvent[] resultEvents)
        {
            var actual = resultEvents.Aggregate(
                "", (a, v) => a + ", " + v.OriginalEvent.EventType + ":" + v.OriginalEvent.DebugDataView);

            var actualMeta = resultEvents.Aggregate(
                "", (a, v) => a + "\r\n" + v.OriginalEvent.EventType + ":" + v.OriginalEvent.DebugMetadataView);


            Debug.WriteLine(
                "Stream: '{0}'\r\n{1}\r\n\r\nExisting events: \r\n{2}\r\n \r\nActual metas:{3}", streamId,
                message, actual, actualMeta);
        }
#endif

        public void PostProjection(string query)
        {
            _manager.CreateContinuousAsync("test-projection", query, _admin).Wait();
            WaitIdle();
        }
    }
    public class TestTest : specification_with_standard_projections_runnning
    {
        //TODO JAG this appears to be the only test that inherits from the base class in cluster
        [DebugBuildFact][Trait("Category", "Explicit")][Trait("Category", "ClientAPI")]
        public void Test()
        {
            PostProjection(@"fromStream('$user-admin').outputState()");

            AssertStreamTail("$projections-test-projection-result", "Result:{}");
        }


        public void Dispose()
        {
            var all = _manager.ListAllAsync(_admin).Result;
            if (all.Any(p => p.Name == "Faulted"))
                Assert.True(false, string.Format("Projections faulted while running the test" + "\r\n" + all));
        }

        public TestTest(StandardProjectionsRunning fixture) : base(fixture)
        {
        }
    }
}