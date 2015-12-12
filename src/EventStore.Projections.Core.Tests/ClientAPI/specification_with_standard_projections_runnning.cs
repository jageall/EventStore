using System;
using System.Diagnostics;
using System.Linq;
using System.Text;
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

namespace EventStore.Projections.Core.Tests.ClientAPI
{
    public class specification_with_standard_projections_runnning : IClassFixture<specification_with_standard_projections_runnning.SpecificationFixture>
    {

        protected UserCredentials _admin = DefaultData.AdminCredentials;

        private SpecificationFixture _fixture;
        protected SpecificationFixture Fixture { get { return _fixture; } }
        protected IEventStoreConnection _conn {get { return _fixture.Connection; }}
        protected ProjectionsManager _manager{get { return _fixture._manager; }}
        public class SpecificationFixture : SpecificationWithDirectoryPerTestFixture

        {
            private MiniNode _node;
            public IEventStoreConnection Connection { get; private set; }
            
            private ProjectionsSubsystem _projections;
            public ProjectionsManager _manager;
            private Action<int, Action, Action, Action> _initialize;
            private Action<specification_with_standard_projections_runnning> _assignStashedValues;

            public SpecificationFixture()
            {

                _assignStashedValues = _ => { };
                _initialize = (workerThreads, standardProjections, given, when) =>
                {
                    _initialize = (_, __, ___, ____) => { };
#if DEBUG
                    QueueStatsCollector.InitializeIdleDetection();
#endif
                    _node = CreateNode(workerThreads);
                    try
                    {
                        Connection = EventStoreConnection.Create(_node.TcpEndPoint);
                        Connection.ConnectAsync().Wait();

                        _manager = new ProjectionsManager(
                            new ConsoleLogger(),
                            _node.ExtHttpEndPoint,
                            TimeSpan.FromMilliseconds(10000));
                        WaitIdle();
                        standardProjections();
                        QueueStatsCollector.WaitIdle();
                        given();
                        when();
                    } catch
                    {
                        try
                        {
                            if (Connection != null)
                                Connection.Close();
                        } catch
                        {
                        }
                        try
                        {
                            if (_node != null)
                                _node.Shutdown();
                        } catch
                        {
                        }

                        throw;
                    }
                };

            }

            private MiniNode CreateNode(int projectionWorkerThreadCount)
            {
                _projections = new ProjectionsSubsystem(projectionWorkerThreadCount, runProjections: ProjectionType.All,
                    startStandardProjections: false);
                var node = new MiniNode(
                    PathName, inMemDb: true, skipInitializeStandardUsersCheck: false,
                    subsystems: new ISubsystem[] { _projections });
                node.Start();
                return node;
            }

            public override void Dispose()
            {
                if (Connection != null)
                    Connection.Close();

                if (_node != null)
                    _node.Shutdown();

                base.Dispose();
#if DEBUG
                QueueStatsCollector.InitializeIdleDetection(false);
#endif
            }
            public void EnsureInitialized(int projectionThreadCount, Action runStandardProjections, Action given, Action when)
            {
                _initialize(projectionThreadCount, runStandardProjections, given, when);
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

        public specification_with_standard_projections_runnning(SpecificationFixture fixture)
        {
            #if (!DEBUG)
                throw new InvalidOperationException("These tests require DEBUG conditional, use [DebugBuildFact] instead of [Fact]");
#else
            
            _fixture = fixture;
            fixture.EnsureInitialized(GivenWorkerThreadCount(),
                () =>
                {
                    if (GivenStandardProjectionsRunning())
                        EnableStandardProjections();
                }, Given, When);
            fixture.AssignStashedValues(this);
#endif
        }

        protected virtual int GivenWorkerThreadCount()
        {
            return 1;
        }

        public void Dispose()
        {
            var all = _manager.ListAllAsync(_admin).Result;
            if (all.Any(p => p.Name == "Faulted"))
                Assert.True(false, string.Format("Projections faulted while running the test" + "\r\n" + all));
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
            _conn.AppendToStreamAsync(stream, ExpectedVersion.Any, new[] { event_by_type_index.with_existing_events.CreateEvent(eventType, data) }).Wait();
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

        protected static void WaitIdle(int multiplier = 1)
        {
            QueueStatsCollector.WaitIdle(multiplier: multiplier);
        }

        [Conditional("DEBUG")]
        protected void AssertStreamTail(string streamId, params string[] events)
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

        protected void Dump(string message, string streamId, ResolvedEvent[] resultEvents)
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

        protected void PostProjection(string query)
        {
            _manager.CreateContinuousAsync("test-projection", query, _admin).Wait();
            WaitIdle();
        }

        protected void PostQuery(string query)
        {
            _manager.CreateTransientAsync("query", query, _admin).Wait();
            WaitIdle();
        }
    }
}