using System;
using System.Collections.Generic;
using System.Linq;
using EventStore.Core.Data;
using EventStore.Core.Messages;
using EventStore.Core.Messaging;
using EventStore.Core.Services;
using EventStore.Projections.Core.Messages;
using EventStore.Projections.Core.Services;
using EventStore.Projections.Core.Services.Management;
using Xunit;

namespace EventStore.Projections.Core.Tests.Services.projections_manager.continuous
{
    public static class a_new_posted_projection
    {
        public abstract class Base : TestFixtureWithProjectionCoreAndManagementServices
        {
            protected string _projectionName;
            protected string _projectionSource;
            protected Type _fakeProjectionType;
            protected ProjectionMode _projectionMode;
            protected bool _checkpointsEnabled;
            protected bool _emitEnabled;
            protected bool _projectionEnabled;

            protected override void Given()
            {
                base.Given();

                _projectionName = "test-projection";
                _projectionSource = @"";
                _fakeProjectionType = typeof (FakeProjection);
                _projectionMode = ProjectionMode.Continuous;
                _checkpointsEnabled = true;
                _emitEnabled = true;
                _projectionEnabled = true;
                
                NoStream("$projections-test-projection-checkpoint");
                NoStream("$projections-test-projection-order");
                NoOtherStreams();
                AllWritesSucceed();
            }

            protected override IEnumerable<WhenStep> When()
            {
                yield return (new SystemMessage.BecomeMaster(Guid.NewGuid()));
                yield return (
                    new ProjectionManagementMessage.Command.Post(
                        new PublishEnvelope(_bus), _projectionMode, _projectionName, ProjectionManagementMessage.RunAs.System,
                        "native:" + _fakeProjectionType.AssemblyQualifiedName, _projectionSource, enabled: _projectionEnabled,
                        checkpointsEnabled: _checkpointsEnabled, emitEnabled: _emitEnabled));
            }
        }

        
        public class when_get_query : Base
        {
            protected override IEnumerable<WhenStep> When()
            {
                foreach (var m in base.When())
                    yield return m;
                yield return
                    (new ProjectionManagementMessage.Command.GetQuery(
                        new PublishEnvelope(_bus), _projectionName, ProjectionManagementMessage.RunAs.Anonymous));
            }

            [Fact]
            public void returns_correct_source()
            {
                Assert.Equal(
                    1, _consumer.HandledMessages.OfType<ProjectionManagementMessage.ProjectionQuery>().Count());
                var projectionQuery =
                    _consumer.HandledMessages.OfType<ProjectionManagementMessage.ProjectionQuery>().Single();
                Assert.Equal(_projectionName, projectionQuery.Name);
                Assert.Equal("", projectionQuery.Query);
            }
        }

        
        public class when_get_state : Base
        {
            protected override void Given()
            {
                base.Given();
                EnableReadAll();
                ExistingEvent("temp1", "test1", "{}", "{}");
            }

            protected override IEnumerable<WhenStep> When()
            {
                foreach (var m in base.When()) yield return m;
                yield return (new ProjectionManagementMessage.Command.GetState(new PublishEnvelope(_bus), _projectionName, ""));
            }

            [Fact]
            public void returns_correct_state()
            {
                Assert.Equal(
                    1, _consumer.HandledMessages.OfType<ProjectionManagementMessage.ProjectionState>().Count());
                Assert.Equal(
                    _projectionName,
                    _consumer.HandledMessages.OfType<ProjectionManagementMessage.ProjectionState>().Single().Name);
                //at least projection initializaed message is here
                Assert.Equal(
                    "{\"data\": 1}", _consumer.HandledMessages.OfType<ProjectionManagementMessage.ProjectionState>().Single().State);
                Assert.Equal(
                    _all.Last(v => !SystemStreams.IsSystemStream(v.Value.EventStreamId)).Key,
                    _consumer.HandledMessages.OfType<ProjectionManagementMessage.ProjectionState>()
                        .Single()
                        .Position.Position);
            }
        }

        
        public class when_failing : Base
        {
            protected override IEnumerable<WhenStep> When()
            {
                foreach (var m in base.When()) yield return m;
                var readerAssignedMessage =
                    _consumer.HandledMessages.OfType<EventReaderSubscriptionMessage.ReaderAssignedReader>().LastOrDefault();
                Assert.NotNull(readerAssignedMessage);
                var reader = readerAssignedMessage.ReaderId;

                yield return
                    (ReaderSubscriptionMessage.CommittedEventDistributed.Sample(
                        reader, new TFPos(100, 50), new TFPos(100, 50), "stream", 1, "stream", 1, false, Guid.NewGuid(),
                        "fail", false, new byte[0], new byte[0], 100, 33.3f));
            }

            [Fact]
            public void publishes_faulted_message()
            {
                Assert.Equal(1, _consumer.HandledMessages.OfType<CoreProjectionStatusMessage.Faulted>().Count());
            }

            [Fact]
            public void the_projection_status_becomes_faulted()
            {
                _manager.Handle(
                    new ProjectionManagementMessage.Command.GetStatistics(
                        new PublishEnvelope(_bus), null, _projectionName, false));

                Assert.Equal(1, _consumer.HandledMessages.OfType<ProjectionManagementMessage.Statistics>().Count());
                Assert.Equal(
                    1,
                    _consumer.HandledMessages.OfType<ProjectionManagementMessage.Statistics>()
                             .Single()
                             .Projections.Length);
                Assert.Equal(
                    _projectionName,
                    _consumer.HandledMessages.OfType<ProjectionManagementMessage.Statistics>()
                             .Single()
                             .Projections.Single()
                             .Name);
                Assert.Equal(
                    ManagedProjectionState.Faulted,
                    _consumer.HandledMessages.OfType<ProjectionManagementMessage.Statistics>()
                             .Single()
                             .Projections.Single()
                             .MasterStatus);
            }
        }
    }
}
