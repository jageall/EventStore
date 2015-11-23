﻿using System;
using EventStore.Projections.Core.Messages;
using EventStore.Projections.Core.Messages.Persisted.Responses;
using EventStore.Projections.Core.Services.Management;
using EventStore.Projections.Core.Services.Processing;
using Xunit;

namespace EventStore.Projections.Core.Tests.Services.projection_core_service_response_writer
{

    public class when_handling_prepared_message : specification_with_projection_core_service_response_writer
    {
        private Guid _projectionId;
        private ProjectionSourceDefinition _definition;

        protected override void Given()
        {
            _projectionId = Guid.NewGuid();

            var builder = new SourceDefinitionBuilder();
            builder.FromStream("s1");
            builder.FromStream("s2");
            builder.IncludeEvent("e1");
            builder.IncludeEvent("e2");
            builder.SetByStream();
            builder.SetResultStreamNameOption("result-stream");
            _definition = ProjectionSourceDefinition.From(builder);
        }

        protected override void When()
        {
            _sut.Handle(new CoreProjectionStatusMessage.Prepared(_projectionId, _definition));
        }

        [Fact]
        public void publishes_prepared_response()
        {
            var command = AssertParsedSingleCommand<Prepared>("$prepared");
            Assert.Equal(_projectionId.ToString("N"), command.Id);
            Assert.Equal(_definition.AllEvents, command.SourceDefinition.AllEvents);
            Assert.Equal(_definition.AllStreams, command.SourceDefinition.AllStreams);
            Assert.Equal(_definition.ByCustomPartitions, command.SourceDefinition.ByCustomPartitions);
            Assert.Equal(_definition.ByStream, command.SourceDefinition.ByStream);
            Assert.Equal(_definition.CatalogStream, command.SourceDefinition.CatalogStream);
            Assert.Equal(_definition.Categories, command.SourceDefinition.Categories);
            Assert.Equal(_definition.Events, command.SourceDefinition.Events);
            Assert.Equal(_definition.LimitingCommitPosition, command.SourceDefinition.LimitingCommitPosition);
            Assert.Equal(_definition.Streams, command.SourceDefinition.Streams);
            Assert.Equal(
                _definition.Options.DefinesCatalogTransform,
                command.SourceDefinition.Options.DefinesCatalogTransform);
            Assert.Equal(_definition.Options.DefinesFold, command.SourceDefinition.Options.DefinesFold);
            Assert.Equal(
                _definition.Options.DefinesStateTransform,
                command.SourceDefinition.Options.DefinesStateTransform);
            Assert.Equal(_definition.Options.DisableParallelism, command.SourceDefinition.Options.DisableParallelism);
            Assert.Equal(
                _definition.Options.ForceProjectionName,
                command.SourceDefinition.Options.ForceProjectionName);
            Assert.Equal(
                _definition.Options.HandlesDeletedNotifications,
                command.SourceDefinition.Options.HandlesDeletedNotifications);
            Assert.Equal(_definition.Options.IncludeLinks, command.SourceDefinition.Options.IncludeLinks);
            Assert.Equal(_definition.Options.IsBiState, command.SourceDefinition.Options.IsBiState);
            Assert.Equal(
                _definition.Options.PartitionResultStreamNamePattern,
                command.SourceDefinition.Options.PartitionResultStreamNamePattern);
            Assert.Equal(_definition.Options.ProcessingLag, command.SourceDefinition.Options.ProcessingLag);
            Assert.Equal(_definition.Options.ProducesResults, command.SourceDefinition.Options.ProducesResults);
            Assert.Equal(_definition.Options.ReorderEvents, command.SourceDefinition.Options.ReorderEvents);
            Assert.Equal(_definition.Options.ResultStreamName, command.SourceDefinition.Options.ResultStreamName);

        }
    }
}