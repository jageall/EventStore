﻿using System;
using System.Collections.Generic;
using System.Linq;
using EventStore.Projections.Core.Messages;
using Xunit;

namespace EventStore.Projections.Core.Tests.Services.projection_core_service_command_reader
{
    
    public class when_receiving_create_prepared_command : specification_with_projection_core_service_command_reader_started
    {
        private const string Query = @"fromStream('$user-admin').outputState()";
        private Guid _projectionId;

        protected override IEnumerable<WhenStep> When()
        {
            _projectionId = Guid.NewGuid();
            yield return
                CreateWriteEvent(
                    "$projections-$" + _serviceId,
                    "$create-prepared",
                    @"{
                        ""id"":""" + _projectionId.ToString("N") + @""",
                          ""config"":{
                             ""runAs"":""user"",
                             ""runAsRoles"":[""a"",""b""],
                             ""checkpointHandledThreshold"":1000,
                             ""checkpointUnhandledBytesThreshold"":10000,
                             ""pendingEventsThreshold"":5000, 
                             ""maxWriteBatchLength"":100,
                             ""emitEventEnabled"":true,
                             ""checkpointsEnabled"":true,
                             ""createTempStreams"":true,
                             ""stopOnEof"":false,
                             ""isSlaveProjection"":false,
                         },
                         ""sourceDefinition"":{
                             ""allEvents"":false,   
                             ""allStreams"":false,
                             ""byStreams"":true,
                             ""byCustomPartitions"":false,
                             ""categories"":[""account""],
                             ""events"":[""added"",""removed""],
                             ""streams"":[],
                             ""catalogStream"":"""",
                             ""limitingCommitPosition"":100000,
                             ""options"":{
                                 ""resultStreamName"":""ResultStreamName"",
                                 ""partitionResultStreamNamePattern"":""PartitionResultStreamNamePattern"",
                                 ""$forceProjectionName"":""ForceProjectionName"",
                                 ""reorderEvents"":false,
                                 ""processingLag"":0,
                                 ""isBiState"":false,
                                 ""definesStateTransform"":false,
                                 ""definesCatalogTransform"":false,
                                 ""producesResults"":true,
                                 ""definesFold"":false,
                                 ""handlesDeletedNotifications"":false,
                                 ""$includeLinks"":false,
                                 ""disableParallelism"":false,
                             },
                         },
                         ""version"":{},
                         ""handlerType"":""JS"",
                         ""query"":""" + Query + @""",
                         ""name"":""test""
                    }",
                    null,
                    true);
        }

        [Fact]
        public void publishes_projection_create_prepapred_message()
        {
            var createPrepared =
                HandledMessages.OfType<CoreProjectionManagementMessage.CreatePrepared>().LastOrDefault();
            Assert.NotNull(createPrepared);
            Assert.Equal(_projectionId, createPrepared.ProjectionId);
            Assert.Equal("JS", createPrepared.HandlerType);
            Assert.Equal(Query, createPrepared.Query);
            Assert.Equal("test", createPrepared.Name);
            Assert.NotNull(createPrepared.Config);
            Assert.Equal("user", createPrepared.Config.RunAs.Identity.Name);
            Assert.True(createPrepared.Config.RunAs.IsInRole("b"));
            Assert.Equal(1000, createPrepared.Config.CheckpointHandledThreshold);
            Assert.Equal(10000, createPrepared.Config.CheckpointUnhandledBytesThreshold);
            Assert.Equal(5000, createPrepared.Config.PendingEventsThreshold);
            Assert.Equal(100, createPrepared.Config.MaxWriteBatchLength);
            Assert.Equal(true, createPrepared.Config.EmitEventEnabled);
            Assert.Equal(true, createPrepared.Config.CheckpointsEnabled);
            Assert.Equal(true, createPrepared.Config.CreateTempStreams);
            Assert.Equal(false, createPrepared.Config.StopOnEof);
            Assert.Equal(false, createPrepared.Config.IsSlaveProjection);
            var projectionSourceDefinition = createPrepared.SourceDefinition as IQuerySources;
            Assert.NotNull(projectionSourceDefinition);
            Assert.Equal(false, projectionSourceDefinition.AllEvents);
            Assert.Equal(false, projectionSourceDefinition.AllStreams);
            Assert.Equal(true, projectionSourceDefinition.ByStreams);
            Assert.Equal(false, projectionSourceDefinition.ByCustomPartitions);
            Assert.True(new[] {"account"}.SequenceEqual(projectionSourceDefinition.Categories));
            Assert.True(new[] {"added", "removed"}.SequenceEqual(projectionSourceDefinition.Events));
            Assert.True(new string[] {}.SequenceEqual(projectionSourceDefinition.Streams));
            Assert.Equal("", projectionSourceDefinition.CatalogStream);
            Assert.Equal(100000, projectionSourceDefinition.LimitingCommitPosition);
            Assert.Equal("ResultStreamName", projectionSourceDefinition.ResultStreamNameOption);
            Assert.Equal(
                "PartitionResultStreamNamePattern",
                projectionSourceDefinition.PartitionResultStreamNamePatternOption);
            Assert.Equal("ForceProjectionName", projectionSourceDefinition.ForceProjectionNameOption);
            Assert.Equal(false, projectionSourceDefinition.ReorderEventsOption);
            Assert.Equal(0, projectionSourceDefinition.ProcessingLagOption);
            Assert.Equal(false, projectionSourceDefinition.IsBiState);
            Assert.Equal(false, projectionSourceDefinition.DefinesStateTransform);
            Assert.Equal(false, projectionSourceDefinition.DefinesCatalogTransform);
            Assert.Equal(true, projectionSourceDefinition.ProducesResults);
            Assert.Equal(false, projectionSourceDefinition.DefinesFold);
            Assert.Equal(false, projectionSourceDefinition.HandlesDeletedNotifications);
            Assert.Equal(false, projectionSourceDefinition.IncludeLinksOption);
            Assert.Equal(false, projectionSourceDefinition.DisableParallelismOption);
        }
    }
}