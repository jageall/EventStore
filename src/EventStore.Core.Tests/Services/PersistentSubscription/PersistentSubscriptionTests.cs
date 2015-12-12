using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using EventStore.ClientAPI;
using EventStore.ClientAPI.Common;
using EventStore.ClientAPI.SystemData;
using EventStore.Core.Data;
using EventStore.Core.Messages;
using EventStore.Core.Services.PersistentSubscription;
using EventStore.Core.Tests.Services.Replication;
using EventStore.Core.TransactionLog.LogRecords;
using Xunit;
using ExpectedVersion = EventStore.Core.Data.ExpectedVersion;
using ResolvedEvent = EventStore.Core.Data.ResolvedEvent;
using EventStore.Core.Tests.ClientAPI;

namespace EventStore.Core.Tests.Services.PersistentSubscription
{
    public class when_creating_persistent_subscription : IClassFixture<when_creating_persistent_subscription.FixtureData>
    {
        public class FixtureData
        {
            public readonly Core.Services.PersistentSubscription.PersistentSubscription _sub;

            public FixtureData()
            {
                _sub = new Core.Services.PersistentSubscription.PersistentSubscription(
                PersistentSubscriptionParamsBuilder.CreateFor("streamName", "groupName")
                    .WithEventLoader(new FakeStreamReader(x => { }))
                    .WithCheckpointReader(new FakeCheckpointReader())
                    .WithCheckpointWriter(new FakeCheckpointWriter(x => { }))
                    .WithMessageParker(new FakeMessageParker()));
            }
        }

        private Core.Services.PersistentSubscription.PersistentSubscription _sub;

        
        public void SetFixture(FixtureData data)
        {
            _sub = data._sub;
        }

        [Fact]
        public void subscription_id_is_set()
        {
            Assert.Equal("streamName:groupName", _sub.SubscriptionId);
        }

        [Fact]
        public void stream_name_is_set()
        {
            Assert.Equal("streamName", _sub.EventStreamId);
        }

        [Fact]
        public void group_name_is_set()
        {
            Assert.Equal("groupName", _sub.GroupName);
        }

        [Fact]
        public void there_are_no_clients()
        {
            Assert.False(_sub.HasClients);
            Assert.Equal(0, _sub.ClientCount);
        }

        [Fact]
        public void null_checkpoint_reader_throws_argument_null()
        {
            Assert.Throws<ArgumentNullException>(() =>
            {
                _sub = new Core.Services.PersistentSubscription.PersistentSubscription(
                    PersistentSubscriptionParamsBuilder.CreateFor("streamName", "groupName")
                        .WithEventLoader(new FakeStreamReader(x => { }))
                        .WithCheckpointReader(null)
                        .WithCheckpointWriter(new FakeCheckpointWriter(x => { }))
                        .WithMessageParker(new FakeMessageParker()));

            });
        }

        [Fact]
        public void null_checkpoint_writer_throws_argument_null()
        {
            Assert.Throws<ArgumentNullException>(() =>
            {
                _sub = new Core.Services.PersistentSubscription.PersistentSubscription(
                    PersistentSubscriptionParamsBuilder.CreateFor("streamName", "groupName")
                        .WithEventLoader(new FakeStreamReader(x => { }))
                        .WithCheckpointReader(new FakeCheckpointReader())
                        .WithCheckpointWriter(null)
                        .WithMessageParker(new FakeMessageParker()));

            });
        }


        [Fact]
        public void null_event_reader_throws_argument_null()
        {
            Assert.Throws<ArgumentNullException>(() =>
            {
                _sub = new Core.Services.PersistentSubscription.PersistentSubscription(
                    PersistentSubscriptionParamsBuilder.CreateFor("streamName", "groupName")
                        .WithEventLoader(null)
                        .WithCheckpointReader(new FakeCheckpointReader())
                        .WithCheckpointWriter(new FakeCheckpointWriter(x => { }))
                        .WithMessageParker(new FakeMessageParker()));

            });
        }

        [Fact]
        public void null_stream_throws_argument_null()
        {
            Assert.Throws<ArgumentNullException>(() =>
            {
                _sub = new Core.Services.PersistentSubscription.PersistentSubscription(
                    PersistentSubscriptionParamsBuilder.CreateFor(null, "groupName")
                        .WithEventLoader(new FakeStreamReader(x => { }))
                        .WithCheckpointReader(new FakeCheckpointReader())
                        .WithCheckpointWriter(new FakeCheckpointWriter(x => { }))
                        .WithMessageParker(new FakeMessageParker()));

            });
        }

        [Fact]
        public void null_groupname_throws_argument_null()
        {
            Assert.Throws<ArgumentNullException>(() =>
            {
                _sub = new Core.Services.PersistentSubscription.PersistentSubscription(
                    PersistentSubscriptionParamsBuilder.CreateFor("streamName", null)
                        .WithEventLoader(new FakeStreamReader(x => { }))
                        .WithCheckpointReader(new FakeCheckpointReader())
                        .WithCheckpointWriter(new FakeCheckpointWriter(x => { }))
                        .WithMessageParker(new FakeMessageParker()));

            });
        }

    }

    public class LiveTests
    {
        [Fact]
        public void live_subscription_pushes_events_to_client()
        {
            var envelope = new FakeEnvelope();
            var reader = new FakeCheckpointReader();
            var sub = new Core.Services.PersistentSubscription.PersistentSubscription(
                PersistentSubscriptionParamsBuilder.CreateFor("streamName", "groupName")
                    .WithEventLoader(new FakeStreamReader(x => { }))
                    .WithCheckpointReader(reader)
                    .WithCheckpointWriter(new FakeCheckpointWriter(x => { }))
                    .WithMessageParker(new FakeMessageParker())
                    .StartFromCurrent());
            reader.Load(null);
            sub.AddClient(Guid.NewGuid(), Guid.NewGuid(), envelope, 10, "foo", "bar");
            sub.NotifyLiveSubscriptionMessage(Helper.BuildFakeEvent(Guid.NewGuid(), "type", "streamName", 0));
            Assert.Equal(1, envelope.Replies.Count);
        }

        [Fact]
        public void live_subscription_with_round_robin_two_pushes_events_to_both()
        {
            var envelope1 = new FakeEnvelope();
            var envelope2 = new FakeEnvelope();
            var reader = new FakeCheckpointReader();
            var sub = new Core.Services.PersistentSubscription.PersistentSubscription(
                PersistentSubscriptionParamsBuilder.CreateFor("streamName", "groupName")
                    .WithEventLoader(new FakeStreamReader(x => { }))
                    .WithCheckpointReader(reader)
                    .WithCheckpointWriter(new FakeCheckpointWriter(x => { }))
                    .WithMessageParker(new FakeMessageParker())
                    .PreferRoundRobin()
                    .StartFromCurrent());
            reader.Load(null);
            sub.AddClient(Guid.NewGuid(), Guid.NewGuid(), envelope1, 10, "foo", "bar");
            sub.AddClient(Guid.NewGuid(), Guid.NewGuid(), envelope2, 10, "foo", "bar");
            sub.NotifyLiveSubscriptionMessage(Helper.BuildFakeEvent(Guid.NewGuid(), "type", "streamName", 0));
            sub.NotifyLiveSubscriptionMessage(Helper.BuildFakeEvent(Guid.NewGuid(), "type", "streamName", 1));
            Assert.Equal(1, envelope1.Replies.Count);
            Assert.Equal(1, envelope2.Replies.Count);
        }

        [Fact]
        public void live_subscription_with_prefer_one_and_two_pushes_events_to_both()
        {
            var envelope1 = new FakeEnvelope();
            var envelope2 = new FakeEnvelope();
            var reader = new FakeCheckpointReader();
            var sub = new Core.Services.PersistentSubscription.PersistentSubscription(
                PersistentSubscriptionParamsBuilder.CreateFor("streamName", "groupName")
                    .WithEventLoader(new FakeStreamReader(x => { }))
                    .WithCheckpointReader(reader)
                    .WithCheckpointWriter(new FakeCheckpointWriter(x => { }))
                    .WithMessageParker(new FakeMessageParker())
                    .PreferDispatchToSingle()
                    .StartFromCurrent());
            reader.Load(null);
            sub.AddClient(Guid.NewGuid(), Guid.NewGuid(), envelope1, 10, "foo", "bar");
            sub.AddClient(Guid.NewGuid(), Guid.NewGuid(), envelope2, 10, "foo", "bar");
            sub.NotifyLiveSubscriptionMessage(Helper.BuildFakeEvent(Guid.NewGuid(), "type", "streamName", 0));
            sub.NotifyLiveSubscriptionMessage(Helper.BuildFakeEvent(Guid.NewGuid(), "type", "streamName", 1));
            Assert.Equal(2, envelope1.Replies.Count);
        }


        [Fact]
        public void subscription_with_pull_sends_data_to_client()
        {
            var envelope1 = new FakeEnvelope();
            var reader = new FakeCheckpointReader();
            var sub = new Core.Services.PersistentSubscription.PersistentSubscription(
                PersistentSubscriptionParamsBuilder.CreateFor("streamName", "groupName")
                    .WithEventLoader(new FakeStreamReader(x => { }))
                    .WithCheckpointReader(reader)
                    .WithCheckpointWriter(new FakeCheckpointWriter(x => { }))
                    .WithMessageParker(new FakeMessageParker())
                    .StartFromBeginning());
            reader.Load(null);
            sub.AddClient(Guid.NewGuid(), Guid.NewGuid(), envelope1, 10, "foo", "bar");
            sub.HandleReadCompleted(new[] { Helper.BuildFakeEvent(Guid.NewGuid(), "type", "streamName", 0) }, 1, false);
            Assert.Equal(1, envelope1.Replies.Count);
        }

        [Fact]
        public void subscription_with_pull_does_not_crash_if_not_ready_yet()
        {
            var envelope1 = new FakeEnvelope();
            var reader = new FakeCheckpointReader();
            var sub = new Core.Services.PersistentSubscription.PersistentSubscription(
                PersistentSubscriptionParamsBuilder.CreateFor("streamName", "groupName")
                    .WithEventLoader(new FakeStreamReader(x => { }))
                    .WithCheckpointReader(reader)
                    .WithCheckpointWriter(new FakeCheckpointWriter(x => { }))
                    .WithMessageParker(new FakeMessageParker())
                    .StartFromBeginning());
            var ex = Record.Exception(() =>
            {
                sub.AddClient(Guid.NewGuid(), Guid.NewGuid(), envelope1, 10, "foo", "bar");
                sub.HandleReadCompleted(new[] { Helper.BuildFakeEvent(Guid.NewGuid(), "type", "streamName", 0) }, 1, false);
            });
            Assert.Null(ex);
        }

        [Fact]
        public void subscription_with_live_data_does_not_crash_if_not_ready_yet()
        {
            var envelope1 = new FakeEnvelope();
            var reader = new FakeCheckpointReader();
            var sub = new Core.Services.PersistentSubscription.PersistentSubscription(
                PersistentSubscriptionParamsBuilder.CreateFor("streamName", "groupName")
                    .WithEventLoader(new FakeStreamReader(x => { }))
                    .WithCheckpointReader(reader)
                    .WithCheckpointWriter(new FakeCheckpointWriter(x => { }))
                    .WithMessageParker(new FakeMessageParker())
                    .StartFromBeginning());
            var ex = Record.Exception(() =>
            {
                sub.AddClient(Guid.NewGuid(), Guid.NewGuid(), envelope1, 10, "foo", "bar");
                sub.NotifyLiveSubscriptionMessage(Helper.BuildFakeEvent(Guid.NewGuid(), "type", "streamName", 0));
            });
            Assert.Null(ex);
        }


        [Fact]
        public void subscription_with_pull_and_round_robin_set_and_two_clients_sends_data_to_client()
        {
            var envelope1 = new FakeEnvelope();
            var envelope2 = new FakeEnvelope();
            var reader = new FakeCheckpointReader();
            var sub = new Core.Services.PersistentSubscription.PersistentSubscription(
                PersistentSubscriptionParamsBuilder.CreateFor("streamName", "groupName")
                    .WithEventLoader(new FakeStreamReader(x => { }))
                    .WithCheckpointReader(reader)
                    .WithCheckpointWriter(new FakeCheckpointWriter(x => { }))
                    .WithMessageParker(new FakeMessageParker())
                    .PreferRoundRobin()
                    .StartFromBeginning());
            reader.Load(null);
            sub.AddClient(Guid.NewGuid(), Guid.NewGuid(), envelope1, 10, "foo", "bar");
            sub.AddClient(Guid.NewGuid(), Guid.NewGuid(), envelope2, 10, "foo", "bar");
            var id1 = Guid.NewGuid();
            var id2 = Guid.NewGuid();
            sub.HandleReadCompleted(new[]
            {
                Helper.BuildFakeEvent(id1, "type", "streamName", 0),
                Helper.BuildFakeEvent(id2, "type", "streamName", 1)
            }, 1, false);
            Assert.Equal(1, envelope1.Replies.Count);
            Assert.Equal(1, envelope2.Replies.Count);
        }


        [Fact]
        public void subscription_with_pull_and_prefer_one_set_and_two_clients_sends_data_to_one_client()
        {
            var envelope1 = new FakeEnvelope();
            var envelope2 = new FakeEnvelope();
            var reader = new FakeCheckpointReader();
            var sub = new Core.Services.PersistentSubscription.PersistentSubscription(
                PersistentSubscriptionParamsBuilder.CreateFor("streamName", "groupName")
                    .WithEventLoader(new FakeStreamReader(x => { }))
                    .WithCheckpointReader(reader)
                    .WithCheckpointWriter(new FakeCheckpointWriter(x => { }))
                    .WithMessageParker(new FakeMessageParker())
                    .PreferDispatchToSingle()
                    .StartFromBeginning());
            reader.Load(null);
            sub.AddClient(Guid.NewGuid(), Guid.NewGuid(), envelope1, 10, "foo", "bar");
            sub.AddClient(Guid.NewGuid(), Guid.NewGuid(), envelope2, 10, "foo", "bar");
            var id1 = Guid.NewGuid();
            var id2 = Guid.NewGuid();
            sub.HandleReadCompleted(new[]
            {
                Helper.BuildFakeEvent(id1, "type", "streamName", 0),
                Helper.BuildFakeEvent(id2, "type", "streamName", 1)
            }, 1, false);
            Assert.Equal(2, envelope1.Replies.Count);
        }
    }

    public class DeleteTests
    {
        [Fact]
        public void subscription_deletes_checkpoint_when_deleted()
        {
            var reader = new FakeCheckpointReader();
            var deleted = false;
            var sub = new Core.Services.PersistentSubscription.PersistentSubscription(
                PersistentSubscriptionParamsBuilder.CreateFor("streamName", "groupName")
                    .WithEventLoader(new FakeStreamReader(x => { }))
                    .WithCheckpointReader(reader)
                    .WithCheckpointWriter(new FakeCheckpointWriter(x => { }, () => { deleted = true; }))
                    .WithMessageParker(new FakeMessageParker())
                    .StartFromCurrent());
            reader.Load(null);
            sub.Delete();
            Assert.True(deleted);
        }

        [Fact]
        public void subscription_deletes_parked_messages_when_deleted()
        {
            var reader = new FakeCheckpointReader();
            var deleted = false;
            var sub = new Core.Services.PersistentSubscription.PersistentSubscription(
                PersistentSubscriptionParamsBuilder.CreateFor("streamName", "groupName")
                    .WithEventLoader(new FakeStreamReader(x => { }))
                    .WithCheckpointReader(reader)
                    .WithCheckpointWriter(new FakeCheckpointWriter(x => { }))
                    .WithMessageParker(new FakeMessageParker(() => { deleted = true; }))
                    .StartFromCurrent());
            reader.Load(null);
            sub.Delete();
            Assert.True(deleted);
        }
    }

    public class SynchronousReadingClient
    {
        [Fact]
        public void subscription_with_less_than_n_events_returns_less_events_to_the_client()
        {
            var reader = new FakeCheckpointReader();
            var sub = new Core.Services.PersistentSubscription.PersistentSubscription(
                PersistentSubscriptionParamsBuilder.CreateFor("streamName", "groupName")
                    .WithEventLoader(new FakeStreamReader(x => { }))
                    .WithCheckpointReader(reader)
                    .WithCheckpointWriter(new FakeCheckpointWriter(x => { }))
                    .WithMessageParker(new FakeMessageParker())
                    .StartFromBeginning());
            reader.Load(null);
            var id1 = Guid.NewGuid();
            var id2 = Guid.NewGuid();
            sub.HandleReadCompleted(new[]
            {
                Helper.BuildFakeEvent(id1, "type", "streamName", 0),
                Helper.BuildFakeEvent(id2, "type", "streamName", 1)
            }, 1, false);
            var result = sub.GetNextNOrLessMessages(5).ToArray();
            Assert.Equal(2, result.Length);
            Assert.Equal(id1, result[0].Event.EventId);
            Assert.Equal(id2, result[1].Event.EventId);
        }

        [Fact]
        public void subscription_with_n_events_returns_n_events_to_the_client()
        {
            var reader = new FakeCheckpointReader();
            var sub = new Core.Services.PersistentSubscription.PersistentSubscription(
                PersistentSubscriptionParamsBuilder.CreateFor("streamName", "groupName")
                    .WithEventLoader(new FakeStreamReader(x => { }))
                    .WithCheckpointReader(reader)
                    .WithCheckpointWriter(new FakeCheckpointWriter(x => { }))
                    .WithMessageParker(new FakeMessageParker())
                    .StartFromBeginning());
            reader.Load(null);
            var id1 = Guid.NewGuid();
            var id2 = Guid.NewGuid();
            sub.HandleReadCompleted(new[]
            {
                Helper.BuildFakeEvent(id1, "type", "streamName", 0),
                Helper.BuildFakeEvent(id2, "type", "streamName", 1)
            }, 1, false);
            var result = sub.GetNextNOrLessMessages(2).ToArray();
            Assert.Equal(2, result.Length);
            Assert.Equal(id1, result[0].Event.EventId);
            Assert.Equal(id2, result[1].Event.EventId);
        }

        [Fact]
        public void subscription_with_more_than_n_events_returns_n_events_to_the_client()
        {
            var reader = new FakeCheckpointReader();
            var sub = new Core.Services.PersistentSubscription.PersistentSubscription(
                PersistentSubscriptionParamsBuilder.CreateFor("streamName", "groupName")
                    .WithEventLoader(new FakeStreamReader(x => { }))
                    .WithCheckpointReader(reader)
                    .WithCheckpointWriter(new FakeCheckpointWriter(x => { }))
                    .WithMessageParker(new FakeMessageParker())
                    .StartFromBeginning());
            reader.Load(null);
            var id1 = Guid.NewGuid();
            var id2 = Guid.NewGuid();
            var id3 = Guid.NewGuid();
            var id4 = Guid.NewGuid();
            sub.HandleReadCompleted(new[]
            {
                Helper.BuildFakeEvent(id1, "type", "streamName", 0),
                Helper.BuildFakeEvent(id2, "type", "streamName", 1),
                Helper.BuildFakeEvent(id3, "type", "streamName", 2),
                Helper.BuildFakeEvent(id4, "type", "streamName", 3)
            }, 1, false);
            var result = sub.GetNextNOrLessMessages(2).ToArray();
            Assert.Equal(2, result.Length);
            Assert.Equal(id1, result[0].Event.EventId);
            Assert.Equal(id2, result[1].Event.EventId);
        }


        [Fact]
        public void subscription_with_no_events_returns_no_events_to_the_client()
        {
            var reader = new FakeCheckpointReader();
            var sub = new Core.Services.PersistentSubscription.PersistentSubscription(
                PersistentSubscriptionParamsBuilder.CreateFor("streamName", "groupName")
                    .WithEventLoader(new FakeStreamReader(x => { }))
                    .WithCheckpointReader(reader)
                    .WithCheckpointWriter(new FakeCheckpointWriter(x => { }))
                    .WithMessageParker(new FakeMessageParker())
                    .StartFromBeginning());
            reader.Load(null);
            var result = sub.GetNextNOrLessMessages(5);
            Assert.Equal(0, result.Count());
        }

    }

    public class Checkpointing
    {
        [Fact]
        public void subscription_does_not_write_checkpoint_when_max_not_hit()
        {
            int cp = -1;
            var envelope1 = new FakeEnvelope();
            var reader = new FakeCheckpointReader();
            var sub = new Core.Services.PersistentSubscription.PersistentSubscription(
                PersistentSubscriptionParamsBuilder.CreateFor("streamName", "groupName")
                    .WithEventLoader(new FakeStreamReader(x => { }))
                    .WithCheckpointReader(reader)
                    .WithCheckpointWriter(new FakeCheckpointWriter(i => cp = i))
                    .WithMessageParker(new FakeMessageParker())
                    .PreferDispatchToSingle()
                    .StartFromBeginning()
                    .MinimumToCheckPoint(5)
                    .MaximumToCheckPoint(20));
            reader.Load(null);
            var corrid = Guid.NewGuid();
            sub.AddClient(corrid, Guid.NewGuid(), envelope1, 10, "foo", "bar");
            sub.AddClient(Guid.NewGuid(), Guid.NewGuid(), envelope1, 10, "foo", "bar");
            var id = Guid.NewGuid();
            sub.HandleReadCompleted(new[]
            {
                Helper.BuildFakeEvent(id, "type", "streamName", 0),
                Helper.BuildFakeEvent(Guid.NewGuid(), "type", "streamName", 1)
            }, 1, false);
            sub.AcknowledgeMessagesProcessed(corrid, new[] { id });
            Assert.Equal(-1, cp);
        }

        [Fact]
        public void subscription_does_not_write_checkpoint_when_min_hit()
        {
            int cp = -1;
            var envelope1 = new FakeEnvelope();
            var reader = new FakeCheckpointReader();
            var sub = new Core.Services.PersistentSubscription.PersistentSubscription(
                PersistentSubscriptionParamsBuilder.CreateFor("streamName", "groupName")
                    .WithEventLoader(new FakeStreamReader(x => { }))
                    .WithCheckpointReader(reader)
                    .WithCheckpointWriter(new FakeCheckpointWriter(i => cp = i))
                    .WithMessageParker(new FakeMessageParker())
                    .PreferDispatchToSingle()
                    .StartFromBeginning()
                    .MinimumToCheckPoint(1)
                    .MaximumToCheckPoint(20));
            reader.Load(null);
            var corrid = Guid.NewGuid();
            sub.AddClient(corrid, Guid.NewGuid(), envelope1, 10, "foo", "bar");
            sub.AddClient(Guid.NewGuid(), Guid.NewGuid(), envelope1, 10, "foo", "bar");
            var id = Guid.NewGuid();
            sub.HandleReadCompleted(new[]
            {
                Helper.BuildFakeEvent(id, "type", "streamName", 0),
                Helper.BuildFakeEvent(Guid.NewGuid(), "type", "streamName", 1)
            }, 1, false);
            sub.AcknowledgeMessagesProcessed(corrid, new[] { id });
            Assert.Equal(-1, cp);
        }

        [Fact]
        public void subscription_does_write_checkpoint_when_max_is_hit()
        {
            int cp = -1;
            var envelope1 = new FakeEnvelope();
            var reader = new FakeCheckpointReader();
            var sub = new Core.Services.PersistentSubscription.PersistentSubscription(
                PersistentSubscriptionParamsBuilder.CreateFor("streamName", "groupName")
                    .WithEventLoader(new FakeStreamReader(x => { }))
                    .WithCheckpointReader(reader)
                    .WithCheckpointWriter(new FakeCheckpointWriter(i => cp = i))
                    .WithMessageParker(new FakeMessageParker())
                    .PreferDispatchToSingle()
                    .StartFromBeginning()
                    .MaximumToCheckPoint(1));
            reader.Load(null);
            var corrid = Guid.NewGuid();
            sub.AddClient(corrid, Guid.NewGuid(), envelope1, 10, "foo", "bar");
            sub.AddClient(Guid.NewGuid(), Guid.NewGuid(), envelope1, 10, "foo", "bar");
            var id = Guid.NewGuid();
            sub.HandleReadCompleted(new[]
            {
                Helper.BuildFakeEvent(id, "type", "streamName", 0),
                Helper.BuildFakeEvent(Guid.NewGuid(), "type", "streamName", 1)
            }, 1, false);
            sub.AcknowledgeMessagesProcessed(corrid, new[] { id });
            Assert.Equal(1, cp);
        }

        [Fact]
        public void subscription_does_not_include_not_acked_messages_when_max_is_hit()
        {
            int cp = -1;
            var envelope1 = new FakeEnvelope();
            var reader = new FakeCheckpointReader();
            var sub = new Core.Services.PersistentSubscription.PersistentSubscription(
                PersistentSubscriptionParamsBuilder.CreateFor("streamName", "groupName")
                    .WithEventLoader(new FakeStreamReader(x => { }))
                    .WithCheckpointReader(reader)
                    .WithCheckpointWriter(new FakeCheckpointWriter(i => cp = i))
                    .WithMessageParker(new FakeMessageParker())
                    .PreferDispatchToSingle()
                    .StartFromBeginning()
                    .MaximumToCheckPoint(1));
            reader.Load(null);
            var corrid = Guid.NewGuid();
            sub.AddClient(corrid, Guid.NewGuid(), envelope1, 10, "foo", "bar");
            sub.AddClient(Guid.NewGuid(), Guid.NewGuid(), envelope1, 10, "foo", "bar");
            var id = Guid.NewGuid();
            sub.HandleReadCompleted(new[]
            {
                Helper.BuildFakeEvent(id, "type", "streamName", 0),
                Helper.BuildFakeEvent(Guid.NewGuid(), "type", "streamName", 1),
                Helper.BuildFakeEvent(Guid.NewGuid(), "type", "streamName", 2),
                Helper.BuildFakeEvent(Guid.NewGuid(), "type", "streamName", 3)
            }, 1, false);
            sub.AcknowledgeMessagesProcessed(corrid, new[] { id });
            Assert.Equal(1, cp);
        }


        [Fact]
        public void subscription_does_write_checkpoint_on_time_when_min_is_hit()
        {
            int cp = -1;
            var envelope1 = new FakeEnvelope();
            var reader = new FakeCheckpointReader();
            var sub = new Core.Services.PersistentSubscription.PersistentSubscription(
                PersistentSubscriptionParamsBuilder.CreateFor("streamName", "groupName")
                    .WithEventLoader(new FakeStreamReader(x => { }))
                    .WithCheckpointReader(reader)
                    .WithCheckpointWriter(new FakeCheckpointWriter(i => cp = i))
                    .PreferDispatchToSingle()
                    .WithMessageParker(new FakeMessageParker())
                    .StartFromBeginning()
                    .MinimumToCheckPoint(1)
                    .MaximumToCheckPoint(5));
            reader.Load(null);
            var corrid = Guid.NewGuid();
            sub.AddClient(corrid, Guid.NewGuid(), envelope1, 10, "foo", "bar");
            sub.AddClient(Guid.NewGuid(), Guid.NewGuid(), envelope1, 10, "foo", "bar");
            var id = Guid.NewGuid();
            sub.HandleReadCompleted(new[]
            {
                Helper.BuildFakeEvent(id, "type", "streamName", 0),
                Helper.BuildFakeEvent(Guid.NewGuid(), "type", "streamName", 1)
            }, 1, false);
            sub.AcknowledgeMessagesProcessed(corrid, new[] { id });
            sub.NotifyClockTick(DateTime.UtcNow);
            Assert.Equal(1, cp);
        }

        [Fact]
        public void subscription_does_not_write_checkpoint_on_time_when_min_is_not_hit()
        {
            int cp = -1;
            var envelope1 = new FakeEnvelope();
            var reader = new FakeCheckpointReader();
            var sub = new Core.Services.PersistentSubscription.PersistentSubscription(
                PersistentSubscriptionParamsBuilder.CreateFor("streamName", "groupName")
                    .WithEventLoader(new FakeStreamReader(x => { }))
                    .WithCheckpointReader(reader)
                    .WithCheckpointWriter(new FakeCheckpointWriter(i => cp = i))
                    .WithMessageParker(new FakeMessageParker())
                    .PreferDispatchToSingle()
                    .StartFromBeginning()
                    .MinimumToCheckPoint(2)
                    .MaximumToCheckPoint(5));
            reader.Load(null);
            var corrid = Guid.NewGuid();
            sub.AddClient(corrid, Guid.NewGuid(), envelope1, 10, "foo", "bar");
            sub.AddClient(Guid.NewGuid(), Guid.NewGuid(), envelope1, 10, "foo", "bar");
            var id = Guid.NewGuid();
            sub.HandleReadCompleted(new[]
            {
                Helper.BuildFakeEvent(id, "type", "streamName", 0),
                Helper.BuildFakeEvent(Guid.NewGuid(), "type", "streamName", 1)
            }, 1, false);
            sub.AcknowledgeMessagesProcessed(corrid, new[] { id });
            sub.NotifyClockTick(DateTime.UtcNow);
            Assert.Equal(1, cp);
        }
    }

    public class TimeoutTests
    {
        [Fact]
        public void with_no_timeouts_to_happen_no_timeouts_happen()
        {
            var envelope1 = new FakeEnvelope();
            var reader = new FakeCheckpointReader();
            var parker = new FakeMessageParker();
            var sub = new Core.Services.PersistentSubscription.PersistentSubscription(
                PersistentSubscriptionParamsBuilder.CreateFor("streamName", "groupName")
                    .WithEventLoader(new FakeStreamReader(x => { }))
                    .WithCheckpointReader(reader)
                    .WithCheckpointWriter(new FakeCheckpointWriter(i => { }))
                    .WithMessageParker(parker)
                    .PreferDispatchToSingle()
                    .StartFromBeginning()
                    .WithMessageTimeoutOf(TimeSpan.FromSeconds(3)));
            reader.Load(null);
            sub.AddClient(Guid.NewGuid(), Guid.NewGuid(), envelope1, 10, "foo", "bar");
            var id1 = Guid.NewGuid();
            var id2 = Guid.NewGuid();
            sub.HandleReadCompleted(new[]
            {
                Helper.BuildFakeEvent(id1, "type", "streamName", 0),
                Helper.BuildFakeEvent(id2, "type", "streamName", 1)
            }, 1, false);
            envelope1.Replies.Clear();
            sub.NotifyClockTick(DateTime.UtcNow.AddSeconds(1));
            Assert.Equal(0, envelope1.Replies.Count);
            Assert.Equal(0, parker.ParkedEvents.Count);
        }

        [Fact]
        public void messages_get_timed_out_and_resent_to_clients()
        {
            var envelope1 = new FakeEnvelope();
            var reader = new FakeCheckpointReader();
            var parker = new FakeMessageParker();
            var sub = new Core.Services.PersistentSubscription.PersistentSubscription(
                PersistentSubscriptionParamsBuilder.CreateFor("streamName", "groupName")
                    .WithEventLoader(new FakeStreamReader(x => { }))
                    .WithCheckpointReader(reader)
                    .WithCheckpointWriter(new FakeCheckpointWriter(i => { }))
                    .WithMessageParker(parker)
                    .PreferDispatchToSingle()
                    .StartFromBeginning()
                    .WithMessageTimeoutOf(TimeSpan.FromSeconds(1)));
            reader.Load(null);
            sub.AddClient(Guid.NewGuid(), Guid.NewGuid(), envelope1, 1, "foo", "bar");
            sub.AddClient(Guid.NewGuid(), Guid.NewGuid(), envelope1, 1, "foo", "bar");
            var id1 = Guid.NewGuid();
            var id2 = Guid.NewGuid();
            sub.HandleReadCompleted(new[]
            {
                Helper.BuildFakeEvent(id1, "type", "streamName", 0),
                Helper.BuildLinkEvent(id2, "streamName", 1, Helper.BuildFakeEvent(Guid.NewGuid(), "type", "streamSource", 0))
                
            }, 1, false);
            envelope1.Replies.Clear();
            sub.NotifyClockTick(DateTime.UtcNow.AddSeconds(3));
            Assert.Equal(2, envelope1.Replies.Count);
            var msg1 = (Messages.ClientMessage.PersistentSubscriptionStreamEventAppeared)envelope1.Replies[0];
            var msg2 = (Messages.ClientMessage.PersistentSubscriptionStreamEventAppeared)envelope1.Replies[1];
            Assert.True(id1 == msg1.Event.OriginalEvent.EventId || id1 == msg2.Event.OriginalEvent.EventId);
            Assert.True(id2 == msg1.Event.OriginalEvent.EventId || id2 == msg2.Event.OriginalEvent.EventId);
            Assert.Equal(0, parker.ParkedEvents.Count);
        }

        [Fact]
        public void messages_get_timed_out_on_synchronous_reads()
        {
            var reader = new FakeCheckpointReader();
            var parker = new FakeMessageParker();
            var sub = new Core.Services.PersistentSubscription.PersistentSubscription(
                PersistentSubscriptionParamsBuilder.CreateFor("streamName", "groupName")
                    .WithEventLoader(new FakeStreamReader(x => { }))
                    .WithCheckpointReader(reader)
                    .WithCheckpointWriter(new FakeCheckpointWriter(i => { }))
                    .WithMessageParker(parker)
                    .PreferDispatchToSingle()
                    .StartFromBeginning()
                    .WithMessageTimeoutOf(TimeSpan.FromSeconds(1)));
            reader.Load(null);
            var id1 = Guid.NewGuid();
            var id2 = Guid.NewGuid();
            sub.HandleReadCompleted(new[]
            {
                Helper.BuildFakeEvent(id1, "type", "streamName", 0),
                Helper.BuildFakeEvent(id2, "type", "streamName", 1)
            }, 1, false);
            sub.GetNextNOrLessMessages(2);
            sub.NotifyClockTick(DateTime.Now.AddSeconds(3));
            var retries = sub.GetNextNOrLessMessages(2).ToArray();
            Assert.Equal(id1, retries[0].Event.EventId);
            Assert.Equal(id2, retries[1].Event.EventId);
            Assert.Equal(0, parker.ParkedEvents.Count);
        }

        [Fact]
        public void messages_dont_get_retried_when_acked_on_synchronous_reads()
        {
            var reader = new FakeCheckpointReader();
            var parker = new FakeMessageParker();
            var sub = new Core.Services.PersistentSubscription.PersistentSubscription(
                PersistentSubscriptionParamsBuilder.CreateFor("streamName", "groupName")
                    .WithEventLoader(new FakeStreamReader(x => { }))
                    .WithCheckpointReader(reader)
                    .WithCheckpointWriter(new FakeCheckpointWriter(i => { }))
                    .WithMessageParker(parker)
                    .PreferDispatchToSingle()
                    .StartFromBeginning()
                    .WithMessageTimeoutOf(TimeSpan.FromSeconds(1)));
            reader.Load(null);
            var id1 = Guid.NewGuid();
            var id2 = Guid.NewGuid();
            sub.HandleReadCompleted(new[]
            {
                Helper.BuildFakeEvent(id1, "type", "streamName", 0),
                Helper.BuildFakeEvent(id2, "type", "streamName", 1)
            }, 1, false);
            sub.GetNextNOrLessMessages(2).ToArray();
            sub.AcknowledgeMessagesProcessed(Guid.Empty, new[] { id1, id2 });
            sub.NotifyClockTick(DateTime.Now.AddSeconds(3));
            var retries = sub.GetNextNOrLessMessages(2).ToArray();
            Assert.Equal(0, retries.Length);
            Assert.Equal(0, parker.ParkedEvents.Count);
        }

        [Fact]
        public void message_gets_timed_out_and_parked_after_max_retry_count()
        {
            var envelope1 = new FakeEnvelope();
            var reader = new FakeCheckpointReader();
            var parker = new FakeMessageParker();
            var sub = new Core.Services.PersistentSubscription.PersistentSubscription(
                PersistentSubscriptionParamsBuilder.CreateFor("streamName", "groupName")
                    .WithEventLoader(new FakeStreamReader(x => { }))
                    .WithCheckpointReader(reader)
                    .WithCheckpointWriter(new FakeCheckpointWriter(i => { }))
                    .WithMessageParker(parker)
                    .PreferDispatchToSingle()
                    .StartFromBeginning()
                    .WithMaxRetriesOf(0)
                    .WithMessageTimeoutOf(TimeSpan.FromSeconds(1)));
            reader.Load(null);
            sub.AddClient(Guid.NewGuid(), Guid.NewGuid(), envelope1, 10, "foo", "bar");
            var id1 = Guid.NewGuid();
            sub.HandleReadCompleted(new[]
            {
                Helper.BuildFakeEvent(id1, "type", "streamName", 0),
            }, 1, false);
            envelope1.Replies.Clear();
            sub.NotifyClockTick(DateTime.UtcNow.AddSeconds(3));
            Assert.Equal(0, envelope1.Replies.Count);
            Assert.Equal(1, parker.ParkedEvents.Count);
            Assert.Equal(id1, parker.ParkedEvents[0].OriginalEvent.EventId);
        }

        [Fact]
        public void multiple_messages_get_timed_out_and_parked_after_max_retry_count()
        {
            var envelope1 = new FakeEnvelope();
            var reader = new FakeCheckpointReader();
            var parker = new FakeMessageParker();
            var sub = new Core.Services.PersistentSubscription.PersistentSubscription(
                PersistentSubscriptionParamsBuilder.CreateFor("streamName", "groupName")
                    .WithEventLoader(new FakeStreamReader(x => { }))
                    .WithCheckpointReader(reader)
                    .WithCheckpointWriter(new FakeCheckpointWriter(i => { }))
                    .WithMessageParker(parker)
                    .PreferDispatchToSingle()
                    .StartFromBeginning()
                    .WithMaxRetriesOf(0)
                    .WithMessageTimeoutOf(TimeSpan.FromSeconds(1)));
            reader.Load(null);
            sub.AddClient(Guid.NewGuid(), Guid.NewGuid(), envelope1, 10, "foo", "bar");
            var id1 = Guid.NewGuid();
            var id2 = Guid.NewGuid();
            sub.HandleReadCompleted(new[]
            {
                Helper.BuildFakeEvent(id1, "type", "streamName", 0),
                Helper.BuildFakeEvent(id2, "type", "streamName", 1),
            }, 1, false);
            envelope1.Replies.Clear();
            sub.NotifyClockTick(DateTime.UtcNow.AddSeconds(3));
            Assert.Equal(0, envelope1.Replies.Count);
            Assert.Equal(2, parker.ParkedEvents.Count);
            Assert.True(id1 == parker.ParkedEvents[0].OriginalEvent.EventId ||
                          id1 == parker.ParkedEvents[1].OriginalEvent.EventId);
            Assert.True(id2 == parker.ParkedEvents[0].OriginalEvent.EventId ||
                          id2 == parker.ParkedEvents[1].OriginalEvent.EventId);
        }

        [Fact]
        public void timeout_park_correctly_tracks_the_available_client_slots()
        {
            var envelope1 = new FakeEnvelope();
            var reader = new FakeCheckpointReader();
            var parker = new FakeMessageParker();
            var sub = new Core.Services.PersistentSubscription.PersistentSubscription(
                PersistentSubscriptionParamsBuilder.CreateFor("streamName", "groupName")
                    .WithEventLoader(new FakeStreamReader(x => { }))
                    .WithCheckpointReader(reader)
                    .WithCheckpointWriter(new FakeCheckpointWriter(i => { }))
                    .WithMessageParker(parker)
                    .WithMaxRetriesOf(0)
                    .WithMessageTimeoutOf(TimeSpan.Zero)
                    .StartFromBeginning());
            reader.Load(null);
            sub.AddClient(Guid.NewGuid(), Guid.NewGuid(), envelope1, 2, "foo", "bar");

            sub.HandleReadCompleted(new[]
            {
                Helper.BuildFakeEvent(Guid.NewGuid(), "type", "streamName", 0),
                Helper.BuildFakeEvent(Guid.NewGuid(), "type", "streamName", 1)
            }, 1, false);

            Assert.Equal(2, envelope1.Replies.Count);

            // Should expire first 2 and send to park.
            sub.NotifyClockTick(DateTime.UtcNow.AddSeconds(1));
            parker.ParkMessageCompleted(0, OperationResult.Success);
            parker.ParkMessageCompleted(1, OperationResult.Success);
            Assert.Equal(2, parker.ParkedEvents.Count);

            // The next 2 should still be sent to client.
            sub.HandleReadCompleted(new[]
            {
                Helper.BuildFakeEvent(Guid.NewGuid(), "type", "streamName", 0),
                Helper.BuildFakeEvent(Guid.NewGuid(), "type", "streamName", 1)
            }, 1, false);

            Assert.Equal(4, envelope1.Replies.Count);
        }

    }

    public class NAKTests
    {
        [Fact]
        public void explicit_nak_with_park_parks_the_message()
        {
            var envelope1 = new FakeEnvelope();
            var reader = new FakeCheckpointReader();
            var parker = new FakeMessageParker();
            var sub = new Core.Services.PersistentSubscription.PersistentSubscription(
                PersistentSubscriptionParamsBuilder.CreateFor("streamName", "groupName")
                    .WithEventLoader(new FakeStreamReader(x => { }))
                    .WithCheckpointReader(reader)
                    .WithCheckpointWriter(new FakeCheckpointWriter(i => { }))
                    .WithMessageParker(parker)
                    .StartFromBeginning());
            reader.Load(null);
            var corrid = Guid.NewGuid();
            sub.AddClient(corrid, Guid.NewGuid(), envelope1, 10, "foo", "bar");
            var id1 = Guid.NewGuid();
            sub.HandleReadCompleted(new[]
            {
                Helper.BuildFakeEvent(id1, "type", "streamName", 0),
            }, 1, false);
            envelope1.Replies.Clear();
            sub.NotAcknowledgeMessagesProcessed(corrid, new[] { id1 }, NakAction.Park, "a reason from client.");
            Assert.Equal(0, envelope1.Replies.Count);
            Assert.Equal(1, parker.ParkedEvents.Count);
            Assert.Equal(id1, parker.ParkedEvents[0].OriginalEvent.EventId);
        }

        [Fact]
        public void explicit_nak_with_skip_skips_the_message()
        {
            var envelope1 = new FakeEnvelope();
            var reader = new FakeCheckpointReader();
            var parker = new FakeMessageParker();
            var sub = new Core.Services.PersistentSubscription.PersistentSubscription(
                PersistentSubscriptionParamsBuilder.CreateFor("streamName", "groupName")
                    .WithEventLoader(new FakeStreamReader(x => { }))
                    .WithCheckpointReader(reader)
                    .WithCheckpointWriter(new FakeCheckpointWriter(i => { }))
                    .WithMessageParker(parker)
                    .StartFromBeginning());
            reader.Load(null);
            var corrid = Guid.NewGuid();
            sub.AddClient(corrid, Guid.NewGuid(), envelope1, 10, "foo", "bar");
            var id1 = Guid.NewGuid();
            sub.HandleReadCompleted(new[]
            {
                Helper.BuildFakeEvent(id1, "type", "streamName", 0),
            }, 1, false);
            envelope1.Replies.Clear();
            sub.NotAcknowledgeMessagesProcessed(corrid, new[] { id1 }, NakAction.Skip, "a reason from client.");
            Assert.Equal(0, envelope1.Replies.Count);
            Assert.Equal(0, parker.ParkedEvents.Count);
        }

        [Fact]
        public void explicit_nak_with_default_retries_the_message()
        {
            var envelope1 = new FakeEnvelope();
            var reader = new FakeCheckpointReader();
            var parker = new FakeMessageParker();
            var sub = new Core.Services.PersistentSubscription.PersistentSubscription(
                PersistentSubscriptionParamsBuilder.CreateFor("streamName", "groupName")
                    .WithEventLoader(new FakeStreamReader(x => { }))
                    .WithCheckpointReader(reader)
                    .WithCheckpointWriter(new FakeCheckpointWriter(i => { }))
                    .WithMessageParker(parker)
                    .StartFromBeginning());
            reader.Load(null);
            var corrid = Guid.NewGuid();
            sub.AddClient(corrid, Guid.NewGuid(), envelope1, 10, "foo", "bar");
            var id1 = Guid.NewGuid();
            sub.HandleReadCompleted(new[]
            {
                Helper.BuildFakeEvent(id1, "type", "streamName", 0),
            }, 1, false);
            envelope1.Replies.Clear();
            sub.NotAcknowledgeMessagesProcessed(corrid, new[] { id1 }, NakAction.Unknown, "a reason from client.");
            Assert.Equal(1, envelope1.Replies.Count);
            Assert.Equal(0, parker.ParkedEvents.Count);
        }

        [Fact]
        public void explicit_nak_with_retry_retries_the_message()
        {
            var envelope1 = new FakeEnvelope();
            var reader = new FakeCheckpointReader();
            var parker = new FakeMessageParker();
            var sub = new Core.Services.PersistentSubscription.PersistentSubscription(
                PersistentSubscriptionParamsBuilder.CreateFor("streamName", "groupName")
                    .WithEventLoader(new FakeStreamReader(x => { }))
                    .WithCheckpointReader(reader)
                    .WithCheckpointWriter(new FakeCheckpointWriter(i => { }))
                    .WithMessageParker(parker)
                    .StartFromBeginning());
            reader.Load(null);
            var corrid = Guid.NewGuid();
            sub.AddClient(corrid, Guid.NewGuid(), envelope1, 10, "foo", "bar");
            var id1 = Guid.NewGuid();
            sub.HandleReadCompleted(new[]
            {
                Helper.BuildFakeEvent(id1, "type", "streamName", 0),
            }, 1, false);
            envelope1.Replies.Clear();
            sub.NotAcknowledgeMessagesProcessed(corrid, new[] { id1 }, NakAction.Retry, "a reason from client.");
            Assert.Equal(1, envelope1.Replies.Count);
            Assert.Equal(id1, ((ClientMessage.PersistentSubscriptionStreamEventAppeared)envelope1.Replies[0]).Event.Event.EventId);
            Assert.Equal(0, parker.ParkedEvents.Count);
        }

        [Fact]
        public void explicit_nak_with_retry_correctly_tracks_the_available_client_slots()
        {
            var envelope1 = new FakeEnvelope();
            var reader = new FakeCheckpointReader();
            var parker = new FakeMessageParker();
            var sub = new Core.Services.PersistentSubscription.PersistentSubscription(
                PersistentSubscriptionParamsBuilder.CreateFor("streamName", "groupName")
                    .WithEventLoader(new FakeStreamReader(x => { }))
                    .WithCheckpointReader(reader)
                    .WithCheckpointWriter(new FakeCheckpointWriter(i => { }))
                    .WithMaxRetriesOf(10)
                    .WithMessageParker(parker)
                    .StartFromBeginning());
            reader.Load(null);
            var corrid = Guid.NewGuid();
            sub.AddClient(corrid, Guid.NewGuid(), envelope1, 10, "foo", "bar");
            var id1 = Guid.NewGuid();
            var ev = Helper.BuildFakeEvent(id1, "type", "streamName", 0);
            sub.HandleReadCompleted(new[]
            {
                ev,
            }, 1, false);

            for (int i = 1; i < 11; i++)
            {
                sub.NotAcknowledgeMessagesProcessed(corrid, new[] { id1 }, NakAction.Retry, "a reason from client.");
                Assert.Equal(i + 1, envelope1.Replies.Count);
            }

            Assert.DoesNotContain(ev, parker.ParkedEvents);

            //This time should be parked
            sub.NotAcknowledgeMessagesProcessed(corrid, new[] { id1 }, NakAction.Retry, "a reason from client.");
            Assert.Equal(11, envelope1.Replies.Count);
            Assert.Contains(ev, parker.ParkedEvents);
        }

        [Fact]
        public void explicit_nak_with_park_correctly_tracks_the_available_client_slots()
        {
            var envelope1 = new FakeEnvelope();
            var reader = new FakeCheckpointReader();
            var parker = new FakeMessageParker();
            var sub = new Core.Services.PersistentSubscription.PersistentSubscription(
                PersistentSubscriptionParamsBuilder.CreateFor("streamName", "groupName")
                    .WithEventLoader(new FakeStreamReader(x => { }))
                    .WithCheckpointReader(reader)
                    .WithCheckpointWriter(new FakeCheckpointWriter(i => { }))
                    .WithMessageParker(parker)
                    .WithMaxRetriesOf(0)
                    .WithMessageTimeoutOf(TimeSpan.Zero)
                    .StartFromBeginning());
            reader.Load(null);
            var corrid = Guid.NewGuid();
            sub.AddClient(corrid, Guid.NewGuid(), envelope1, 1, "foo", "bar");

            var id1 = Guid.NewGuid();
            var id2 = Guid.NewGuid();
            sub.HandleReadCompleted(new[]
            {
                Helper.BuildFakeEvent(id1, "type", "streamName", 0),
                Helper.BuildFakeEvent(id2, "type", "streamName", 1),
                Helper.BuildFakeEvent(Guid.NewGuid(), "type", "streamName", 2)
            }, 1, false);

            Assert.Equal(1, envelope1.Replies.Count);

            sub.NotAcknowledgeMessagesProcessed(corrid, new[] { id1 }, NakAction.Park, "a reason from client.");
            Assert.Equal(2, envelope1.Replies.Count);
            Assert.Single(parker.ParkedEvents, e => e.Event.EventId == id1);
            
            sub.NotAcknowledgeMessagesProcessed(corrid, new[] { id2 }, NakAction.Park, "a reason from client.");
            Assert.Single(parker.ParkedEvents, e => e.Event.EventId == id2);
            Assert.Equal(3, envelope1.Replies.Count);
        }
    }

    public class AddingClientTests
    {
        [Fact]
        public void adding_a_client_adds_the_client()
        {
            var sub = new Core.Services.PersistentSubscription.PersistentSubscription(
                PersistentSubscriptionParamsBuilder.CreateFor("streamName", "groupName")
                    .WithEventLoader(new FakeStreamReader(x => { }))
                    .WithCheckpointReader(new FakeCheckpointReader())
                    .WithMessageParker(new FakeMessageParker())
                    .WithCheckpointWriter(new FakeCheckpointWriter(x => { })));

            sub.AddClient(Guid.NewGuid(), Guid.NewGuid(), new FakeEnvelope(), 1, "foo", "bar");
            Assert.True(sub.HasClients);
            Assert.Equal(1, sub.ClientCount);
        }
    }

    public class RemoveClientTests
    {
        [Fact]
        public void unsubscribing_a_client_retries_inflight_messages_immediately()
        {
            var client1Envelope = new FakeEnvelope();
            var client2Envelope = new FakeEnvelope();

            var fakeCheckpointReader = new FakeCheckpointReader();
            var sub = new Core.Services.PersistentSubscription.PersistentSubscription(
                PersistentSubscriptionParamsBuilder.CreateFor("streamName", "groupName")
                    .WithEventLoader(new FakeStreamReader(x => { }))
                    .WithCheckpointReader(fakeCheckpointReader)
                    .WithMessageParker(new FakeMessageParker())
                    .PreferRoundRobin()
                    .StartFromCurrent()
                    .WithCheckpointWriter(new FakeCheckpointWriter(x => { })));

            fakeCheckpointReader.Load(null);

            sub.AddClient(Guid.NewGuid(), Guid.NewGuid(), client1Envelope, 10, "foo", "bar");
            var client2Id = Guid.NewGuid();
            sub.AddClient(client2Id, Guid.NewGuid(), client2Envelope, 10, "foo", "bar");


            Assert.True(sub.HasClients);
            Assert.Equal(2, sub.ClientCount);

            sub.NotifyLiveSubscriptionMessage(Helper.BuildFakeEvent(Guid.NewGuid(), "type", "streamName", 0));
            sub.NotifyLiveSubscriptionMessage(Helper.BuildFakeEvent(Guid.NewGuid(), "type", "streamName", 1));

            Assert.Equal(1, client1Envelope.Replies.Count);
            Assert.Equal(1, client2Envelope.Replies.Count);

            sub.RemoveClientByCorrelationId(client2Id, false);
            Assert.Equal(1, sub.ClientCount);

            // Message 2 should be retried on client 1 as it wasn't acked.
            Assert.Equal(2, client1Envelope.Replies.Count);
            Assert.Equal(1, client2Envelope.Replies.Count);
        }

        [Fact]
        public void disconnecting_a_client_retries_inflight_messages_immediately()
        {
            var client1Envelope = new FakeEnvelope();
            var client2Envelope = new FakeEnvelope();

            var fakeCheckpointReader = new FakeCheckpointReader();
            var sub = new Core.Services.PersistentSubscription.PersistentSubscription(
                PersistentSubscriptionParamsBuilder.CreateFor("streamName", "groupName")
                    .WithEventLoader(new FakeStreamReader(x => { }))
                    .WithCheckpointReader(fakeCheckpointReader)
                    .WithMessageParker(new FakeMessageParker())
                    .PreferRoundRobin()
                    .StartFromCurrent()
            .WithCheckpointWriter(new FakeCheckpointWriter(x => { })));

            fakeCheckpointReader.Load(null);

            sub.AddClient(Guid.NewGuid(), Guid.NewGuid(), client1Envelope, 10, "foo", "bar");
            var connectionId = Guid.NewGuid();
            sub.AddClient(Guid.NewGuid(), connectionId, client2Envelope, 10, "foo", "bar");

            Assert.True(sub.HasClients);
            Assert.Equal(2, sub.ClientCount);

            sub.NotifyLiveSubscriptionMessage(Helper.BuildFakeEvent(Guid.NewGuid(), "type", "streamName", 0));
            sub.NotifyLiveSubscriptionMessage(Helper.BuildFakeEvent(Guid.NewGuid(), "type", "streamName", 1));

            Assert.Equal(1, client1Envelope.Replies.Count);
            Assert.Equal(1, client2Envelope.Replies.Count);

            sub.RemoveClientByConnectionId(connectionId);

            Assert.Equal(1, sub.ClientCount);

            // Message 2 should be retried on client 1 as it wasn't acked.
            Assert.Equal(2, client1Envelope.Replies.Count);
            Assert.Equal(1, client2Envelope.Replies.Count);
        }
    }

    public class DeadlockTest : SpecificationWithMiniNode
    {
        protected override void Given()
        {
            //_conn = BuildConnection(Node);
            //_conn.ConnectAsync().Wait();
        }

        protected override void When()
        {
        }

        [Fact(Skip = "very long test")]
        public void read_whilst_ack_doesnt_deadlock_with_request_response_dispatcher()
        {
            var persistentSubscriptionSettings = PersistentSubscriptionSettings.Create().Build();
            var userCredentials = DefaultData.AdminCredentials;
            _conn.CreatePersistentSubscriptionAsync("TestStream", "TestGroup", persistentSubscriptionSettings, userCredentials).Wait();

            const int count = 5000;
            _conn.AppendToStreamAsync("TestStream", ExpectedVersion.Any, CreateEvent().Take(count)).Wait();


            var received = 0;
            var manualResetEventSlim = new ManualResetEventSlim();
            var sub1 = _conn.ConnectToPersistentSubscription("TestStream", "TestGroup", (sub, ev) =>
            {
                received++;
                if (received == count)
                {
                    manualResetEventSlim.Set();
                }
            },
                (sub, reason, ex) => { });
            Assert.True(manualResetEventSlim.Wait(TimeSpan.FromSeconds(30)), "Failed to receive all events in 2 minutes. Assume event store is deadlocked.");
            sub1.Stop(TimeSpan.FromSeconds(10));
            _conn.Close();
        }

        private static IEnumerable<EventData> CreateEvent()
        {
            while (true)
            {
                yield return new EventData(Guid.NewGuid(), "testtype", false, new byte[0], new byte[0]);
            }
        }
    }

    public class Helper
    {
        public static ResolvedEvent BuildFakeEvent(Guid id, string type, string stream, int version)
        {
            return
                ResolvedEvent.ForUnresolvedEvent(new EventRecord(version, 1234567, Guid.NewGuid(), id, 1234567, 1234, stream, version,
                    DateTime.UtcNow, PrepareFlags.SingleWrite, type, new byte[0], new byte[0]));
        }

        public static ResolvedEvent BuildLinkEvent(Guid id, string stream, int version, ResolvedEvent ev, bool resolved = true)
        {
            var link = new EventRecord(version, 1234567, Guid.NewGuid(), id, 1234567, 1234, stream, version, DateTime.UtcNow, PrepareFlags.SingleWrite, SystemEventTypes.LinkTo, Encoding.UTF8.GetBytes(string.Format("{0}@{1}", ev.OriginalEventNumber, ev.OriginalStreamId)), new byte[0]);
            if (resolved)
                return ResolvedEvent.ForResolvedLink(ev.Event, link);
            else
                return ResolvedEvent.ForUnresolvedEvent(link);
        }
    }

    class FakeStreamReader : IPersistentSubscriptionStreamReader
    {
        private readonly Action<int> _action;

        public FakeStreamReader(Action<int> action)
        {
            _action = action;
        }

        public void BeginReadEvents(string stream, int startEventNumber, int countToLoad, int batchSize, bool resolveLinkTos,
            Action<ResolvedEvent[], int, bool> onEventsFound)
        {
            _action(startEventNumber);
        }
    }

    class FakeCheckpointReader : IPersistentSubscriptionCheckpointReader
    {
        private Action<int?> _onStateLoaded;

        public void BeginLoadState(string subscriptionId, Action<int?> onStateLoaded)
        {
            _onStateLoaded = onStateLoaded;
        }

        public void Load(int? state)
        {
            _onStateLoaded(state);
        }
    }

    class FakeMessageParker : IPersistentSubscriptionMessageParker
    {
        private Action<int?> _readEndSequenceCompleted;
        private Action<ResolvedEvent, OperationResult> _parkMessageCompleted;
        public List<ResolvedEvent> ParkedEvents = new List<ResolvedEvent>();
        private readonly Action _deleteAction;

        public FakeMessageParker() { }

        public FakeMessageParker(Action deleteAction)
        {
            _deleteAction = deleteAction;
        }

        public int MarkedAsProcessed { get; private set; }

        public void ReadEndSequenceCompleted(int sequence)
        {
            if (_readEndSequenceCompleted != null) _readEndSequenceCompleted(sequence);
        }

        public void ParkMessageCompleted(int idx, OperationResult result)
        {
            if (_parkMessageCompleted != null) _parkMessageCompleted(ParkedEvents[idx], result);
        }

        public void BeginParkMessage(ResolvedEvent ev, string reason, Action<ResolvedEvent, OperationResult> completed)
        {
            ParkedEvents.Add(ev);
            _parkMessageCompleted = completed;
        }

        public void BeginReadEndSequence(Action<int?> completed)
        {
            _readEndSequenceCompleted = completed;
        }

        public void BeginMarkParkedMessagesReprocessed(int sequence)
        {
            MarkedAsProcessed = sequence;
        }
        public void BeginDelete(Action<IPersistentSubscriptionMessageParker> completed)
        {
            if (_deleteAction != null)
            {
                _deleteAction();
            }
        }
    }


    class FakeCheckpointWriter : IPersistentSubscriptionCheckpointWriter
    {
        private readonly Action<int> _action;
        private readonly Action _deleteAction;

        public FakeCheckpointWriter(Action<int> action, Action deleteAction = null)
        {
            _action = action;
            _deleteAction = deleteAction;
        }

        public void BeginWriteState(int state)
        {
            _action(state);
        }

        public void BeginDelete(Action<IPersistentSubscriptionCheckpointWriter> completed)
        {
            if (_deleteAction != null)
            {
                _deleteAction();
            }
        }
    }
}
