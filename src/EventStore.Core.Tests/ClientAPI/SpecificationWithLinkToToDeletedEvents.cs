using System;
using System.Text;
using EventStore.ClientAPI;
using EventStore.ClientAPI.Common;
using EventStore.ClientAPI.SystemData;
using EventStore.Core.Messages;

namespace EventStore.Core.Tests.ClientAPI
{
    public abstract class SpecificationWithLinkToToDeletedEvents : SpecificationWithMiniNode
    {
        protected string LinkedStreamName;
        protected string DeletedStreamName;
        protected override void Given()
        {
            var creds = DefaultData.AdminCredentials;
            var linkedStreamName = LinkedStreamName = Guid.NewGuid().ToString();
            var deletedStreamName = DeletedStreamName = Guid.NewGuid().ToString();
            _conn.AppendToStreamAsync(deletedStreamName, ExpectedVersion.Any, creds,
                new EventData(Guid.NewGuid(), "testing", true, Encoding.UTF8.GetBytes("{'foo' : 4}"), new byte[0])).Wait();
            _conn.AppendToStreamAsync(linkedStreamName, ExpectedVersion.Any, creds,
                new EventData(Guid.NewGuid(), SystemEventTypes.LinkTo, false, Encoding.UTF8.GetBytes("0@" + deletedStreamName), new byte[0])).Wait();
            _conn.DeleteStreamAsync(deletedStreamName, ExpectedVersion.Any).Wait();
            Fixture.AddStashedValueAssignment(this, instance =>
            {
                instance.LinkedStreamName = linkedStreamName;
                instance.DeletedStreamName = deletedStreamName;
            });
        }

        public SpecificationWithLinkToToDeletedEvents(SpecificationFixture fixture) : base(fixture)
        {
        }
    }
}