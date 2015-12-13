﻿using EventStore.ClientAPI;
using EventStore.Core.Tests.ClientAPI.Helpers;
using EventStore.Core.Tests.Helpers;

namespace EventStore.Core.Tests.ClientAPI.Embedded
{
    public class read_event_should : ClientAPI.read_event_should
    {
        protected override IEventStoreConnection BuildConnection(MiniNode node)
        {
            return EmbeddedTestConnection.To(node);
        }

        public read_event_should(SpecificationFixture fixture) : base(fixture)
        {
        }
    }
}