using EventStore.ClientAPI;
using EventStore.ClientAPI.Embedded;
using EventStore.Core.Tests.Helpers;

namespace EventStore.Core.Tests.ClientAPI.Embedded
{
    public class when_working_with_stream_metadata_as_structured_info : ClientAPI.when_working_with_stream_metadata_as_structured_info
    {
        public when_working_with_stream_metadata_as_structured_info(ConnectedMiniNodeFixture data) : base(data)
        {
        }
    }
}
