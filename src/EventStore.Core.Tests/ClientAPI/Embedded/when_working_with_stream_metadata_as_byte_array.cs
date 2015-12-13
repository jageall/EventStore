using EventStore.ClientAPI;
using EventStore.Core.Tests.ClientAPI.Helpers;
using EventStore.Core.Tests.Helpers;

namespace EventStore.Core.Tests.ClientAPI.Embedded
{
    public class when_working_with_stream_metadata_as_byte_array : ClientAPI.when_working_with_stream_metadata_as_byte_array
    {
        public when_working_with_stream_metadata_as_byte_array(ConnectedMiniNodeFixture data) : base(data)
        {
        }
    }
}
