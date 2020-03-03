using System;
using System.Threading.Tasks;
using EventStore.Core.Authentication;
using EventStore.Core.Bus;
using EventStore.Core.Messages;
using EventStore.Core.Messaging;
using EventStore.Client.Operations;
using Grpc.Core;

namespace EventStore.Core.Services.Transport.Grpc {
	public partial class Operations
		: EventStore.Client.Operations.Operations.OperationsBase {
		private readonly IPublisher _publisher;

		public Operations(IPublisher publisher) {
			if (publisher == null) throw new ArgumentNullException(nameof(publisher));
			_publisher = publisher;
		}
	}
}
