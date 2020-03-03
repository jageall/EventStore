using System;
using EventStore.Core.Authentication;
using EventStore.Core.Bus;

namespace EventStore.Core.Services.Transport.Grpc {
	public partial class PersistentSubscriptions
		: EventStore.Client.PersistentSubscriptions.PersistentSubscriptions.PersistentSubscriptionsBase {
		private readonly IPublisher _publisher;

		public PersistentSubscriptions(IPublisher publisher) {
			if (publisher == null) throw new ArgumentNullException(nameof(publisher));
			_publisher = publisher;
		}
	}
}
