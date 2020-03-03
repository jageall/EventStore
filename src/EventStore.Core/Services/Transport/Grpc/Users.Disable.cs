using System.Threading.Tasks;
using EventStore.Core.Messages;
using EventStore.Core.Messaging;
using EventStore.Client.Users;
using Grpc.Core;

namespace EventStore.Core.Services.Transport.Grpc {
	public partial class Users {
		public override async Task<DisableResp> Disable(DisableReq request, ServerCallContext context) {
			var options = request.Options;

			var user = context.GetHttpContext().User;

			var disableSource = new TaskCompletionSource<bool>();

			var envelope = new CallbackEnvelope(OnMessage);

			_publisher.Publish(new UserManagementMessage.Disable(envelope, user, options.LoginName));

			await disableSource.Task.ConfigureAwait(false);

			return new DisableResp();

			void OnMessage(Message message) {
				if (HandleErrors(options.LoginName, message, disableSource)) return;

				disableSource.TrySetResult(true);
			}
		}
	}
}
