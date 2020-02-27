using System;
using System.Linq;
using System.Security.Claims;
using System.Threading;
using System.Threading.Tasks;
using EventStore.Core.Bus;
using EventStore.Core.Messages;
using EventStore.Core.Messaging;
using EventStore.Core.Services;
using Microsoft.AspNetCore.SignalR.Protocol;

namespace EventStore.Core.Authorization
{
	public class 
		LegacyStreamPermissionAssertion : IAssertion {
		private readonly IPublisher _publisher;
		public AssertionInformation Information { get; } = new AssertionInformation("stream", "legacy acl", Grant.Unknown);
		public LegacyStreamPermissionAssertion(IPublisher publisher) {
			_publisher = publisher;
		}
		public Grant Grant { get; } = Grant.Unknown;
		public ValueTask<bool> Evaluate(ClaimsPrincipal cp, Operation operation, PolicyInformation policy, Evaluation result) {
			var streamId = FindStreamId(operation.Parameters.Span);
				
			if(streamId.IsCompleted)
				return CheckStreamAccess(cp, operation, policy, result, streamId.Result);
			return CheckStreamAccessAsync(streamId, cp, operation, policy, result);
		}

		private async ValueTask<bool> CheckStreamAccessAsync(ValueTask<string> pending, ClaimsPrincipal cp,
			Operation operation, PolicyInformation policy, Evaluation result) {
			var streamId = await pending.ConfigureAwait(false);
			return await CheckStreamAccess(cp, operation, policy, result, streamId).ConfigureAwait(false);
		}

		private ValueTask<bool> CheckStreamAccess(ClaimsPrincipal cp, Operation operation, PolicyInformation policy,
			Evaluation result, string streamId)
		{
			if (streamId == null)
			{
				result.Add(new AssertionMatch(policy, new AssertionInformation("streamId", "streamId is null", Grant.Deny)));
				return new ValueTask<bool>(true);
			}

			if (streamId == SystemStreams.AllStream &&
			    (operation == Operations.Streams.Delete || operation == Operations.Streams.Delete))
			{
				result.Add(new AssertionMatch(policy,
					new AssertionInformation("streamId", $"{operation.Action} denied on $all", Grant.Deny)));
				return new ValueTask<bool>(true);
			}

			var action = operation.Action;
			if (SystemStreams.IsMetastream(streamId))
			{
				action = operation.Action switch
				{
					"read" => "metadataRead",
					"write" => "metadataWrite",
					_ => null
				};
				streamId = SystemStreams.OriginalStreamOf(streamId);
			}

			return action switch
			{
				"read" => Check(cp, operation, action, streamId, policy, result),
				"write" => Check(cp, operation, action, streamId, policy, result),
				"delete" => Check(cp, operation, action, streamId, policy, result),
				"metadataWrite" => Check(cp, operation, action, streamId, policy, result),
				"metadataRead" => Check(cp, operation, action, streamId, policy, result),
				null => InvalidMetadataOperation(operation, policy, result),
				_ => throw new ArgumentOutOfRangeException(nameof(operation.Action), action)
			};
		}

		private ValueTask<bool> Check(ClaimsPrincipal cp, Operation operation, string action, string streamId,
			PolicyInformation policy, Evaluation result) {
			var preChecks = IsSystemOrAdmin(cp, operation,  policy, result);
			if (preChecks.IsCompleted && preChecks.Result)
				return preChecks;

			return CheckAsync(preChecks, cp, action, streamId, policy, result);
		}

		private ValueTask<bool> IsSystemOrAdmin(ClaimsPrincipal cp, Operation operation,
			PolicyInformation policy, Evaluation result) {
			var isSystem = WellKnownAssertions.System.Evaluate(cp, operation, policy, result);
			if (isSystem.IsCompleted) {
				if (isSystem.Result)
					return isSystem;
				return WellKnownAssertions.Admin.Evaluate(cp, operation, policy, result);
			}
			
			// This should never be run, but is required for the compilation to be reasonable
			return IsSystemOrAdminAsync(isSystem, cp, operation, policy, result);
		}

		private async ValueTask<bool> IsSystemOrAdminAsync(ValueTask<bool> isSystem, ClaimsPrincipal cp, Operation operation,
			PolicyInformation policy, Evaluation result) {
			if (await isSystem.ConfigureAwait(false)) { return true; }

			return await WellKnownAssertions.Admin.Evaluate(cp, operation, policy, result).ConfigureAwait(false);

		}

		private async ValueTask<bool> CheckAsync(ValueTask<bool> preChecks, ClaimsPrincipal cp, string action, string streamId, PolicyInformation policy, Evaluation result) {
			var isSystemOrAdmin = await preChecks.ConfigureAwait(false);
			if (isSystemOrAdmin) return true;
			var acl = await StorageMessage.EffectiveAcl.LoadAsync(_publisher, streamId).ConfigureAwait(false);
			var roles = RolesFor(action, acl);
			if (roles.Any(x => x == SystemRoles.All)) {
				result.Add(new AssertionMatch(policy,
					new AssertionInformation("stream", "public stream", Grant.Allow)));
				return true;
			}

			for (int i = 0; i < roles.Length; i++) {
				var role = roles[i];
				if (cp.FindFirst(x => (x.Type == ClaimTypes.Name || x.Type == ClaimTypes.Role) && x.Value == role)
					is Claim matched) {
					result.Add(new AssertionMatch(policy, new AssertionInformation("role match", role, Grant.Allow), matched));
					return true;
				}
			}

			return false;
		}

		ValueTask<bool> InvalidMetadataOperation(Operation operation, PolicyInformation policy, Evaluation result) {
			result.Add(new AssertionMatch(policy,
				new AssertionInformation("metadata", $"invalid metadata operation {operation.Action}",
					Grant.Deny)));
			return new ValueTask<bool>(true);
		}
		ValueTask<string>  FindStreamId(ReadOnlySpan<Parameter> parameters) {
			string transactionId = null;
			for (int i = 0; i < parameters.Length; i++) {
				if (parameters[i].Name == "streamId") return new ValueTask<string>(parameters[i].Value);
				if (parameters[i].Name == "transactionId") transactionId = parameters[i].Value;
			}

			if (transactionId != null) {
				return FindStreamFromTransactionId(long.Parse(transactionId));
			}
			return new ValueTask<string>((string)null);
		}

		ValueTask<string> FindStreamFromTransactionId(long transactionId) {
			var envelope = new StreamIdFromTransactionIdEnvelope(CancellationToken.None);
			_publisher.Publish(new StorageMessage.StreamIdFromTransactionIdRequest(transactionId, envelope));
			return new ValueTask<string>(envelope.Task);
		}

		string[] RolesFor(string action, StorageMessage.EffectiveAcl acl) {
			return action switch {
				"read" => acl.Stream?.ReadRoles ?? acl.System?.ReadRoles ?? acl.Default?.ReadRoles,
				"write" => acl.Stream?.WriteRoles ?? acl.System?.WriteRoles ?? acl.Default?.WriteRoles,
				"delete" => acl.Stream?.DeleteRoles ?? acl.System?.DeleteRoles ?? acl.Default?.DeleteRoles,
				"metadataRead" => acl.Stream?.MetaReadRoles ?? acl.System?.MetaReadRoles ?? acl.Default?.MetaReadRoles,
				"metadataWrite" => acl.Stream?.MetaWriteRoles ?? acl.System?.MetaWriteRoles ?? acl.Default?.MetaWriteRoles,
				_ => Array.Empty<string>()
			};
		}

		class StreamIdFromTransactionIdEnvelope : IEnvelope{
			private readonly TaskCompletionSource<string> _tcs;
			private readonly CancellationTokenRegistration _tokenRegistration;
			private static readonly Action<object> CancelAction = Cancel;

			private static void Cancel(object obj) {
				var instance = (StreamIdFromTransactionIdEnvelope)obj;
				instance._tcs.TrySetCanceled();
				instance._tokenRegistration.Dispose();
			}

			public StreamIdFromTransactionIdEnvelope(CancellationToken token) {
				_tcs = new TaskCompletionSource<string>();
				_tokenRegistration = token.Register(CancelAction, this);
			}
			public void ReplyWith<T>(T message) where T : Message {
				_tokenRegistration.Dispose();
				if (message is StorageMessage.StreamIdFromTransactionIdResponse response) {
					_tcs.TrySetResult(response.StreamId);
				} else {
					_tcs.TrySetException(new InvalidOperationException($"Wrong message type {message.GetType()}"));
				}
			}

			public Task<string> Task => _tcs.Task;
		}
	}
}
