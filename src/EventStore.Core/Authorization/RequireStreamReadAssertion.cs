using System;
using System.Security.Claims;
using System.Threading.Tasks;

namespace EventStore.Core.Authorization
{
	public class RequireStreamReadAssertion : IAssertion {
		private readonly LegacyStreamPermissionAssertion _streamAssertion;
		public AssertionInformation Information { get; }
		public RequireStreamReadAssertion(LegacyStreamPermissionAssertion streamAssertion) {
			_streamAssertion = streamAssertion;
			Information = streamAssertion.Information;
		}
		static readonly Operation StreamRead = new Operation(Operations.Streams.Read);
		public Grant Grant { get; } = Grant.Unknown;
		public ValueTask<bool> Evaluate(ClaimsPrincipal cp, Operation operation, PolicyInformation policy, Evaluation result) {
			if (operation == Operations.Subscriptions.Connect ||
				operation == Operations.Subscriptions.Ack ||
			    operation == Operations.Subscriptions.ReadMessages || 
			    operation == Operations.Subscriptions.Nack || 
			    operation == Operations.Subscriptions.ReplayParked) {
				var stream = FindStreamId(operation.Parameters.Span);
				return _streamAssertion.Evaluate(cp,
					StreamRead.WithParameter(Operations.Streams.Parameters.StreamId(stream)), policy, result);
			}

			return new ValueTask<bool>(false);
		}

		string FindStreamId(ReadOnlySpan<Parameter> parameters) {
			for (int i = 0; i < parameters.Length; i++) {
				if (parameters[i].Name == "streamId") return parameters[i].Value;
			}

			return null;
		}
	}
}
