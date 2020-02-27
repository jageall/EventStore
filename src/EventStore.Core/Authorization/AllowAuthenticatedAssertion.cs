using System;
using System.Linq;
using System.Security.Claims;
using System.Threading.Tasks;

namespace EventStore.Core.Authorization
{
	class AllowAuthenticatedAssertion : IAssertion {
		public Grant Grant { get; } = Grant.Unknown;
		public AssertionInformation Information { get; } = new AssertionInformation("match", "authenticated", Grant.Unknown);
		public ValueTask<bool> Evaluate(ClaimsPrincipal cp, Operation operation, PolicyInformation policy, Evaluation result) {
			if (!cp.Claims.Any() || cp.Claims.Any(x => string.Equals(x.Type, ClaimTypes.Anonymous, StringComparison.Ordinal))) {
				result.Add(new AssertionMatch(policy, new AssertionInformation("match", "authenticated", Grant.Deny)));
			} else {
				result.Add(new AssertionMatch(policy, new AssertionInformation("match", "authenticated", Grant.Allow)));
			}
			return new ValueTask<bool>(true);
		}
	}
}