using System.Security.Claims;
using System.Threading.Tasks;

namespace EventStore.Core.Authorization
{
	class AllowAnonymousAssertion : IAssertion {
		public Grant Grant { get; } = Grant.Allow;
		public AssertionInformation Information { get; } = new AssertionInformation("authenticated", "not anonymous", Grant.Unknown);
		public ValueTask<bool> Evaluate(ClaimsPrincipal cp, Operation operation, PolicyInformation policy, Evaluation result) {
			result.Add(new AssertionMatch(policy, new AssertionInformation("match", "allow anonymous", Grant.Allow)));
			return new ValueTask<bool>(true);
		}
	}
}