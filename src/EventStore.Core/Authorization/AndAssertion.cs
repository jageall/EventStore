using System;
using System.Linq;
using System.Security.Claims;
using System.Threading.Tasks;

namespace EventStore.Core.Authorization
{
	class AndAssertion : IAssertion {
		private readonly ReadOnlyMemory<IAssertion> _assertions;
		private readonly AssertionInformation _failedToMatchAllSubAssertions;
		public AssertionInformation Information { get; } 
		public AndAssertion(params IAssertion[] assertions) {
			_assertions = assertions.OrderBy(x=>x.Grant).ToArray();
			_failedToMatchAllSubAssertions = new AssertionInformation("and", $"({string.Join(",", _assertions.Span.ToArray().Select(x=>x.ToString()))})",Grant.Deny);
			Information = new AssertionInformation("and", $"({string.Join(",", _assertions.ToArray().Select(x=>x.ToString()))})",Grant.Unknown);
		}

		public Grant Grant { get; } = Grant.Unknown;

		public ValueTask<bool> Evaluate(ClaimsPrincipal cp, Operation operation, PolicyInformation policy, Evaluation result) {

			var remaining = _assertions;
			while (!remaining.IsEmpty) {
				var pending = remaining.Span[0].Evaluate(cp, operation, policy, result);
				remaining = remaining.Slice(1);
				if (!pending.IsCompleted)
					return EvaluateAsync(pending, remaining, cp, operation, policy, result);
				if (!pending.Result) {
					result.Add(new AssertionMatch(policy, _failedToMatchAllSubAssertions));
					return new ValueTask<bool>(false);
				}
			}

			return new ValueTask<bool>(true);
		}

		private async ValueTask<bool> EvaluateAsync(ValueTask<bool> pending, ReadOnlyMemory<IAssertion> remaining,
			ClaimsPrincipal cp, Operation operation, PolicyInformation policy, Evaluation result) {
			bool evaluated;
			while ((evaluated = await pending.ConfigureAwait(false)) && !remaining.IsEmpty) {
				
				pending = remaining.Span[0].Evaluate(cp, operation, policy, result);
				remaining = remaining.Slice(1);
			}
			if(!evaluated)
				result.Add(new AssertionMatch(policy, _failedToMatchAllSubAssertions));
			return evaluated;
		}
	}
}
