using System;
using System.Security.Claims;
using System.Threading;
using System.Threading.Tasks;

namespace EventStore.Core.Authorization
{
	public class PolicyEvaluator : IPolicyEvaluator {
		
		private readonly ReadOnlyPolicy _policy;
		private readonly PolicyInformation _policyInfo;
		private static readonly AssertionInformation DeniedByDefault = new AssertionInformation("default", "denied by default", Grant.Deny);

		public PolicyEvaluator(ReadOnlyPolicy policy) {
			_policy = policy;
			_policyInfo = policy.Information;
		}
		public ValueTask<EvaluationResult> EvaluateAsync(ClaimsPrincipal cp, Operation operation, CancellationToken ct) {

			var evaluation = new Evaluation(operation);

			if (_policy.TryGetAssertions(operation, out var assertions)) {
				while (!assertions.IsEmpty && evaluation.Grant != Grant.Deny) {
					if (ct.IsCancellationRequested) break;
					var assertion = assertions.Span[0];
					assertions = assertions.Slice(1);
					var evaluate = assertion.Evaluate(cp, operation, _policyInfo, evaluation);
					if (!evaluate.IsCompleted)
						return EvaluateAsync(evaluate, cp, operation, assertions, evaluation, ct);
				}
			}

			if (evaluation.Grant == Grant.Unknown) {
				evaluation.Add(new AssertionMatch(_policyInfo, DeniedByDefault));
			}

			return new ValueTask<EvaluationResult>(evaluation.ToResult());
		}

		async ValueTask<EvaluationResult> EvaluateAsync(
			ValueTask<bool> pending, 
			ClaimsPrincipal cp, 
			Operation operation, 
			ReadOnlyMemory<IAssertion> assertions, 
			Evaluation evaluation, 
			CancellationToken ct) {
			do {
				if (ct.IsCancellationRequested) break;
				await pending.ConfigureAwait(false);
				if (ct.IsCancellationRequested) break;
				if (assertions.IsEmpty) break;
				pending = assertions.Span[0].Evaluate(cp, operation, _policyInfo, evaluation);
				assertions = assertions.Slice(1);
			} while (evaluation.Grant != Grant.Deny);

			if (evaluation.Grant == Grant.Unknown)
				evaluation.Add(new AssertionMatch(_policyInfo, DeniedByDefault));

			return evaluation.ToResult();
		}
	}
}
