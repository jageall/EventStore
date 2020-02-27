using System;
using System.Security.Claims;
using System.Threading.Tasks;

namespace EventStore.Core.Authorization
{
	public class ClaimValueMatchesParameterValueAssertion : IAssertion {
		private readonly string _claimType;
		private readonly string _parameterName;
		public AssertionInformation Information { get; }
		public ClaimValueMatchesParameterValueAssertion(string claimType, string parameterName, Grant grant) {
			_claimType = claimType;
			_parameterName = parameterName;
			Grant = grant;
			Information = new AssertionInformation("match", $"{_claimType} : {_parameterName}", Grant);
		}
		public Grant Grant { get; }
		public ValueTask<bool> Evaluate(ClaimsPrincipal cp, Operation operation, PolicyInformation policy, Evaluation result) {
			if (cp.FindFirst(_claimType) is Claim matchedClaim &&
			    operation.Parameters.Span.Contains(new Parameter(_parameterName, matchedClaim.Value))) {
				result.Add(new AssertionMatch(policy, Information, matchedClaim));
				return new ValueTask<bool>(true);
			}
			return new ValueTask<bool>(false);
		}
	}
}