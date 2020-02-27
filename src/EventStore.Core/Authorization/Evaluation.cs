using System.Collections.Generic;

namespace EventStore.Core.Authorization
{
	public class Evaluation {
		private readonly Operation _operation;
		readonly List<AssertionMatch> _matches;
		public Grant Grant { get; private set; }

		public Evaluation(Operation operation) {
			_operation = operation;
			_matches = new List<AssertionMatch>();
			Grant = Grant.Unknown;
		}

		public void Add(AssertionMatch match) {
			if (match.Assertion.Grant > Grant)
				Grant = match.Assertion.Grant;
			_matches.Add(match);
		}

		public EvaluationResult ToResult() {
			return new EvaluationResult(_operation, Grant, _matches);
		}
	}
}