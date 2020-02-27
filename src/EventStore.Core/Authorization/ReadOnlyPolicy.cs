using System;
using System.Collections.Generic;

namespace EventStore.Core.Authorization
{
	public class ReadOnlyPolicy {
		private readonly string _name;
		private readonly long _version;
		private readonly DateTimeOffset _validFrom;
		private readonly IReadOnlyDictionary<OperationDefinition, ReadOnlyMemory<IAssertion>> _assertions;

		public ReadOnlyPolicy(string name, long version, DateTimeOffset validFrom, IReadOnlyDictionary<OperationDefinition, ReadOnlyMemory<IAssertion>> assertions) {
			_name = name;
			_version = version;
			_validFrom = validFrom;
			_assertions = assertions;
		}

		public bool TryGetAssertions(OperationDefinition operation, out ReadOnlyMemory<IAssertion> assertions) {
			return _assertions.TryGetValue(operation, out assertions);
		}

		public PolicyInformation Information => new PolicyInformation(_name, _version, DateTimeOffset.MaxValue);
	}
}