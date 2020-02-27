using System;

namespace EventStore.Core.Authorization
{
	public class PolicyInformation {
		public long Version { get; }
		public readonly string Name;
		public readonly DateTimeOffset Expires;

		public PolicyInformation(string name, long version, DateTimeOffset expires) {
			Version = version;
			Name = name;
			Expires = expires;
		}

		public override string ToString() {
			return $"Policy : {Name} {Version} {Expires}";
		}
	}
}