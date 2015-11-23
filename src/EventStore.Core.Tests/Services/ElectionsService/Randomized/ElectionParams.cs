using System.Collections.Generic;
using System.Linq;

namespace EventStore.Core.Tests.Services.ElectionsService.Randomized
{
    public static class ElectionParams
    {
		#if CI_TESTS 
			public const int TestRunCount = 10;
		#else
			public const int TestRunCount = 1;
		#endif
        
        public const int MaxIterationCount = 25000;


        public static IEnumerable<object[]> TestRuns
        {
            get
            {
                return Enumerable.Range(0, TestRunCount).Select(x=>new object[]{x});
            }
        }

        public static IEnumerable<object[]> TenRuns
        {
            get
            {
                return Enumerable.Range(0, 10).Select(x => new object[] { x });
            }
        }
    }
}