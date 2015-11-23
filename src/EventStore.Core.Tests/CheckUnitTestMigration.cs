using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using Xunit;

namespace EventStore.Core.Tests
{
    public class CheckUnitTestMigration
    {
        [Fact]
        public void AllFactAttributesAreOnPublicClasses()
        {
            var assembly = GetType().Assembly;
            Type[] types;
            try
            {
                types = assembly.GetTypes();
            }
            catch (ReflectionTypeLoadException ex)
            {
                types = ex.Types;
            }

            var nonPublicMethodsWithFactAttributes = types.SelectMany(x => x.GetMethods(BindingFlags.Instance | BindingFlags.Public))
                .Where(
                    x =>
                        (x.GetCustomAttributes(typeof (FactAttribute), true) ?? new object[] {}).Length > 0 &&
                        !x.IsPublic).Select(x=> string.Format("{0}{1}()", x.DeclaringType.FullName, x.Name)).ToArray();

            
            Assert.False(nonPublicMethodsWithFactAttributes.Any(), string.Join(Environment.NewLine, nonPublicMethodsWithFactAttributes));
        }
    }
}
