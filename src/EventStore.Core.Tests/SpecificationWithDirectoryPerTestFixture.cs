using System;
using System.IO;

namespace EventStore.Core.Tests
{
    public class SpecificationWithDirectoryPerTestFixture : IDisposable
    {
        protected internal string PathName;
 
        public string GetTempFilePath()
        {
            var typeName = GetType().Name.Length > 30 ? GetType().Name.Substring(0, 30) : GetType().Name;
            return Path.Combine(PathName, string.Format("{0}-{1}", Guid.NewGuid(), typeName));
        }

        public string GetFilePathFor(string fileName)
        {
            return Path.Combine(PathName, fileName);
        }

        public SpecificationWithDirectoryPerTestFixture()
        {
            var typeName = GetType().Name.Length > 30 ? GetType().Name.Substring(0, 30) : GetType().Name;
            PathName = Path.Combine(Path.GetTempPath(), string.Format("{0}-{1}", Guid.NewGuid(), typeName));
            Directory.CreateDirectory(PathName);
        }

        public virtual void Dispose()
        {
            //kill whole tree
            Directory.Delete(PathName, true);
        }

    }
}