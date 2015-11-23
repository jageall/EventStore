using System.IO;
using EventStore.Core.Exceptions;
using EventStore.Core.Index;
using Xunit;

namespace EventStore.Core.Tests.Index
{
    public class when_opening_ptable_without_right_flag_in_header : SpecificationWithFile
    {
        public when_opening_ptable_without_right_flag_in_header()
        {
            using (var stream = File.OpenWrite(Filename))
            {
                var bytes = new byte[128];
                bytes[0] = 0x27;
                stream.Write(bytes,0, bytes.Length);
            }
        }

        [Fact]
        public void the_invalid_file_exception_is_thrown()
        {
            var exc = Assert.Throws<CorruptIndexException>(() => PTable.FromFile(Filename, 16));
            Assert.IsType<InvalidFileException>(exc.InnerException);
        }
    }
}
