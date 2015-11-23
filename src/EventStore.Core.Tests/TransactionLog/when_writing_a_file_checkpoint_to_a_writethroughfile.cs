using System;
using System.Threading;
using EventStore.Core.TransactionLog.Checkpoint;
using Xunit;

namespace EventStore.Core.Tests.TransactionLog
{
    public class when_writing_a_file_checkpoint_to_a_writethroughfile : SpecificationWithFile
    {
        [Fact]
        [Trait("Platform", "WIN")]
        public void a_null_file_throws_argumentnullexception()
        {
            Assert.Throws<ArgumentNullException>(() => new FileCheckpoint(null));
        }

        [Fact]
        [Trait("Platform", "WIN")]
        public void name_is_set()
        {
            var checksum = new WriteThroughFileCheckpoint("filename", "test");
            Assert.Equal("test", checksum.Name);
            checksum.Close();
        }

        [Fact]
        [Trait("Platform", "WIN")]
        public void reading_off_same_instance_gives_most_up_to_date_info()
        {
            var checkSum = new WriteThroughFileCheckpoint(Filename);
            checkSum.Write(0xDEAD);
            checkSum.Flush();
            var read = checkSum.Read();
            checkSum.Close();
            Assert.Equal(0xDEAD, read);
        }

        [Fact]
        [Trait("Platform", "WIN")]
        public void can_read_existing_checksum()
        {
            var checksum = new WriteThroughFileCheckpoint(Filename);
            checksum.Write(0xDEAD);
            checksum.Close();
            checksum = new WriteThroughFileCheckpoint(Filename);
            var val = checksum.Read();
            checksum.Close();
            Assert.Equal(0xDEAD, val);
        }
        [Fact]
        [Trait("Platform", "WIN")]
        public void the_new_value_is_not_accessible_if_not_flushed_even_with_delay()
        {
            var checkSum = new WriteThroughFileCheckpoint(Filename);
            var readChecksum = new WriteThroughFileCheckpoint(Filename);
            checkSum.Write(1011);
            Thread.Sleep(200);
            Assert.Equal(0, readChecksum.Read());
            checkSum.Close();
            readChecksum.Close();
        }

        [Fact]
        [Trait("Platform", "WIN")]
        public void the_new_value_is_accessible_after_flush()
        {
            var checkSum = new WriteThroughFileCheckpoint(Filename);
            var readChecksum = new WriteThroughFileCheckpoint(Filename);
            checkSum.Write(1011);
            checkSum.Flush();
            Assert.Equal(1011, readChecksum.Read());
            checkSum.Close();
            readChecksum.Close();
        }
    }
}
