using System;
using System.IO;
using System.Linq;
using EventStore.Core.Index;
using EventStore.Core.Util;
using Xunit;

namespace EventStore.Core.Tests.Index
{
    public class saving_empty_index_to_a_file: IClassFixture<SpecificationWithDirectoryPerTestFixture>
    {
        private string _filename;
        private IndexMap _map;

        public void SetFixture(SpecificationWithDirectoryPerTestFixture data)
        {
            _filename = data.GetFilePathFor("indexfile");
            _map = IndexMap.FromFile(_filename);
            _map.SaveToFile(_filename);
        }

        [Fact]
        public void the_file_exists()
        {
            Assert.True(File.Exists(_filename));
        }

        [Fact]
        public void the_file_contains_correct_data()
        {
            using (var fs = File.OpenRead(_filename))
            using (var reader = new StreamReader(fs))
            {
                var text = reader.ReadToEnd();
                var lines = text.Replace("\r", "").Split('\n');

                fs.Position = 32;
                var md5 = MD5Hash.GetHashFor(fs);
                var md5String = BitConverter.ToString(md5).Replace("-", "");

                Assert.Equal(4, lines.Count());
                Assert.Equal(md5String, lines[0]);
                Assert.Equal(PTable.Version.ToString(), lines[1]);
                Assert.Equal("-1/-1", lines[2]);
                Assert.Equal("", lines[3]);
            }
        }

        [Fact]
        public void saved_file_could_be_read_correctly_and_without_errors()
        {
            var map = IndexMap.FromFile(_filename);

            Assert.Equal(-1, map.PrepareCheckpoint);
            Assert.Equal(-1, map.CommitCheckpoint);
        }
    }
}