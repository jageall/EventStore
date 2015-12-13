using System;
using EventStore.ClientAPI;
using EventStore.ClientAPI.Exceptions;
using EventStore.Common.Utils;
using EventStore.Core.Data;
using EventStore.Core.Tests.ClientAPI.Helpers;
using EventStore.Core.Tests.Helpers;
using Xunit;
using ExpectedVersion = EventStore.ClientAPI.ExpectedVersion;
using StreamMetadata = EventStore.ClientAPI.StreamMetadata;

namespace EventStore.Core.Tests.ClientAPI
{
    public class when_working_with_stream_metadata_as_structured_info : IClassFixture<ConnectedMiniNodeFixture>
    {
        private MiniNode _node;
        private IEventStoreConnection _connection;

        public when_working_with_stream_metadata_as_structured_info(ConnectedMiniNodeFixture data)
        {
            _node = data.Node;
            _connection = data.Connection;
        }

        [Fact]
        public void setting_empty_metadata_works()
        {
            const string stream = "setting_empty_metadata_works";

            _connection.SetStreamMetadataAsync(stream, ExpectedVersion.EmptyStream, StreamMetadata.Create()).Wait();

            var meta = _connection.GetStreamMetadataAsRawBytesAsync(stream).Result;
            Assert.Equal(stream, meta.Stream);
            Assert.Equal(false, meta.IsStreamDeleted);
            Assert.Equal(0, meta.MetastreamVersion);
            Assert.Equal(Helper.UTF8NoBom.GetBytes("{}"), meta.StreamMetadata);
        }

        [Fact]
        public void setting_metadata_few_times_returns_last_metadata_info()
        {
            const string stream = "setting_metadata_few_times_returns_last_metadata_info";
            var metadata = StreamMetadata.Create(17, TimeSpan.FromSeconds(0xDEADBEEF), 10, TimeSpan.FromSeconds(0xABACABA));
            _connection.SetStreamMetadataAsync(stream, ExpectedVersion.EmptyStream, metadata).Wait();

            var meta = _connection.GetStreamMetadataAsync(stream).Result;
            Assert.Equal(stream, meta.Stream);
            Assert.Equal(false, meta.IsStreamDeleted);
            Assert.Equal(0, meta.MetastreamVersion);
            Assert.Equal(metadata.MaxCount, meta.StreamMetadata.MaxCount);
            Assert.Equal(metadata.MaxAge, meta.StreamMetadata.MaxAge);
            Assert.Equal(metadata.TruncateBefore, meta.StreamMetadata.TruncateBefore);
            Assert.Equal(metadata.CacheControl, meta.StreamMetadata.CacheControl);

            metadata = StreamMetadata.Create(37, TimeSpan.FromSeconds(0xBEEFDEAD), 24, TimeSpan.FromSeconds(0xDABACABAD));
            _connection.SetStreamMetadataAsync(stream, 0, metadata).Wait();

            meta = _connection.GetStreamMetadataAsync(stream).Result;
            Assert.Equal(stream, meta.Stream);
            Assert.Equal(false, meta.IsStreamDeleted);
            Assert.Equal(1, meta.MetastreamVersion);
            Assert.Equal(metadata.MaxCount, meta.StreamMetadata.MaxCount);
            Assert.Equal(metadata.MaxAge, meta.StreamMetadata.MaxAge);
            Assert.Equal(metadata.TruncateBefore, meta.StreamMetadata.TruncateBefore);
            Assert.Equal(metadata.CacheControl, meta.StreamMetadata.CacheControl);
        }

        [Fact]
        public void trying_to_set_metadata_with_wrong_expected_version_fails()
        {
            const string stream = "trying_to_set_metadata_with_wrong_expected_version_fails";
            var thrown = Assert.Throws<AggregateException>(
                () => _connection.SetStreamMetadataAsync(stream, 2, StreamMetadata.Create()).Result);
            Assert.IsType<WrongExpectedVersionException>(thrown.InnerException);
        }

        [Fact]
        public void setting_metadata_with_expected_version_any_works()
        {
            const string stream = "setting_metadata_with_expected_version_any_works";
            var metadata = StreamMetadata.Create(17, TimeSpan.FromSeconds(0xDEADBEEF), 10, TimeSpan.FromSeconds(0xABACABA));
            _connection.SetStreamMetadataAsync(stream, ExpectedVersion.Any, metadata).Wait();

            var meta = _connection.GetStreamMetadataAsync(stream).Result;
            Assert.Equal(stream, meta.Stream);
            Assert.Equal(false, meta.IsStreamDeleted);
            Assert.Equal(0, meta.MetastreamVersion);
            Assert.Equal(metadata.MaxCount, meta.StreamMetadata.MaxCount);
            Assert.Equal(metadata.MaxAge, meta.StreamMetadata.MaxAge);
            Assert.Equal(metadata.TruncateBefore, meta.StreamMetadata.TruncateBefore);
            Assert.Equal(metadata.CacheControl, meta.StreamMetadata.CacheControl);

            metadata = StreamMetadata.Create(37, TimeSpan.FromSeconds(0xBEEFDEAD), 24, TimeSpan.FromSeconds(0xDABACABAD));
            _connection.SetStreamMetadataAsync(stream, ExpectedVersion.Any, metadata).Wait();

            meta = _connection.GetStreamMetadataAsync(stream).Result;
            Assert.Equal(stream, meta.Stream);
            Assert.Equal(false, meta.IsStreamDeleted);
            Assert.Equal(1, meta.MetastreamVersion);
            Assert.Equal(metadata.MaxCount, meta.StreamMetadata.MaxCount);
            Assert.Equal(metadata.MaxAge, meta.StreamMetadata.MaxAge);
            Assert.Equal(metadata.TruncateBefore, meta.StreamMetadata.TruncateBefore);
            Assert.Equal(metadata.CacheControl, meta.StreamMetadata.CacheControl);
        }

        [Fact]
        public void setting_metadata_for_not_existing_stream_works()
        {
            const string stream = "setting_metadata_for_not_existing_stream_works";
            var metadata = StreamMetadata.Create(17, TimeSpan.FromSeconds(0xDEADBEEF),  10,TimeSpan.FromSeconds(0xABACABA));
            _connection.SetStreamMetadataAsync(stream, ExpectedVersion.EmptyStream, metadata).Wait();

            var meta = _connection.GetStreamMetadataAsync(stream).Result;
            Assert.Equal(stream, meta.Stream);
            Assert.Equal(false, meta.IsStreamDeleted);
            Assert.Equal(0, meta.MetastreamVersion);
            Assert.Equal(metadata.MaxCount, meta.StreamMetadata.MaxCount);
            Assert.Equal(metadata.MaxAge, meta.StreamMetadata.MaxAge);
            Assert.Equal(metadata.TruncateBefore, meta.StreamMetadata.TruncateBefore);
            Assert.Equal(metadata.CacheControl, meta.StreamMetadata.CacheControl);
        }

        [Fact]
        public void setting_metadata_for_existing_stream_works()
        {
            const string stream = "setting_metadata_for_existing_stream_works";

            _connection.AppendToStreamAsync(stream, ExpectedVersion.EmptyStream, TestEvent.NewTestEvent()).Wait();

            var metadata = StreamMetadata.Create(17, TimeSpan.FromSeconds(0xDEADBEEF), 10, TimeSpan.FromSeconds(0xABACABA));
            _connection.SetStreamMetadataAsync(stream, ExpectedVersion.EmptyStream, metadata).Wait();

            var meta = _connection.GetStreamMetadataAsync(stream).Result;
            Assert.Equal(stream, meta.Stream);
            Assert.Equal(false, meta.IsStreamDeleted);
            Assert.Equal(0, meta.MetastreamVersion);
            Assert.Equal(metadata.MaxCount, meta.StreamMetadata.MaxCount);
            Assert.Equal(metadata.MaxAge, meta.StreamMetadata.MaxAge);
            Assert.Equal(metadata.TruncateBefore, meta.StreamMetadata.TruncateBefore);
            Assert.Equal(metadata.CacheControl, meta.StreamMetadata.CacheControl);
        }

        [Fact]
        public void getting_metadata_for_nonexisting_stream_returns_empty_stream_metadata()
        {
            const string stream = "getting_metadata_for_nonexisting_stream_returns_empty_stream_metadata";

            var meta = _connection.GetStreamMetadataAsync(stream).Result;
            Assert.Equal(stream, meta.Stream);
            Assert.Equal(false, meta.IsStreamDeleted);
            Assert.Equal(-1, meta.MetastreamVersion);
            Assert.Equal(null, meta.StreamMetadata.MaxCount);
            Assert.Equal(null, meta.StreamMetadata.MaxAge);
            Assert.Equal(null, meta.StreamMetadata.TruncateBefore);
            Assert.Equal(null, meta.StreamMetadata.CacheControl);
        }

        [Fact(Skip = "You can't get stream metadata for metastream through ClientAPI")]
        public void getting_metadata_for_metastream_returns_correct_metadata()
        {
            const string stream = "$$getting_metadata_for_metastream_returns_correct_metadata";

            var meta = _connection.GetStreamMetadataAsync(stream).Result;
            Assert.Equal(stream, meta.Stream);
            Assert.Equal(false, meta.IsStreamDeleted);
            Assert.Equal(-1, meta.MetastreamVersion);
            Assert.Equal(1, meta.StreamMetadata.MaxCount);
            Assert.Equal(null, meta.StreamMetadata.MaxAge);
            Assert.Equal(null, meta.StreamMetadata.TruncateBefore);
            Assert.Equal(null, meta.StreamMetadata.CacheControl);
        }

        [Fact]
        public void getting_metadata_for_deleted_stream_returns_empty_stream_metadata_and_signals_stream_deletion()
        {
            const string stream = "getting_metadata_for_deleted_stream_returns_empty_stream_metadata_and_signals_stream_deletion";

            var metadata = StreamMetadata.Create(17, TimeSpan.FromSeconds(0xDEADBEEF), 10, TimeSpan.FromSeconds(0xABACABA));
            _connection.SetStreamMetadataAsync(stream, ExpectedVersion.EmptyStream, metadata).Wait();

            _connection.DeleteStreamAsync(stream, ExpectedVersion.EmptyStream, hardDelete: true).Wait();

            var meta = _connection.GetStreamMetadataAsync(stream).Result;
            Assert.Equal(stream, meta.Stream);
            Assert.Equal(true, meta.IsStreamDeleted);
            Assert.Equal(EventNumber.DeletedStream, meta.MetastreamVersion);
            Assert.Equal(null, meta.StreamMetadata.MaxCount);
            Assert.Equal(null, meta.StreamMetadata.MaxAge);
            Assert.Equal(null, meta.StreamMetadata.TruncateBefore);
            Assert.Equal(null, meta.StreamMetadata.CacheControl);
            Assert.Equal(null, meta.StreamMetadata.Acl);
        }

        [Fact]
        public void setting_correctly_formatted_metadata_as_raw_allows_to_read_it_as_structured_metadata()
        {
            const string stream = "setting_correctly_formatted_metadata_as_raw_allows_to_read_it_as_structured_metadata";

            var rawMeta = Helper.UTF8NoBom.GetBytes(@"{
                                                           ""$maxCount"": 17,
                                                           ""$maxAge"": 123321,
                                                           ""$tb"": 23,
                                                           ""$cacheControl"": 7654321,
                                                           ""$acl"": {
                                                               ""$r"": ""readRole"",
                                                               ""$w"": ""writeRole"",
                                                               ""$d"": ""deleteRole"",
                                                               ""$mw"": ""metaWriteRole""
                                                           },
                                                           ""customString"": ""a string"",
                                                           ""customInt"": -179,
                                                           ""customDouble"": 1.7,
                                                           ""customLong"": 123123123123123123,
                                                           ""customBool"": true,
                                                           ""customNullable"": null,
                                                           ""customRawJson"": {
                                                               ""subProperty"": 999
                                                           }
                                                      }");

            _connection.SetStreamMetadataAsync(stream, ExpectedVersion.EmptyStream, rawMeta).Wait();

            var meta = _connection.GetStreamMetadataAsync(stream).Result;
            Assert.Equal(stream, meta.Stream);
            Assert.Equal(false, meta.IsStreamDeleted);
            Assert.Equal(0, meta.MetastreamVersion);
            Assert.Equal(17, meta.StreamMetadata.MaxCount);
            Assert.Equal(TimeSpan.FromSeconds(123321), meta.StreamMetadata.MaxAge);
            Assert.Equal(23, meta.StreamMetadata.TruncateBefore);
            Assert.Equal(TimeSpan.FromSeconds(7654321), meta.StreamMetadata.CacheControl);
            
            Assert.NotNull(meta.StreamMetadata.Acl);
            Assert.Equal("readRole", meta.StreamMetadata.Acl.ReadRole);
            Assert.Equal("writeRole", meta.StreamMetadata.Acl.WriteRole);
            Assert.Equal("deleteRole", meta.StreamMetadata.Acl.DeleteRole);
            // meta role removed to allow reading
//            Assert.Equal("metaReadRole", meta.StreamMetadata.Acl.MetaReadRole);
            Assert.Equal("metaWriteRole", meta.StreamMetadata.Acl.MetaWriteRole);

            Assert.Equal("a string", meta.StreamMetadata.GetValue<string>("customString"));
            Assert.Equal(-179, meta.StreamMetadata.GetValue<int>("customInt"));
            Assert.Equal(1.7, meta.StreamMetadata.GetValue<double>("customDouble"));
            Assert.Equal(123123123123123123L, meta.StreamMetadata.GetValue<long>("customLong"));
            Assert.Equal(true, meta.StreamMetadata.GetValue<bool>("customBool"));
            Assert.Equal(null, meta.StreamMetadata.GetValue<int?>("customNullable"));
            Assert.Equal(@"{""subProperty"":999}", meta.StreamMetadata.GetValueAsRawJsonString("customRawJson"));
        }

        [Fact]
        public void setting_structured_metadata_with_custom_properties_returns_them_untouched()
        {
            const string stream = "setting_structured_metadata_with_custom_properties_returns_them_untouched";

            StreamMetadata metadata = StreamMetadata.Build()
                                                    .SetMaxCount(17)
                                                    .SetMaxAge(TimeSpan.FromSeconds(123321))
                                                    .SetTruncateBefore(23)
                                                    .SetCacheControl(TimeSpan.FromSeconds(7654321))
                                                    .SetReadRole("readRole")
                                                    .SetWriteRole("writeRole")
                                                    .SetDeleteRole("deleteRole")
                                                    //.SetMetadataReadRole("metaReadRole")
                                                    .SetMetadataWriteRole("metaWriteRole")
                                                    .SetCustomProperty("customString", "a string")
                                                    .SetCustomProperty("customInt", -179)
                                                    .SetCustomProperty("customDouble", 1.7)
                                                    .SetCustomProperty("customLong", 123123123123123123L)
                                                    .SetCustomProperty("customBool", true)
                                                    .SetCustomProperty("customNullable", new int?())
                                                    .SetCustomPropertyWithValueAsRawJsonString("customRawJson", 
                                                                                               @"{
                                                                                                       ""subProperty"": 999
                                                                                                 }");

            _connection.SetStreamMetadataAsync(stream, ExpectedVersion.EmptyStream, metadata).Wait();

            var meta = _connection.GetStreamMetadataAsync(stream).Result;
            Assert.Equal(stream, meta.Stream);
            Assert.Equal(false, meta.IsStreamDeleted);
            Assert.Equal(0, meta.MetastreamVersion);
            Assert.Equal(17, meta.StreamMetadata.MaxCount);
            Assert.Equal(TimeSpan.FromSeconds(123321), meta.StreamMetadata.MaxAge);
            Assert.Equal(23, meta.StreamMetadata.TruncateBefore);
            Assert.Equal(TimeSpan.FromSeconds(7654321), meta.StreamMetadata.CacheControl);

            Assert.NotNull(meta.StreamMetadata.Acl);
            Assert.Equal("readRole", meta.StreamMetadata.Acl.ReadRole);
            Assert.Equal("writeRole", meta.StreamMetadata.Acl.WriteRole);
            Assert.Equal("deleteRole", meta.StreamMetadata.Acl.DeleteRole);
            //Assert.Equal("metaReadRole", meta.StreamMetadata.Acl.MetaReadRole);
            Assert.Equal("metaWriteRole", meta.StreamMetadata.Acl.MetaWriteRole);
            
            Assert.Equal("a string", meta.StreamMetadata.GetValue<string>("customString"));
            Assert.Equal(-179, meta.StreamMetadata.GetValue<int>("customInt"));
            Assert.Equal(1.7, meta.StreamMetadata.GetValue<double>("customDouble"));
            Assert.Equal(123123123123123123L, meta.StreamMetadata.GetValue<long>("customLong"));
            Assert.Equal(true, meta.StreamMetadata.GetValue<bool>("customBool"));
            Assert.Equal(null, meta.StreamMetadata.GetValue<int?>("customNullable"));
            Assert.Equal(@"{""subProperty"":999}", meta.StreamMetadata.GetValueAsRawJsonString("customRawJson"));
        }

        [Fact]
        public void setting_structured_metadata_with_multiple_roles_can_be_read_back()
        {
            const string stream = "setting_structured_metadata_with_multiple_roles_can_be_read_back";

            StreamMetadata metadata = StreamMetadata.Build()
                                                    .SetReadRoles(new [] {"r1", "r2", "r3"})
                                                    .SetWriteRoles(new[] { "w1", "w2" })
                                                    .SetDeleteRoles(new[] { "d1", "d2", "d3", "d4" })
                                                    .SetMetadataWriteRoles(new[] { "mw1", "mw2" });

            _connection.SetStreamMetadataAsync(stream, ExpectedVersion.EmptyStream, metadata).Wait();

            var meta = _connection.GetStreamMetadataAsync(stream).Result;
            Assert.Equal(stream, meta.Stream);
            Assert.Equal(false, meta.IsStreamDeleted);
            Assert.Equal(0, meta.MetastreamVersion);

            Assert.NotNull(meta.StreamMetadata.Acl);
            Assert.Equal(new[] { "r1", "r2", "r3" }, meta.StreamMetadata.Acl.ReadRoles);
            Assert.Equal(new[] { "w1", "w2" }, meta.StreamMetadata.Acl.WriteRoles);
            Assert.Equal(new[] { "d1", "d2", "d3", "d4" }, meta.StreamMetadata.Acl.DeleteRoles);
            Assert.Equal(new[] { "mw1", "mw2" }, meta.StreamMetadata.Acl.MetaWriteRoles);
        }

        [Fact]
        public void setting_correct_metadata_with_multiple_roles_in_acl_allows_to_read_it_as_structured_metadata()
        {
            const string stream = "setting_correct_metadata_with_multiple_roles_in_acl_allows_to_read_it_as_structured_metadata";

            var rawMeta = Helper.UTF8NoBom.GetBytes(@"{
                                                           ""$acl"": {
                                                               ""$r"": [""r1"", ""r2"", ""r3""],
                                                               ""$w"": [""w1"", ""w2""],
                                                               ""$d"": [""d1"", ""d2"", ""d3"", ""d4""],
                                                               ""$mw"": [""mw1"", ""mw2""],
                                                           }
                                                      }");

            _connection.SetStreamMetadataAsync(stream, ExpectedVersion.EmptyStream, rawMeta).Wait();

            var meta = _connection.GetStreamMetadataAsync(stream).Result;
            Assert.Equal(stream, meta.Stream);
            Assert.Equal(false, meta.IsStreamDeleted);
            Assert.Equal(0, meta.MetastreamVersion);

            Assert.NotNull(meta.StreamMetadata.Acl);
            Assert.Equal(new[] { "r1", "r2", "r3" }, meta.StreamMetadata.Acl.ReadRoles);
            Assert.Equal(new[] { "w1", "w2" }, meta.StreamMetadata.Acl.WriteRoles);
            Assert.Equal(new[] { "d1", "d2", "d3", "d4" }, meta.StreamMetadata.Acl.DeleteRoles);
            Assert.Equal(new[] { "mw1", "mw2" }, meta.StreamMetadata.Acl.MetaWriteRoles);
        }
    }
}
