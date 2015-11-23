﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using EventStore.Common.Utils;
using EventStore.Core.Data;
using EventStore.Core.Services.Transport.Http;
using EventStore.Core.TransactionLog.LogRecords;
using EventStore.Transport.Http.Codecs;
using Xunit;
using Newtonsoft.Json;

namespace EventStore.Core.Tests.Services.Transport.Http
{
    internal static class ByteArrayExtensions {
        public static string AsString(this byte[] data) {
            return Helper.UTF8NoBom.GetString(data ?? new byte[0]);
        }
    }
    internal static class FakeRequest
    {
        internal const string JsonData = "{\"field1\":\"value1\",\"field2\":\"value2\"}";
        internal const string JsonMetadata = "{\"meta-field1\":\"meta-value1\"}";
        internal const string JsonData2 = "{\"field1\":\"value1\",\"field2\":\"value2\"}";
        internal const string JsonMetadata2 = "{\"meta-field1\":\"meta-value1\"}";

        internal const string XmlData = "<field1>value1</field1><field2>value2</field2>";
        internal const string XmlMetadata = "<meta-field1>meta-value1</meta-field1>";
        internal const string XmlData2 = "<field1>value1</field1><field2>value2</field2>";
        internal const string XmlMetadata2 = "<meta-field1>meta-value1</meta-field1>";

        private static string JsonEventWriteFormat
        {
            get
            {
                return "{{\"eventId\":{0},\"eventType\":{1},\"data\":{2},\"metadata\":{3}}}";
            }
        }

        private static string JsonEventReadFormat
        {
            get
            {
                return "{{\"eventStreamId\":{0},\"eventNumber\":{1},\"eventType\":{2},\"eventId\":{3},\"data\":{4},\"metadata\":{5}}}";
            }
        }

        private static string XmlEventWriteFormat
        {
            get
            {
                return "<event><eventId>{0}</eventId><eventType>{1}</eventType><data>{2}</data><metadata>{3}</metadata></event>";
            }
        }

        private static string XmlEventReadFormat
        {
            get
            {
                return "<event><eventStreamId>{0}</eventStreamId><eventNumber>{1}</eventNumber><eventType>{2}</eventType><eventId>{3}</eventId><data>{4}</data><metadata>{5}</metadata></event>";
            }
        }

        public static string GetJsonWrite(string data, string metadata)
        {
            return GetJsonWrite(new[] {Tuple.Create(data, metadata)});
        }

        public static string GetJsonWrite(params Tuple<string, string>[] events)
        {
            return string.Format("[{0}]",
                string.Join(",", events.Select(x => 
                    string.Format(JsonEventWriteFormat, string.Format("\"{0}\"", Guid.NewGuid()), "\"type\"", x.Item1, x.Item2))));
        }

        public static string GetJsonEventReadResult(ResolvedEvent evnt, bool dataJson = true, bool metadataJson = true)
        {
            return string.Format(JsonEventReadFormat,
                                 WrapIntoQuotes(evnt.Event.EventStreamId),
                                 evnt.Event.EventNumber,
                                 WrapIntoQuotes(evnt.Event.EventType),
                                 WrapIntoQuotes(evnt.Event.EventId.ToString()),
                                 dataJson ? JsonData : WrapIntoQuotes(AsString(evnt.Event.Data)),
                                 metadataJson ? JsonMetadata : WrapIntoQuotes(AsString(evnt.Event.Metadata)));
        }

        public static string GetJsonEventsReadResult(IEnumerable<ResolvedEvent> events, bool dataJson = true, bool metadataJson = true)
        {
            return string.Format("[{0}]", string.Join(",", events.Select(x => GetJsonEventReadResult(x, dataJson, metadataJson))));
        }

        public static string GetXmlWrite(string data, string metadata)
        {
            return GetXmlWrite(new[] {Tuple.Create(data, metadata)});
        }

        public static string GetXmlWrite(params Tuple<string, string>[] events)
        {
            return string.Format("<events>{0}</events>",
                string.Join("\n", events.Select(x =>
                    string.Format(XmlEventWriteFormat, Guid.NewGuid(), "type", x.Item1, x.Item2))));
        }

        public static string GetXmlEventReadResult(ResolvedEvent evnt, bool dataJson = true, bool metadataJson = true)
        {
            return string.Format(XmlEventReadFormat,                                  
                                 evnt.Event.EventStreamId,
                                 evnt.Event.EventNumber,
                                 evnt.Event.EventType,
                                 evnt.Event.EventId,
                                 dataJson ? XmlData : AsString(evnt.Event.Data),
                                 metadataJson ? XmlMetadata : AsString(evnt.Event.Metadata));
        }

        public static string GetXmlEventsReadResult(IEnumerable<ResolvedEvent> events, bool dataJson = true, bool metadataJson = true)
        {
            return string.Format("<events>{0}</events>", 
                                 string.Join("\n", events.Select(x => GetXmlEventReadResult(x, dataJson, metadataJson))));
        }

        public static string AsString(byte[] bytes)
        {
            return Helper.UTF8NoBom.GetString(bytes ?? new byte[0]);
        }

        private static string WrapIntoQuotes(string s)
        {
            return string.Format("\"{0}\"", s);
        }
    }

    public abstract class do_not_use_indentation_for_json : IDisposable
    {
        protected do_not_use_indentation_for_json()
        {
            JsonCodec.Formatting = Formatting.None;
        }

        public void Dispose()
        {
            JsonCodec.Formatting = Formatting.Indented;
        }
    }

    public class when_writing_events_and_content_type_is_json : do_not_use_indentation_for_json
    {
        [Fact]
        public void should_just_count_as_body_if_just_json()
        {
            var codec = Codec.Json;
            var request = FakeRequest.JsonData;
            var id = Guid.NewGuid();
            var type = "EventType";
            var events = AutoEventConverter.SmartParse(request, codec, id, type);
            Assert.NotNull(events);
            Assert.Equal(1, events.Length);

            Assert.True(events[0].IsJson);
            Assert.Equal(events[0].EventId, id);
            Assert.Equal(events[0].EventType, type);
            Assert.Equal(FakeRequest.JsonData, events[0].Data.AsString());
            Assert.Equal(string.Empty, events[0].Metadata.AsString());
        }
    }

    public  class when_writing_events_and_content_type_is_xml 
    {
        [Fact]
        public void should_just_count_as_body_if_just_xml()
        {
            var codec = Codec.Xml;
            var request = FakeRequest.XmlData;
            var id = Guid.NewGuid();
            var type = "EventType";
            var events = AutoEventConverter.SmartParse(request, codec, id, type);
            Assert.NotNull(events);
            Assert.Equal(1, events.Length);

            Assert.False(events[0].IsJson);
            Assert.Equal(events[0].EventId, id);
            Assert.Equal(events[0].EventType, type);
            Assert.Equal(FakeRequest.XmlData, events[0].Data.AsString());
            Assert.Equal(string.Empty, events[0].Metadata.AsString());
        }
    }

    public class when_writing_events_and_content_type_is_events_json : do_not_use_indentation_for_json
    {
        [Fact]
        public void should_store_data_as_json_if_valid_and_metadata_as_string_if_not()
        {
            var codec = Codec.EventsJson;
            var request = FakeRequest.GetJsonWrite(new[]
                                                   {
                                                           Tuple.Create(FakeRequest.JsonData, "\"metadata\""),
                                                           Tuple.Create(FakeRequest.JsonData2, "\"metadata2\"")
                                                   });

            var events = AutoEventConverter.SmartParse(request, codec, Guid.Empty);
            Assert.Equal(2, events.Length);

            Assert.True(events[0].IsJson);
            Assert.Equal(FakeRequest.JsonData, events[0].Data.AsString());
            Assert.Equal("metadata", events[0].Metadata.AsString());

            Assert.True(events[1].IsJson);
            Assert.Equal(FakeRequest.JsonData2, events[1].Data.AsString());
            Assert.Equal("metadata2", events[1].Metadata.AsString());
        }

        [Fact]
        public void should_store_metadata_as_json_if_its_valid_and_data_as_string_if_its_not()
        {
            var codec = Codec.EventsJson;
            var request = FakeRequest.GetJsonWrite(new[]
                                                   {
                                                        Tuple.Create("\"data\"", FakeRequest.JsonMetadata),
                                                        Tuple.Create("\"data2\"", FakeRequest.JsonMetadata2)
                                                   });

            var events = AutoEventConverter.SmartParse(request, codec, Guid.Empty);
            Assert.Equal(2, events.Length);

            Assert.True(events[0].IsJson);
            Assert.Equal("data", events[0].Data.AsString());
            Assert.Equal(FakeRequest.JsonMetadata, events[0].Metadata.AsString());

            Assert.True(events[1].IsJson);
            Assert.Equal("data2", events[1].Data.AsString());
            Assert.Equal(FakeRequest.JsonMetadata2, events[1].Metadata.AsString());
        }

        [Fact]
        public void should_store_both_data_and_metadata_as_json_if_both_are_valid_json_objects()
        {
            var codec = Codec.EventsJson;
            var request = FakeRequest.GetJsonWrite(new[]
                                                   {
                                                        Tuple.Create(FakeRequest.JsonData, FakeRequest.JsonMetadata),
                                                        Tuple.Create(FakeRequest.JsonData2, FakeRequest.JsonMetadata2)
                                                   });

            var events = AutoEventConverter.SmartParse(request, codec, Guid.Empty);
            Assert.Equal(2, events.Length);

            Assert.True(events[0].IsJson);
            Assert.Equal(FakeRequest.JsonData, events[0].Data.AsString());
            Assert.Equal(FakeRequest.JsonMetadata, events[0].Metadata.AsString());

            Assert.True(events[1].IsJson);
            Assert.Equal(FakeRequest.JsonData2, events[1].Data.AsString());
            Assert.Equal(FakeRequest.JsonMetadata2, events[1].Metadata.AsString());
        }

        [Fact]
        public void should_store_both_data_and_metadata_as_string_if_both_are_not_valid_json_objects()
        {
            var codec = Codec.EventsJson;
            var request = FakeRequest.GetJsonWrite(new[]
                                                   {
                                                        Tuple.Create("\"data\"", "\"metadata\""),
                                                        Tuple.Create("\"data2\"", "\"metadata2\"")
                                                   });

            var events = AutoEventConverter.SmartParse(request, codec, Guid.Empty);
            Assert.Equal(2, events.Length);

            Assert.False(events[0].IsJson);
            Assert.Equal("data", events[0].Data.AsString());
            Assert.Equal("metadata", events[0].Metadata.AsString());

            Assert.False(events[1].IsJson);
            Assert.Equal("data2", events[1].Data.AsString());
            Assert.Equal("metadata2", events[1].Metadata.AsString());
        }

        [Fact]
        public void should_do_its_best_at_preserving_data_format_with_multiple_events()
        {
            var codec = Codec.EventsJson;
            var request = FakeRequest.GetJsonWrite(new[]
                                                   {
                                                           Tuple.Create(FakeRequest.JsonData, FakeRequest.JsonMetadata),
                                                           Tuple.Create("\"data2\"", "\"metadata2\"")
                                                   });

            var events = AutoEventConverter.SmartParse(request, codec, Guid.Empty);
            Assert.Equal(2, events.Length);

            Assert.True(events[0].IsJson);
            Assert.Equal(FakeRequest.JsonData, events[0].Data.AsString());
            Assert.Equal(FakeRequest.JsonMetadata, events[0].Metadata.AsString());

            Assert.False(events[1].IsJson);
            Assert.Equal("data2", events[1].Data.AsString());
            Assert.Equal("metadata2", events[1].Metadata.AsString());
        }
    }

    public class when_writing_events_and_content_type_is_events_xml : do_not_use_indentation_for_json
    {
        [Fact]
        public void should_convert_data_to_json_if_its_valid_xobject_and_metadata_as_string_if_its_not()
        {
            var codec = Codec.EventsXml;
            var request = FakeRequest.GetXmlWrite(FakeRequest.XmlData, "metadata");

            var events = AutoEventConverter.SmartParse(request, codec, Guid.Empty);
            var converted = events.Single();

            Assert.True(converted.IsJson);
            Assert.Equal(FakeRequest.JsonData, converted.Data.AsString());
            Assert.Equal("metadata", converted.Metadata.AsString());
        }

        [Fact]
        public void should_convert_metadata_to_json_if_its_valid_xobject_and_data_as_string_if_its_not()
        {
            var codec = Codec.EventsXml;
            var request = FakeRequest.GetXmlWrite("data", FakeRequest.XmlMetadata);

            var events = AutoEventConverter.SmartParse(request, codec, Guid.Empty);
            var converted = events.Single();

            Assert.True(converted.IsJson);
            Assert.Equal("data", converted.Data.AsString());
            Assert.Equal(FakeRequest.JsonMetadata, converted.Metadata.AsString());
        }

        [Fact]
        public void should_convert_data_and_metadata_to_json_if_both_are_valid_xobjects()
        {
            var codec = Codec.EventsXml;
            var request = FakeRequest.GetXmlWrite(FakeRequest.XmlData, FakeRequest.XmlMetadata);

            var events = AutoEventConverter.SmartParse(request, codec, Guid.Empty);
            var converted = events.Single();

            Assert.True(converted.IsJson);
            Assert.Equal(FakeRequest.JsonData, converted.Data.AsString());
            Assert.Equal(FakeRequest.JsonMetadata, converted.Metadata.AsString());
        }

        [Fact]
        public void should_store_both_data_and_metadata_as_string_if_both_are_not_valid_xobjects_objects()
        {
            var codec = Codec.EventsXml;
            var request = FakeRequest.GetXmlWrite("data", "metadata");

            var events = AutoEventConverter.SmartParse(request, codec, Guid.Empty);
            var converted = events.Single();

            Assert.True(!converted.IsJson);
            Assert.Equal("data", converted.Data.AsString());
            Assert.Equal("metadata", converted.Metadata.AsString());
        }

        [Fact]
        public void should_do_its_best_at_preserving_data_format_with_multiple_events()
        {
            var codec = Codec.EventsXml;
            var request = FakeRequest.GetXmlWrite(new[]
                                                  {
                                                        Tuple.Create(FakeRequest.XmlData, FakeRequest.XmlMetadata),
                                                        Tuple.Create("data2", "metadata2")
                                                  });

            var events = AutoEventConverter.SmartParse(request, codec, Guid.Empty);
            Assert.Equal(2, events.Length);

            Assert.True(events[0].IsJson);
            Assert.Equal(FakeRequest.JsonData, events[0].Data.AsString());
            Assert.Equal(FakeRequest.JsonMetadata, events[0].Metadata.AsString());

            Assert.False(events[1].IsJson);
            Assert.Equal("data2", events[1].Data.AsString());
            Assert.Equal("metadata2", events[1].Metadata.AsString());
        }
    }

    public class when_reading_events_and_accept_type_is_json : do_not_use_indentation_for_json
    {
        [Fact]
        public void should_return_json_data_if_data_was_originally_written_as_xobject_and_metadata_as_string()
        {
            var request = FakeRequest.GetXmlWrite(FakeRequest.XmlData, "metadata");

            var events = AutoEventConverter.SmartParse(request, Codec.EventsXml, Guid.Empty);
            var evnt = events.Single();
            var resolvedEvent = GenerateResolvedEvent(evnt.Data, evnt.Metadata);
            var expected = FakeRequest.GetJsonEventReadResult(resolvedEvent, dataJson: true, metadataJson: false);
            var converted = AutoEventConverter.SmartFormat(resolvedEvent, Codec.EventJson);

            Assert.Equal(expected, converted);
        }

        [Fact]
        public void should_return_json_data_if_data_was_originally_written_as_jobject_and_metadata_as_string()
        {
            var request = FakeRequest.GetJsonWrite(FakeRequest.JsonData, "\"metadata\"");

            var events = AutoEventConverter.SmartParse(request, Codec.EventsJson, Guid.Empty);
            var evnt = events.Single();
            var resolvedEvent = GenerateResolvedEvent(evnt.Data, evnt.Metadata);
            var expected = FakeRequest.GetJsonEventReadResult(resolvedEvent, dataJson: true, metadataJson: false);
            var converted = AutoEventConverter.SmartFormat(resolvedEvent, Codec.EventJson);

            Assert.Equal(expected, converted);
        }

        [Fact]
        public void should_return_json_metadata_if_metadata_was_originally_written_as_xobject_and_data_as_string()
        {
            var request = FakeRequest.GetXmlWrite("data", FakeRequest.XmlMetadata);

            var events = AutoEventConverter.SmartParse(request, Codec.EventsXml, Guid.Empty);
            var evnt = events.Single();

            var resolvedEvent = GenerateResolvedEvent(evnt.Data, evnt.Metadata);

            var expected = FakeRequest.GetJsonEventReadResult(resolvedEvent, dataJson: false, metadataJson: true);
            var converted = AutoEventConverter.SmartFormat(resolvedEvent, Codec.EventJson);

            Assert.Equal(expected, converted);
        }

        [Fact]
        public void should_return_json_metadata_if_metadata_was_originally_written_as_jobject_and_data_as_string()
        {
            var request = FakeRequest.GetJsonWrite("\"data\"", FakeRequest.JsonMetadata);

            var events = AutoEventConverter.SmartParse(request, Codec.EventsJson, Guid.Empty);
            var evnt = events.Single();
            var resolvedEvent = GenerateResolvedEvent(evnt.Data, evnt.Metadata);
            var expected = FakeRequest.GetJsonEventReadResult(resolvedEvent, dataJson: false, metadataJson: true);
            var converted = AutoEventConverter.SmartFormat(resolvedEvent, Codec.EventJson);

            Assert.Equal(expected, converted);
        }

        [Fact]
        public void should_return_json_data_and_json_metadata_if_both_were_written_as_xobjects()
        {
            var request = FakeRequest.GetXmlWrite(FakeRequest.XmlData, FakeRequest.XmlMetadata);

            var events = AutoEventConverter.SmartParse(request, Codec.EventsXml, Guid.Empty);
            var evnt = events.Single();
            var resolvedEvent = GenerateResolvedEvent(evnt.Data, evnt.Metadata);
            var expected = FakeRequest.GetJsonEventReadResult(resolvedEvent, dataJson: true, metadataJson: true);
            var converted = AutoEventConverter.SmartFormat(resolvedEvent, Codec.EventJson);

            Assert.Equal(expected, converted);
        }

        [Fact]
        public void should_return_json_data_and_json_metadata_if_both_were_written_as_jobjects()
        {
            var request = FakeRequest.GetJsonWrite(FakeRequest.JsonData, FakeRequest.JsonMetadata);

            var events = AutoEventConverter.SmartParse(request, Codec.EventsJson, Guid.Empty);
            var evnt = events.Single();
            var resolvedEvent = GenerateResolvedEvent(evnt.Data, evnt.Metadata);
            var expected = FakeRequest.GetJsonEventReadResult(resolvedEvent, dataJson: true, metadataJson: true);
            var converted = AutoEventConverter.SmartFormat(resolvedEvent, Codec.EventJson);

            Assert.Equal(expected, converted);
        }

        [Fact]
        public void should_return_string_data_and_string_metadata_if_both_were_written_as_string_using_json_write()
        {
            var request = FakeRequest.GetJsonWrite("\"data\"", "\"metadata\"");

            var events = AutoEventConverter.SmartParse(request, Codec.EventsJson, Guid.Empty);
            var evnt = events.Single();
            var resolvedEvent = GenerateResolvedEvent(evnt.Data, evnt.Metadata);
            var expected = FakeRequest.GetJsonEventReadResult(resolvedEvent, dataJson: false, metadataJson: false);
            var converted = AutoEventConverter.SmartFormat(resolvedEvent, Codec.EventJson);

            Assert.Equal(expected, converted);
        }

        [Fact]
        public void should_return_string_data_and_string_metadata_if_both_were_written_as_string_using_xml_write()
        {
            var request = FakeRequest.GetXmlWrite("data", "metadata");

            var events = AutoEventConverter.SmartParse(request, Codec.EventsXml, Guid.Empty);
            var evnt = events.Single();
            var resolvedEvent = GenerateResolvedEvent(evnt.Data, evnt.Metadata);
            var expected = FakeRequest.GetJsonEventReadResult(resolvedEvent, dataJson: false, metadataJson: false);
            var converted = AutoEventConverter.SmartFormat(resolvedEvent, Codec.EventJson);

            Assert.Equal(expected, converted);
        }

        private ResolvedEvent GenerateResolvedEvent(byte[] data, byte[] metadata)
        {
            return ResolvedEvent.ForUnresolvedEvent(new EventRecord(0, 0, Guid.NewGuid(), Guid.NewGuid(), 0, 0, "stream", 0, 
                                     DateTime.MinValue, PrepareFlags.IsJson, "type", data, metadata));
        }
    }

    public class when_reading_events_and_accept_type_is_xml : do_not_use_indentation_for_json
    {
        [Fact]
        public void should_return_xml_data_if_data_was_originally_written_as_xobject_and_metadata_as_string()
        {
            var request = FakeRequest.GetXmlWrite(FakeRequest.XmlData, "metadata");

            var events = AutoEventConverter.SmartParse(request, Codec.EventsXml, Guid.Empty);
            var evnt = events.Single();
            var resolvedEvent = GenerateResolvedEvent(evnt.Data, evnt.Metadata);
            var expected = FakeRequest.GetXmlEventReadResult(resolvedEvent, dataJson: true, metadataJson: false);
            var converted = AutoEventConverter.SmartFormat(resolvedEvent, Codec.EventXml);

            Assert.Equal(expected, converted);
        }

        [Fact]
        public void should_return_xml_data_if_data_was_originally_written_as_jobject_and_metadata_as_string()
        {
            var request = FakeRequest.GetJsonWrite(FakeRequest.JsonData, "\"metadata\"");

            var events = AutoEventConverter.SmartParse(request, Codec.EventsJson, Guid.Empty);
            var evnt = events.Single();
            var resolvedEvent = GenerateResolvedEvent(evnt.Data, evnt.Metadata);
            var expected = FakeRequest.GetXmlEventReadResult(resolvedEvent, dataJson: true, metadataJson: false);
            var converted = AutoEventConverter.SmartFormat(resolvedEvent, Codec.EventXml);

            Assert.Equal(expected, converted);
        }

        [Fact]
        public void should_return_xml_metadata_if_metadata_was_originally_written_as_xobject_and_data_as_string()
        {
            var request = FakeRequest.GetXmlWrite("data", FakeRequest.XmlMetadata);

            var events = AutoEventConverter.SmartParse(request, Codec.EventsXml, Guid.Empty);
            var evnt = events.Single();
            var resolvedEvent = GenerateResolvedEvent(evnt.Data, evnt.Metadata);
            var expected = FakeRequest.GetXmlEventReadResult(resolvedEvent, dataJson: false, metadataJson: true);
            var converted = AutoEventConverter.SmartFormat(resolvedEvent, Codec.EventXml);

            Assert.Equal(expected, converted);
        }

        [Fact]
        public void should_return_xml_metadata_if_metadata_was_originally_written_as_jobject_and_data_as_string()
        {
            var request = FakeRequest.GetJsonWrite("\"data\"", FakeRequest.JsonMetadata);

            var events = AutoEventConverter.SmartParse(request, Codec.EventsJson, Guid.Empty);
            var evnt = events.Single();
            var resolvedEvent = GenerateResolvedEvent(evnt.Data, evnt.Metadata);
            var expected = FakeRequest.GetXmlEventReadResult(resolvedEvent, dataJson: false, metadataJson: true);
            var converted = AutoEventConverter.SmartFormat(resolvedEvent, Codec.EventXml);

            Assert.Equal(expected, converted);
        }

        [Fact]
        public void should_return_xml_data_and_xml_metadata_if_both_were_written_as_xobjects()
        {
            var request = FakeRequest.GetXmlWrite(FakeRequest.XmlData, FakeRequest.XmlMetadata);

            var events = AutoEventConverter.SmartParse(request, Codec.EventsXml, Guid.Empty);
            var evnt = events.Single();
            var resolvedEvent = GenerateResolvedEvent(evnt.Data, evnt.Metadata);
            var expected = FakeRequest.GetXmlEventReadResult(resolvedEvent, dataJson: true, metadataJson: true);
            var converted = AutoEventConverter.SmartFormat(resolvedEvent, Codec.EventXml);

            Assert.Equal(expected, converted);
        }

        [Fact]
        public void should_return_xml_data_and_xml_metadata_if_both_were_written_as_jobjects()
        {
            var request = FakeRequest.GetJsonWrite(FakeRequest.JsonData, FakeRequest.JsonMetadata);

            var events = AutoEventConverter.SmartParse(request, Codec.EventsJson, Guid.Empty);
            var evnt = events.Single();
            var resolvedEvent = GenerateResolvedEvent(evnt.Data, evnt.Metadata);
            var expected = FakeRequest.GetXmlEventReadResult(resolvedEvent, dataJson: true, metadataJson: true);
            var converted = AutoEventConverter.SmartFormat(resolvedEvent, Codec.EventXml);

            Assert.Equal(expected, converted);
        }

        [Fact]
        public void should_return_string_data_and_string_metadata_if_both_were_written_as_string_using_json_events_write()
        {
            var request = FakeRequest.GetJsonWrite("\"data\"", "\"metadata\"");

            var events = AutoEventConverter.SmartParse(request, Codec.EventsJson, Guid.Empty);
            var evnt = events.Single();
            var resolvedEvent = GenerateResolvedEvent(evnt.Data, evnt.Metadata);
            var expected = FakeRequest.GetXmlEventReadResult(resolvedEvent, dataJson: false, metadataJson: false);
            var converted = AutoEventConverter.SmartFormat(resolvedEvent, Codec.EventXml);

            Assert.Equal(expected, converted);
        }

        [Fact]
        public void should_return_string_data_and_string_metadata_if_both_were_written_as_string_using_xml_events_write()
        {
            var request = FakeRequest.GetXmlWrite("data", "metadata");

            var events = AutoEventConverter.SmartParse(request, Codec.EventsXml, Guid.Empty);
            var evnt = events.Single();
            var resolvedEvent = GenerateResolvedEvent(evnt.Data, evnt.Metadata);
            var expected = FakeRequest.GetXmlEventReadResult(resolvedEvent, dataJson: false, metadataJson: false);
            var converted = AutoEventConverter.SmartFormat(resolvedEvent, Codec.EventXml);

            Assert.Equal(expected, converted);
        }

        private ResolvedEvent GenerateResolvedEvent(byte[] data, byte[] metadata)
        {
            return ResolvedEvent.ForUnresolvedEvent(new EventRecord(0, 0, Guid.NewGuid(), Guid.NewGuid(), 0, 0, "stream", 0,
                                     DateTime.MinValue, PrepareFlags.IsJson, "type", data, metadata));
        }
    }
}
