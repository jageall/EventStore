using System.Collections.Generic;
using System.Linq;
using EventStore.Common.Utils;
using Xunit;
using Newtonsoft.Json.Linq;

namespace EventStore.Core.Tests.Helpers
{
    public static class HelperExtensions
    {
        public static bool IsBetween(this int n, int a, int b)
        {
            return n >= a && n <= b;
        }

        public static bool AreEqual<TKey, TValue>(this IDictionary<TKey, TValue> first, IDictionary<TKey, TValue> second)
        {
            if (first.Count != second.Count)
                return false;

            TValue value;
            return first.All(kvp => second.TryGetValue(kvp.Key, out value) && value.Equals(kvp.Value));
        }

        public static void AssertJObject(JObject expected, JObject response, string path)
        {
            foreach (KeyValuePair<string, JToken> v in expected)
            {
                JToken vv;
                var propertyName = v.Key;
                if (propertyName.StartsWith("___"))
                    propertyName = "$" + propertyName.Substring(3);
                if (propertyName.EndsWith("___"))
                {
                    if (response.TryGetValue(propertyName.Substring(0, propertyName.Length - "___".Length), out vv))
                    {
                        Assert.True(false,string.Format("{0}/{1} found, but it is explicitly forbidden", path, propertyName));
                    }
                }
                else if (propertyName.EndsWith("___exists"))
                {
                    if (!response.TryGetValue(propertyName.Substring(0, propertyName.Length - "___exists".Length), out vv))
                    {
                        Assert.True(false, string.Format("{0}/{1} not found, but it is explicitly required", path, propertyName));
                    }
                }
                else if (!response.TryGetValue(propertyName, out vv))
                {
                    Assert.True(false, string.Format("{0}/{1} not found in '{2}'", path, propertyName, response));
                }
                else
                {
                    Assert.True(
                        v.Value.Type == vv.Type, string.Format("{0}/{1} type is {2}, but {3} is expected", path, propertyName, vv.Type,
                        v.Value.Type));
                    if (v.Value.Type == JTokenType.Object)
                    {
                        AssertJObject(v.Value as JObject, vv as JObject, path + "/" + propertyName);
                    }
                    else if (v.Value.Type == JTokenType.Array)
                    {
                        AssertJArray(v.Value as JArray, vv as JArray, path + "/" + propertyName);
                    }
                    else if (v.Value is JValue)
                    {
                        Assert.Equal(((JValue) (v.Value)).Value , ((JValue) vv).Value);
                    }
                    else
                        Assert.True(false,"Failed");
                }
            }
        }

        public static void AssertJArray(JArray expected, JArray response, string path)
        {
            for (int index = 0; index < expected.Count; index++)
            {
                JToken v = expected.Count > index ? expected[index] : new JValue((object)null);
                JToken vv = response.Count > index ? response[index] : new JValue((object) null);
                Assert.True(
                    v.Type == vv.Type, string.Format("{0}/{1} type is {2}, but {3} is expected", path, index, vv.Type,
                    v.Type));
                if (v.Type == JTokenType.Object)
                {
                    AssertJObject(v as JObject, vv as JObject, path + "/" + index);
                }
                else if (v.Type == JTokenType.Array)
                {
                    AssertJArray(v as JArray, vv as JArray, path + "/" + index);
                }
                else if (v is JValue)
                {
                    Assert.Equal(((JValue) v).Value, ((JValue) vv).Value);
                }
                else
                    Assert.True(false, "failed");
            }
        }

        public static void AssertJson<T>(T expected, JObject response)
        {
            var serialized = expected.ToJson();
            var jobject = serialized.ParseJson<JObject>();

            var path = "/";

            AssertJObject(jobject, response, path);
        }
    }
}