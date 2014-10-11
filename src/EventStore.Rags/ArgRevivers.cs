using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Reflection;

namespace EventStore.Rags
{
    internal static class ArgRevivers
    {
        private static Dictionary<Type, Func<string, string, object>> revivers;
        private static Dictionary<Type, Func<string, string, object>> Revivers
        {
            get
            {
                if (revivers == null)
                {
                    revivers = new Dictionary<Type, Func<string, string, object>>();
                    LoadDefaultRevivers(revivers);
                }
                return revivers;
            }
        }

        public static void Register<T>(Func<string, string, T> reviver)
        {
            if (reviver == null) throw new ArgumentNullException("reviver");
            revivers.Add(typeof (T), (x,y) => reviver(x,y));
        }

        internal static bool CanRevive(Type t)
        {
            if (Revivers.ContainsKey(t) ||
                t.IsEnum ||
                (t.GetInterfaces().Contains(typeof(IList)) && t.IsGenericType && CanRevive(t.GetGenericArguments()[0])) ||
                (t.IsArray && CanRevive(t.GetElementType())))
                return true;

            return System.ComponentModel.TypeDescriptor.GetConverter(t).CanConvertFrom(typeof(string)) || Revivers.ContainsKey(t);
        }

        internal static object ReviveEnum(Type t, string value, bool ignoreCase)
        {
            if (value.Contains(","))
            {
                int ret = 0;
                var values = value.Split(',').Select(v => v.Trim());
                foreach (var enumValue in values)
                {
                    try
                    {
                        ret = ret | ParseEnumValue(t, enumValue, ignoreCase);
                    }
                    catch (Exception ex)
                    {
                        throw new ValidationArgException(enumValue + " is not a valid value for type " + t.Name + ", options are " + string.Join(", ", Enum.GetNames(t)));
                    }
                }

                return Enum.ToObject(t, ret);
            }
            else
            {
                try
                {
                    return Enum.ToObject(t, ParseEnumValue(t, value, ignoreCase));
                }
                catch (Exception ex)
                {
                    if (value == string.Empty) value = "<empty>";
                    throw new ValidationArgException(value + " is not a valid value for type " + t.Name + ", options are " + string.Join(", ", Enum.GetNames(t)));
                }
            }
        }

        private static int ParseEnumValue(Type t, string valueString, bool ignoreCase)
        {
            int rawInt;

            if (int.TryParse(valueString, out rawInt))
            {
                return (int)Enum.ToObject(t, rawInt);
            }

            object enumShortcutMatch;
            if (t.TryMatchEnumShortcut(valueString, ignoreCase, out enumShortcutMatch))
            {
                return (int)enumShortcutMatch;
            }

            return (int)Enum.Parse(t, valueString, ignoreCase);
        }

        internal static object Revive(Type t, string name, string value)
        {
            if (t.IsArray == false && t.GetInterfaces().Contains(typeof(IList)))
            {
                var list = (IList)Activator.CreateInstance(t);
                // TODO - Maybe support custom delimiters via an attribute on the property
                // TODO - Maybe do a full parse of the value to check for quoted strings
                if (string.IsNullOrWhiteSpace(value)) return list;
                foreach (var element in value.Split(','))
                {
                    list.Add(Revive(t.GetGenericArguments()[0], name + "_element", element));
                }
                return list;
            }
            if (t.IsArray)
            {
                var elements = value.Split(',');

                if (string.IsNullOrWhiteSpace(value) != false) return Array.CreateInstance(t.GetElementType(), 0);
                var array = Array.CreateInstance(t.GetElementType(), elements.Length);
                for (var i = 0; i < array.Length; i++)
                {
                    array.SetValue(Revive(t.GetElementType(), name + "[" + i + "]", elements[i]), i);
                }
                return array;
            }
            if (Revivers.ContainsKey(t))
                return Revivers[t].Invoke(name, value);
            if (System.ComponentModel.TypeDescriptor.GetConverter(t).CanConvertFrom(typeof(string)))
                return System.ComponentModel.TypeDescriptor.GetConverter(t).ConvertFromString(value);
            // Intentionally not an InvalidArgDefinitionException.  Other internal code should call 
            // CanRevive and this block should never be executed.
            throw new ArgumentException("Cannot revive type " + t.FullName + ". Callers should be calling CanRevive before calling Revive()");
        }

        private static void LoadDefaultRevivers(Dictionary<Type, Func<string, string, object>> revivers)
        {
            Register((prop, val) => val != null && val.ToLower().ToString() != "false" && val != "0");

            Register((prop, val) =>
            {
                Guid ret;
                if (Guid.TryParse(val, out ret) == false) throw new FormatException(String.Format("value for {0} must be a Guid: {1}", prop, val));
                return ret;
            });

            Register((prop, val) =>
            {
                byte ret;
                if (byte.TryParse(val, out ret) == false) throw new FormatException(String.Format("value for {0} must be a byte: {1}", prop, val));
                return ret;
            });

            Register((prop, val) =>
            {
                int ret;
                if (int.TryParse(val, out ret) == false) throw new FormatException(String.Format("value for {0} must be an integer: {1}", prop, val));
                return ret;
            });

            Register((prop, val) =>
            {
                long ret;
                if (long.TryParse(val, out ret) == false) throw new FormatException(String.Format("value for {0} must be an integer: {1}", prop, val));
                return ret;
            });

            Register((prop, val) =>
            {
                double ret;
                if (double.TryParse(val, out ret) == false) throw new FormatException(String.Format("value for {0} must be a number: {1}", prop, val));
                return ret;
            });

            Register((prop, val) => val);

            Register((prop, val) =>
            {
                DateTime ret;
                if (DateTime.TryParse(val, out ret) == false) throw new FormatException(String.Format("value for {0} must be a valid date time: {1}", prop, val));
                return ret;
            });

            Register((prop, val) =>
            {
                if (val != null) throw new ArgException("The value for " + prop + " cannot be specified on the command line");
                return new SecureStringArgument(prop);
            });

            Register((prop, val) =>
            {
                try
                {
                    return new Uri(val);
                }
                catch (UriFormatException)
                {
                    throw new UriFormatException("value must be a valid URI: " + val);
                }
            });

            revivers.Add(typeof(IPAddress), (prop, val) => IPAddress.Parse(val));

            revivers.Add(typeof(IPEndPoint), (prop, val) =>
            {
                var parts = val.Split(':');
                return new IPEndPoint(IPAddress.Parse(parts[0]), Int32.Parse(parts[1]));
            });

        }
    }
}