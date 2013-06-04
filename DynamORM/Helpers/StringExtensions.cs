/*
 * DynamORM - Dynamic Object-Relational Mapping library.
 * Copyright (c) 2012, Grzegorz Russek (grzegorz.russek@gmail.com)
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * Redistributions of source code must retain the above copyright notice,
 * this list of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF
 * THE POSSIBILITY OF SUCH DAMAGE.
*/

using System;
using System.Collections;
using System.Reflection;
using System.Text;

namespace DynamORM.Helpers
{
    /// <summary>Class containing useful string extensions.</summary>
    internal static class StringExtensions
    {
        static StringExtensions()
        {
            InvalidMultipartMemberChars = _InvalidMultipartMemberChars.ToCharArray();
            InvalidMemberChars = _InvalidMemberChars.ToCharArray();
        }

        private static readonly string _InvalidMultipartMemberChars = " +-*/^%[]{}()!\"\\&=?¿";
        private static readonly string _InvalidMemberChars = "." + _InvalidMultipartMemberChars;

        /// <summary>
        /// Gets an array with some invalid characters that cannot be used with multipart names for class members.
        /// </summary>
        public static char[] InvalidMultipartMemberChars { get; private set; }

        /// <summary>
        /// Gets an array with some invalid characters that cannot be used with names for class members.
        /// </summary>
        public static char[] InvalidMemberChars { get; private set; }

        /// <summary>
        /// Provides with an alternate and generic way to obtain an alternate string representation for this instance,
        /// applying the following rules:
        /// <para>- Null values are returned as with the <see cref="NullString"/> value, or a null object.</para>
        /// <para>- Enum values are translated into their string representation.</para>
        /// <para>- If the type has override the 'ToString' method then it is used.</para>
        /// <para>- If it is a dictionary, then a collection of key/value pairs where the value part is also translated.</para>
        /// <para>- If it is a collection, then a collection of value items also translated.</para>
        /// <para>- If it has public public properties (or if not, if it has public fields), the collection of name/value
        /// pairs, with the values translated.</para>
        /// <para>- Finally it falls back to the standard 'type.FullName' mechanism.</para>
        /// </summary>
        /// <param name="obj">The object to obtain its alternate string representation from.</param>
        /// <param name="brackets">The brackets to use if needed. If not null it must be at least a 2-chars' array containing
        /// the opening and closing brackets.</param>
        /// <param name="nullString">Representation of null string..</param>
        /// <returns>The alternate string representation of this object.</returns>
        public static string Sketch(this object obj, char[] brackets = null, string nullString = "(null)")
        {
            if (obj == null) return nullString;
            if (obj is string) return (string)obj;

            Type type = obj.GetType();
            if (type.IsEnum) return obj.ToString();

            // If the ToString() method has been overriden (by the type itself, or by its parents), let's use it...
            MethodInfo method = type.GetMethod("ToString", Type.EmptyTypes);
            if (method.DeclaringType != typeof(object)) return obj.ToString();

            // For alll other cases...
            StringBuilder sb = new StringBuilder();
            bool first = true;

            // Dictionaries...
            if (obj is IDictionary)
            {
                if (brackets == null || brackets.Length < 2)
                    brackets = "[]".ToCharArray();

                sb.AppendFormat("{0}", brackets[0]); first = true; foreach (DictionaryEntry kvp in (IDictionary)obj)
                {
                    if (!first) sb.Append(", "); else first = false;
                    sb.AppendFormat("'{0}'='{1}'", kvp.Key.Sketch(), kvp.Value.Sketch());
                }

                sb.AppendFormat("{0}", brackets[1]);
                return sb.ToString();
            }

            // IEnumerables...
            IEnumerator ator = null;
            if (obj is IEnumerable)
                ator = ((IEnumerable)obj).GetEnumerator();
            else
            {
                method = type.GetMethod("GetEnumerator", Type.EmptyTypes);
                if (method != null)
                    ator = (IEnumerator)method.Invoke(obj, null);
            }

            if (ator != null)
            {
                if (brackets == null || brackets.Length < 2) brackets = "[]".ToCharArray();
                sb.AppendFormat("{0}", brackets[0]); first = true; while (ator.MoveNext())
                {
                    if (!first) sb.Append(", "); else first = false;
                    sb.AppendFormat("{0}", ator.Current.Sketch());
                }

                sb.AppendFormat("{0}", brackets[1]);

                if (ator is IDisposable)
                    ((IDisposable)ator).Dispose();

                return sb.ToString();
            }

            // As a last resort, using the public properties (or fields if needed, or type name)...
            BindingFlags flags = BindingFlags.Public | BindingFlags.Instance | BindingFlags.FlattenHierarchy;
            PropertyInfo[] props = type.GetProperties(flags);
            FieldInfo[] infos = type.GetFields(flags);

            if (props.Length == 0 && infos.Length == 0) sb.Append(type.FullName); // Fallback if needed
            else
            {
                if (brackets == null || brackets.Length < 2) brackets = "{}".ToCharArray();
                sb.AppendFormat("{0}", brackets[0]);
                first = true;

                if (props.Length != 0)
                {
                    foreach (var prop in props)
                    {
                        if (!first) sb.Append(", "); else first = false;
                        sb.AppendFormat("{0}='{1}'", prop.Name, prop.GetValue(obj, null).Sketch());
                    }
                }
                else
                {
                    if (infos.Length != 0)
                    {
                        foreach (var info in infos)
                        {
                            if (!first) sb.Append(", "); else first = false;
                            sb.AppendFormat("{0}='{1}'", info.Name, info.GetValue(obj).Sketch());
                        }
                    }
                }

                sb.AppendFormat("{0}", brackets[1]);
            }

            // And returning...
            return sb.ToString();
        }

        /// <summary>
        /// Returns true if the target string contains any of the characters given.
        /// </summary>
        /// <param name="source">The target string. It cannot be null.</param>
        /// <param name="items">An array containing the characters to test. It cannot be null. If empty false is returned.</param>
        /// <returns>True if the target string contains any of the characters given, false otherwise.</returns>
        public static bool ContainsAny(this string source, char[] items)
        {
            if (source == null) throw new ArgumentNullException("source", "Source string cannot be null.");
            if (items == null) throw new ArgumentNullException("items", "Array of characters to test cannot be null.");

            if (items.Length == 0) return false; // No characters to validate
            int ix = source.IndexOfAny(items);
            return ix >= 0 ? true : false;
        }

        /// <summary>
        /// Returns a new validated string using the rules given.
        /// </summary>
        /// <param name="source">The source string.</param>
        /// <param name="desc">A description of the source string to build errors and exceptions if needed.</param>
        /// <param name="canbeNull">True if the returned string can be null.</param>
        /// <param name="canbeEmpty">True if the returned string can be empty.</param>
        /// <param name="trim">True to trim the returned string.</param>
        /// <param name="trimStart">True to left-trim the returned string.</param>
        /// <param name="trimEnd">True to right-trim the returned string.</param>
        /// <param name="minLen">If >= 0, the min valid length for the returned string.</param>
        /// <param name="maxLen">If >= 0, the max valid length for the returned string.</param>
        /// <param name="padLeft">If not '\0', the character to use to left-pad the returned string if needed.</param>
        /// <param name="padRight">If not '\0', the character to use to right-pad the returned string if needed.</param>
        /// <param name="invalidChars">If not null, an array containing invalid chars that must not appear in the returned
        /// string.</param>
        /// <param name="validChars">If not null, an array containing the only characters that are considered valid for the
        /// returned string.</param>
        /// <returns>A new validated string.</returns>
        public static string Validated(this string source, string desc = null,
            bool canbeNull = false, bool canbeEmpty = false,
            bool trim = true, bool trimStart = false, bool trimEnd = false,
            int minLen = -1, int maxLen = -1, char padLeft = '\0', char padRight = '\0',
            char[] invalidChars = null, char[] validChars = null)
        {
            // Assuring a valid descriptor...
            if (string.IsNullOrWhiteSpace(desc)) desc = "Source";

            // Validating if null sources are accepted...
            if (source == null)
            {
                if (!canbeNull) throw new ArgumentNullException(desc, string.Format("{0} cannot be null.", desc));
                return null;
            }

            // Trimming if needed...
            if (trim && !(trimStart || trimEnd)) source = source.Trim();
            else
            {
                if (trimStart) source = source.TrimStart(' ');
                if (trimEnd) source = source.TrimEnd(' ');
            }

            // Adjusting lenght...
            if (minLen > 0)
            {
                if (padLeft != '\0') source = source.PadLeft(minLen, padLeft);
                if (padRight != '\0') source = source.PadRight(minLen, padRight);
            }

            if (maxLen > 0)
            {
                if (padLeft != '\0') source = source.PadLeft(maxLen, padLeft);
                if (padRight != '\0') source = source.PadRight(maxLen, padRight);
            }

            // Validating emptyness and lenghts...
            if (source.Length == 0)
            {
                if (!canbeEmpty) throw new ArgumentException(string.Format("{0} cannot be empty.", desc));
                return string.Empty;
            }

            if (minLen >= 0 && source.Length < minLen) throw new ArgumentException(string.Format("Lenght of {0} '{1}' is lower than '{2}'.", desc, source, minLen));
            if (maxLen >= 0 && source.Length > maxLen) throw new ArgumentException(string.Format("Lenght of {0} '{1}' is bigger than '{2}'.", desc, source, maxLen));

            // Checking invalid chars...
            if (invalidChars != null)
            {
                int n = source.IndexOfAny(invalidChars);
                if (n >= 0) throw new ArgumentException(string.Format("Invalid character '{0}' found in {1} '{2}'.", source[n], desc, source));
            }

            // Checking valid chars...
            if (validChars != null)
            {
                int n = validChars.ToString().IndexOfAny(source.ToCharArray());
                if (n >= 0) throw new ArgumentException(string.Format("Invalid character '{0}' found in {1} '{2}'.", validChars.ToString()[n], desc, source));
            }

            return source;
        }

        /// <summary>
        /// Splits the given string with the 'something AS alias' format, returning a tuple containing its 'something' and 'alias' parts.
        /// If no alias is detected, then its component in the tuple returned is null and all the contents from the source
        /// string are considered as the 'something' part.
        /// </summary>
        /// <param name="source">The source string.</param>
        /// <returns>A tuple containing the 'something' and 'alias' parts.</returns>
        public static Tuple<string, string> SplitSomethingAndAlias(this string source)
        {
            source = source.Validated("[Something AS Alias]");

            string something = null;
            string alias = null;
            int n = source.LastIndexOf(" AS ", StringComparison.OrdinalIgnoreCase);

            if (n < 0)
                something = source;
            else
            {
                something = source.Substring(0, n);
                alias = source.Substring(n + 4);
            }

            return new Tuple<string, string>(something, alias);
        }

        /// <summary>Allows to replace parameters inside of string.</summary>
        /// <param name="stringToFill">String containing parameters in format <c>[$ParameterName]</c>.</param>
        /// <param name="getValue">Function that should return value that will be placed in string in place of placed parameter.</param>
        /// <param name="prefix">Prefix of the parameter. This value can't be null or empty, default value <code>[$</code>.</param>
        /// <param name="sufix">Suffix of the parameter. This value can't be null or empty, default value <code>]</code>.</param>
        /// <returns>Parsed string.</returns>
        public static string FillStringWithVariables(this string stringToFill, Func<string, string> getValue, string prefix = "[$", string sufix = "]")
        {
            int startPos = 0, endPos = 0;
            prefix.Validated();
            sufix.Validated();

            startPos = stringToFill.IndexOf(prefix, startPos);
            while (startPos >= 0)
            {
                endPos = stringToFill.IndexOf(sufix, startPos + prefix.Length);
                int nextStartPos = stringToFill.IndexOf(prefix, startPos + prefix.Length);

                if (endPos > startPos + prefix.Length + 1 && (nextStartPos > endPos || nextStartPos == -1))
                {
                    string paramName = stringToFill.Substring(startPos + prefix.Length, endPos - (startPos + prefix.Length));

                    stringToFill = stringToFill
                        .Remove(startPos, (endPos - startPos) + sufix.Length)
                        .Insert(startPos, getValue(paramName));
                }

                startPos = stringToFill.IndexOf(prefix, startPos + prefix.Length);
            }

            return stringToFill;
        }
    }
}