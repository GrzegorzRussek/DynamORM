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

using System.Collections.Generic;
using System.Linq;

namespace DynamORM.Helpers
{
    /// <summary>Defines methods to support the comparison of collections for equality.</summary>
    /// <typeparam name="T">The type of collection to compare.</typeparam>
    public class CollectionComparer<T> : IEqualityComparer<IEnumerable<T>>
    {
        /// <summary>Determines whether the specified objects are equal.</summary>
        /// <param name="first">The first object of type T to compare.</param>
        /// <param name="second">The second object of type T to compare.</param>
        /// <returns>Returns <c>true</c> if the specified objects are equal; otherwise, <c>false</c>.</returns>
        bool IEqualityComparer<IEnumerable<T>>.Equals(IEnumerable<T> first, IEnumerable<T> second)
        {
            return Equals(first, second);
        }

        /// <summary>Returns a hash code for the specified object.</summary>
        /// <param name="enumerable">The enumerable for which a hash code is to be returned.</param>
        /// <returns>A hash code for the specified object.</returns>
        int IEqualityComparer<IEnumerable<T>>.GetHashCode(IEnumerable<T> enumerable)
        {
            return GetHashCode(enumerable);
        }

        /// <summary>Returns a hash code for the specified object.</summary>
        /// <param name="enumerable">The enumerable for which a hash code is to be returned.</param>
        /// <returns>A hash code for the specified object.</returns>
        public static int GetHashCode(IEnumerable<T> enumerable)
        {
            int hash = 17;

            foreach (T val in enumerable.OrderBy(x => x))
                hash = (hash * 23) + val.GetHashCode();

            return hash;
        }

        /// <summary>Determines whether the specified objects are equal.</summary>
        /// <param name="first">The first object of type T to compare.</param>
        /// <param name="second">The second object of type T to compare.</param>
        /// <returns>Returns <c>true</c> if the specified objects are equal; otherwise, <c>false</c>.</returns>
        public static bool Equals(IEnumerable<T> first, IEnumerable<T> second)
        {
            if ((first == null) != (second == null))
                return false;

            if (!object.ReferenceEquals(first, second) && (first != null))
            {
                if (first.Count() != second.Count())
                    return false;

                if ((first.Count() != 0) && HaveMismatchedElement(first, second))
                    return false;
            }

            return true;
        }

        private static bool HaveMismatchedElement(IEnumerable<T> first, IEnumerable<T> second)
        {
            int firstCount;
            int secondCount;

            Dictionary<T, int> firstElementCounts = GetElementCounts(first, out firstCount);
            Dictionary<T, int> secondElementCounts = GetElementCounts(second, out secondCount);

            if (firstCount != secondCount)
                return true;

            foreach (KeyValuePair<T, int> kvp in firstElementCounts)
                if (kvp.Value != (secondElementCounts.TryGetNullable(kvp.Key) ?? 0))
                    return true;

            return false;
        }

        private static Dictionary<T, int> GetElementCounts(IEnumerable<T> enumerable, out int nullCount)
        {
            Dictionary<T, int> dictionary = new Dictionary<T, int>();
            nullCount = 0;

            foreach (T element in enumerable)
            {
                if (element == null)
                    nullCount++;
                else
                {
                    int count = dictionary.TryGetNullable(element) ?? 0;
                    dictionary[element] = ++count;
                }
            }

            return dictionary;
        }
    }
}