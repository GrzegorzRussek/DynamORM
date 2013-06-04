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

namespace DynamORM.Helpers
{
    /// <summary>Class contains unclassified extensions.</summary>
    public static class UnclassifiedExtensions
    {
        /// <summary>Easy way to use conditional value.</summary>
        /// <remarks>Includes <see cref="DBNull.Value"/>.</remarks>
        /// <typeparam name="T">Input object type to check.</typeparam>
        /// <typeparam name="R">Result type.</typeparam>
        /// <param name="obj">The object to check.</param>
        /// <param name="func">The select function.</param>
        /// <param name="elseValue">The else value.</param>
        /// <returns>Selected value or default value.</returns>
        /// <example>It lets you do this:
        /// <code>var lname = thingy.NullOr(t => t.Name).NullOr(n => n.ToLower());</code>
        /// which is more fluent and (IMO) easier to read than this:
        /// <code>var lname = (thingy != null ? thingy.Name : null) != null ? thingy.Name.ToLower() : null;</code>
        /// </example>
        public static R NullOr<T, R>(this T obj, Func<T, R> func, R elseValue = default(R)) where T : class
        {
            return obj != null && obj != DBNull.Value ?
                func(obj) : elseValue;
        }

        /// <summary>Easy way to use conditional value.</summary>
        /// <remarks>Includes <see cref="DBNull.Value"/>.</remarks>
        /// <typeparam name="T">Input object type to check.</typeparam>
        /// <typeparam name="R">Result type.</typeparam>
        /// <param name="obj">The object to check.</param>
        /// <param name="func">The select function.</param>
        /// <param name="elseFunc">The else value function.</param>
        /// <returns>Selected value or default value.</returns>
        /// <example>It lets you do this:
        /// <code>var lname = thingy.NullOr(t => t.Name).NullOr(n => n.ToLower());</code>
        /// which is more fluent and (IMO) easier to read than this:
        /// <code>var lname = (thingy != null ? thingy.Name : null) != null ? thingy.Name.ToLower() : null;</code>
        /// </example>
        public static R NullOrFn<T, R>(this T obj, Func<T, R> func, Func<R> elseFunc = null) where T : class
        {
            // Old if to avoid recurency.
            return obj != null && obj != DBNull.Value ?
                func(obj) : elseFunc != null ? elseFunc() : default(R);
        }
    }
}