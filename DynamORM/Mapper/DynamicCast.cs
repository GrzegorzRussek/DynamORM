/*
 * DynamORM - Dynamic Object-Relational Mapping library.
 * Copyright (c) 2012-2015, Grzegorz Russek (grzegorz.russek@gmail.com)
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
using System.Collections.Concurrent;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;

namespace DynamORM.Mapper
{
    /// <summary>Type cast helper.</summary>
    public static class DynamicCast
    {
        /// <summary>Gets the default value.</summary>
        /// <param name="type">The type.</param>
        /// <returns>Default instance.</returns>
        public static object GetDefaultValue(this Type type)
        {
            return type.IsValueType ? TypeDefaults.GetOrAdd(type, t => Activator.CreateInstance(t)) : null;
        }

        /// <summary>Casts the object to this type.</summary>
        /// <param name="type">The type to which cast value.</param>
        /// <param name="val">The value to cast.</param>
        /// <returns>Value casted to new type.</returns>
        public static object CastObject(this Type type, object val)
        {
            return GetConverter(type, val)(val);
        }

        private static readonly ConcurrentDictionary<Type, object> TypeDefaults = new ConcurrentDictionary<Type, object>();
        private static readonly ConcurrentDictionary<Type, Func<object, object>> TypeAsCasts = new ConcurrentDictionary<Type, Func<object, object>>();
        private static readonly ConcurrentDictionary<PairOfTypes, Func<object, object>> TypeConvert = new ConcurrentDictionary<PairOfTypes, Func<object, object>>();
        private static readonly ParameterExpression ConvParameter = Expression.Parameter(typeof(object), "val");

        [MethodImpl(MethodImplOptions.Synchronized)]
        private static Func<object, object> GetConverter(Type targetType, object val)
        {
            Func<object, object> fn;

            if (!targetType.IsValueType && !val.GetType().IsValueType)
            {
                if (!TypeAsCasts.TryGetValue(targetType, out fn))
                {
                    UnaryExpression instanceCast = Expression.TypeAs(ConvParameter, targetType);

                    fn = Expression.Lambda<Func<object, object>>(Expression.TypeAs(instanceCast, typeof(object)), ConvParameter).Compile();
                    TypeAsCasts.AddOrUpdate(targetType, fn, (t, f) => fn);
                }
            }
            else
            {
                var fromType = val != null ? val.GetType() : typeof(object);
                var key = new PairOfTypes(fromType, targetType);
                if (TypeConvert.TryGetValue(key, out fn))
                    return fn;

                fn = (Func<object, object>)Expression.Lambda(Expression.Convert(Expression.Convert(Expression.Convert(ConvParameter, fromType), targetType), typeof(object)), ConvParameter).Compile();
                TypeConvert.AddOrUpdate(key, fn, (t, f) => fn);
            }

            return fn;
        }

        private class PairOfTypes
        {
            private readonly Type _first;
            private readonly Type _second;

            public PairOfTypes(Type first, Type second)
            {
                this._first = first;
                this._second = second;
            }

            public override int GetHashCode()
            {
                return (31 * _first.GetHashCode()) + _second.GetHashCode();
            }

            public override bool Equals(object obj)
            {
                if (obj == this)
                    return true;

                var other = obj as PairOfTypes;
                if (other == null)
                    return false;

                return _first.Equals(other._first)
                    && _second.Equals(other._second);
            }
        }
    }
}