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
using System.Linq.Expressions;
using System.Reflection;

namespace DynamORM.Mapper
{
    /// <summary>Dynamic property invoker.</summary>
    public class DynamicPropertyInvoker
    {
        /// <summary>Gets value getter.</summary>
        public Func<object, object> Get { get; private set; }

        /// <summary>Gets value setter.</summary>
        public Action<object, object> Set { get; private set; }

        /// <summary>Gets name of property.</summary>
        public string Name { get; private set; }

        /// <summary>Gets type column description.</summary>
        public ColumnAttribute Column { get; private set; }

        /// <summary>Gets a value indicating whether this <see cref="DynamicPropertyInvoker"/> is ignored in some cases.</summary>
        public bool Ignore { get; private set; }

        /// <summary>Initializes a new instance of the <see cref="DynamicPropertyInvoker" /> class.</summary>
        /// <param name="property">Property info to be invoked in the future.</param>
        /// <param name="attr">Column attribute if exist.</param>
        public DynamicPropertyInvoker(PropertyInfo property, ColumnAttribute attr)
        {
            Name = property.Name;

            var ignore = property.GetCustomAttributes(typeof(IgnoreAttribute), false);

            Ignore = ignore != null && ignore.Length > 0;

            Column = attr;

            Get = CreateGetter(property);
            Set = CreateSetter(property);
        }

        private Func<object, object> CreateGetter(PropertyInfo property)
        {
            if (!property.CanRead)
                return null;

            var objParm = Expression.Parameter(typeof(object), "o");

            return Expression.Lambda<Func<object, object>>(
                Expression.Convert(
                    Expression.Property(
                        Expression.TypeAs(objParm, property.DeclaringType),
                        property.Name),
                    typeof(object)), objParm).Compile();
        }

        private Action<object, object> CreateSetter(PropertyInfo property)
        {
            if (!property.CanWrite)
                return null;

            var objParm = Expression.Parameter(typeof(object), "o");
            var valueParm = Expression.Parameter(typeof(object), "value");

            return Expression.Lambda<Action<object, object>>(
                Expression.Assign(
                    Expression.Property(
                        Expression.Convert(objParm, property.DeclaringType),
                        property.Name),
                    Expression.Convert(valueParm, property.PropertyType)),
                    objParm, valueParm).Compile();
        }
    }
}