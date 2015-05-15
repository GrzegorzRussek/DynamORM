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
using System.Data;
using System.Linq.Expressions;
using System.Reflection;

namespace DynamORM.Mapper
{
    /// <summary>Dynamic property invoker.</summary>
    public class DynamicPropertyInvoker
    {
        internal class ParameterSpec
        {
            public string Name { get; set; }

            public DbType Type { get; set; }

            public int Ordinal { get; set; }
        }

        /// <summary>Gets the type of property.</summary>
        public Type Type { get; private set; }

        /// <summary>Gets value getter.</summary>
        public Func<object, object> Get { get; private set; }

        /// <summary>Gets value setter.</summary>
        public Action<object, object> Setter { get; private set; }

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
            Type = property.PropertyType;

            object[] ignore = property.GetCustomAttributes(typeof(IgnoreAttribute), false);

            Ignore = ignore != null && ignore.Length > 0;

            Column = attr;

            if (property.CanRead)
                Get = CreateGetter(property);

            if (property.CanWrite)
                Setter = CreateSetter(property);
        }

        private Func<object, object> CreateGetter(PropertyInfo property)
        {
            if (!property.CanRead)
                return null;

            ParameterExpression objParm = Expression.Parameter(typeof(object), "o");

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

            ParameterExpression objParm = Expression.Parameter(typeof(object), "o");
            ParameterExpression valueParm = Expression.Parameter(typeof(object), "value");

            return Expression.Lambda<Action<object, object>>(
                Expression.Assign(
                    Expression.Property(
                        Expression.Convert(objParm, property.DeclaringType),
                        property.Name),
                    Expression.Convert(valueParm, property.PropertyType)),
                    objParm, valueParm).Compile();
        }

        /// <summary>Sets the specified value to destination object.</summary>
        /// <param name="dest">The destination object.</param>
        /// <param name="val">The value.</param>
        public void Set(object dest, object val)
        {
            Type type = Nullable.GetUnderlyingType(Type) ?? Type;
            bool nullable = Type.IsGenericType && Type.GetGenericTypeDefinition() == typeof(Nullable<>);

            try
            {
                if (val == null && type.IsValueType)
                {
                    if (nullable)
                        Setter(dest, null);
                    else
                        Setter(dest, Activator.CreateInstance(Type));
                }
                else if ((val == null && !type.IsValueType) || (val != null && type == val.GetType()))
                    Setter(dest, val);
                else if (type.IsEnum && val.GetType().IsValueType)
                    Setter(dest, Enum.ToObject(type, val));
                else if (type.IsEnum)
                    Setter(dest, Enum.Parse(type, val.ToString()));
                else if (Type == typeof(string) && val.GetType() == typeof(Guid))
                    Setter(dest, val.ToString());
                else if (Type == typeof(Guid) && val.GetType() == typeof(string))
                {
                    Guid g;
                    Setter(dest, Guid.TryParse((string)val, out g) ? g : Guid.Empty);
                }
                else
                    Setter(dest, Convert.ChangeType(val, type));
            }
            catch (Exception ex)
            {
                throw new InvalidCastException(
                    string.Format("Error trying to convert value '{0}' of type '{1}' to value of type '{2}{3}' in object of type '{4}'",
                        val.ToString(), val.GetType(), type.FullName, nullable ? "(NULLABLE)" : string.Empty, dest.GetType().FullName),
                    ex);
            }
        }

        #region Type command cache

        internal ParameterSpec InsertCommandParameter { get; set; }

        internal ParameterSpec UpdateCommandParameter { get; set; }

        internal ParameterSpec DeleteCommandParameter { get; set; }

        #endregion Type command cache
    }
}