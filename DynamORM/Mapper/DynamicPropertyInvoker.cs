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
using System.Collections.Generic;
using System.Data;
using System.Linq;
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

        private Type _arrayType;
        private bool _genericEnumerable;

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

        /// <summary>Gets a value indicating whether this instance hold data contract type.</summary>
        public bool IsDataContract { get; private set; }

        /// <summary>Initializes a new instance of the <see cref="DynamicPropertyInvoker" /> class.</summary>
        /// <param name="property">Property info to be invoked in the future.</param>
        /// <param name="attr">Column attribute if exist.</param>
        public DynamicPropertyInvoker(PropertyInfo property, ColumnAttribute attr)
        {
            Name = property.Name;
            Type = property.PropertyType;

            object[] ignore = property.GetCustomAttributes(typeof(IgnoreAttribute), false);

            Ignore = ignore != null && ignore.Length > 0;

            _arrayType = Type.IsArray ? Type.GetElementType() :
                Type.IsGenericEnumerable() ? Type.GetGenericArguments().First() :
                Type;

            _genericEnumerable = Type.IsGenericEnumerable();

            IsDataContract = _arrayType.GetCustomAttributes(false).Any(x => x.GetType().Name == "DataContractAttribute");

            if (_arrayType.IsArray)
                throw new InvalidOperationException("Jagged arrays are not supported");

            if (_arrayType.IsGenericEnumerable())
                throw new InvalidOperationException("Enumerables of enumerables are not supported");

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
            object value = null;

            try
            {
                if (Type.IsArray || _genericEnumerable)
                {
                    var lst = (val as IEnumerable<object>).Select(x => GetElementVal(_arrayType, x)).ToList();

                    value = Array.CreateInstance(_arrayType, lst.Count);

                    int i = 0;
                    foreach (var e in lst)
                        ((Array)value).SetValue(e, i++);
                }
                else
                    value = GetElementVal(Type, val);

                Setter(dest, value);
            }
            catch (Exception ex)
            {
                throw new InvalidCastException(
                    string.Format("Error trying to convert value '{0}' of type '{1}' to value of type '{2}' in object of type '{3}'",
                        (val ?? string.Empty).ToString(), val.GetType(), Type.FullName, dest.GetType().FullName),
                    ex);
            }
        }

        private object GetElementVal(System.Type etype, object val)
        {
            bool nullable = etype.IsGenericType && etype.GetGenericTypeDefinition() == typeof(Nullable<>);
            Type type = Nullable.GetUnderlyingType(etype) ?? etype;

            if (val == null && type.IsValueType)
            {
                if (nullable)
                    return null;
                else
                    return Activator.CreateInstance(Type);
            }
            else if ((val == null && !type.IsValueType) || (val != null && type == val.GetType()))
                return val;
            else if (type.IsEnum && val.GetType().IsValueType)
                return Enum.ToObject(type, val);
            else if (type.IsEnum)
                return Enum.Parse(type, val.ToString());
            else if (Type == typeof(string) && val.GetType() == typeof(Guid))
                return val.ToString();
            else if (Type == typeof(Guid) && val.GetType() == typeof(string))
            {
                Guid g;
                return Guid.TryParse((string)val, out g) ? g : Guid.Empty;
            }
            else if (IsDataContract)
                return val.Map(type);
            else
                return Convert.ChangeType(val, type);
        }

        #region Type command cache

        internal ParameterSpec InsertCommandParameter { get; set; }

        internal ParameterSpec UpdateCommandParameter { get; set; }

        internal ParameterSpec DeleteCommandParameter { get; set; }

        #endregion Type command cache
    }
}