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
using DynamORM.Validation;

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

        /// <summary>Gets the array type of property if main type is a form of collection.</summary>
        public Type ArrayType { get; private set; }

        /// <summary>Gets a value indicating whether this property is in fact a generic list.</summary>
        public bool IsGnericEnumerable { get; private set; }

        /// <summary>Gets the type of property.</summary>
        public Type Type { get; private set; }

        /// <summary>Gets value getter.</summary>
        public Func<object, object> Get { get; private set; }

        /// <summary>Gets value setter.</summary>
        public Action<object, object> Setter { get; private set; }

        /// <summary>Gets the property information.</summary>
        public PropertyInfo PropertyInfo { get; private set; }

        /// <summary>Gets name of property.</summary>
        public string Name { get; private set; }

        /// <summary>Gets type column description.</summary>
        public ColumnAttribute Column { get; private set; }

        /// <summary>Gets type list of property requirements.</summary>
        public List<RequiredAttribute> Requirements { get; private set; }

        /// <summary>Gets a value indicating whether this <see cref="DynamicPropertyInvoker"/> is ignored in some cases.</summary>
        public bool Ignore { get; private set; }

        /// <summary>Gets a value indicating whether this instance hold data contract type.</summary>
        public bool IsDataContract { get; private set; }

        /// <summary>Initializes a new instance of the <see cref="DynamicPropertyInvoker" /> class.</summary>
        /// <param name="property">Property info to be invoked in the future.</param>
        /// <param name="attr">Column attribute if exist.</param>
        public DynamicPropertyInvoker(PropertyInfo property, ColumnAttribute attr)
        {
            PropertyInfo = property;
            Name = property.Name;
            Type = property.PropertyType;

            object[] ignore = property.GetCustomAttributes(typeof(IgnoreAttribute), false);
            Requirements = property.GetCustomAttributes(typeof(RequiredAttribute), false).Cast<RequiredAttribute>().ToList();

            Ignore = ignore != null && ignore.Length > 0;

            IsGnericEnumerable = Type.IsGenericEnumerable();

            ArrayType = Type.IsArray ? Type.GetElementType() :
                IsGnericEnumerable ? Type.GetGenericArguments().First() :
                Type;

            IsDataContract = ArrayType.GetCustomAttributes(false).Any(x => x.GetType().Name == "DataContractAttribute");

            if (ArrayType.IsArray)
                throw new InvalidOperationException("Jagged arrays are not supported");

            if (ArrayType.IsGenericEnumerable())
                throw new InvalidOperationException("Enumerables of enumerables are not supported");

            Column = attr;

            if (attr != null && attr.AllowNull && Type.IsNullableType())
                attr.AllowNull = false;

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
                if (!Type.IsAssignableFrom(val.GetType()))
                {
                    if (Type.IsArray || IsGnericEnumerable)
                    {
                        if (val != null)
                        {
                            if (val is IEnumerable<object>)
                            {
                                var lst = (val as IEnumerable<object>).Select(x => GetElementVal(ArrayType, x)).ToList();

                                value = Array.CreateInstance(ArrayType, lst.Count);

                                int i = 0;
                                foreach (var e in lst)
                                    ((Array)value).SetValue(e, i++);
                            }
                            else
                            {
                                value = Array.CreateInstance(ArrayType, 1);
                                ((Array)value).SetValue(GetElementVal(ArrayType, val), 0);
                            }
                        }
                        else
                            value = Array.CreateInstance(ArrayType, 0);
                    }
                    else
                        value = GetElementVal(Type, val);
                }
                else
                    value = val;

                Setter(dest, value);
            }
            catch (Exception ex)
            {
                throw new DynamicMapperException(
                    string.Format("Error trying to convert and set value '{0}' of type '{1}' to type '{2}' in object of type '{3}'",
                        val == null ? string.Empty : val.ToString(), val.GetType(), Type.FullName, dest.GetType().FullName),
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
                try
                {
                    return Enum.Parse(type, val.ToString());
                }
                catch (ArgumentException)
                {
                    if (nullable)
                        return null;

                    throw;
                }
            else if (Type == typeof(string) && val.GetType() == typeof(Guid))
                return val.ToString();
            else if (Type == typeof(Guid) && val.GetType() == typeof(string))
            {
                Guid g;
                return Guid.TryParse((string)val, out g) ? g : Guid.Empty;
            }
            else if (!typeof(IConvertible).IsAssignableFrom(type) && (IsDataContract || (!type.IsValueType && val is IDictionary<string, object>)))
                return val.Map(type);
            else
                try
                {
                    return Convert.ChangeType(val, type);
                }
                catch
                {
                    if (nullable)
                        return null;

                    throw;
                }
        }

        #region Type command cache

        internal ParameterSpec InsertCommandParameter { get; set; }

        internal ParameterSpec UpdateCommandParameter { get; set; }

        internal ParameterSpec DeleteCommandParameter { get; set; }

        #endregion Type command cache
    }
}