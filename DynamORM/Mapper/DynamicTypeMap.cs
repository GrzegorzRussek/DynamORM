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
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using DynamORM.Validation;

namespace DynamORM.Mapper
{
    /// <summary>Represents type columnMap.</summary>
    public class DynamicTypeMap
    {
        /// <summary>Gets mapper destination type creator.</summary>
        public Type Type { get; private set; }

        /// <summary>Gets type table description.</summary>
        public TableAttribute Table { get; private set; }

        /// <summary>Gets object creator.</summary>
        public Func<object> Creator { get; private set; }

        /// <summary>Gets map of columns to properties.</summary>
        /// <remarks>Key: Column name (lower), Value: <see cref="DynamicPropertyInvoker"/>.</remarks>
        public Dictionary<string, DynamicPropertyInvoker> ColumnsMap { get; private set; }

        /// <summary>Gets map of properties to column.</summary>
        /// <remarks>Key: Property name, Value: Column name.</remarks>
        public Dictionary<string, string> PropertyMap { get; private set; }

        /// <summary>Gets list of ignored properties.</summary>
        public List<string> Ignored { get; private set; }

        /// <summary>Initializes a new instance of the <see cref="DynamicTypeMap" /> class.</summary>
        /// <param name="type">Type to which columnMap objects.</param>
        public DynamicTypeMap(Type type)
        {
            Type = type;

            object[] attr = type.GetCustomAttributes(typeof(TableAttribute), false);

            if (attr != null && attr.Length > 0)
                Table = (TableAttribute)attr[0];

            Creator = CreateCreator();
            CreateColumnAndPropertyMap();
        }

        private void CreateColumnAndPropertyMap()
        {
            Dictionary<string, DynamicPropertyInvoker> columnMap = new Dictionary<string, DynamicPropertyInvoker>();
            Dictionary<string, string> propertyMap = new Dictionary<string, string>();
            List<string> ignored = new List<string>();

            foreach (PropertyInfo pi in GetAllMembers(Type).Where(x => x is PropertyInfo).Cast<PropertyInfo>())
            {
                ColumnAttribute attr = null;

                object[] attrs = pi.GetCustomAttributes(typeof(ColumnAttribute), true);

                if (attrs != null && attrs.Length > 0)
                    attr = (ColumnAttribute)attrs[0];

                string col = attr == null || string.IsNullOrEmpty(attr.Name) ? pi.Name : attr.Name;

                DynamicPropertyInvoker val = new DynamicPropertyInvoker(pi, attr);
                columnMap.Add(col.ToLower(), val);

                propertyMap.Add(pi.Name, col);

                if (val.Ignore)
                    ignored.Add(pi.Name);
            }

            ColumnsMap = columnMap;
            PropertyMap = propertyMap;

            Ignored = ignored; ////columnMap.Where(i => i.Value.Ignore).Select(i => i.Value.Name).ToList();
        }

        private Func<object> CreateCreator()
        {
            var c = Type.GetConstructor(Type.EmptyTypes);
            if (c == null)
                c = Type.GetConstructor(BindingFlags.Instance | BindingFlags.NonPublic, null, Type.EmptyTypes, null);

            if (c != null)
                return Expression.Lambda<Func<object>>(Expression.New(Type)).Compile();

            return null;
        }

        /// <summary>Create object of <see cref="DynamicTypeMap.Type"/> type and fill values from <c>source</c>.</summary>
        /// <param name="source">Object containing values that will be mapped to newly created object.</param>
        /// <returns>New object of <see cref="DynamicTypeMap.Type"/> type with matching values from <c>source</c>.</returns>
        public object Create(object source)
        {
            return Map(source, Creator());
        }


        /// <summary>Create object of <see cref="DynamicTypeMap.Type"/> type and fill values from <c>source</c> using property names.</summary>
        /// <param name="source">Object containing values that will be mapped to newly created object.</param>
        /// <returns>New object of <see cref="DynamicTypeMap.Type"/> type with matching values from <c>source</c>.</returns>
        public object CreateByProperty(object source)
        {
            return MapByProperty(source, Creator());
        }

        /// <summary>Fill values from <c>source</c> to <see cref="DynamicTypeMap.Type"/> object in <c>destination</c>.</summary>
        /// <param name="source">Object containing values that will be mapped to newly created object.</param>
        /// <param name="destination">Object of <see cref="DynamicTypeMap.Type"/> type to which copy values from <c>source</c>.</param>
        /// <returns>Object of <see cref="DynamicTypeMap.Type"/> type with matching values from <c>source</c>.</returns>
        public object Map(object source, object destination)
        {
            DynamicPropertyInvoker dpi = null;

            foreach (KeyValuePair<string, object> item in source.ToDictionary())
            {
                if (ColumnsMap.TryGetValue(item.Key.ToLower(), out dpi) && item.Value != null)
                    if (dpi.Setter != null)
                        dpi.Set(destination, item.Value);
            }

            return destination;
        }

        /// <summary>Fill values from <c>source</c> to <see cref="DynamicTypeMap.Type"/> object in <c>destination</c> using property names.</summary>
        /// <param name="source">Object containing values that will be mapped to newly created object.</param>
        /// <param name="destination">Object of <see cref="DynamicTypeMap.Type"/> type to which copy values from <c>source</c>.</param>
        /// <returns>Object of <see cref="DynamicTypeMap.Type"/> type with matching values from <c>source</c>.</returns>
        public object MapByProperty(object source, object destination)
        {
            string cn = null;
            DynamicPropertyInvoker dpi = null;

            foreach (KeyValuePair<string, object> item in source.ToDictionary())
            {
                if (PropertyMap.TryGetValue(item.Key, out cn) && item.Value != null)
                    if (ColumnsMap.TryGetValue(cn.ToLower(), out dpi) && item.Value != null)
                        if (dpi.Setter != null)
                            dpi.Set(destination, item.Value);
            }

            return destination;
        }

        /// <summary>Validates the object.</summary>
        /// <param name="val">The value.</param>
        /// <returns>List of not valid results.</returns>
        public IList<ValidationResult> ValidateObject(object val)
        {
            var result = new List<ValidationResult>();

            if (val == null || val.GetType() != Type)
                return null;

            foreach (var prop in ColumnsMap.Values)
            {
                if (prop.Requirements == null || !prop.Requirements.Any())
                    continue;

                var v = prop.Get(val);

                foreach (var r in prop.Requirements.Where(x => !x.ElementRequirement))
                {
                    var valid = r.ValidateSimpleValue(prop, v);

                    if (valid == ValidateResult.Valid)
                    {
                        if (prop.Type.IsArray || prop.IsGnericEnumerable)
                        {
                            var map = DynamicMapperCache.GetMapper(prop.ArrayType);

                            var list = v as IEnumerable<object>;

                            if (list == null)
                            {
                                var enumerable = v as IEnumerable;
                                if (enumerable != null)
                                    list = enumerable.Cast<object>();
                            }

                            if (list != null)
                                foreach (var item in list)
                                {
                                    if (prop.Requirements.Any(x => x.ElementRequirement))
                                    {
                                        foreach (var re in prop.Requirements.Where(x => x.ElementRequirement))
                                        {
                                            var validelem = re.ValidateSimpleValue(prop.ArrayType, prop.ArrayType.IsGenericEnumerable(), item);

                                            if (validelem == ValidateResult.NotSupported)
                                            {
                                                result.AddRange(map.ValidateObject(item));
                                                break;
                                            }
                                            else if (validelem != ValidateResult.Valid)
                                                result.Add(new ValidationResult()
                                                {
                                                    Property = prop,
                                                    Requirement = r,
                                                    Value = item,
                                                    Result = validelem,
                                                });
                                        }
                                    }
                                    else
                                        result.AddRange(map.ValidateObject(item));
                                }
                        }

                        continue;
                    }

                    if (valid == ValidateResult.NotSupported)
                    {
                        result.AddRange(DynamicMapperCache.GetMapper(prop.Type).ValidateObject(v));
                        continue;
                    }

                    result.Add(new ValidationResult()
                    {
                        Property = prop,
                        Requirement = r,
                        Value = v,
                        Result = valid,
                    });
                }
            }

            return result;
        }

        private IEnumerable<MemberInfo> GetAllMembers(Type type)
        {
            if (type.IsInterface)
            {
                List<MemberInfo> members = new List<MemberInfo>();
                List<Type> considered = new List<Type>();
                Queue<Type> queue = new Queue<Type>();

                considered.Add(type);
                queue.Enqueue(type);

                while (queue.Count > 0)
                {
                    Type subType = queue.Dequeue();
                    foreach (Type subInterface in subType.GetInterfaces())
                    {
                        if (considered.Contains(subInterface)) continue;

                        considered.Add(subInterface);
                        queue.Enqueue(subInterface);
                    }

                    MemberInfo[] typeProperties = subType.GetMembers(
                        BindingFlags.FlattenHierarchy
                        | BindingFlags.Public
                        | BindingFlags.Instance);

                    IEnumerable<MemberInfo> newPropertyInfos = typeProperties
                        .Where(x => !members.Contains(x));

                    members.InsertRange(0, newPropertyInfos);
                }

                return members;
            }

            return type.GetMembers(BindingFlags.FlattenHierarchy | BindingFlags.Public | BindingFlags.Instance);
        }

        #region Type command cache

        internal string InsertCommandText { get; set; }

        internal string UpdateCommandText { get; set; }

        internal string DeleteCommandText { get; set; }

        #endregion Type command cache
    }
}