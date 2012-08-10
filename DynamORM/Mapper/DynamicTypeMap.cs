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
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

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
        public Dictionary<string, DynamicPropertyInvoker> ColumnsMap { get; private set; }

        /// <summary>Gets  map of properties to column.</summary>
        public Dictionary<string, string> PropertyMap { get; private set; }

        /// <summary>Gets list of ignored properties.</summary>
        public List<string> Ignored { get; private set; }

        /// <summary>Initializes a new instance of the <see cref="DynamicTypeMap" /> class.</summary>
        /// <param name="type">Type to which columnMap objects.</param>
        public DynamicTypeMap(Type type)
        {
            Type = type;

            var attr = type.GetCustomAttributes(typeof(TableAttribute), false);

            if (attr != null && attr.Length > 0)
                Table = (TableAttribute)attr[0];

            Creator = CreateCreator();
            CreateColumnAndPropertyMap();
        }

        private void CreateColumnAndPropertyMap()
        {
            var columnMap = new Dictionary<string, DynamicPropertyInvoker>();
            var propertyMap = new Dictionary<string, string>();

            foreach (var pi in Type.GetProperties())
            {
                ColumnAttribute attr = null;

                var attrs = pi.GetCustomAttributes(typeof(ColumnAttribute), true);

                if (attrs != null && attrs.Length > 0)
                    attr = (ColumnAttribute)attrs[0];

                string col = attr == null || string.IsNullOrEmpty(attr.Name) ? pi.Name : attr.Name;

                var val = new DynamicPropertyInvoker(pi, attr);
                columnMap.Add(col.ToLower(), val);

                propertyMap.Add(pi.Name, col);
            }

            ColumnsMap = columnMap;
            PropertyMap = propertyMap;

            Ignored = columnMap.Where(i => i.Value.Ignore).Select(i => i.Value.Name).ToList();
        }

        private Func<object> CreateCreator()
        {
            if (Type.GetConstructor(Type.EmptyTypes) != null)
                return Expression.Lambda<Func<object>>(Expression.New(Type)).Compile();

            return null;
        }

        /// <summary>Create object of <see cref="DynamicTypeMap.Type"/> type and fill values from <c>source</c>.</summary>
        /// <param name="source">Object containing values that will be mapped to newy created object.</param>
        /// <returns>New object of <see cref="DynamicTypeMap.Type"/> type with matching values from <c>source</c>.</returns>
        public object Create(object source)
        {
            return Map(source, Creator());
        }

        /// <summary>Fill values from <c>source</c> to <see cref="DynamicTypeMap.Type"/> object in <c>destination</c>.</summary>
        /// <param name="source">Object containing values that will be mapped to newy created object.</param>
        /// <param name="destination">Object of <see cref="DynamicTypeMap.Type"/> type to which copy values from <c>source</c>.</param>
        /// <returns>Object of <see cref="DynamicTypeMap.Type"/> type with matching values from <c>source</c>.</returns>
        public object Map(object source, object destination)
        {
            DynamicPropertyInvoker dpi = null;

            foreach (var item in source.ToDictionary())
            {
                if (ColumnsMap.TryGetValue(item.Key.ToLower(), out dpi) && item.Value != null)
                    if (dpi.Set != null)
                        dpi.Set(destination, item.Value);
            }

            return destination;
        }
    }
}