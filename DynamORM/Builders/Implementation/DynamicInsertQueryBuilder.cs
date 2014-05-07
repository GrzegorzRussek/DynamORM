/*
 * DynamORM - Dynamic Object-Relational Mapping library.
 * Copyright (c) 2012, Grzegorz Russek (grzegorz.russek@gmail.com)
 * All rights reserved.
 *
 * Some of methods in this code file is based on Kerosene ORM solution
 * for parsing dynamic lambda expressions by Moisés Barba Cebeira
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
using System.Linq;
using DynamORM.Helpers.Dynamics;
using DynamORM.Mapper;

namespace DynamORM.Builders.Implementation
{
    /// <summary>Implementation of dynamic insert query builder.</summary>
    internal class DynamicInsertQueryBuilder : DynamicModifyBuilder, IDynamicInsertQueryBuilder
    {
        private string _columns;
        private string _values;

        /// <summary>
        /// Initializes a new instance of the <see cref="DynamicInsertQueryBuilder"/> class.
        /// </summary>
        /// <param name="db">The database.</param>
        internal DynamicInsertQueryBuilder(DynamicDatabase db)
            : base(db)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="DynamicInsertQueryBuilder"/> class.
        /// </summary>
        /// <param name="db">The database.</param>
        /// <param name="tableName">Name of the table.</param>
        public DynamicInsertQueryBuilder(DynamicDatabase db, string tableName)
            : base(db, tableName)
        {
        }

        /// <summary>Generates the text this command will execute against the underlying database.</summary>
        /// <returns>The text to execute against the underlying database.</returns>
        /// <remarks>This method must be override by derived classes.</remarks>
        public override string CommandText()
        {
            var info = Tables.Single();
            return string.Format("INSERT INTO {0}{1} ({2}) VALUES ({3})",
                string.IsNullOrEmpty(info.Owner) ? string.Empty : string.Format("{0}.", Database.DecorateName(info.Owner)),
                Database.DecorateName(info.Name), _columns, _values);
        }

        #region Insert

        /// <summary>
        /// Specifies the columns to insert using the dynamic lambda expressions given. Each expression correspond to one
        /// column, and can:
        /// <para>- Resolve to a string, in this case a '=' must appear in the string.</para>
        /// <para>- Resolve to a expression with the form: 'x => x.Column = Value'.</para>
        /// </summary>
        /// <param name="fn">The specifications.</param>
        /// <param name="func">The specifications.</param>
        /// <returns>This instance to permit chaining.</returns>
        public virtual IDynamicInsertQueryBuilder Values(Func<dynamic, object> fn, params Func<dynamic, object>[] func)
        {
            if (fn == null)
                throw new ArgumentNullException("Array of specifications cannot be null.");

            int index = InsertFunc(-1, fn);

            if (func != null)
                foreach (var f in func)
                    index = InsertFunc(index, f);

            return this;
        }

        private int InsertFunc(int index, Func<dynamic, object> f)
        {
            index++;

            if (f == null)
                throw new ArgumentNullException(string.Format("Specification #{0} cannot be null.", index));

            using (var parser = DynamicParser.Parse(f))
            {
                var result = parser.Result;
                if (result == null)
                    throw new ArgumentException(string.Format("Specification #{0} resolves to null.", index));

                string main = null;
                string value = null;
                string str = null;

                // When 'x => x.Table.Column = value' or 'x => x.Column = value'...
                if (result is DynamicParser.Node.SetMember)
                {
                    var node = (DynamicParser.Node.SetMember)result;

                    DynamicSchemaColumn? col = GetColumnFromSchema(node.Name);
                    main = Database.DecorateName(node.Name);
                    value = Parse(node.Value, pars: Parameters, nulls: true, columnSchema: col);

                    _columns = _columns == null ? main : string.Format("{0}, {1}", _columns, main);
                    _values = _values == null ? value : string.Format("{0}, {1}", _values, value);
                    return index;
                }
                else if (!(result is DynamicParser.Node) && !result.GetType().IsValueType)
                {
                    Insert(result);
                    return index;
                }

                // Other specifications are considered invalid...
                var err = string.Format("Specification '{0}' is invalid.", result);
                str = Parse(result);
                if (str.Contains("=")) err += " May have you used a '==' instead of a '=' operator?";
                throw new ArgumentException(err);
            }
        }

        /// <summary>Add insert fields.</summary>
        /// <param name="column">Insert column.</param>
        /// <param name="value">Insert value.</param>
        /// <returns>Builder instance.</returns>
        public virtual IDynamicInsertQueryBuilder Insert(string column, object value)
        {
            if (value is DynamicColumn)
            {
                var v = (DynamicColumn)value;

                if (string.IsNullOrEmpty(v.ColumnName))
                    v.ColumnName = column;

                return Insert(v);
            }

            return Insert(new DynamicColumn
            {
                ColumnName = column,
                Value = value,
            });
        }

        /// <summary>Add insert fields.</summary>
        /// <param name="o">Set insert value as properties and values of an object.</param>
        /// <returns>Builder instance.</returns>
        public virtual IDynamicInsertQueryBuilder Insert(object o)
        {
            if (o is DynamicColumn)
            {
                var column = (DynamicColumn)o;
                DynamicSchemaColumn? col = column.Schema ?? GetColumnFromSchema(column.ColumnName);

                string main = FixObjectName(column.ColumnName, onlyColumn: true);
                string value = Parse(column.Value, pars: Parameters, nulls: true, columnSchema: col);

                _columns = _columns == null ? main : string.Format("{0}, {1}", _columns, main);
                _values = _values == null ? value : string.Format("{0}, {1}", _values, value);

                return this;
            }

            var dict = o.ToDictionary();
            var mapper = DynamicMapperCache.GetMapper(o.GetType());

            if (mapper != null)
            {
                foreach (var con in dict)
                    if (!mapper.Ignored.Contains(con.Key))
                    {
                        var colName = mapper.PropertyMap.TryGetValue(con.Key) ?? con.Key;
                        var propMap = mapper.ColumnsMap.TryGetValue(colName.ToLower());

                        if (propMap == null || propMap.Column == null || !propMap.Column.IsNoInsert)
                            Insert(colName, con.Value);
                    }
            }
            else
                foreach (var con in dict)
                    Insert(con.Key, con.Value);

            return this;
        }

        #endregion Insert
    }
}