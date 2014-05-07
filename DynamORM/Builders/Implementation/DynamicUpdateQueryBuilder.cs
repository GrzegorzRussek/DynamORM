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
using DynamORM.Builders.Extensions;
using DynamORM.Helpers.Dynamics;
using DynamORM.Mapper;

namespace DynamORM.Builders.Implementation
{
    /// <summary>Update query builder.</summary>
    internal class DynamicUpdateQueryBuilder : DynamicModifyBuilder, IDynamicUpdateQueryBuilder, DynamicQueryBuilder.IQueryWithWhere
    {
        private string _columns;

        internal DynamicUpdateQueryBuilder(DynamicDatabase db)
            : base(db)
        {
        }

        public DynamicUpdateQueryBuilder(DynamicDatabase db, string tableName)
            : base(db, tableName)
        {
        }

        /// <summary>Generates the text this command will execute against the underlying database.</summary>
        /// <returns>The text to execute against the underlying database.</returns>
        /// <remarks>This method must be override by derived classes.</remarks>
        public override string CommandText()
        {
            var info = Tables.Single();
            return string.Format("UPDATE {0}{1} SET {2}{3}{4}",
                string.IsNullOrEmpty(info.Owner) ? string.Empty : string.Format("{0}.", Database.DecorateName(info.Owner)),
                Database.DecorateName(info.Name), _columns,
                string.IsNullOrEmpty(WhereCondition) ? string.Empty : " WHERE ",
                WhereCondition);
        }

        #region Update

        /// <summary>Add update value or where condition using schema.</summary>
        /// <param name="column">Update or where column name.</param>
        /// <param name="value">Column value.</param>
        /// <returns>Builder instance.</returns>
        public virtual IDynamicUpdateQueryBuilder Update(string column, object value)
        {
            DynamicSchemaColumn? col = GetColumnFromSchema(column);

            if (!col.HasValue && SupportSchema)
                throw new InvalidOperationException(string.Format("Column '{0}' not found in schema, can't use universal approach.", column));

            if (col.HasValue && col.Value.IsKey)
                Where(column, value);
            else
                Values(column, value);

            return this;
        }

        /// <summary>Add update values and where condition columns using schema.</summary>
        /// <param name="conditions">Set values or conditions as properties and values of an object.</param>
        /// <returns>Builder instance.</returns>
        public virtual IDynamicUpdateQueryBuilder Update(object conditions)
        {
            if (conditions is DynamicColumn)
            {
                var column = (DynamicColumn)conditions;

                DynamicSchemaColumn? col = column.Schema ?? GetColumnFromSchema(column.ColumnName);

                if (!col.HasValue && SupportSchema)
                    throw new InvalidOperationException(string.Format("Column '{0}' not found in schema, can't use universal approach.", column));

                if (col.HasValue && col.Value.IsKey)
                    Where(column);
                else
                    Values(column.ColumnName, column.Value);

                return this;
            }

            var dict = conditions.ToDictionary();
            var mapper = DynamicMapperCache.GetMapper(conditions.GetType());

            foreach (var con in dict)
            {
                if (mapper.Ignored.Contains(con.Key))
                    continue;

                string colName = mapper != null ? mapper.PropertyMap.TryGetValue(con.Key) ?? con.Key : con.Key;
                DynamicSchemaColumn? col = GetColumnFromSchema(colName);

                if (!col.HasValue && SupportSchema)
                    throw new InvalidOperationException(string.Format("Column '{0}' not found in schema, can't use universal approach.", colName));

                if (col.HasValue)
                {
                    colName = col.Value.Name;

                    if (col.Value.IsKey)
                    {
                        Where(colName, con.Value);

                        continue;
                    }
                }

                var propMap = mapper.ColumnsMap.TryGetValue(colName.ToLower());
                if (propMap == null || propMap.Column == null || !propMap.Column.IsNoUpdate)
                    Values(colName, con.Value);
            }

            return this;
        }

        #endregion Update

        #region Values

        /// <summary>
        /// Specifies the columns to update using the dynamic lambda expressions given. Each expression correspond to one
        /// column, and can:
        /// <para>- Resolve to a string, in this case a '=' must appear in the string.</para>
        /// <para>- Resolve to a expression with the form: 'x =&gt; x.Column = Value'.</para>
        /// </summary>
        /// <param name="func">The specifications.</param>
        /// <returns>This instance to permit chaining.</returns>
        public virtual IDynamicUpdateQueryBuilder Set(params Func<dynamic, object>[] func)
        {
            if (func == null)
                throw new ArgumentNullException("Array of specifications cannot be null.");

            int index = -1;
            foreach (var f in func)
            {
                index++;
                if (f == null)
                    throw new ArgumentNullException(string.Format("Specification #{0} cannot be null.", index));
                var result = DynamicParser.Parse(f).Result;

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

                    str = string.Format("{0} = {1}", main, value);
                    _columns = _columns == null ? str : string.Format("{0}, {1}", _columns, str);
                    continue;
                }
                else if (!(result is DynamicParser.Node) && !result.GetType().IsValueType)
                {
                    Values(result);
                    continue;
                }

                // Other specifications are considered invalid...
                var err = string.Format("Specification '{0}' is invalid.", result);
                str = Parse(result);
                if (str.Contains("=")) err += " May have you used a '==' instead of a '=' operator?";
                throw new ArgumentException(err);
            }

            return this;
        }

        /// <summary>Add insert fields.</summary>
        /// <param name="column">Insert column.</param>
        /// <param name="value">Insert value.</param>
        /// <returns>Builder instance.</returns>
        public virtual IDynamicUpdateQueryBuilder Values(string column, object value)
        {
            if (value is DynamicColumn)
            {
                var v = (DynamicColumn)value;

                if (string.IsNullOrEmpty(v.ColumnName))
                    v.ColumnName = column;

                return Values(v);
            }

            return Values(new DynamicColumn
            {
                ColumnName = column,
                Value = value,
            });
        }

        /// <summary>Add insert fields.</summary>
        /// <param name="o">Set insert value as properties and values of an object.</param>
        /// <returns>Builder instance.</returns>
        public virtual IDynamicUpdateQueryBuilder Values(object o)
        {
            if (o is DynamicColumn)
            {
                var column = (DynamicColumn)o;
                DynamicSchemaColumn? col = column.Schema ?? GetColumnFromSchema(column.ColumnName);

                string main = FixObjectName(column.ColumnName, onlyColumn: true);
                string value = Parse(column.Value, pars: Parameters, nulls: true, columnSchema: col);

                var str = string.Format("{0} = {1}", main, value);
                _columns = _columns == null ? str : string.Format("{0}, {1}", _columns, str);

                return this;
            }

            var dict = o.ToDictionary();
            var mapper = DynamicMapperCache.GetMapper(o.GetType());

            if (mapper != null)
            {
                foreach (var con in dict)
                    if (!mapper.Ignored.Contains(con.Key))
                        Values(mapper.PropertyMap.TryGetValue(con.Key) ?? con.Key, con.Value);
            }
            else
                foreach (var con in dict)
                    Values(con.Key, con.Value);

            return this;
        }

        #endregion Values

        #region Where

        /// <summary>
        /// Adds to the 'Where' clause the contents obtained from parsing the dynamic lambda expression given. The condition
        /// is parsed to the appropriate syntax, where the specific customs virtual methods supported by the parser are used
        /// as needed.
        /// <para>- If several Where() methods are chained their contents are, by default, concatenated with an 'AND' operator.</para>
        /// <para>- The 'And()' and 'Or()' virtual method can be used to concatenate with an 'OR' or an 'AND' operator, as in:
        /// 'Where( x => x.Or( condition ) )'.</para>
        /// </summary>
        /// <param name="func">The specification.</param>
        /// <returns>This instance to permit chaining.</returns>
        public virtual IDynamicUpdateQueryBuilder Where(Func<dynamic, object> func)
        {
            return this.InternalWhere(func);
        }

        /// <summary>Add where condition.</summary>
        /// <param name="column">Condition column with operator and value.</param>
        /// <returns>Builder instance.</returns>
        public virtual IDynamicUpdateQueryBuilder Where(DynamicColumn column)
        {
            return this.InternalWhere(column);
        }

        /// <summary>Add where condition.</summary>
        /// <param name="column">Condition column.</param>
        /// <param name="op">Condition operator.</param>
        /// <param name="value">Condition value.</param>
        /// <returns>Builder instance.</returns>
        public virtual IDynamicUpdateQueryBuilder Where(string column, DynamicColumn.CompareOperator op, object value)
        {
            return this.InternalWhere(column, op, value);
        }

        /// <summary>Add where condition.</summary>
        /// <param name="column">Condition column.</param>
        /// <param name="value">Condition value.</param>
        /// <returns>Builder instance.</returns>
        public virtual IDynamicUpdateQueryBuilder Where(string column, object value)
        {
            return this.InternalWhere(column, value);
        }

        /// <summary>Add where condition.</summary>
        /// <param name="conditions">Set conditions as properties and values of an object.</param>
        /// <param name="schema">If <c>true</c> use schema to determine key columns and ignore those which
        /// aren't keys.</param>
        /// <returns>Builder instance.</returns>
        public virtual IDynamicUpdateQueryBuilder Where(object conditions, bool schema = false)
        {
            return this.InternalWhere(conditions, schema);
        }

        #endregion Where
    }
}