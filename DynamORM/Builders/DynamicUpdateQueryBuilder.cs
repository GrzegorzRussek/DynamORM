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
using System.Data;
using System.Text;
using DynamORM.Mapper;

namespace DynamORM.Builders
{
    /// <summary>Update query builder.</summary>
    public class DynamicUpdateQueryBuilder : DynamicQueryBuilder<DynamicUpdateQueryBuilder>
    {
        /// <summary>Gets list of columns that will be selected.</summary>
        public IDictionary<string, DynamicColumn> ValueColumns { get; private set; }

        /// <summary>Initializes a new instance of the <see cref="DynamicUpdateQueryBuilder" /> class.</summary>
        /// <param name="table">Parent dynamic table.</param>
        public DynamicUpdateQueryBuilder(DynamicTable table)
            : base(table)
        {
            ValueColumns = new Dictionary<string, DynamicColumn>();
        }

        /// <summary>Add update value or where condition using schema.</summary>
        /// <param name="column">Update or where column name and value.</param>
        /// <returns>Builder instance.</returns>
        public virtual DynamicUpdateQueryBuilder Update(DynamicColumn column)
        {
            var col = Schema.TryGetNullable(column.ColumnName.ToLower());

            if (!col.HasValue && SupportSchema)
                throw new InvalidOperationException(string.Format("Column '{0}' not found in schema, can't use universal approach.", column));

            if (col.HasValue && col.Value.IsKey)
                Where(column);
            else
                Values(column.ColumnName, column.Value);

            return this;
        }

        /// <summary>Add update value or where condition using schema.</summary>
        /// <param name="column">Update or where column name.</param>
        /// <param name="value">Column value.</param>
        /// <returns>Builder instance.</returns>
        public virtual DynamicUpdateQueryBuilder Update(string column, object value)
        {
            var col = Schema.TryGetNullable(column.ToLower());

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
        public virtual DynamicUpdateQueryBuilder Update(object conditions)
        {
            if (conditions is DynamicColumn)
                return Update((DynamicColumn)conditions);

            var dict = conditions.ToDictionary();
            var mapper = DynamicMapperCache.GetMapper(conditions.GetType());

            foreach (var con in dict)
            {
                if (mapper.Ignored.Contains(con.Key))
                    continue;

                string colName = mapper != null ? mapper.PropertyMap.TryGetValue(con.Key) ?? con.Key : con.Key;
                var col = Schema.TryGetNullable(colName.ToLower());

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

                Values(colName, con.Value);
            }

            return this;
        }

        /// <summary>Add update fields.</summary>
        /// <param name="column">Update column and value.</param>
        /// <returns>Builder instance.</returns>
        public virtual DynamicUpdateQueryBuilder Values(DynamicColumn column)
        {
            if (ValueColumns.ContainsKey(column.ColumnName.ToLower()))
                ValueColumns[column.ColumnName.ToLower()] = column;
            else
                ValueColumns.Add(column.ColumnName.ToLower(), column);

            return this;
        }

        /// <summary>Add update fields.</summary>
        /// <param name="column">Update column.</param>
        /// <param name="value">Update value.</param>
        /// <returns>Builder instance.</returns>
        public virtual DynamicUpdateQueryBuilder Values(string column, object value)
        {
            if (value is DynamicColumn)
            {
                var v = (DynamicColumn)value;

                if (string.IsNullOrEmpty(v.ColumnName))
                    v.ColumnName = column;

                return Values(v);
            }

            if (ValueColumns.ContainsKey(column.ToLower()))
                ValueColumns[column.ToLower()].Value = value;
            else
                ValueColumns.Add(column.ToLower(), new DynamicColumn
                {
                    ColumnName = column,
                    Value = value
                });

            return this;
        }

        /// <summary>Add update fields.</summary>
        /// <param name="values">Set update value as properties and values of an object.</param>
        /// <returns>Builder instance.</returns>
        public virtual DynamicUpdateQueryBuilder Values(object values)
        {
            if (values is DynamicColumn)
                return Values((DynamicColumn)values);

            var dict = values.ToDictionary();
            var mapper = DynamicMapperCache.GetMapper(values.GetType());

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

        /// <summary>Fill command with query.</summary>
        /// <param name="command">Command to fill.</param>
        /// <returns>Filled instance of <see cref="IDbCommand"/>.</returns>
        public override IDbCommand FillCommand(IDbCommand command)
        {
            if (ValueColumns.Count == 0)
                throw new InvalidOperationException("Update query should contain columns to change.");

            StringBuilder sb = new StringBuilder();
            var db = DynamicTable.Database;

            sb.Append("UPDATE ");
            db.DecorateName(sb, TableName);
            sb.Append(" SET ");

            bool first = true;

            foreach (var v in ValueColumns)
            {
                int pos = command.Parameters.Count;

                if (!first)
                    sb.Append(", ");

                db.DecorateName(sb, v.Value.ColumnName);
                sb.Append(" = ");
                db.GetParameterName(sb, pos);

                command.AddParameter(this, v.Value);

                first = false;
            }

            FillWhere(command, sb);

            return command.SetCommand(sb.ToString());
        }

        /// <summary>Execute this builder.</summary>
        /// <returns>Number of affected rows.</returns>
        public override dynamic Execute()
        {
            return DynamicTable.Execute(this);
        }
    }
}