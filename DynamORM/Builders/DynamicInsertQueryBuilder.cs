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
    /// <summary>Insert query builder.</summary>
    public class DynamicInsertQueryBuilder : DynamicQueryBuilder<DynamicInsertQueryBuilder>
    {
        /// <summary>Gets list of columns that will be seected.</summary>
        public IDictionary<string, DynamicColumn> ValueColumns { get; private set; }

        /// <summary>Initializes a new instance of the <see cref="DynamicInsertQueryBuilder"/> class.</summary>
        /// <param name="table">Parent dynamic table.</param>
        public DynamicInsertQueryBuilder(DynamicTable table)
            : base(table)
        {
            ValueColumns = new Dictionary<string, DynamicColumn>();
        }

        /// <summary>Add insert fields.</summary>
        /// <param name="column">Insert column and value.</param>
        /// <returns>Builder instance.</returns>
        public virtual DynamicInsertQueryBuilder Insert(DynamicColumn column)
        {
            if (ValueColumns.ContainsKey(column.ColumnName.ToLower()))
                ValueColumns[column.ColumnName.ToLower()] = column;
            else
                ValueColumns.Add(column.ColumnName.ToLower(), column);

            return this;
        }

        /// <summary>Add insert fields.</summary>
        /// <param name="column">Insert column.</param>
        /// <param name="value">Insert value.</param>
        /// <returns>Builder instance.</returns>
        public virtual DynamicInsertQueryBuilder Insert(string column, object value)
        {
            if (value is DynamicColumn)
            {
                var v = (DynamicColumn)value;

                if (string.IsNullOrEmpty(v.ColumnName))
                    v.ColumnName = column;

                return Insert(v);
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

        /// <summary>Add insert fields.</summary>
        /// <param name="o">Set insert value as properties and values of an object.</param>
        /// <returns>Builder instance.</returns>
        public virtual DynamicInsertQueryBuilder Insert(object o)
        {
            var dict = o.ToDictionary();
            var mapper = DynamicMapperCache.GetMapper(o.GetType());

            if (mapper != null)
            {
                foreach (var con in dict)
                    if (!mapper.Ignored.Contains(con.Key))
                        Insert(mapper.PropertyMap.TryGetValue(con.Key) ?? con.Key, con.Value);
            }
            else
                foreach (var con in dict)
                    Insert(con.Key, con.Value);

            return this;
        }

        /// <summary>Add where condition.</summary>
        /// <param name="column">Condition column with operator and value.</param>
        /// <returns>Builder instance.</returns>
        public override DynamicInsertQueryBuilder Where(DynamicColumn column)
        {
            throw new NotSupportedException();
        }

        /// <summary>Add where condition.</summary>
        /// <param name="column">Condition column.</param>
        /// <param name="op">Condition operator.</param>
        /// <param name="value">Condition value.</param>
        /// <returns>Builder instance.</returns>
        public override DynamicInsertQueryBuilder Where(string column, DynamicColumn.CompareOperator op, object value)
        {
            throw new NotSupportedException();
        }

        /// <summary>Add where condition.</summary>
        /// <param name="column">Condition column.</param>
        /// <param name="value">Condition value.</param>
        /// <returns>Builder instance.</returns>
        public override DynamicInsertQueryBuilder Where(string column, object value)
        {
            throw new NotSupportedException();
        }

        /// <summary>Add where condition.</summary>
        /// <param name="conditions">Set conditions as properties and values of an object.</param>
        /// <param name="schema">If <c>true</c> use schema to determine key columns and ignore those which
        /// aren't keys.</param>
        /// <returns>Builder instance.</returns>
        public override DynamicInsertQueryBuilder Where(object conditions, bool schema = false)
        {
            throw new NotSupportedException();
        }

        /// <summary>Fill command with query.</summary>
        /// <param name="command">Command to fill.</param>
        /// <returns>Filled instance of <see cref="IDbCommand"/>.</returns>
        public override IDbCommand FillCommand(IDbCommand command)
        {
            if (ValueColumns.Count == 0)
                throw new InvalidOperationException("Insert query should contain columns to change.");

            StringBuilder builderColumns = new StringBuilder();
            StringBuilder builderValues = new StringBuilder();

            bool first = true;
            var db = DynamicTable.Database;

            foreach (var v in ValueColumns)
            {
                int pos = command.Parameters.Count;

                if (!first)
                {
                    builderColumns.Append(", ");
                    builderValues.Append(", ");
                }

                db.DecorateName(builderColumns, v.Value.ColumnName);
                db.GetParameterName(builderValues, pos);

                command.AddParameter(this, v.Value);

                first = false;
            }

            return command.SetCommand("INSERT INTO {0} ({1}) VALUES ({2})", TableName, builderColumns, builderValues);
        }

        /// <summary>Execute this builder.</summary>
        /// <returns>Number of affected rows.</returns>
        public override dynamic Execute()
        {
            return DynamicTable.Execute(this);
        }
    }
}