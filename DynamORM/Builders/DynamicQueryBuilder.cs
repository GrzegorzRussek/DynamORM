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
using System.Linq;
using System.Text;
using DynamORM.Mapper;

namespace DynamORM.Builders
{
    /// <summary>Base query builder.</summary>
    /// <typeparam name="T">Return type of methods that should return self.</typeparam>
    public abstract class DynamicQueryBuilder<T> : IDynamicQueryBuilder where T : class
    {
        /// <summary>Gets <see cref="DynamicTable"/> instance.</summary>
        public DynamicTable DynamicTable { get; private set; }

        /// <summary>Gets where conditions.</summary>
        public List<DynamicColumn> WhereConditions { get; private set; }

        /// <summary>Gets table schema.</summary>
        public Dictionary<string, DynamicSchemaColumn> Schema { get; private set; }

        /// <summary>Gets a value indicating whether database supports standard schema.</summary>
        public bool SupportSchema { get; private set; }

        /// <summary>Gets or sets a value indicating whether set parameters for null values.</summary>
        public bool VirtualMode { get; set; }

        /// <summary>Gets table name.</summary>
        public string TableName { get; private set; }

        /// <summary>Initializes a new instance of the DynamicQueryBuilder class.</summary>
        /// <param name="table">Parent dynamic table.</param>
        public DynamicQueryBuilder(DynamicTable table)
        {
            DynamicTable = table;
            TableName = table.TableName;
            VirtualMode = false;

            WhereConditions = new List<DynamicColumn>();

            SupportSchema = (DynamicTable.Database.Options & DynamicDatabaseOptions.SupportSchema) == DynamicDatabaseOptions.SupportSchema;

            Schema = DynamicTable.Schema;
        }

        /// <summary>Set table name.</summary>
        /// <param name="name">Name of table.</param>
        /// <returns>Builder instance.</returns>
        public virtual T Table(string name)
        {
            if (TableName.ToLower() != name.ToLower())
            {
                TableName = name;

                Schema = DynamicTable.Database.GetSchema(TableName);

                if (Schema == null)
                    throw new InvalidOperationException("Can't assign type as a table for which schema can't be build.");
            }

            return this as T;
        }

        /// <summary>Set table name.</summary>
        /// <typeparam name="Y">Type representing table.</typeparam>
        /// <returns>Builder instance.</returns>
        public virtual T Table<Y>()
        {
            return Table(typeof(T));
        }

        /// <summary>Set table name.</summary>
        /// <param name="type">Type representing table.</param>
        /// <returns>Builder instance.</returns>
        public virtual T Table(Type type)
        {
            var mapper = DynamicMapperCache.GetMapper(type);
            string name = string.Empty;

            if (mapper == null)
                throw new InvalidOperationException("Cant assign unmapable type as a table.");
            else
                name = mapper.Table == null || string.IsNullOrEmpty(mapper.Table.Name) ?
                    mapper.Type.Name : mapper.Table.Name;

            if (TableName.ToLower() != name.ToLower())
            {
                TableName = name;

                Schema = DynamicTable.Database.GetSchema(type);
            }

            return this as T;
        }

        /// <summary>Add where condition.</summary>
        /// <param name="column">Condition column with operator and value.</param>
        /// <returns>Builder instance.</returns>
        public virtual T Where(DynamicColumn column)
        {
            WhereConditions.Add(column);

            return this as T;
        }

        /// <summary>Add where condition.</summary>
        /// <param name="column">Condition column.</param>
        /// <param name="op">Condition operator.</param>
        /// <param name="value">Condition value.</param>
        /// <returns>Builder instance.</returns>
        public virtual T Where(string column, DynamicColumn.CompareOperator op, object value)
        {
            if (value is DynamicColumn)
            {
                var v = (DynamicColumn)value;

                if (string.IsNullOrEmpty(v.ColumnName))
                    v.ColumnName = column;

                return Where(v);
            }
            else if (value is IEnumerable<DynamicColumn>)
            {
                foreach (var v in (IEnumerable<DynamicColumn>)value)
                    Where(v);

                return this as T;
            }

            WhereConditions.Add(new DynamicColumn
            {
                ColumnName = column,
                Operator = op,
                Value = value
            });

            return this as T;
        }

        /// <summary>Add where condition.</summary>
        /// <param name="column">Condition column.</param>
        /// <param name="value">Condition value.</param>
        /// <returns>Builder instance.</returns>
        public virtual T Where(string column, object value)
        {
            return Where(column, DynamicColumn.CompareOperator.Eq, value);
        }

        /// <summary>Add where condition.</summary>
        /// <param name="conditions">Set conditions as properties and values of an object.</param>
        /// <param name="schema">If <c>true</c> use schema to determine key columns and ignore those which
        /// aren't keys.</param>
        /// <returns>Builder instance.</returns>
        public virtual T Where(object conditions, bool schema = false)
        {
            if (conditions is DynamicColumn)
                return Where((DynamicColumn)conditions);

            var dict = conditions.ToDictionary();
            var mapper = DynamicMapperCache.GetMapper(conditions.GetType());

            foreach (var con in dict)
            {
                if (mapper.Ignored.Contains(con.Key))
                    continue;

                string colName = mapper != null ? mapper.PropertyMap.TryGetValue(con.Key) ?? con.Key : con.Key;

                DynamicSchemaColumn? col = null;

                if (schema)
                {
                    col = Schema.TryGetNullable(colName.ToLower());

                    if (!col.HasValue)
                        throw new InvalidOperationException(string.Format("Column '{0}' not found in schema, can't use universal approach.", con.Key));

                    if (!col.Value.IsKey)
                        continue;

                    colName = col.Value.Name;
                }

                Where(colName, con.Value);
            }

            return this as T;
        }

        /// <summary>Get string representation of operator.</summary>
        /// <param name="op">Operator object.</param>
        /// <returns>String representation of operator.</returns>
        public string ToOperator(DynamicColumn.CompareOperator op)
        {
            switch (op)
            {
                case DynamicColumn.CompareOperator.Eq: return "=";
                case DynamicColumn.CompareOperator.Not: return "<>";
                case DynamicColumn.CompareOperator.Like: return "LIKE";
                case DynamicColumn.CompareOperator.NotLike: return "NOT LIKE";
                case DynamicColumn.CompareOperator.Lt: return "<";
                case DynamicColumn.CompareOperator.Lte: return "<=";
                case DynamicColumn.CompareOperator.Gt: return ">";
                case DynamicColumn.CompareOperator.Gte: return ">=";
                case DynamicColumn.CompareOperator.Between:
                case DynamicColumn.CompareOperator.In:
                default:
                    throw new ArgumentException(string.Format("This operator ('{0}') requires more than conversion to string.", op));
            }
        }

        /// <summary>Fill where part of a query.</summary>
        /// <param name="command">Command to fill.</param>
        /// <param name="sb">String builder.</param>
        public virtual void FillWhere(IDbCommand command, StringBuilder sb)
        {
            // Yes, this method qualifies fo refactoring... but it's fast
            bool first = true;
            var db = DynamicTable.Database;

            foreach (var v in WhereConditions)
            {
                var col = Schema.TryGetNullable(v.ColumnName.ToLower());

                string column = col.HasValue ? col.Value.Name : v.ColumnName;

                if ((column.IndexOf(db.LeftDecorator) == -1 || column.IndexOf(db.LeftDecorator) == -1) &&
                    (column.IndexOf('(') == -1 || column.IndexOf(')') == -1))
                    column = db.DecorateName(column);

                if ((v.Value == null || v.Value == DBNull.Value) && !VirtualMode && !v.VirtualColumn)
                {
                    #region Null operators

                    if (v.Operator == DynamicColumn.CompareOperator.Not || v.Operator == DynamicColumn.CompareOperator.Eq)
                        sb.AppendFormat(" {0} {1}{2} IS{3} NULL{4}",
                            first ? "WHERE" : v.Or ? "OR" : "AND",
                            v.BeginBlock ? "(" : string.Empty,
                            column,
                            v.Operator == DynamicColumn.CompareOperator.Not ? " NOT" : string.Empty,
                            v.EndBlock ? ")" : string.Empty);
                    else
                        throw new InvalidOperationException("NULL can only be compared by IS or IS NOT operator.");

                    #endregion
                }
                else if (v.Operator != DynamicColumn.CompareOperator.In &&
                    v.Operator != DynamicColumn.CompareOperator.Between)
                {
                    #region Standard operators

                    int pos = command.Parameters.Count;

                    sb.AppendFormat(" {0} {1}{2} {3} ",
                        first ? "WHERE" : v.Or ? "OR" : "AND",
                        v.BeginBlock ? "(" : string.Empty,
                        column,
                        ToOperator(v.Operator));

                    db.GetParameterName(sb, pos);

                    if (v.EndBlock)
                        sb.Append(")");

                    command.AddParameter(this, v);

                    #endregion
                }
                else if (((object)v.Value).GetType().IsCollection() || v.Value is IEnumerable<object>)
                {
                    #region In or Between operator

                    if (v.Operator == DynamicColumn.CompareOperator.Between)
                    {
                        #region Between operator

                        var vals = (v.Value as IEnumerable<object>).Take(2).ToList();

                        if (vals == null && v.Value is Array)
                            vals = ((Array)v.Value).Cast<object>().ToList();

                        if (vals.Count == 2)
                        {
                            sb.AppendFormat(" {0} {1}{2} BETWEEN ",
                                first ? "WHERE" : v.Or ? "OR" : "AND",
                                v.BeginBlock ? "(" : string.Empty,
                                column);

                            // From parameter
                            db.GetParameterName(sb, command.Parameters.Count);
                            v.Value = vals[0];
                            command.AddParameter(this, v);

                            sb.Append(" AND ");

                            // To parameter
                            db.GetParameterName(sb, command.Parameters.Count);
                            v.Value = vals[1];
                            command.AddParameter(this, v);

                            if (v.EndBlock)
                                sb.Append(")");

                            // Reset value
                            v.Value = vals;
                        }
                        else
                            throw new InvalidOperationException("BETWEEN must have two values.");

                        #endregion
                    }
                    else if (v.Operator == DynamicColumn.CompareOperator.In)
                    {
                        #region In operator

                        sb.AppendFormat(" {0} {1}{2} IN(",
                            first ? "WHERE" : v.Or ? "OR" : "AND",
                            v.BeginBlock ? "(" : string.Empty,
                            column);

                        bool firstParam = true;

                        var vals = v.Value as IEnumerable<object>;

                        if (vals == null && v.Value is Array)
                            vals = ((Array)v.Value).Cast<object>() as IEnumerable<object>;

                        foreach (var val in vals)
                        {
                            int pos = command.Parameters.Count;

                            if (!firstParam)
                                sb.Append(", ");

                            db.GetParameterName(sb, pos);
                            v.Value = val;

                            command.AddParameter(this, v);

                            firstParam = false;
                        }

                        v.Value = vals;

                        sb.Append(")");

                        if (v.EndBlock)
                            sb.Append(")");

                        #endregion
                    }
                    else
                        throw new Exception("BAZINGA. You have reached unreachable code.");

                    #endregion
                }
                else
                    throw new InvalidOperationException(
                        string.Format("Operator was {0}, but value wasn't enumerable. Column: '{1}'", v.Operator.ToString().ToUpper(), col));

                first = false;
            }
        }

        /// <summary>Fill command with query.</summary>
        /// <param name="command">Command to fill.</param>
        /// <returns>Filled instance of <see cref="IDbCommand"/>.</returns>
        public abstract IDbCommand FillCommand(IDbCommand command);

        /// <summary>Execute this builder.</summary>
        /// <returns>Result of an execution..</returns>
        public abstract dynamic Execute();
    }
}