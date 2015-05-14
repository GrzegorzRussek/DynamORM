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
using System.Collections.Generic;
using DynamORM.Builders.Implementation;
using DynamORM.Helpers;
using DynamORM.Helpers.Dynamics;
using DynamORM.Mapper;

namespace DynamORM.Builders.Extensions
{
    internal static class DynamicWhereQueryExtensions
    {
        #region Where

        internal static T InternalWhere<T>(this T builder, Func<dynamic, object> func) where T : DynamicQueryBuilder, DynamicQueryBuilder.IQueryWithWhere
        {
            return builder.InternalWhere(false, false, func);
        }

        internal static T InternalWhere<T>(this T builder, bool addBeginBrace, bool addEndBrace, Func<dynamic, object> func) where T : DynamicQueryBuilder, DynamicQueryBuilder.IQueryWithWhere
        {
            if (func == null) throw new ArgumentNullException("Array of functions cannot be null.");

            using (DynamicParser parser = DynamicParser.Parse(func))
            {
                string condition = null;
                bool and = true;

                object result = parser.Result;
                if (result is string)
                {
                    condition = (string)result;

                    if (condition.ToUpper().IndexOf("OR") == 0)
                    {
                        and = false;
                        condition = condition.Substring(3);
                    }
                    else if (condition.ToUpper().IndexOf("AND") == 0)
                        condition = condition.Substring(4);
                }
                else if (!(result is DynamicParser.Node) && !result.GetType().IsValueType)
                    return builder.InternalWhere(result);
                else
                {
                    // Intercepting the 'x => x.And()' and 'x => x.Or()' virtual methods...
                    if (result is DynamicParser.Node.Method && ((DynamicParser.Node.Method)result).Host is DynamicParser.Node.Argument)
                    {
                        DynamicParser.Node.Method node = (DynamicParser.Node.Method)result;
                        string name = node.Name.ToUpper();
                        if (name == "AND" || name == "OR")
                        {
                            object[] args = ((DynamicParser.Node.Method)node).Arguments;
                            if (args == null) throw new ArgumentNullException("arg", string.Format("{0} is not a parameterless method.", name));
                            if (args.Length != 1) throw new ArgumentException(string.Format("{0} requires one and only one parameter: {1}.", name, args.Sketch()));

                            and = name == "AND" ? true : false;
                            result = args[0];
                        }
                    }

                    // Just parsing the contents now...
                    condition = builder.Parse(result, pars: builder.Parameters).Validated("Where condition");
                }

                if (addBeginBrace) builder.OpenBracketsCount++;
                if (addEndBrace) builder.OpenBracketsCount--;

                if (builder.WhereCondition == null)
                    builder.WhereCondition = string.Format("{0}{1}{2}",
                        addBeginBrace ? "(" : string.Empty, condition, addEndBrace ? ")" : string.Empty);
                else
                    builder.WhereCondition = string.Format("{0} {1} {2}{3}{4}", builder.WhereCondition, and ? "AND" : "OR",
                        addBeginBrace ? "(" : string.Empty, condition, addEndBrace ? ")" : string.Empty);
            }

            return builder;
        }

        internal static T InternalWhere<T>(this T builder, DynamicColumn column) where T : DynamicQueryBuilder, DynamicQueryBuilder.IQueryWithWhere
        {
            bool virt = builder.VirtualMode;
            if (column.VirtualColumn.HasValue)
                builder.VirtualMode = column.VirtualColumn.Value;

            Action<IParameter> modParam = (p) =>
            {
                if (column.Schema.HasValue)
                    p.Schema = column.Schema;

                if (!p.Schema.HasValue)
                    p.Schema = column.Schema ?? builder.GetColumnFromSchema(column.ColumnName);
            };

            builder.CreateTemporaryParameterAction(modParam);

            // It's kind of uglu, but... well it works.
            if (column.Or)
                switch (column.Operator)
                {
                    default:
                    case DynamicColumn.CompareOperator.Eq: builder.InternalWhere(column.BeginBlock, column.EndBlock, x => x.Or(x(builder.FixObjectName(column.ColumnName)) == column.Value)); break;
                    case DynamicColumn.CompareOperator.Not: builder.InternalWhere(column.BeginBlock, column.EndBlock, x => x.Or(x(builder.FixObjectName(column.ColumnName)) != column.Value)); break;
                    case DynamicColumn.CompareOperator.Like: builder.InternalWhere(column.BeginBlock, column.EndBlock, x => x.Or(x(builder.FixObjectName(column.ColumnName)).Like(column.Value))); break;
                    case DynamicColumn.CompareOperator.NotLike: builder.InternalWhere(column.BeginBlock, column.EndBlock, x => x.Or(x(builder.FixObjectName(column.ColumnName)).NotLike(column.Value))); break;
                    case DynamicColumn.CompareOperator.In: builder.InternalWhere(column.BeginBlock, column.EndBlock, x => x.Or(x(builder.FixObjectName(column.ColumnName)).In(column.Value))); break;
                    case DynamicColumn.CompareOperator.Lt: builder.InternalWhere(column.BeginBlock, column.EndBlock, x => x.Or(x(builder.FixObjectName(column.ColumnName)) < column.Value)); break;
                    case DynamicColumn.CompareOperator.Lte: builder.InternalWhere(column.BeginBlock, column.EndBlock, x => x.Or(x(builder.FixObjectName(column.ColumnName)) <= column.Value)); break;
                    case DynamicColumn.CompareOperator.Gt: builder.InternalWhere(column.BeginBlock, column.EndBlock, x => x.Or(x(builder.FixObjectName(column.ColumnName)) > column.Value)); break;
                    case DynamicColumn.CompareOperator.Gte: builder.InternalWhere(column.BeginBlock, column.EndBlock, x => x.Or(x(builder.FixObjectName(column.ColumnName)) >= column.Value)); break;
                    case DynamicColumn.CompareOperator.Between: builder.InternalWhere(column.BeginBlock, column.EndBlock, x => x.Or(x(builder.FixObjectName(column.ColumnName)).Between(column.Value))); break;
                }
            else
                switch (column.Operator)
                {
                    default:
                    case DynamicColumn.CompareOperator.Eq: builder.InternalWhere(column.BeginBlock, column.EndBlock, x => x(builder.FixObjectName(column.ColumnName)) == column.Value); break;
                    case DynamicColumn.CompareOperator.Not: builder.InternalWhere(column.BeginBlock, column.EndBlock, x => x(builder.FixObjectName(column.ColumnName)) != column.Value); break;
                    case DynamicColumn.CompareOperator.Like: builder.InternalWhere(column.BeginBlock, column.EndBlock, x => x(builder.FixObjectName(column.ColumnName)).Like(column.Value)); break;
                    case DynamicColumn.CompareOperator.NotLike: builder.InternalWhere(column.BeginBlock, column.EndBlock, x => x(builder.FixObjectName(column.ColumnName)).NotLike(column.Value)); break;
                    case DynamicColumn.CompareOperator.In: builder.InternalWhere(column.BeginBlock, column.EndBlock, x => x(builder.FixObjectName(column.ColumnName)).In(column.Value)); break;
                    case DynamicColumn.CompareOperator.Lt: builder.InternalWhere(column.BeginBlock, column.EndBlock, x => x(builder.FixObjectName(column.ColumnName)) < column.Value); break;
                    case DynamicColumn.CompareOperator.Lte: builder.InternalWhere(column.BeginBlock, column.EndBlock, x => x(builder.FixObjectName(column.ColumnName)) <= column.Value); break;
                    case DynamicColumn.CompareOperator.Gt: builder.InternalWhere(column.BeginBlock, column.EndBlock, x => x(builder.FixObjectName(column.ColumnName)) > column.Value); break;
                    case DynamicColumn.CompareOperator.Gte: builder.InternalWhere(column.BeginBlock, column.EndBlock, x => x(builder.FixObjectName(column.ColumnName)) >= column.Value); break;
                    case DynamicColumn.CompareOperator.Between: builder.InternalWhere(column.BeginBlock, column.EndBlock, x => x(builder.FixObjectName(column.ColumnName)).Between(column.Value)); break;
                }

            builder.OnCreateTemporaryParameter.Remove(modParam);
            builder.VirtualMode = virt;

            return builder;
        }

        internal static T InternalWhere<T>(this T builder, string column, DynamicColumn.CompareOperator op, object value) where T : DynamicQueryBuilder, DynamicQueryBuilder.IQueryWithWhere
        {
            if (value is DynamicColumn)
            {
                DynamicColumn v = (DynamicColumn)value;

                if (string.IsNullOrEmpty(v.ColumnName))
                    v.ColumnName = column;

                return builder.InternalWhere(v);
            }
            else if (value is IEnumerable<DynamicColumn>)
            {
                foreach (DynamicColumn v in (IEnumerable<DynamicColumn>)value)
                    builder.InternalWhere(v);

                return builder;
            }

            return builder.InternalWhere(new DynamicColumn
            {
                ColumnName = column,
                Operator = op,
                Value = value
            });
        }

        internal static T InternalWhere<T>(this T builder, string column, object value) where T : DynamicQueryBuilder, DynamicQueryBuilder.IQueryWithWhere
        {
            return builder.InternalWhere(column, DynamicColumn.CompareOperator.Eq, value);
        }

        internal static T InternalWhere<T>(this T builder, object conditions, bool schema = false) where T : DynamicQueryBuilder, DynamicQueryBuilder.IQueryWithWhere
        {
            if (conditions is DynamicColumn)
                return builder.InternalWhere((DynamicColumn)conditions);
            else if (conditions is IEnumerable<DynamicColumn>)
            {
                foreach (DynamicColumn v in (IEnumerable<DynamicColumn>)conditions)
                    builder.InternalWhere(v);

                return builder;
            }

            IDictionary<string, object> dict = conditions.ToDictionary();
            DynamicTypeMap mapper = DynamicMapperCache.GetMapper(conditions.GetType());
            string table = dict.TryGetValue("_table").NullOr(x => x.ToString(), string.Empty);

            foreach (KeyValuePair<string, object> condition in dict)
            {
                if (mapper.Ignored.Contains(condition.Key) || condition.Key == "_table")
                    continue;

                string colName = mapper != null ? mapper.PropertyMap.TryGetValue(condition.Key) ?? condition.Key : condition.Key;

                DynamicSchemaColumn? col = null;

                // This should be used on typed queries or update/delete steatements, which usualy operate on a single table.
                if (schema)
                {
                    col = builder.GetColumnFromSchema(colName, mapper, table);

                    if ((!col.HasValue || !col.Value.IsKey) &&
                        (mapper == null || mapper.ColumnsMap.TryGetValue(colName).NullOr(m => m.Ignore || m.Column.NullOr(c => !c.IsKey, true), true)))
                        continue;

                    colName = col.HasValue ? col.Value.Name : colName;
                }

                if (!string.IsNullOrEmpty(table))
                    builder.InternalWhere(x => x(builder.FixObjectName(string.Format("{0}.{1}", table, colName))) == condition.Value);
                else
                    builder.InternalWhere(x => x(builder.FixObjectName(colName)) == condition.Value);
            }

            return builder;
        }

        #endregion Where
    }
}