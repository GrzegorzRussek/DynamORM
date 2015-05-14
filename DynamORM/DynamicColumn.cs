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
using System.Text;

namespace DynamORM
{
    /// <summary>Small utility class to manage single columns.</summary>
    public class DynamicColumn
    {
        #region Enums

        /// <summary>Order By Order.</summary>
        public enum SortOrder
        {
            /// <summary>Ascending order.</summary>
            Asc,

            /// <summary>Descending order.</summary>
            Desc
        }

        /// <summary>Dynamic query operators.</summary>
        public enum CompareOperator
        {
            /// <summary>Equals operator (default).</summary>
            Eq,

            /// <summary>Not equal operator.</summary>
            Not,

            /// <summary>Like operator.</summary>
            Like,

            /// <summary>Not like operator.</summary>
            NotLike,

            /// <summary>In operator.</summary>
            In,

            /// <summary>Less than operator.</summary>
            Lt,

            /// <summary>Less or equal operator.</summary>
            Lte,

            /// <summary>Greater than operator.</summary>
            Gt,

            /// <summary>Greater or equal operator.</summary>
            Gte,

            /// <summary>Between two values.</summary>
            Between,
        }

        #endregion Enums

        #region Constructors

        /// <summary>Initializes a new instance of the <see cref="DynamicColumn" /> class.</summary>
        public DynamicColumn()
        {
        }

        /// <summary>Initializes a new instance of the <see cref="DynamicColumn" /> class.</summary>
        /// <remarks>Constructor provided for easier object creation in queries.</remarks>
        /// <param name="columnName">Name of column to set.</param>
        public DynamicColumn(string columnName)
            : this()
        {
            ColumnName = columnName;
        }

        /// <summary>Initializes a new instance of the <see cref="DynamicColumn" /> class.</summary>
        /// <remarks>Constructor provided for easier object creation in queries.</remarks>
        /// <param name="columnName">Name of column to set.</param>
        /// <param name="oper">Compare column to value(s) operator.</param>
        /// <param name="value">Parameter value(s).</param>
        public DynamicColumn(string columnName, CompareOperator oper, object value)
            : this(columnName)
        {
            Operator = oper;
            Value = value;
        }

        #endregion Constructors

        #region Properties

        /// <summary>Gets or sets column name.</summary>
        public string ColumnName { get; set; }

        /// <summary>Gets or sets column alias.</summary>
        /// <remarks>Select specific.</remarks>
        public string Alias { get; set; }

        /// <summary>Gets or sets aggregate function used on column.</summary>
        /// <remarks>Select specific.</remarks>
        public string Aggregate { get; set; }

        /// <summary>Gets or sets order direction.</summary>
        public SortOrder Order { get; set; }

        /// <summary>Gets or sets value for parameters.</summary>
        public object Value { get; set; }

        /// <summary>Gets or sets condition operator.</summary>
        public CompareOperator Operator { get; set; }

        /// <summary>Gets or sets a value indicating whether this condition will be treated as or condition.</summary>
        public bool Or { get; set; }

        /// <summary>Gets or sets a value indicating whether start new block in where statement.</summary>
        public bool BeginBlock { get; set; }

        /// <summary>Gets or sets a value indicating whether end existing block in where statement.</summary>
        public bool EndBlock { get; set; }

        /// <summary>Gets or sets a value indicating whether set parameters for null values.</summary>
        public bool? VirtualColumn { get; set; }

        /// <summary>Gets or sets schema representation of a column.</summary>
        /// <remarks>Workaround to providers issues which sometimes pass wrong
        /// data o schema. For example decimal has precision of 255 in SQL
        /// server.</remarks>
        public DynamicSchemaColumn? Schema { get; set; }

        #endregion Properties

        #region Query creation helpers

        #region Operators

        private DynamicColumn SetOperatorAndValue(CompareOperator c, object v)
        {
            Operator = c;
            Value = v;

            return this;
        }

        /// <summary>Helper method setting <see cref="DynamicColumn.Operator"/>
        /// to <see cref="CompareOperator.Eq"/> and
        /// <see cref="DynamicColumn.Value"/> to provided <c>value</c>.</summary>
        /// <param name="value">Value of parameter to set.</param>
        /// <returns>Returns self.</returns>
        public DynamicColumn Eq(object value)
        {
            return SetOperatorAndValue(CompareOperator.Eq, value);
        }

        /// <summary>Helper method setting <see cref="DynamicColumn.Operator"/>
        /// to <see cref="CompareOperator.Not"/> and
        /// <see cref="DynamicColumn.Value"/> to provided <c>value</c>.</summary>
        /// <param name="value">Value of parameter to set.</param>
        /// <returns>Returns self.</returns>
        public DynamicColumn Not(object value)
        {
            return SetOperatorAndValue(CompareOperator.Not, value);
        }

        /// <summary>Helper method setting <see cref="DynamicColumn.Operator"/>
        /// to <see cref="CompareOperator.Like"/> and
        /// <see cref="DynamicColumn.Value"/> to provided <c>value</c>.</summary>
        /// <param name="value">Value of parameter to set.</param>
        /// <returns>Returns self.</returns>
        public DynamicColumn Like(object value)
        {
            return SetOperatorAndValue(CompareOperator.Like, value);
        }

        /// <summary>Helper method setting <see cref="DynamicColumn.Operator"/>
        /// to <see cref="CompareOperator.NotLike"/> and
        /// <see cref="DynamicColumn.Value"/> to provided <c>value</c>.</summary>
        /// <param name="value">Value of parameter to set.</param>
        /// <returns>Returns self.</returns>
        public DynamicColumn NotLike(object value)
        {
            return SetOperatorAndValue(CompareOperator.NotLike, value);
        }

        /// <summary>Helper method setting <see cref="DynamicColumn.Operator"/>
        /// to <see cref="CompareOperator.Gt"/> and
        /// <see cref="DynamicColumn.Value"/> to provided <c>value</c>.</summary>
        /// <param name="value">Value of parameter to set.</param>
        /// <returns>Returns self.</returns>
        public DynamicColumn Greater(object value)
        {
            return SetOperatorAndValue(CompareOperator.Gt, value);
        }

        /// <summary>Helper method setting <see cref="DynamicColumn.Operator"/>
        /// to <see cref="CompareOperator.Lt"/> and
        /// <see cref="DynamicColumn.Value"/> to provided <c>value</c>.</summary>
        /// <param name="value">Value of parameter to set.</param>
        /// <returns>Returns self.</returns>
        public DynamicColumn Less(object value)
        {
            return SetOperatorAndValue(CompareOperator.Lt, value);
        }

        /// <summary>Helper method setting <see cref="DynamicColumn.Operator"/>
        /// to <see cref="CompareOperator.Gte"/> and
        /// <see cref="DynamicColumn.Value"/> to provided <c>value</c>.</summary>
        /// <param name="value">Value of parameter to set.</param>
        /// <returns>Returns self.</returns>
        public DynamicColumn GreaterOrEqual(object value)
        {
            return SetOperatorAndValue(CompareOperator.Gte, value);
        }

        /// <summary>Helper method setting <see cref="DynamicColumn.Operator"/>
        /// to <see cref="CompareOperator.Lte"/> and
        /// <see cref="DynamicColumn.Value"/> to provided <c>value</c>.</summary>
        /// <param name="value">Value of parameter to set.</param>
        /// <returns>Returns self.</returns>
        public DynamicColumn LessOrEqual(object value)
        {
            return SetOperatorAndValue(CompareOperator.Lte, value);
        }

        /// <summary>Helper method setting <see cref="DynamicColumn.Operator"/>
        /// to <see cref="CompareOperator.Between"/> and
        /// <see cref="DynamicColumn.Value"/> to provided values.</summary>
        /// <param name="from">Value of from parameter to set.</param>
        /// <param name="to">Value of to parameter to set.</param>
        /// <returns>Returns self.</returns>
        public DynamicColumn Between(object from, object to)
        {
            return SetOperatorAndValue(CompareOperator.Between, new[] { from, to });
        }

        /// <summary>Helper method setting <see cref="DynamicColumn.Operator"/>
        /// to <see cref="CompareOperator.In"/> and
        /// <see cref="DynamicColumn.Value"/> to provided values.</summary>
        /// <param name="values">Values of parameters to set.</param>
        /// <returns>Returns self.</returns>
        public DynamicColumn In(IEnumerable<object> values)
        {
            return SetOperatorAndValue(CompareOperator.In, values);
        }

        /// <summary>Helper method setting <see cref="DynamicColumn.Operator"/>
        /// to <see cref="CompareOperator.In"/> and
        /// <see cref="DynamicColumn.Value"/> to provided values.</summary>
        /// <param name="values">Values of parameters to set.</param>
        /// <returns>Returns self.</returns>
        public DynamicColumn In(params object[] values)
        {
            if (values.Length == 1 && (values[0].GetType().IsCollection() || values[0] is IEnumerable<object>))
                return SetOperatorAndValue(CompareOperator.In, values[0]);

            return SetOperatorAndValue(CompareOperator.In, values);
        }

        #endregion Operators

        #region Order

        /// <summary>Helper method setting <see cref="DynamicColumn.Order"/>
        /// to <see cref="SortOrder.Asc"/>..</summary>
        /// <returns>Returns self.</returns>
        public DynamicColumn Asc()
        {
            Order = SortOrder.Asc;
            return this;
        }

        /// <summary>Helper method setting <see cref="DynamicColumn.Order"/>
        /// to <see cref="SortOrder.Desc"/>..</summary>
        /// <returns>Returns self.</returns>
        public DynamicColumn Desc()
        {
            Order = SortOrder.Desc;
            return this;
        }

        #endregion Order

        #region Other

        /// <summary>Helper method setting
        /// <see cref="DynamicColumn.ColumnName"/>
        /// to provided <c>name</c>.</summary>
        /// <param name="name">Name to set.</param>
        /// <returns>Returns self.</returns>
        public DynamicColumn SetName(string name)
        {
            ColumnName = name;
            return this;
        }

        /// <summary>Helper method setting
        /// <see cref="DynamicColumn.Alias"/>
        /// to provided <c>alias</c>.</summary>
        /// <param name="alias">Alias to set.</param>
        /// <returns>Returns self.</returns>
        public DynamicColumn SetAlias(string alias)
        {
            Alias = alias;
            return this;
        }

        /// <summary>Helper method setting
        /// <see cref="DynamicColumn.Aggregate"/>
        /// to provided <c>aggregate</c>.</summary>
        /// <param name="aggregate">Aggregate to set.</param>
        /// <returns>Returns self.</returns>
        public DynamicColumn SetAggregate(string aggregate)
        {
            Aggregate = aggregate;
            return this;
        }

        /// <summary>Sets the begin block flag.</summary>
        /// <param name="begin">If set to <c>true</c> [begin].</param>
        /// <returns>Returns self.</returns>
        public DynamicColumn SetBeginBlock(bool begin = true)
        {
            BeginBlock = begin;
            return this;
        }

        /// <summary>Sets the end block flag.</summary>
        /// <param name="end">If set to <c>true</c> [end].</param>
        /// <returns>Returns self.</returns>
        public DynamicColumn SetEndBlock(bool end = true)
        {
            EndBlock = end;
            return this;
        }

        /// <summary>Sets the or flag.</summary>
        /// <param name="or">If set to <c>true</c> [or].</param>
        /// <returns>Returns self.</returns>
        public DynamicColumn SetOr(bool or = true)
        {
            Or = or;
            return this;
        }

        /// <summary>Sets the virtual column.</summary>
        /// <param name="virt">Set virtual column value.</param>
        /// <returns>Returns self.</returns>
        public DynamicColumn SetVirtualColumn(bool? virt)
        {
            VirtualColumn = virt;
            return this;
        }

        #endregion Other

        #endregion Query creation helpers

        #region Parsing

        /// <summary>Parse column for select query.</summary>
        /// <remarks>Column format consist of <c>Column Name</c>, <c>Alias</c> and
        /// <c>Aggregate function</c> in this order separated by '<c>:</c>'.</remarks>
        /// <param name="column">Column string.</param>
        /// <returns>Instance of <see cref="DynamicColumn"/>.</returns>
        public static DynamicColumn ParseSelectColumn(string column)
        {
            // Split column description
            string[] parts = column.Split(':');

            if (parts.Length > 0)
            {
                DynamicColumn ret = new DynamicColumn() { ColumnName = parts[0] };

                if (parts.Length > 1)
                    ret.Alias = parts[1];

                if (parts.Length > 2)
                    ret.Aggregate = parts[2];

                return ret;
            }

            return null;
        }

        /// <summary>Parse column for order by in query.</summary>
        /// <remarks>Column format consist of <c>Column Name</c> and
        /// <c>Direction</c> in this order separated by '<c>:</c>'.</remarks>
        /// <param name="column">Column string.</param>
        /// <returns>Instance of <see cref="DynamicColumn"/>.</returns>
        public static DynamicColumn ParseOrderByColumn(string column)
        {
            // Split column description
            string[] parts = column.Split(':');

            if (parts.Length > 0)
            {
                DynamicColumn ret = new DynamicColumn() { ColumnName = parts[0] };

                if (parts.Length > 1)
                    ret.Order = parts[1].ToLower() == "d" || parts[1].ToLower() == "desc" ? SortOrder.Desc : SortOrder.Asc;

                if (parts.Length > 2)
                    ret.Alias = parts[2];

                return ret;
            }

            return null;
        }

        #endregion Parsing

        #region ToSQL

        internal string ToSQLSelectColumn(DynamicDatabase db)
        {
            StringBuilder sb = new StringBuilder();
            ToSQLSelectColumn(db, sb);
            return sb.ToString();
        }

        internal void ToSQLSelectColumn(DynamicDatabase db, StringBuilder sb)
        {
            string column = ColumnName == "*" ? "*" : ColumnName;

            if (column != "*" &&
                (column.IndexOf(db.LeftDecorator) == -1 || column.IndexOf(db.RightDecorator) == -1) &&
                (column.IndexOf('(') == -1 || column.IndexOf(')') == -1))
                column = db.DecorateName(column);

            string alias = Alias;

            if (!string.IsNullOrEmpty(Aggregate))
            {
                sb.AppendFormat("{0}({1})", Aggregate, column);

                alias = string.IsNullOrEmpty(alias) ?
                    ColumnName == "*" ? Guid.NewGuid().ToString() : ColumnName :
                    alias;
            }
            else
                sb.Append(column);

            if (!string.IsNullOrEmpty(alias))
                sb.AppendFormat(" AS {0}", alias);
        }

        internal string ToSQLGroupByColumn(DynamicDatabase db)
        {
            StringBuilder sb = new StringBuilder();
            ToSQLGroupByColumn(db, sb);
            return sb.ToString();
        }

        internal void ToSQLGroupByColumn(DynamicDatabase db, StringBuilder sb)
        {
            sb.Append(db.DecorateName(ColumnName));
        }

        internal string ToSQLOrderByColumn(DynamicDatabase db)
        {
            StringBuilder sb = new StringBuilder();
            ToSQLOrderByColumn(db, sb);
            return sb.ToString();
        }

        internal void ToSQLOrderByColumn(DynamicDatabase db, StringBuilder sb)
        {
            if (!string.IsNullOrEmpty(Alias))
                sb.Append(Alias);
            else
                sb.Append(db.DecorateName(ColumnName));

            sb.AppendFormat(" {0}", Order.ToString().ToUpper());
        }

        #endregion ToSQL
    }
}