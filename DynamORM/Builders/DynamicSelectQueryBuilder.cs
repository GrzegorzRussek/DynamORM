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

namespace DynamORM.Builders
{
    /// <summary>Select query builder.</summary>
    public class DynamicSelectQueryBuilder : DynamicQueryBuilder<DynamicSelectQueryBuilder>
    {
        /// <summary>Gets dictionary of columns that will be selected.</summary>
        public List<DynamicColumn> Columns { get; private set; }

        /// <summary>Gets dictionary of columns that will be used to group query.</summary>
        public List<DynamicColumn> Group { get; private set; }

        /// <summary>Gets dictionary of columns that will be used to order query.</summary>
        public List<DynamicColumn> Order { get; private set; }

        private int? _top = null;
        private int? _limit = null;
        private int? _offset = null;
        private bool _distinct = false;

        /// <summary>Initializes a new instance of the <see cref="DynamicSelectQueryBuilder" /> class.</summary>
        /// <param name="table">Parent dynamic table.</param>
        public DynamicSelectQueryBuilder(DynamicTable table)
            : base(table)
        {
            Columns = new List<DynamicColumn>();
            Group = new List<DynamicColumn>();
            Order = new List<DynamicColumn>();
        }

        /// <summary>Add select columns.</summary>
        /// <param name="columns">Columns to add to object.</param>
        /// <returns>Builder instance.</returns>
        public DynamicSelectQueryBuilder Select(params DynamicColumn[] columns)
        {
            foreach (var col in columns)
                Columns.Add(col);

            return this;
        }

        /// <summary>Add select columns.</summary>
        /// <param name="columns">Columns to add to object.</param>
        /// <remarks>Column format consist of <c>Column Name</c>, <c>Alias</c> and
        /// <c>Aggregate function</c> in this order separated by '<c>:</c>'.</remarks>
        /// <returns>Builder instance.</returns>
        public DynamicSelectQueryBuilder Select(params string[] columns)
        {
            return Select(columns.Select(c => DynamicColumn.ParseSelectColumn(c)).ToArray());
        }

        /// <summary>Add select columns.</summary>
        /// <param name="columns">Columns to group by.</param>
        /// <returns>Builder instance.</returns>
        public DynamicSelectQueryBuilder GroupBy(params DynamicColumn[] columns)
        {
            foreach (var col in columns)
                Group.Add(col);

            return this;
        }

        /// <summary>Add select columns.</summary>
        /// <param name="columns">Columns to group by.</param>
        /// <remarks>Column format consist of <c>Column Name</c> and
        /// <c>Alias</c> in this order separated by '<c>:</c>'.</remarks>
        /// <returns>Builder instance.</returns>
        public DynamicSelectQueryBuilder GroupBy(params string[] columns)
        {
            return GroupBy(columns.Select(c => DynamicColumn.ParseSelectColumn(c)).ToArray());
        }

        /// <summary>Add select columns.</summary>
        /// <param name="columns">Columns to order by.</param>
        /// <returns>Builder instance.</returns>
        public DynamicSelectQueryBuilder OrderBy(params DynamicColumn[] columns)
        {
            foreach (var col in columns)
                Order.Add(col);

            return this;
        }

        /// <summary>Add select columns.</summary>
        /// <param name="columns">Columns to order by.</param>
        /// <remarks>Column format consist of <c>Column Name</c> and
        /// <c>Alias</c> in this order separated by '<c>:</c>'.</remarks>
        /// <returns>Builder instance.</returns>
        public DynamicSelectQueryBuilder OrderBy(params string[] columns)
        {
            return OrderBy(columns.Select(c => DynamicColumn.ParseOrderByColumn(c)).ToArray());
        }

        /// <summary>Set top if database support it.</summary>
        /// <param name="top">How many objects select.</param>
        /// <returns>Builder instance.</returns>
        public DynamicSelectQueryBuilder Top(int? top)
        {
            if ((DynamicTable.Database.Options & DynamicDatabaseOptions.SupportTop) != DynamicDatabaseOptions.SupportTop)
                throw new NotSupportedException("Database doesn't support TOP clause.");

            _top = top;
            return this;
        }

        /// <summary>Set top if database support it.</summary>
        /// <param name="limit">How many objects select.</param>
        /// <returns>Builder instance.</returns>
        public DynamicSelectQueryBuilder Limit(int? limit)
        {
            if ((DynamicTable.Database.Options & DynamicDatabaseOptions.SupportLimitOffset) != DynamicDatabaseOptions.SupportLimitOffset)
                throw new NotSupportedException("Database doesn't support LIMIT clause.");

            _limit = limit;
            return this;
        }

        /// <summary>Set top if database support it.</summary>
        /// <param name="offset">How many objects skip selecting.</param>
        /// <returns>Builder instance.</returns>
        public DynamicSelectQueryBuilder Offset(int? offset)
        {
            if ((DynamicTable.Database.Options & DynamicDatabaseOptions.SupportLimitOffset) != DynamicDatabaseOptions.SupportLimitOffset)
                throw new NotSupportedException("Database doesn't support OFFSET clause.");

            _offset = offset;
            return this;
        }

        /// <summary>Set distinct mode.</summary>
        /// <param name="distinct">Distinct mode.</param>
        /// <returns>Builder instance.</returns>
        public DynamicSelectQueryBuilder Distinct(bool distinct = true)
        {
            _distinct = distinct;
            return this;
        }

        /// <summary>Fill command with query.</summary>
        /// <param name="command">Command to fill.</param>
        /// <returns>Filled instance of <see cref="IDbCommand"/>.</returns>
        public override IDbCommand FillCommand(IDbCommand command)
        {
            StringBuilder sb = new StringBuilder();

            var db = DynamicTable.Database;

            sb.AppendFormat("SELECT{0}{1} ",
                _distinct ? " DISTINCT" : string.Empty,
                _top.HasValue ? string.Format(" TOP {0}", _top.Value) : string.Empty);

            BuildColumns(sb, db);

            sb.Append(" FROM ");
            db.DecorateName(sb, TableName);

            FillWhere(command, sb);

            BuildGroup(sb, db);
            BuildOrder(sb, db);

            if (_limit.HasValue)
                sb.AppendFormat(" LIMIT {0}", _limit.Value);

            if (_offset.HasValue)
                sb.AppendFormat(" OFFSET {0}", _offset.Value);

            return command.SetCommand(sb.ToString());
        }

        private void BuildColumns(StringBuilder sb, DynamicDatabase db)
        {
            if (Columns.Count > 0)
            {
                bool first = true;

                // Not pretty but blazing fast
                Columns.ForEach(c =>
                {
                    if (!first)
                        sb.Append(", ");

                    c.ToSQLSelectColumn(db, sb);
                    first = false;
                });
            }
            else
                sb.Append("*");
        }

        private void BuildGroup(StringBuilder sb, DynamicDatabase db)
        {
            if (Group.Count > 0)
            {
                sb.Append(" GROUP BY ");
                bool first = true;

                // Not pretty but blazing fast
                Group.ForEach(c =>
                {
                    if (!first)
                        sb.Append(", ");

                    c.ToSQLGroupByColumn(db, sb);
                    first = false;
                });
            }
        }

        private void BuildOrder(StringBuilder sb, DynamicDatabase db)
        {
            if (Order.Count > 0)
            {
                sb.Append(" ORDER BY ");
                bool first = true;

                // Not pretty but blazing fast
                Order.ForEach(c =>
                {
                    if (!first)
                        sb.Append(", ");

                    c.ToSQLOrderByColumn(db, sb);
                    first = false;
                });
            }
        }

        /// <summary>Execute this builder.</summary>
        /// <returns>Enumerator of objects expanded from query.</returns>
        public override dynamic Execute()
        {
            if (Columns.Count <= 1 && ((_top.HasValue && _top.Value == 1) || (_limit.HasValue && _limit.Value == 1)))
                return DynamicTable.Scalar(this);
            else
                return DynamicTable.Query(this);
        }
    }
}