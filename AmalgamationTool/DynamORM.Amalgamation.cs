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
 *
 * See: http://opensource.org/licenses/bsd-license.php
 *
 * Supported preprocessor flags:
 *  * DYNAMORM_OMMIT_OLDSYNTAX - Remove dynamic table functionality
 *  * DYNAMORM_OMMIT_GENERICEXECUTION - Remove generic execution functionality
 *  * DYNAMORM_OMMIT_TRYPARSE - Remove TryParse helpers (also applies DYNAMORM_OMMIT_GENERICEXECUTION)
*/

using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Data;
using System.Data.Common;
using System.Dynamic;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.Serialization;
using System.Text;
using DynamORM.Builders;
using DynamORM.Builders.Extensions;
using DynamORM.Builders.Implementation;
using DynamORM.Helpers;
using DynamORM.Helpers.Dynamics;
using DynamORM.Mapper;

[module: System.Diagnostics.CodeAnalysis.SuppressMessage("StyleCop.CSharp.MaintainabilityRules", "SA1402:FileMayOnlyContainASingleClass", Justification = "This is a generated file which generates all the necessary support classes.")]
[module: System.Diagnostics.CodeAnalysis.SuppressMessage("StyleCop.CSharp.MaintainabilityRules", "SA1403:FileMayOnlyContainASingleNamespace", Justification = "This is a generated file which generates all the necessary support classes.")]

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
            var parts = column.Split(':');

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
            var parts = column.Split(':');

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

    /// <summary>Helper class to easy manage command.</summary>
    public class DynamicCommand : IDbCommand, IExtendedDisposable
    {
        private IDbCommand _command;
        private int? _commandTimeout = null;
        private DynamicConnection _con;
        private DynamicDatabase _db;
        ////private long _poolStamp = 0;

        /// <summary>Initializes a new instance of the <see cref="DynamicCommand"/> class.</summary>
        /// <param name="con">The connection.</param>
        /// <param name="db">The database manager.</param>
        internal DynamicCommand(DynamicConnection con, DynamicDatabase db)
        {
            IsDisposed = false;
            _con = con;
            _db = db;

            lock (_db.SyncLock)
            {
                if (!_db.CommandsPool.ContainsKey(_con.Connection))
                    throw new InvalidOperationException("Can't create command using disposed connection.");
                else
                {
                    _command = _con.Connection.CreateCommand();
                    _db.CommandsPool[_con.Connection].Add(this);
                }
            }
        }

        /// <summary>Initializes a new instance of the <see cref="DynamicCommand"/> class.</summary>
        /// <param name="db">The database manager.</param>
        /// <remarks>Used internally to create command without context.</remarks>
        internal DynamicCommand(DynamicDatabase db)
        {
            IsDisposed = false;
            _db = db;
            _command = db.Provider.CreateCommand();
        }

        /// <summary>Prepare command for execution.</summary>
        /// <returns>Returns edited <see cref="System.Data.IDbCommand"/> instance.</returns>
        internal IDbCommand PrepareForExecution()
        {
            // TODO: Fix that
            ////if (_poolStamp < _db.PoolStamp)
            {
                _command.CommandTimeout = _commandTimeout ?? _db.CommandTimeout ?? _command.CommandTimeout;

                if (_db.TransactionPool[_command.Connection].Count > 0)
                    _command.Transaction = _db.TransactionPool[_command.Connection].Peek();
                else
                    _command.Transaction = null;

                ////_poolStamp = _db.PoolStamp;
            }

            if (_db.DumpCommands)
                _db.DumpCommand(_command);

            return _command;
        }

        #region IDbCommand Members

        /// <summary>
        /// Attempts to cancels the execution of an <see cref="T:System.Data.IDbCommand"/>.
        /// </summary>
        public void Cancel()
        {
            _command.Cancel();
        }

        /// <summary>
        /// Gets or sets the text command to run against the data source.
        /// </summary>
        /// <returns>The text command to execute. The default value is an empty string ("").</returns>
        public string CommandText { get { return _command.CommandText; } set { _command.CommandText = value; } }

        /// <summary>
        /// Gets or sets the wait time before terminating the attempt to execute a command and generating an error.
        /// </summary>
        /// <returns>The time (in seconds) to wait for the command to execute. The default value is 30 seconds.</returns>
        /// <exception cref="T:System.ArgumentException">The property value assigned is less than 0. </exception>
        public int CommandTimeout { get { return _commandTimeout ?? _command.CommandTimeout; } set { _commandTimeout = value; } }

        /// <summary>Gets or sets how the <see cref="P:System.Data.IDbCommand.CommandText"/> property is interpreted.</summary>
        public CommandType CommandType { get { return _command.CommandType; } set { _command.CommandType = value; } }

        /// <summary>Gets or sets the <see cref="T:System.Data.IDbConnection"/>
        /// used by this instance of the <see cref="T:System.Data.IDbCommand"/>.</summary>
        /// <returns>The connection to the data source.</returns>
        public IDbConnection Connection
        {
            get { return _con; }

            set
            {
                _con = value as DynamicConnection;

                if (_con != null)
                {
                    ////_poolStamp = 0;
                    _command.Connection = _con.Connection;
                }
                else if (value == null)
                {
                    _command.Transaction = null;
                    _command.Connection = null;
                }
                else
                    throw new InvalidOperationException("Can't assign direct IDbConnection implementation. This property accepts only DynamORM implementation of IDbConnection.");
            }
        }

        /// <summary>Creates a new instance of an
        /// <see cref="T:System.Data.IDbDataParameter"/> object.</summary>
        /// <returns>An <see cref="IDbDataParameter"/> object.</returns>
        public IDbDataParameter CreateParameter()
        {
            return _command.CreateParameter();
        }

        /// <summary>Executes an SQL statement against the Connection object of a
        /// data provider, and returns the number of rows affected.</summary>
        /// <returns>The number of rows affected.</returns>
        public int ExecuteNonQuery()
        {
            try
            {
                return PrepareForExecution().ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                throw new DynamicQueryException(ex, this);
            }
        }

        /// <summary>Executes the <see cref="P:System.Data.IDbCommand.CommandText"/>
        /// against the <see cref="P:System.Data.IDbCommand.Connection"/>,
        /// and builds an <see cref="T:System.Data.IDataReader"/> using one
        /// of the <see cref="T:System.Data.CommandBehavior"/> values.
        /// </summary><param name="behavior">One of the
        /// <see cref="T:System.Data.CommandBehavior"/> values.</param>
        /// <returns>An <see cref="T:System.Data.IDataReader"/> object.</returns>
        public IDataReader ExecuteReader(CommandBehavior behavior)
        {
            try
            {
                return PrepareForExecution().ExecuteReader(behavior);
            }
            catch (Exception ex)
            {
                throw new DynamicQueryException(ex, this);
            }
        }

        /// <summary>Executes the <see cref="P:System.Data.IDbCommand.CommandText"/>
        /// against the <see cref="P:System.Data.IDbCommand.Connection"/> and
        /// builds an <see cref="T:System.Data.IDataReader"/>.</summary>
        /// <returns>An <see cref="T:System.Data.IDataReader"/> object.</returns>
        public IDataReader ExecuteReader()
        {
            try
            {
                return PrepareForExecution().ExecuteReader();
            }
            catch (Exception ex)
            {
                throw new DynamicQueryException(ex, this);
            }
        }

        /// <summary>Executes the query, and returns the first column of the
        /// first row in the result set returned by the query. Extra columns or
        /// rows are ignored.</summary>
        /// <returns>The first column of the first row in the result set.</returns>
        public object ExecuteScalar()
        {
            try
            {
                return PrepareForExecution().ExecuteScalar();
            }
            catch (Exception ex)
            {
                throw new DynamicQueryException(ex, this);
            }
        }

        /// <summary>Gets the <see cref="T:System.Data.IDataParameterCollection"/>.</summary>
        public IDataParameterCollection Parameters
        {
            get { return _command.Parameters; }
        }

        /// <summary>Creates a prepared (or compiled) version of the command on the data source.</summary>
        public void Prepare()
        {
            try
            {
                _command.Prepare();
            }
            catch (Exception ex)
            {
                throw new DynamicQueryException("Error preparing command.", ex, this);
            }
        }

        /// <summary>Gets or sets the transaction within which the Command
        /// object of a data provider executes.</summary>
        /// <remarks>It's does nothing, transaction is peeked from transaction
        /// pool of a connection. This is only a dummy.</remarks>
        public IDbTransaction Transaction { get { return null; } set { } }

        /// <summary>Gets or sets how command results are applied to the <see cref="T:System.Data.DataRow"/>
        /// when used by the <see cref="M:System.Data.IDataAdapter.Update(System.Data.DataSet)"/>
        /// method of a <see cref="T:System.Data.Common.DbDataAdapter"/>.</summary>
        /// <returns>One of the <see cref="T:System.Data.UpdateRowSource"/> values. The default is
        /// Both unless the command is automatically generated. Then the default is None.</returns>
        /// <exception cref="T:System.ArgumentException">The value entered was not one of the
        /// <see cref="T:System.Data.UpdateRowSource"/> values. </exception>
        public UpdateRowSource UpdatedRowSource { get { return _command.UpdatedRowSource; } set { _command.UpdatedRowSource = value; } }

        #endregion IDbCommand Members

        #region IExtendedDisposable Members

        /// <summary>Performs application-defined tasks associated with
        /// freeing, releasing, or resetting unmanaged resources.</summary>
        public void Dispose()
        {
            lock (_db.SyncLock)
            {
                if (_con != null)
                {
                    var pool = _db.CommandsPool.TryGetValue(_con.Connection);

                    if (pool != null && pool.Contains(this))
                        pool.Remove(this);
                }

                IsDisposed = true;

                _command.Dispose();
            }
        }

        /// <summary>Gets a value indicating whether this instance is disposed.</summary>
        public bool IsDisposed { get; private set; }

        #endregion IExtendedDisposable Members
    }

    /// <summary>Connection wrapper.</summary>
    /// <remarks>This class is only connection holder, connection is managed by
    /// <see cref="DynamicDatabase"/> instance.</remarks>
    public class DynamicConnection : IDbConnection, IExtendedDisposable
    {
        private DynamicDatabase _db;
        private bool _singleTransaction;

        /// <summary>Gets underlying connection.</summary>
        internal IDbConnection Connection { get; private set; }

        /// <summary>Initializes a new instance of the <see cref="DynamicConnection" /> class.</summary>
        /// <param name="db">Database connection manager.</param>
        /// <param name="con">Active connection.</param>
        /// <param name="singleTransaction">Are we using single transaction mode? I so... act correctly.</param>
        internal DynamicConnection(DynamicDatabase db, IDbConnection con, bool singleTransaction)
        {
            IsDisposed = false;
            _db = db;
            Connection = con;
            _singleTransaction = singleTransaction;
        }

        /// <summary>Begins a database transaction.</summary>
        /// <param name="il">One of the <see cref="System.Data.IsolationLevel"/> values.</param>
        /// <param name="custom">Custom parameter describing transaction options.</param>
        /// <param name="disposed">This action is invoked when transaction is disposed.</param>
        /// <returns>Returns <see cref="DynamicTransaction"/> representation.</returns>
        internal DynamicTransaction BeginTransaction(IsolationLevel? il, object custom, Action disposed)
        {
            return new DynamicTransaction(_db, this, _singleTransaction, il, disposed, null);
        }

        #region IDbConnection Members

        /// <summary>Creates and returns a Command object associated with the connection.</summary>
        /// <returns>A Command object associated with the connection.</returns>
        public IDbCommand CreateCommand()
        {
            return new DynamicCommand(this, _db);
        }

        /// <summary>Begins a database transaction.</summary>
        /// <returns>Returns <see cref="DynamicTransaction"/> representation.</returns>
        public IDbTransaction BeginTransaction()
        {
            return BeginTransaction(null, null, null);
        }

        /// <summary>Begins a database transaction with the specified
        /// <see cref="System.Data.IsolationLevel"/> value.</summary>
        /// <param name="il">One of the <see cref="System.Data.IsolationLevel"/> values.</param>
        /// <returns>Returns <see cref="DynamicTransaction"/> representation.</returns>
        public IDbTransaction BeginTransaction(IsolationLevel il)
        {
            return BeginTransaction(il, null, null);
        }

        /// <summary>Begins a database transaction with the specified
        /// <see cref="System.Data.IsolationLevel"/> value.</summary>
        /// <param name="custom">Custom parameter describing transaction options.</param>
        /// <returns>Returns <see cref="DynamicTransaction"/> representation.</returns>
        public IDbTransaction BeginTransaction(object custom)
        {
            return BeginTransaction(null, custom, null);
        }

        /// <summary>Changes the current database for an open Connection object.</summary>
        /// <param name="databaseName">The name of the database to use in place of the current database.</param>
        /// <remarks>This operation is not supported in <c>DynamORM</c>. and will throw <see cref="NotSupportedException"/>.</remarks>
        /// <exception cref="NotSupportedException">Thrown always.</exception>
        public void ChangeDatabase(string databaseName)
        {
            throw new NotSupportedException("This operation is not supported in DynamORM.");
        }

        /// <summary>Opens a database connection with the settings specified by
        /// the ConnectionString property of the provider-specific
        /// Connection object.</summary>
        /// <remarks>Does nothing. <see cref="DynamicDatabase"/> handles
        /// opening connections.</remarks>
        public void Open()
        {
        }

        /// <summary>Closes the connection to the database.</summary>
        /// <remarks>Does nothing. <see cref="DynamicDatabase"/> handles
        /// closing connections. Only way to close it is to dispose connection.
        /// It will close if this is multi connection configuration, otherwise
        /// it will stay open until <see cref="DynamicDatabase"/> is not
        /// disposed.</remarks>
        public void Close()
        {
        }

        /// <summary>Gets or sets the string used to open a database.</summary>
        /// <remarks>Changing connection string operation is not supported in <c>DynamORM</c>.
        /// and will throw <see cref="NotSupportedException"/>.</remarks>
        /// <exception cref="NotSupportedException">Thrown always when set is attempted.</exception>
        public string ConnectionString
        {
            get { return Connection.ConnectionString; }
            set { throw new NotSupportedException("This operation is not supported in DynamORM."); }
        }

        /// <summary>Gets the time to wait while trying to establish a connection
        /// before terminating the attempt and generating an error.</summary>
        public int ConnectionTimeout
        {
            get { return Connection.ConnectionTimeout; }
        }

        /// <summary>Gets the name of the current database or the database
        /// to be used after a connection is opened.</summary>
        public string Database
        {
            get { throw new NotImplementedException(); }
        }

        /// <summary>Gets the current state of the connection.</summary>
        public ConnectionState State
        {
            get { return Connection.State; }
        }

        #endregion IDbConnection Members

        #region IExtendedDisposable Members

        /// <summary>Performs application-defined tasks associated with freeing,
        /// releasing, or resetting unmanaged resources.</summary>
        public void Dispose()
        {
            _db.Close(Connection);
            IsDisposed = true;
        }

        /// <summary>Gets a value indicating whether this instance is disposed.</summary>
        public bool IsDisposed { get; private set; }

        #endregion IExtendedDisposable Members
    }

    /// <summary>Dynamic database is a class responsible for managing database.</summary>
    public class DynamicDatabase : IExtendedDisposable
    {
        #region Internal fields and properties

        private DbProviderFactory _provider;
        private string _connectionString;
        private bool _singleConnection;
        private bool _singleTransaction;
        private string _leftDecorator = "\"";
        private string _rightDecorator = "\"";
        private bool _leftDecoratorIsInInvalidMembersChars = true;
        private bool _rightDecoratorIsInInvalidMembersChars = true;
        private string _parameterFormat = "@{0}";
        private int? _commandTimeout = null;
        private long _poolStamp = 0;

        private DynamicConnection _tempConn = null;

        /// <summary>Provides lock object for this database instance.</summary>
        internal readonly object SyncLock = new object();

        /// <summary>Gets or sets timestamp of last transaction pool or configuration change.</summary>
        /// <remarks>This property is used to allow commands to determine if
        /// they need to update transaction object or not.</remarks>
        internal long PoolStamp
        {
            get
            {
                long r = 0;

                lock (SyncLock)
                    r = _poolStamp;

                return r;
            }

            set
            {
                lock (SyncLock)
                    _poolStamp = value;
            }
        }

        /// <summary>Gets pool of connections and transactions.</summary>
        internal Dictionary<IDbConnection, Stack<IDbTransaction>> TransactionPool { get; private set; }

        /// <summary>Gets pool of connections and commands.</summary>
        /// <remarks>Pool should contain dynamic commands instead of native ones.</remarks>
        internal Dictionary<IDbConnection, List<IDbCommand>> CommandsPool { get; private set; }

        /// <summary>Gets schema columns cache.</summary>
        internal Dictionary<string, Dictionary<string, DynamicSchemaColumn>> Schema { get; private set; }

#if !DYNAMORM_OMMIT_OLDSYNTAX

        /// <summary>Gets tables cache for this database instance.</summary>
        internal Dictionary<string, DynamicTable> TablesCache { get; private set; }

#endif

        #endregion Internal fields and properties

        #region Properties and Constructors

        /// <summary>Gets database options.</summary>
        public DynamicDatabaseOptions Options { get; private set; }

        /// <summary>Gets or sets command timeout.</summary>
        public int? CommandTimeout { get { return _commandTimeout; } set { _commandTimeout = value; _poolStamp = DateTime.Now.Ticks; } }

        /// <summary>Gets the database provider.</summary>
        public DbProviderFactory Provider { get { return _provider; } }

        /// <summary>Gets or sets a value indicating whether
        /// dump commands to console or not.</summary>
        public bool DumpCommands { get; set; }

        /// <summary>Initializes a new instance of the <see cref="DynamicDatabase" /> class.</summary>
        /// <param name="provider">Database provider by name.</param>
        /// <param name="connectionString">Connection string to provided database.</param>
        /// <param name="options">Connection options.</param>
        public DynamicDatabase(string provider, string connectionString, DynamicDatabaseOptions options)
            : this(DbProviderFactories.GetFactory(provider), connectionString, options)
        {
        }

        /// <summary>Initializes a new instance of the <see cref="DynamicDatabase" /> class.</summary>
        /// <param name="provider">Database provider.</param>
        /// <param name="connectionString">Connection string to provided database.</param>
        /// <param name="options">Connection options.</param>
        public DynamicDatabase(DbProviderFactory provider, string connectionString, DynamicDatabaseOptions options)
        {
            IsDisposed = false;
            _provider = provider;

            InitCommon(connectionString, options);
        }

        /// <summary>Initializes a new instance of the <see cref="DynamicDatabase" /> class.</summary>
        /// <param name="connection">Active database connection.</param>
        /// <param name="options">Connection options. <see cref="DynamicDatabaseOptions.SingleConnection"/> required.</param>
        public DynamicDatabase(IDbConnection connection, DynamicDatabaseOptions options)
        {
            IsDisposed = false;
            InitCommon(connection.ConnectionString, options);
            TransactionPool.Add(connection, new Stack<IDbTransaction>());

            if (!_singleConnection)
                throw new InvalidOperationException("This constructor accepts only connections with DynamicDatabaseOptions.SingleConnection option.");
        }

        private void InitCommon(string connectionString, DynamicDatabaseOptions options)
        {
            _connectionString = connectionString;
            Options = options;

            _singleConnection = (options & DynamicDatabaseOptions.SingleConnection) == DynamicDatabaseOptions.SingleConnection;
            _singleTransaction = (options & DynamicDatabaseOptions.SingleTransaction) == DynamicDatabaseOptions.SingleTransaction;
            DumpCommands = (options & DynamicDatabaseOptions.DumpCommands) == DynamicDatabaseOptions.DumpCommands;

            TransactionPool = new Dictionary<IDbConnection, Stack<IDbTransaction>>();
            CommandsPool = new Dictionary<IDbConnection, List<IDbCommand>>();
            Schema = new Dictionary<string, Dictionary<string, DynamicSchemaColumn>>();
#if !DYNAMORM_OMMIT_OLDSYNTAX
            TablesCache = new Dictionary<string, DynamicTable>();
#endif
        }

        #endregion Properties and Constructors

        #region Table

#if !DYNAMORM_OMMIT_OLDSYNTAX

        /// <summary>Gets dynamic table which is a simple ORM using dynamic objects.</summary>
        /// <param name="action">The action with instance of <see cref="DynamicTable"/> as parameter.</param>
        /// <param name="table">Table name.</param>
        /// <param name="keys">Override keys in schema.</param>
        /// <param name="owner">Owner of the table.</param>
        public void Table(Action<dynamic> action, string table = "", string[] keys = null, string owner = "")
        {
            using (dynamic t = Table(table, keys, owner))
                action(t);
        }

        /// <summary>Gets dynamic table which is a simple ORM using dynamic objects.</summary>
        /// <typeparam name="T">Type used to determine table name.</typeparam>
        /// <param name="action">The action with instance of <see cref="DynamicTable"/> as parameter.</param>
        /// <param name="keys">Override keys in schema.</param>
        public void Table<T>(Action<dynamic> action, string[] keys = null)
        {
            using (dynamic t = Table<T>(keys))
                action(t);
        }

        /// <summary>Gets dynamic table which is a simple ORM using dynamic objects.</summary>
        /// <param name="table">Table name.</param>
        /// <param name="keys">Override keys in schema.</param>
        /// <param name="owner">Owner of the table.</param>
        /// <returns>Instance of <see cref="DynamicTable"/>.</returns>
        public dynamic Table(string table = "", string[] keys = null, string owner = "")
        {
            string key = string.Concat(
                table == null ? string.Empty : table,
                keys == null ? string.Empty : string.Join("_|_", keys));

            DynamicTable dt = null;
            lock (SyncLock)
                dt = TablesCache.TryGetValue(key) ??
                    TablesCache.AddAndPassValue(key,
                        new DynamicTable(this, table, owner, keys));

            return dt;
        }

        /// <summary>Gets dynamic table which is a simple ORM using dynamic objects.</summary>
        /// <typeparam name="T">Type used to determine table name.</typeparam>
        /// <param name="keys">Override keys in schema.</param>
        /// <returns>Instance of <see cref="DynamicTable"/>.</returns>
        public dynamic Table<T>(string[] keys = null)
        {
            Type table = typeof(T);
            string key = string.Concat(
                table.FullName,
                keys == null ? string.Empty : string.Join("_|_", keys));

            DynamicTable dt = null;
            lock (SyncLock)
                dt = TablesCache.TryGetValue(key) ??
                    TablesCache.AddAndPassValue(key,
                        new DynamicTable(this, table, keys));

            return dt;
        }

        /// <summary>Removes cached table.</summary>
        /// <param name="dynamicTable">Disposed dynamic table.</param>
        internal void RemoveFromCache(DynamicTable dynamicTable)
        {
            foreach (var item in TablesCache.Where(kvp => kvp.Value == dynamicTable).ToList())
                TablesCache.Remove(item.Key);
        }

#endif

        #endregion Table

        #region From/Insert/Update/Delete

        /// <summary>
        /// Adds to the <code>FROM</code> clause the contents obtained by parsing the dynamic lambda expressions given. The supported
        /// formats are:
        /// <para>- Resolve to a string: <code>x => "owner.Table AS Alias"</code>, where the alias part is optional.</para>
        /// <para>- Resolve to an expression: <code>x => x.owner.Table.As( x.Alias )</code>, where the alias part is optional.</para>
        /// <para>- Generic expression: <code>x => x( expression ).As( x.Alias )</code>, where the alias part is mandatory. In this
        /// case the alias is not annotated.</para>
        /// </summary>
        /// <param name="fn">The specification.</param>
        /// <returns>This instance to permit chaining.</returns>
        public virtual IDynamicSelectQueryBuilder From(Func<dynamic, object> fn)
        {
            return new DynamicSelectQueryBuilder(this).From(fn);
        }

        /// <summary>Adds to the <code>FROM</code> clause using <see cref="Type"/>.</summary>
        /// <typeparam name="T">Type which can be represented in database.</typeparam>
        /// <returns>This instance to permit chaining.</returns>
        public virtual IDynamicSelectQueryBuilder From<T>()
        {
            return new DynamicSelectQueryBuilder(this).From(x => x(typeof(T)));
        }

        /// <summary>Adds to the <code>FROM</code> clause using <see cref="Type"/>.</summary>
        /// <param name="t">Type which can be represented in database.</param>
        /// <returns>This instance to permit chaining.</returns>
        public virtual IDynamicSelectQueryBuilder From(Type t)
        {
            return new DynamicSelectQueryBuilder(this).From(x => x(t));
        }

        /// <summary>
        /// Adds to the <code>INSERT INTO</code> clause the contents obtained by parsing the dynamic lambda expressions given. The supported
        /// formats are:
        /// <para>- Resolve to a string: <code>x => "owner.Table"</code>.</para>
        /// <para>- Resolve to a type: <code>x => typeof(SomeClass)</code>.</para>
        /// <para>- Resolve to an expression: <code>x => x.owner.Table</code>.</para>
        /// <para>- Generic expression: <code>x => x( expression )</code>. Expression can
        /// be <see cref="string"/> or <see cref="Type"/>.</para>
        /// </summary>
        /// <param name="func">The specification.</param>
        /// <returns>This instance to permit chaining.</returns>
        public virtual IDynamicInsertQueryBuilder Insert(Func<dynamic, object> func)
        {
            return new DynamicInsertQueryBuilder(this).Table(func);
        }

        /// <summary>Adds to the <code>INSERT INTO</code> clause using <see cref="Type"/>.</summary>
        /// <typeparam name="T">Type which can be represented in database.</typeparam>
        /// <returns>This instance to permit chaining.</returns>
        public virtual IDynamicInsertQueryBuilder Insert<T>()
        {
            return new DynamicInsertQueryBuilder(this).Table(typeof(T));
        }

        /// <summary>Adds to the <code>INSERT INTO</code> clause using <see cref="Type"/>.</summary>
        /// <param name="t">Type which can be represented in database.</param>
        /// <returns>This instance to permit chaining.</returns>
        public virtual IDynamicInsertQueryBuilder Insert(Type t)
        {
            return new DynamicInsertQueryBuilder(this).Table(t);
        }

        /// <summary>Bulk insert objects into database.</summary>
        /// <typeparam name="T">Type of objects to insert.</typeparam>
        /// <param name="e">Enumerable containing instances of objects to insert.</param>
        /// <returns>Number of inserted rows.</returns>
        public virtual int Insert<T>(IEnumerable<T> e) where T : class
        {
            int affected = 0;
            var mapper = DynamicMapperCache.GetMapper(typeof(T));

            if (mapper != null)
            {
                using (var con = Open())
                using (var tra = con.BeginTransaction())
                using (var cmd = con.CreateCommand())
                {
                    try
                    {
                        var parameters = new Dictionary<IDbDataParameter, DynamicPropertyInvoker>();

                        if (!string.IsNullOrEmpty(mapper.InsertCommandText))
                        {
                            cmd.CommandText = mapper.InsertCommandText;

                            foreach (var col in mapper.ColumnsMap.Values
                                .Where(di => !di.Ignore && di.InsertCommandParameter != null)
                                .OrderBy(di => di.InsertCommandParameter.Ordinal))
                            {
                                var para = cmd.CreateParameter();
                                para.ParameterName = col.InsertCommandParameter.Name;
                                para.DbType = col.InsertCommandParameter.Type;
                                cmd.Parameters.Add(para);

                                parameters[para] = col;
                            }
                        }
                        else
                            PrepareBatchInsert<T>(mapper, cmd, parameters);

                        foreach (var o in e)
                        {
                            foreach (var m in parameters)
                                m.Key.Value = m.Value.Get(o);

                            affected += cmd.ExecuteNonQuery();
                        }

                        tra.Commit();
                    }
                    catch (Exception ex)
                    {
                        if (tra != null)
                            tra.Rollback();

                        affected = 0;

                        var problematicCommand = new StringBuilder();
                        cmd.Dump(problematicCommand);

                        throw new InvalidOperationException(problematicCommand.ToString(), ex);
                    }
                }
            }

            return affected;
        }

        /// <summary>
        /// Adds to the <code>UPDATE</code> clause the contents obtained by parsing the dynamic lambda expressions given. The supported
        /// formats are:
        /// <para>- Resolve to a string: <code>x => "owner.Table"</code>.</para>
        /// <para>- Resolve to a type: <code>x => typeof(SomeClass)</code>.</para>
        /// <para>- Resolve to an expression: <code>x => x.owner.Table</code>.</para>
        /// <para>- Generic expression: <code>x => x( expression )</code>. Expression can
        /// be <see cref="string"/> or <see cref="Type"/>.</para>
        /// </summary>
        /// <param name="func">The specification.</param>
        /// <returns>This instance to permit chaining.</returns>
        public virtual IDynamicUpdateQueryBuilder Update(Func<dynamic, object> func)
        {
            return new DynamicUpdateQueryBuilder(this).Table(func);
        }

        /// <summary>Adds to the <code>UPDATE</code> clause using <see cref="Type"/>.</summary>
        /// <typeparam name="T">Type which can be represented in database.</typeparam>
        /// <returns>This instance to permit chaining.</returns>
        public virtual IDynamicUpdateQueryBuilder Update<T>()
        {
            return new DynamicUpdateQueryBuilder(this).Table(typeof(T));
        }

        /// <summary>Adds to the <code>UPDATE</code> clause using <see cref="Type"/>.</summary>
        /// <param name="t">Type which can be represented in database.</param>
        /// <returns>This instance to permit chaining.</returns>
        public virtual IDynamicUpdateQueryBuilder Update(Type t)
        {
            return new DynamicUpdateQueryBuilder(this).Table(t);
        }

        /// <summary>Bulk update objects in database.</summary>
        /// <typeparam name="T">Type of objects to update.</typeparam>
        /// <param name="e">Enumerable containing instances of objects to update.</param>
        /// <returns>Number of updated rows.</returns>
        public virtual int Update<T>(IEnumerable<T> e) where T : class
        {
            int affected = 0;
            var mapper = DynamicMapperCache.GetMapper(typeof(T));

            if (mapper != null)
            {
                using (var con = Open())
                using (var tra = con.BeginTransaction())
                using (var cmd = con.CreateCommand())
                {
                    try
                    {
                        var parameters = new Dictionary<IDbDataParameter, DynamicPropertyInvoker>();

                        if (!string.IsNullOrEmpty(mapper.UpdateCommandText))
                        {
                            cmd.CommandText = mapper.UpdateCommandText;

                            foreach (var col in mapper.ColumnsMap.Values
                                .Where(di => !di.Ignore && di.UpdateCommandParameter != null)
                                .OrderBy(di => di.UpdateCommandParameter.Ordinal))
                            {
                                var para = cmd.CreateParameter();
                                para.ParameterName = col.UpdateCommandParameter.Name;
                                para.DbType = col.UpdateCommandParameter.Type;
                                cmd.Parameters.Add(para);

                                parameters[para] = col;
                            }
                        }
                        else
                            PrepareBatchUpdate<T>(mapper, cmd, parameters);

                        foreach (var o in e)
                        {
                            foreach (var m in parameters)
                                m.Key.Value = m.Value.Get(o);

                            affected += cmd.ExecuteNonQuery();
                        }

                        tra.Commit();
                    }
                    catch (Exception ex)
                    {
                        if (tra != null)
                            tra.Rollback();

                        affected = 0;

                        var problematicCommand = new StringBuilder();
                        cmd.Dump(problematicCommand);

                        throw new InvalidOperationException(problematicCommand.ToString(), ex);
                    }
                }
            }

            return affected;
        }

        /// <summary>Bulk update or insert objects into database.</summary>
        /// <typeparam name="T">Type of objects to update or insert.</typeparam>
        /// <param name="e">Enumerable containing instances of objects to update or insert.</param>
        /// <returns>Number of updated or inserted rows.</returns>
        public virtual int UpdateOrInsert<T>(IEnumerable<T> e) where T : class
        {
            int affected = 0;
            var mapper = DynamicMapperCache.GetMapper(typeof(T));

            if (mapper != null)
            {
                using (var con = Open())
                using (var tra = con.BeginTransaction())
                using (var cmdUp = con.CreateCommand())
                using (var cmdIn = con.CreateCommand())
                {
                    try
                    {
                        #region Update

                        var parametersUp = new Dictionary<IDbDataParameter, DynamicPropertyInvoker>();

                        if (!string.IsNullOrEmpty(mapper.UpdateCommandText))
                        {
                            cmdUp.CommandText = mapper.UpdateCommandText;

                            foreach (var col in mapper.ColumnsMap.Values
                                .Where(di => !di.Ignore && di.UpdateCommandParameter != null)
                                .OrderBy(di => di.UpdateCommandParameter.Ordinal))
                            {
                                var para = cmdUp.CreateParameter();
                                para.ParameterName = col.UpdateCommandParameter.Name;
                                para.DbType = col.UpdateCommandParameter.Type;
                                cmdUp.Parameters.Add(para);

                                parametersUp[para] = col;
                            }
                        }
                        else
                            PrepareBatchUpdate<T>(mapper, cmdUp, parametersUp);

                        #endregion Update

                        #region Insert

                        var parametersIn = new Dictionary<IDbDataParameter, DynamicPropertyInvoker>();

                        if (!string.IsNullOrEmpty(mapper.InsertCommandText))
                        {
                            cmdIn.CommandText = mapper.InsertCommandText;

                            foreach (var col in mapper.ColumnsMap.Values
                                .Where(di => !di.Ignore && di.InsertCommandParameter != null)
                                .OrderBy(di => di.InsertCommandParameter.Ordinal))
                            {
                                var para = cmdIn.CreateParameter();
                                para.ParameterName = col.InsertCommandParameter.Name;
                                para.DbType = col.InsertCommandParameter.Type;
                                cmdIn.Parameters.Add(para);

                                parametersIn[para] = col;
                            }
                        }
                        else
                            PrepareBatchInsert<T>(mapper, cmdIn, parametersIn);

                        #endregion Insert

                        foreach (var o in e)
                        {
                            foreach (var m in parametersUp)
                                m.Key.Value = m.Value.Get(o);

                            int a = cmdUp.ExecuteNonQuery();
                            if (a == 0)
                            {
                                foreach (var m in parametersIn)
                                    m.Key.Value = m.Value.Get(o);

                                a = cmdIn.ExecuteNonQuery();
                            }

                            affected += a;
                        }

                        tra.Commit();
                    }
                    catch (Exception ex)
                    {
                        if (tra != null)
                            tra.Rollback();

                        affected = 0;

                        var problematicCommand = new StringBuilder();
                        cmdUp.Dump(problematicCommand);

                        throw new InvalidOperationException(problematicCommand.ToString(), ex);
                    }
                }
            }

            return affected;
        }

        /// <summary>
        /// Adds to the <code>DELETE FROM</code> clause the contents obtained by parsing the dynamic lambda expressions given. The supported
        /// formats are:
        /// <para>- Resolve to a string: <code>x => "owner.Table"</code>.</para>
        /// <para>- Resolve to a type: <code>x => typeof(SomeClass)</code>.</para>
        /// <para>- Resolve to an expression: <code>x => x.owner.Table</code>.</para>
        /// <para>- Generic expression: <code>x => x( expression )</code>. Expression can
        /// be <see cref="string"/> or <see cref="Type"/>.</para>
        /// </summary>
        /// <param name="func">The specification.</param>
        /// <returns>This instance to permit chaining.</returns>
        public virtual IDynamicDeleteQueryBuilder Delete(Func<dynamic, object> func)
        {
            return new DynamicDeleteQueryBuilder(this).Table(func);
        }

        /// <summary>Adds to the <code>DELETE FROM</code> clause using <see cref="Type"/>.</summary>
        /// <typeparam name="T">Type which can be represented in database.</typeparam>
        /// <returns>This instance to permit chaining.</returns>
        public virtual IDynamicDeleteQueryBuilder Delete<T>()
        {
            return new DynamicDeleteQueryBuilder(this).Table(typeof(T));
        }

        /// <summary>Bulk delete objects in database.</summary>
        /// <typeparam name="T">Type of objects to delete.</typeparam>
        /// <param name="e">Enumerable containing instances of objects to delete.</param>
        /// <returns>Number of deleted rows.</returns>
        public virtual int Delete<T>(IEnumerable<T> e) where T : class
        {
            int affected = 0;
            var mapper = DynamicMapperCache.GetMapper(typeof(T));

            if (mapper != null)
            {
                using (var con = Open())
                using (var tra = con.BeginTransaction())
                using (var cmd = con.CreateCommand())
                {
                    try
                    {
                        var parameters = new Dictionary<IDbDataParameter, DynamicPropertyInvoker>();

                        if (!string.IsNullOrEmpty(mapper.DeleteCommandText))
                        {
                            cmd.CommandText = mapper.DeleteCommandText;

                            foreach (var col in mapper.ColumnsMap.Values
                                .Where(di => !di.Ignore && di.DeleteCommandParameter != null)
                                .OrderBy(di => di.DeleteCommandParameter.Ordinal))
                            {
                                var para = cmd.CreateParameter();
                                para.ParameterName = col.DeleteCommandParameter.Name;
                                para.DbType = col.DeleteCommandParameter.Type;
                                cmd.Parameters.Add(para);

                                parameters[para] = col;
                            }
                        }
                        else
                            PrepareBatchDelete<T>(mapper, cmd, parameters);

                        foreach (var o in e)
                        {
                            foreach (var m in parameters)
                                m.Key.Value = m.Value.Get(o);

                            affected += cmd.ExecuteNonQuery();
                        }

                        tra.Commit();
                    }
                    catch (Exception ex)
                    {
                        if (tra != null)
                            tra.Rollback();

                        affected = 0;

                        var problematicCommand = new StringBuilder();
                        cmd.Dump(problematicCommand);

                        throw new InvalidOperationException(problematicCommand.ToString(), ex);
                    }
                }
            }

            return affected;
        }

        private void PrepareBatchInsert<T>(DynamicTypeMap mapper, IDbCommand cmd, Dictionary<IDbDataParameter, DynamicPropertyInvoker> parameters)
        {
            DynamicPropertyInvoker currentprop = null;
            var temp = new Dictionary<string, DynamicPropertyInvoker>();
            var schema = this.GetSchema<T>();
            int ord = 0;

            var ib = Insert<T>()
                .SetVirtualMode(true)
                .CreateTemporaryParameterAction(p => temp[p.Name] = currentprop)
                .CreateParameterAction((p, cp) =>
                {
                    parameters[cp] = temp[p.Name];
                    parameters[cp].InsertCommandParameter = new DynamicPropertyInvoker.ParameterSpec
                    {
                        Name = cp.ParameterName,
                        Type = cp.DbType,
                        Ordinal = ord++,
                    };
                });

            foreach (var prop in mapper.PropertyMap)
                if (!mapper.Ignored.Contains(prop.Key))
                {
                    var col = mapper.PropertyMap.TryGetValue(prop.Key) ?? prop.Key;
                    currentprop = mapper.ColumnsMap.TryGetValue(col.ToLower());

                    if (currentprop.Ignore || (currentprop.Column != null && currentprop.Column.IsNoInsert))
                        continue;

                    if (currentprop.Get != null)
                        ib.Insert(new DynamicColumn()
                        {
                            ColumnName = col,
                            Schema = schema == null ? null : schema.TryGetNullable(col.ToLower()),
                            Operator = DynamicColumn.CompareOperator.Eq,
                            Value = null,
                            VirtualColumn = true,
                        });
                }

            ib.FillCommand(cmd);

            // Cache command
            mapper.InsertCommandText = cmd.CommandText;
        }

        private void PrepareBatchUpdate<T>(DynamicTypeMap mapper, IDbCommand cmd, Dictionary<IDbDataParameter, DynamicPropertyInvoker> parameters)
        {
            DynamicPropertyInvoker currentprop = null;
            var temp = new Dictionary<string, DynamicPropertyInvoker>();
            var schema = this.GetSchema<T>();
            int ord = 0;

            var ib = Update<T>()
                .SetVirtualMode(true)
                .CreateTemporaryParameterAction(p => temp[p.Name] = currentprop)
                .CreateParameterAction((p, cp) =>
                {
                    parameters[cp] = temp[p.Name];
                    parameters[cp].UpdateCommandParameter = new DynamicPropertyInvoker.ParameterSpec
                    {
                        Name = cp.ParameterName,
                        Type = cp.DbType,
                        Ordinal = ord++,
                    };
                });

            foreach (var prop in mapper.PropertyMap)
                if (!mapper.Ignored.Contains(prop.Key))
                {
                    var col = mapper.PropertyMap.TryGetValue(prop.Key) ?? prop.Key;
                    currentprop = mapper.ColumnsMap.TryGetValue(col.ToLower());

                    if (currentprop.Ignore)
                        continue;

                    if (currentprop.Get != null)
                    {
                        var colS = schema == null ? null : schema.TryGetNullable(col.ToLower());

                        if (colS.HasValue)
                        {
                            if (colS.Value.IsKey)
                                ib.Where(new DynamicColumn()
                                {
                                    ColumnName = col,
                                    Schema = colS,
                                    Operator = DynamicColumn.CompareOperator.Eq,
                                    Value = null,
                                    VirtualColumn = true,
                                });
                            else if (currentprop.Column == null || !currentprop.Column.IsNoUpdate)
                                ib.Values(new DynamicColumn()
                                {
                                    ColumnName = col,
                                    Schema = colS,
                                    Operator = DynamicColumn.CompareOperator.Eq,
                                    Value = null,
                                    VirtualColumn = true,
                                });
                        }
                        else if (currentprop.Column != null && currentprop.Column.IsKey)
                            ib.Where(new DynamicColumn()
                            {
                                ColumnName = col,
                                Schema = colS,
                                Operator = DynamicColumn.CompareOperator.Eq,
                                Value = null,
                                VirtualColumn = true,
                            });
                    }
                }

            ib.FillCommand(cmd);

            // Cache command
            mapper.UpdateCommandText = cmd.CommandText;
        }

        private void PrepareBatchDelete<T>(DynamicTypeMap mapper, IDbCommand cmd, Dictionary<IDbDataParameter, DynamicPropertyInvoker> parameters)
        {
            DynamicPropertyInvoker currentprop = null;
            var temp = new Dictionary<string, DynamicPropertyInvoker>();
            var schema = this.GetSchema<T>();
            int ord = 0;

            var ib = Delete<T>()
                .SetVirtualMode(true)
                .CreateTemporaryParameterAction(p => temp[p.Name] = currentprop)
                .CreateParameterAction((p, cp) =>
                {
                    parameters[cp] = temp[p.Name];
                    parameters[cp].DeleteCommandParameter = new DynamicPropertyInvoker.ParameterSpec
                    {
                        Name = cp.ParameterName,
                        Type = cp.DbType,
                        Ordinal = ord++,
                    };
                });

            foreach (var prop in mapper.PropertyMap)
                if (!mapper.Ignored.Contains(prop.Key))
                {
                    var col = mapper.PropertyMap.TryGetValue(prop.Key) ?? prop.Key;
                    currentprop = mapper.ColumnsMap.TryGetValue(col.ToLower());

                    if (currentprop.Ignore)
                        continue;

                    if (currentprop.Get != null)
                    {
                        var colS = schema == null ? null : schema.TryGetNullable(col.ToLower());

                        if (colS != null)
                        {
                            if (colS.Value.IsKey)
                                ib.Where(new DynamicColumn()
                                {
                                    ColumnName = col,
                                    Schema = colS,
                                    Operator = DynamicColumn.CompareOperator.Eq,
                                    Value = null,
                                    VirtualColumn = true,
                                });
                        }
                        else if (currentprop.Column != null && currentprop.Column.IsKey)
                            ib.Where(new DynamicColumn()
                            {
                                ColumnName = col,
                                Schema = colS,
                                Operator = DynamicColumn.CompareOperator.Eq,
                                Value = null,
                                VirtualColumn = true,
                            });
                    }
                }

            ib.FillCommand(cmd);

            // Cache command
            mapper.DeleteCommandText = cmd.CommandText;
        }

        #endregion From/Insert/Update/Delete

        #region Schema

        /// <summary>Builds query cache if necessary and returns it.</summary>
        /// <param name="builder">The builder containing query to read schema from.</param>
        /// <returns>Query schema.</returns>
        public Dictionary<string, DynamicSchemaColumn> GetQuerySchema(IDynamicSelectQueryBuilder builder)
        {
            using (var con = Open())
            using (var cmd = con.CreateCommand().SetCommand(builder))
                return ReadSchema(cmd)
                    .Distinct()
                    .ToDictionary(k => k.Name.ToLower(), k => k);
        }

        /// <summary>Builds query cache if necessary and returns it.</summary>
        /// <param name="sql">SQL query from which read schema.</param>
        /// <param name="args">SQL query arguments.</param>
        /// <returns>Query schema.</returns>
        public Dictionary<string, DynamicSchemaColumn> GetQuerySchema(string sql, params object[] args)
        {
            using (var con = Open())
            using (var cmd = con.CreateCommand().SetCommand(sql, args))
                return ReadSchema(cmd)
                    .Distinct()
                    .ToDictionary(k => k.Name.ToLower(), k => k);
        }

        /// <summary>Builds table cache if necessary and returns it.</summary>
        /// <param name="table">Name of table for which build schema.</param>
        /// <param name="owner">Owner of table for which build schema.</param>
        /// <returns>Table schema.</returns>
        public Dictionary<string, DynamicSchemaColumn> GetSchema(string table, string owner = null)
        {
            Dictionary<string, DynamicSchemaColumn> schema = null;

            lock (SyncLock)
                schema = Schema.TryGetValue(table.ToLower()) ??
                    BuildAndCacheSchema(table, null, owner);

            return schema;
        }

        /// <summary>Builds table cache if necessary and returns it.</summary>
        /// <typeparam name="T">Type of table for which build schema.</typeparam>
        /// <returns>Table schema or null if type was anonymous.</returns>
        public Dictionary<string, DynamicSchemaColumn> GetSchema<T>()
        {
            if (typeof(T).IsAnonymous())
                return null;

            Dictionary<string, DynamicSchemaColumn> schema = null;

            lock (SyncLock)
                schema = Schema.TryGetValue(typeof(T).FullName) ??
                    BuildAndCacheSchema(null, DynamicMapperCache.GetMapper<T>());

            return schema;
        }

        /// <summary>Builds table cache if necessary and returns it.</summary>
        /// <param name="table">Type of table for which build schema.</param>
        /// <returns>Table schema or null if type was anonymous.</returns>
        public Dictionary<string, DynamicSchemaColumn> GetSchema(Type table)
        {
            if (table == null || table.IsAnonymous() || table.IsValueType)
                return null;

            Dictionary<string, DynamicSchemaColumn> schema = null;

            lock (SyncLock)
                schema = Schema.TryGetValue(table.FullName) ??
                    BuildAndCacheSchema(null, DynamicMapperCache.GetMapper(table));

            return schema;
        }

        /// <summary>Clears the schema from cache.</summary>
        /// <remarks>Use this method to refresh table information.</remarks>
        /// <param name="table">Name of table for which clear schema.</param>
        /// <param name="owner">Owner of table for which clear schema.</param>
        public void ClearSchema(string table = null, string owner = null)
        {
            lock (SyncLock)
                if (Schema.ContainsKey(table.ToLower()))
                    Schema.Remove(table.ToLower());
        }

        /// <summary>Clears the schema from cache.</summary>
        /// <remarks>Use this method to refresh table information.</remarks>
        /// <typeparam name="T">Type of table for which clear schema.</typeparam>
        public void ClearSchema<T>()
        {
            ClearSchema(typeof(T));
        }

        /// <summary>Clears the schema from cache.</summary>
        /// <remarks>Use this method to refresh table information.</remarks>
        /// <param name="table">Type of table for which clear schema.</param>
        public void ClearSchema(Type table)
        {
            lock (SyncLock)
                if (Schema.ContainsKey(table.FullName))
                    Schema.Remove(table.FullName);
        }

        /// <summary>Clears the all schemas from cache.</summary>
        /// <remarks>Use this method to refresh all table information.</remarks>
        public void ClearSchema()
        {
            lock (SyncLock)
                Schema.Clear();
        }

        /// <summary>Get schema describing objects from reader.</summary>
        /// <param name="table">Table from which extract column info.</param>
        /// <param name="owner">Owner of table from which extract column info.</param>
        /// <returns>List of <see cref="DynamicSchemaColumn"/> objects .
        /// If your database doesn't get those values in upper case (like most of the databases) you should override this method.</returns>
        protected virtual IEnumerable<DynamicSchemaColumn> ReadSchema(string table, string owner)
        {
            using (var con = Open())
            using (var cmd = con.CreateCommand()
                .SetCommand(string.Format("SELECT * FROM {0}{1} WHERE 1 = 0",
                    !string.IsNullOrEmpty(owner) ? string.Format("{0}.", DecorateName(owner)) : string.Empty,
                    DecorateName(table))))
                return ReadSchema(cmd).ToList();
        }

        /// <summary>Get schema describing objects from reader.</summary>
        /// <param name="cmd">Command containing query to execute.</param>
        /// <returns>List of <see cref="DynamicSchemaColumn"/> objects .
        /// If your database doesn't get those values in upper case (like most of the databases) you should override this method.</returns>
        protected virtual IEnumerable<DynamicSchemaColumn> ReadSchema(IDbCommand cmd)
        {
            using (var rdr = cmd.ExecuteReader(CommandBehavior.SchemaOnly | CommandBehavior.KeyInfo))
                foreach (DataRow col in rdr.GetSchemaTable().Rows)
                {
                    var c = col.RowToDynamicUpper();

                    yield return new DynamicSchemaColumn
                    {
                        Name = c.COLUMNNAME,
                        Type = DynamicExtensions.TypeMap.TryGetNullable((Type)c.DATATYPE) ?? DbType.String,
                        IsKey = c.ISKEY ?? false,
                        IsUnique = c.ISUNIQUE ?? false,
                        Size = (int)(c.COLUMNSIZE ?? 0),
                        Precision = (byte)(c.NUMERICPRECISION ?? 0),
                        Scale = (byte)(c.NUMERICSCALE ?? 0)
                    };
                }
        }

        private Dictionary<string, DynamicSchemaColumn> BuildAndCacheSchema(string tableName, DynamicTypeMap mapper, string owner = null)
        {
            Dictionary<string, DynamicSchemaColumn> schema = null;

            if (mapper != null)
                tableName = mapper.Table == null || string.IsNullOrEmpty(mapper.Table.Name) ?
                    mapper.Type.Name : mapper.Table.Name;

            bool databaseSchemaSupport = !string.IsNullOrEmpty(tableName) &&
                (Options & DynamicDatabaseOptions.SupportSchema) == DynamicDatabaseOptions.SupportSchema;
            bool mapperSchema = mapper != null && mapper.Table != null && (mapper.Table.Override || !databaseSchemaSupport);

            #region Database schema

            if (databaseSchemaSupport && !Schema.ContainsKey(tableName.ToLower()))
            {
                schema = ReadSchema(tableName, owner)
                    .Distinct()
                    .ToDictionary(k => k.Name.ToLower(), k => k);

                Schema[tableName.ToLower()] = schema;
            }

            #endregion Database schema

            #region Type schema

            if ((mapperSchema && !Schema.ContainsKey(mapper.Type.FullName)) ||
                (schema == null && mapper != null && !mapper.Type.IsAnonymous()))
            {
                // TODO: Ged rid of this monster below...
                if (databaseSchemaSupport)
                {
                    #region Merge with db schema

                    schema = mapper.ColumnsMap.ToDictionary(k => k.Key, (v) =>
                    {
                        DynamicSchemaColumn? col = Schema[tableName.ToLower()].TryGetNullable(v.Key);

                        return new DynamicSchemaColumn
                        {
                            Name = DynamicExtensions.Coalesce<string>(
                                v.Value.Column == null || string.IsNullOrEmpty(v.Value.Column.Name) ? null : v.Value.Column.Name,
                                col.HasValue && !string.IsNullOrEmpty(col.Value.Name) ? col.Value.Name : null,
                                v.Value.Name),
                            IsKey = DynamicExtensions.CoalesceNullable<bool>(
                                v.Value.Column != null ? v.Value.Column.IsKey : false,
                                col.HasValue ? col.Value.IsKey : false).Value,
                            Type = DynamicExtensions.CoalesceNullable<DbType>(
                                v.Value.Column != null ? v.Value.Column.Type : null,
                                col.HasValue ? col.Value.Type : DynamicExtensions.TypeMap.TryGetNullable(v.Value.Type) ?? DbType.String).Value,
                            IsUnique = DynamicExtensions.CoalesceNullable<bool>(
                                v.Value.Column != null ? v.Value.Column.IsUnique : null,
                                col.HasValue ? col.Value.IsUnique : false).Value,
                            Size = DynamicExtensions.CoalesceNullable<int>(
                                v.Value.Column != null ? v.Value.Column.Size : null,
                                col.HasValue ? col.Value.Size : 0).Value,
                            Precision = DynamicExtensions.CoalesceNullable<byte>(
                                v.Value.Column != null ? v.Value.Column.Precision : null,
                                col.HasValue ? col.Value.Precision : (byte)0).Value,
                            Scale = DynamicExtensions.CoalesceNullable<byte>(
                                v.Value.Column != null ? v.Value.Column.Scale : null,
                                col.HasValue ? col.Value.Scale : (byte)0).Value,
                        };
                    });

                    #endregion Merge with db schema
                }
                else
                {
                    #region MapEnumerable based only on type

                    schema = mapper.ColumnsMap.ToDictionary(k => k.Key,
                        v => new DynamicSchemaColumn
                        {
                            Name = DynamicExtensions.Coalesce<string>(v.Value.Column == null || string.IsNullOrEmpty(v.Value.Column.Name) ? null : v.Value.Column.Name, v.Value.Name),
                            IsKey = DynamicExtensions.CoalesceNullable<bool>(v.Value.Column != null ? v.Value.Column.IsKey : false, false).Value,
                            Type = DynamicExtensions.CoalesceNullable<DbType>(v.Value.Column != null ? v.Value.Column.Type : null, DynamicExtensions.TypeMap.TryGetNullable(v.Value.Type) ?? DbType.String).Value,
                            IsUnique = DynamicExtensions.CoalesceNullable<bool>(v.Value.Column != null ? v.Value.Column.IsUnique : null, false).Value,
                            Size = DynamicExtensions.CoalesceNullable<int>(v.Value.Column != null ? v.Value.Column.Size : null, 0).Value,
                            Precision = DynamicExtensions.CoalesceNullable<byte>(v.Value.Column != null ? v.Value.Column.Precision : null, 0).Value,
                            Scale = DynamicExtensions.CoalesceNullable<byte>(v.Value.Column != null ? v.Value.Column.Scale : null, 0).Value,
                        });

                    #endregion MapEnumerable based only on type
                }
            }

            if (mapper != null && schema != null)
                Schema[mapper.Type.FullName] = schema;

            #endregion Type schema

            return schema;
        }

        #endregion Schema

        #region Decorators

        /// <summary>Gets or sets left side decorator for database objects.</summary>
        public string LeftDecorator
        {
            get { return _leftDecorator; }
            set
            {
                _leftDecorator = value;
                _leftDecoratorIsInInvalidMembersChars =
                    _leftDecorator.Length == 1 && StringExtensions.InvalidMemberChars.Contains(_leftDecorator[0]);
            }
        }

        /// <summary>Gets or sets right side decorator for database objects.</summary>
        public string RightDecorator
        {
            get { return _rightDecorator; }
            set
            {
                _rightDecorator = value;
                _rightDecoratorIsInInvalidMembersChars =
                    _rightDecorator.Length == 1 && StringExtensions.InvalidMemberChars.Contains(_rightDecorator[0]);
            }
        }

        /// <summary>Gets or sets parameter name format.</summary>
        public string ParameterFormat { get { return _parameterFormat; } set { _parameterFormat = value; } }

        /// <summary>Decorate string representing name of database object.</summary>
        /// <param name="name">Name of database object.</param>
        /// <returns>Decorated name of database object.</returns>
        public string DecorateName(string name)
        {
            return String.Concat(_leftDecorator, name, _rightDecorator);
        }

        /// <summary>Strip string representing name of database object from decorators.</summary>
        /// <param name="name">Decorated name of database object.</param>
        /// <returns>Not decorated name of database object.</returns>
        public string StripName(string name)
        {
            string res = name.Trim(StringExtensions.InvalidMemberChars);

            if (!_leftDecoratorIsInInvalidMembersChars && res.StartsWith(_leftDecorator))
                res = res.Substring(_leftDecorator.Length);

            if (!_rightDecoratorIsInInvalidMembersChars && res.EndsWith(_rightDecorator))
                res = res.Substring(0, res.Length - _rightDecorator.Length);

            return res;
        }

        /// <summary>Decorate string representing name of database object.</summary>
        /// <param name="sb">String builder to which add decorated name.</param>
        /// <param name="name">Name of database object.</param>
        public void DecorateName(StringBuilder sb, string name)
        {
            sb.Append(_leftDecorator);
            sb.Append(name);
            sb.Append(_rightDecorator);
        }

        /// <summary>Get database parameter name.</summary>
        /// <param name="parameter">Friendly parameter name or number.</param>
        /// <returns>Formatted parameter name.</returns>
        public string GetParameterName(object parameter)
        {
            return String.Format(_parameterFormat, parameter).Replace(" ", "_");
        }

        /// <summary>Get database parameter name.</summary>
        /// <param name="sb">String builder to which add parameter name.</param>
        /// <param name="parameter">Friendly parameter name or number.</param>
        public void GetParameterName(StringBuilder sb, object parameter)
        {
            sb.AppendFormat(_parameterFormat, parameter.ToString().Replace(" ", "_"));
        }

        /// <summary>Dumps the command into console output.</summary>
        /// <param name="cmd">The command to dump.</param>
        public virtual void DumpCommand(IDbCommand cmd)
        {
            cmd.Dump(Console.Out);
        }

        #endregion Decorators

        #region Connection

        /// <summary>Open managed connection.</summary>
        /// <returns>Opened connection.</returns>
        public IDbConnection Open()
        {
            IDbConnection conn = null;
            DynamicConnection ret = null;
            bool opened = false;

            lock (SyncLock)
            {
                if (_tempConn == null)
                {
                    if (TransactionPool.Count == 0 || !_singleConnection)
                    {
                        conn = _provider.CreateConnection();
                        conn.ConnectionString = _connectionString;
                        conn.Open();
                        opened = true;

                        TransactionPool.Add(conn, new Stack<IDbTransaction>());
                        CommandsPool.Add(conn, new List<IDbCommand>());
                    }
                    else
                    {
                        conn = TransactionPool.Keys.First();

                        if (conn.State != ConnectionState.Open)
                        {
                            conn.Open();
                            opened = true;
                        }
                    }

                    ret = new DynamicConnection(this, conn, _singleTransaction);
                }
                else
                    ret = _tempConn;
            }

            if (opened)
                ExecuteInitCommands(ret);

            return ret;
        }

        /// <summary>Close connection if we are allowed to.</summary>
        /// <param name="connection">Connection to manage.</param>
        internal void Close(IDbConnection connection)
        {
            if (connection == null)
                return;

            if (!_singleConnection && connection != null && TransactionPool.ContainsKey(connection))
            {
                // Close all commands
                if (CommandsPool.ContainsKey(connection))
                {
                    var tmp = CommandsPool[connection].ToList();
                    tmp.ForEach(cmd => cmd.Dispose());

                    CommandsPool[connection].Clear();
                }

                // Rollback remaining transactions
                while (TransactionPool[connection].Count > 0)
                {
                    IDbTransaction trans = TransactionPool[connection].Pop();
                    trans.Rollback();
                    trans.Dispose();
                }

                // Close connection
                if (connection.State == ConnectionState.Open)
                    connection.Close();

                // remove from pools
                lock (SyncLock)
                {
                    TransactionPool.Remove(connection);
                    CommandsPool.Remove(connection);
                }

                // Set stamp
                _poolStamp = DateTime.Now.Ticks;

                // Dispose the corpse
                connection.Dispose();
            }
        }

        /// <summary>Gets or sets contains commands executed when connection is opened.</summary>
        public List<string> InitCommands { get; set; }

        private void ExecuteInitCommands(IDbConnection conn)
        {
            if (InitCommands != null)
                using (IDbCommand command = conn.CreateCommand())
                    foreach (string commandText in InitCommands)
                        command
                            .SetCommand(commandText)
                            .ExecuteNonQuery();
        }

        #endregion Connection

        #region Transaction

        /// <summary>Begins a global database transaction.</summary>
        /// <remarks>Using this method connection is set to single open
        /// connection until all transactions are finished.</remarks>
        /// <returns>Returns <see cref="DynamicTransaction"/> representation.</returns>
        public IDbTransaction BeginTransaction()
        {
            _tempConn = Open() as DynamicConnection;

            return _tempConn.BeginTransaction(null, null, () =>
            {
                var t = TransactionPool.TryGetValue(_tempConn.Connection);

                if (t == null | t.Count == 0)
                {
                    _tempConn.Dispose();
                    _tempConn = null;
                }
            });
        }

        /// <summary>Begins a database transaction with the specified
        /// <see cref="System.Data.IsolationLevel"/> value.</summary>
        /// <param name="il">One of the <see cref="System.Data.IsolationLevel"/> values.</param>
        /// <returns>Returns <see cref="DynamicTransaction"/> representation.</returns>
        public IDbTransaction BeginTransaction(IsolationLevel il)
        {
            _tempConn = Open() as DynamicConnection;

            return _tempConn.BeginTransaction(il, null, () =>
            {
                var t = TransactionPool.TryGetValue(_tempConn.Connection);

                if (t == null | t.Count == 0)
                {
                    _tempConn.Dispose();
                    _tempConn = null;
                }
            });
        }

        /// <summary>Begins a database transaction with the specified
        /// <see cref="System.Data.IsolationLevel"/> value.</summary>
        /// <param name="custom">Custom parameter describing transaction options.</param>
        /// <returns>Returns <see cref="DynamicTransaction"/> representation.</returns>
        public IDbTransaction BeginTransaction(object custom)
        {
            _tempConn = Open() as DynamicConnection;

            return _tempConn.BeginTransaction(null, custom, () =>
            {
                var t = TransactionPool.TryGetValue(_tempConn.Connection);

                if (t == null | t.Count == 0)
                {
                    _tempConn.Dispose();
                    _tempConn = null;
                }
            });
        }

        #endregion Transaction

        #region IExtendedDisposable Members

        /// <summary>Performs application-defined tasks associated with freeing,
        /// releasing, or resetting unmanaged resources.</summary>
        public void Dispose()
        {
#if !DYNAMORM_OMMIT_OLDSYNTAX
            var tables = TablesCache.Values.ToList();
            TablesCache.Clear();

            tables.ForEach(t => t.Dispose());
#endif

            foreach (var con in TransactionPool)
            {
                // Close all commands
                if (CommandsPool.ContainsKey(con.Key))
                {
                    var tmp = CommandsPool[con.Key].ToList();
                    tmp.ForEach(cmd => cmd.Dispose());

                    CommandsPool[con.Key].Clear();
                }

                // Rollback remaining transactions
                while (con.Value.Count > 0)
                {
                    IDbTransaction trans = con.Value.Pop();
                    trans.Rollback();
                    trans.Dispose();
                }

                // Close connection
                if (con.Key.State == ConnectionState.Open)
                    con.Key.Close();

                // Dispose it
                con.Key.Dispose();
            }

            // Clear pools
            lock (SyncLock)
            {
                TransactionPool.Clear();
                CommandsPool.Clear();
            }

            IsDisposed = true;
        }

        /// <summary>Gets a value indicating whether this instance is disposed.</summary>
        public bool IsDisposed { get; private set; }

        #endregion IExtendedDisposable Members
    }

    /// <summary>Represents database connection options.</summary>
    [Flags]
    [System.Reflection.ObfuscationAttribute(Feature = "renaming", ApplyToMembers = true)]
    public enum DynamicDatabaseOptions
    {
        /// <summary>No specific options.</summary>
        None = 0x00000000,

        /// <summary>Only single persistent database connection.</summary>
        SingleConnection = 0x00000001,

        /// <summary>Only one transaction.</summary>
        SingleTransaction = 0x00000002,

        /// <summary>Database supports top syntax (SELECT TOP x ... FROM ...).</summary>
        SupportTop = 0x00000080,

        /// <summary>Database supports limit offset syntax (SELECT ... FROM ... LIMIT x OFFSET y).</summary>
        SupportLimitOffset = 0x00000040,

        /// <summary>Database support standard schema.</summary>
        SupportSchema = 0x00000010,

        /// <summary>Database support stored procedures (EXEC procedure ...).</summary>
        SupportStoredProcedures = 0x00000020,

        /// <summary>Debug option allowing to enable command dumps by default.</summary>
        DumpCommands = 0x01000000,
    }

    /// <summary>Extension to ORM objects.</summary>
    public static class DynamicExtensions
    {
        #region Type column map

        /// <summary>MapEnumerable of .NET types to <see cref="DbType"/>.</summary>
        public static readonly Dictionary<Type, DbType> TypeMap = new Dictionary<Type, DbType>()
        {
            { typeof(byte), DbType.Byte },
            { typeof(sbyte), DbType.SByte },
            { typeof(short), DbType.Int16 },
            { typeof(ushort), DbType.UInt16 },
            { typeof(int), DbType.Int32 },
            { typeof(uint), DbType.UInt32 },
            { typeof(long), DbType.Int64 },
            { typeof(ulong), DbType.UInt64 },
            { typeof(float), DbType.Single },
            { typeof(double), DbType.Double },
            { typeof(decimal), DbType.Decimal },
            { typeof(bool), DbType.Boolean },
            { typeof(string), DbType.String },
            { typeof(char), DbType.StringFixedLength },
            { typeof(Guid), DbType.Guid },
            { typeof(DateTime), DbType.DateTime },
            { typeof(DateTimeOffset), DbType.DateTimeOffset },
            { typeof(byte[]), DbType.Binary },
            { typeof(byte?), DbType.Byte },
            { typeof(sbyte?), DbType.SByte },
            { typeof(short?), DbType.Int16 },
            { typeof(ushort?), DbType.UInt16 },
            { typeof(int?), DbType.Int32 },
            { typeof(uint?), DbType.UInt32 },
            { typeof(long?), DbType.Int64 },
            { typeof(ulong?), DbType.UInt64 },
            { typeof(float?), DbType.Single },
            { typeof(double?), DbType.Double },
            { typeof(decimal?), DbType.Decimal },
            { typeof(bool?), DbType.Boolean },
            { typeof(char?), DbType.StringFixedLength },
            { typeof(Guid?), DbType.Guid },
            { typeof(DateTime?), DbType.DateTime },
            { typeof(DateTimeOffset?), DbType.DateTimeOffset }
        };

        #endregion Type column map

        #region Command extensions

        /// <summary>Set <see cref="System.Data.IDbCommand"/> connection on the fly.</summary>
        /// <param name="command"><see cref="System.Data.IDbCommand"/> in which changes will be made.</param>
        /// <param name="connection"><see cref="System.Data.IDbConnection"/> which will be set to <see cref="System.Data.IDbCommand"/> instance.</param>
        /// <returns>Returns edited <see cref="System.Data.IDbCommand"/> instance.</returns>
        public static IDbCommand SetConnection(this IDbCommand command, IDbConnection connection)
        {
            command.Connection = connection;

            return command;
        }

        /// <summary>Set <see cref="System.Data.IDbCommand"/> connection on the fly.</summary>
        /// <param name="command"><see cref="System.Data.IDbCommand"/> in which changes will be made.</param>
        /// <param name="transaction"><see cref="System.Data.IDbTransaction"/> which will be set to <see cref="System.Data.IDbCommand"/> instance.</param>
        /// <returns>Returns edited <see cref="System.Data.IDbCommand"/> instance.</returns>
        public static IDbCommand SetTransaction(this IDbCommand command, IDbTransaction transaction)
        {
            command.Transaction = transaction;

            return command;
        }

        #region SetCommand

        /// <summary>Set <see cref="System.Data.IDbCommand"/> properties on the fly.</summary>
        /// <param name="command"><see cref="System.Data.IDbCommand"/> in which changes will be made.</param>
        /// <param name="commandType">Indicates or specifies how the <see cref="System.Data.IDbCommand.CommandText"/> property is interpreted.</param>
        /// <param name="commandTimeout">The wait time before terminating the attempt to execute a command and generating an error.</param>
        /// <param name="commandText">The text command to run against the data source.</param>
        /// <param name="args">Arguments used to format command.</param>
        /// <returns>Returns edited <see cref="System.Data.IDbCommand"/> instance.</returns>
        public static IDbCommand SetCommand(this IDbCommand command, CommandType commandType, int commandTimeout, string commandText, params object[] args)
        {
            command.CommandType = commandType;
            command.CommandTimeout = commandTimeout;

            if (args != null && args.Length > 0)
                command.CommandText = string.Format(commandText, args);
            else
                command.CommandText = commandText;

            return command;
        }

        /// <summary>Set <see cref="System.Data.IDbCommand"/> properties on the fly.</summary>
        /// <param name="command"><see cref="System.Data.IDbCommand"/> in which changes will be made.</param>
        /// <param name="commandTimeout">The wait time before terminating the attempt to execute a command and generating an error.</param>
        /// <param name="commandText">The text command to run against the data source.</param>
        /// <param name="args">Arguments used to format command.</param>
        /// <returns>Returns edited <see cref="System.Data.IDbCommand"/> instance.</returns>
        public static IDbCommand SetCommand(this IDbCommand command, int commandTimeout, string commandText, params object[] args)
        {
            command.CommandTimeout = commandTimeout;

            if (args != null && args.Length > 0)
                command.CommandText = string.Format(commandText, args);
            else
                command.CommandText = commandText;

            return command;
        }

        /// <summary>Set <see cref="System.Data.IDbCommand"/> properties on the fly.</summary>
        /// <param name="command"><see cref="System.Data.IDbCommand"/> in which changes will be made.</param>
        /// <param name="commandType">Indicates or specifies how the <see cref="System.Data.IDbCommand.CommandText"/> property is interpreted.</param>
        /// <param name="commandText">The text command to run against the data source.</param>
        /// <param name="args">Arguments used to format command.</param>
        /// <returns>Returns edited <see cref="System.Data.IDbCommand"/> instance.</returns>
        public static IDbCommand SetCommand(this IDbCommand command, CommandType commandType, string commandText, params object[] args)
        {
            command.CommandType = commandType;

            if (args != null && args.Length > 0)
                command.CommandText = string.Format(commandText, args);
            else
                command.CommandText = commandText;

            return command;
        }

        /// <summary>Set <see cref="System.Data.IDbCommand"/> properties on the fly.</summary>
        /// <param name="command"><see cref="System.Data.IDbCommand"/> in which changes will be made.</param>
        /// <param name="commandText">The text command to run against the data source.</param>
        /// <param name="args">Arguments used to format command.</param>
        /// <returns>Returns edited <see cref="System.Data.IDbCommand"/> instance.</returns>
        public static IDbCommand SetCommand(this IDbCommand command, string commandText, params object[] args)
        {
            if (args != null && args.Length > 0)
                command.CommandText = string.Format(commandText, args);
            else
                command.CommandText = commandText;

            return command;
        }

        /// <summary>Set <see cref="System.Data.IDbCommand"/> properties on the fly.</summary>
        /// <param name="command"><see cref="System.Data.IDbCommand"/> in which changes will be made.</param>
        /// <param name="builder">Command builder.</param>
        /// <returns>Returns edited <see cref="System.Data.IDbCommand"/> instance.</returns>
        public static IDbCommand SetCommand(this IDbCommand command, IDynamicQueryBuilder builder)
        {
            builder.FillCommand(command);

            return command;
        }

        #endregion SetCommand

        #region AddParameter

        /// <summary>Extension method for adding in a bunch of parameters.</summary>
        /// <param name="cmd">Command to handle.</param>
        /// <param name="database">Database object required to get proper formatting.</param>
        /// <param name="args">Items to add.</param>
        /// <returns>Returns edited <see cref="System.Data.IDbCommand"/> instance.</returns>
        public static IDbCommand AddParameters(this IDbCommand cmd, DynamicDatabase database, params object[] args)
        {
            if (args != null && args.Count() > 0)
                foreach (var item in args)
                    cmd.AddParameter(database, item);

            return cmd;
        }

        /// <summary>Extension method for adding in a bunch of parameters.</summary>
        /// <param name="cmd">Command to handle.</param>
        /// <param name="database">Database object required to get proper formatting.</param>
        /// <param name="args">Items to add in an expando object.</param>
        /// <returns>Returns edited <see cref="System.Data.IDbCommand"/> instance.</returns>
        public static IDbCommand AddParameters(this IDbCommand cmd, DynamicDatabase database, ExpandoObject args)
        {
            if (args != null && args.Count() > 0)
                foreach (var item in args.ToDictionary())
                    cmd.AddParameter(database, item.Key, item.Value);

            return cmd;
        }

        /// <summary>Extension for adding single parameter determining only type of object.</summary>
        /// <param name="cmd">Command to handle.</param>
        /// <param name="database">Database object required to get proper formatting.</param>
        /// <param name="item">Items to add.</param>
        /// <returns>Returns edited <see cref="System.Data.IDbCommand"/> instance.</returns>
        public static IDbCommand AddParameter(this IDbCommand cmd, DynamicDatabase database, object item)
        {
            return cmd.AddParameter(database, database.GetParameterName(cmd.Parameters.Count), item);
        }

        /// <summary>Extension for adding single parameter determining only type of object.</summary>
        /// <param name="cmd">Command to handle.</param>
        /// <param name="database">Database object required to get proper formatting.</param>
        /// <param name="name">Name of parameter.</param>
        /// <param name="item">Items to add.</param>
        /// <returns>Returns edited <see cref="System.Data.IDbCommand"/> instance.</returns>
        public static IDbCommand AddParameter(this IDbCommand cmd, DynamicDatabase database, string name, object item)
        {
            var p = cmd.CreateParameter();
            p.ParameterName = name;

            if (item == null || item == DBNull.Value)
                p.Value = DBNull.Value;
            else
            {
                Type type = item.GetType();

                p.DbType = TypeMap.TryGetNullable(type) ?? DbType.String;

                if (type == typeof(ExpandoObject))
                    p.Value = ((IDictionary<string, object>)item).Values.FirstOrDefault();
                else
                    p.Value = item;

                if (p.DbType == DbType.String)
                    p.Size = item.ToString().Length > 4000 ? -1 : 4000;
            }

            cmd.Parameters.Add(p);

            return cmd;
        }

        /// <summary>Extension for adding single parameter determining only type of object.</summary>
        /// <param name="cmd">Command to handle.</param>
        /// <param name="builder">Query builder containing schema.</param>
        /// <param name="col">Column schema to use.</param>
        /// <param name="value">Parameter value.</param>
        /// <returns>Returns edited <see cref="System.Data.IDbCommand"/> instance.</returns>
        public static IDbCommand AddParameter(this IDbCommand cmd, IDynamicQueryBuilder builder, DynamicSchemaColumn? col, object value)
        {
            var p = cmd.CreateParameter();
            p.ParameterName = builder.Database.GetParameterName(cmd.Parameters.Count);

            if (col.HasValue)
            {
                p.DbType = col.Value.Type;

                if ((builder.Database.Options & DynamicDatabaseOptions.SupportSchema) == DynamicDatabaseOptions.SupportSchema)
                {
                    p.Size = col.Value.Size;
                    p.Precision = col.Value.Precision;
                    p.Scale = col.Value.Scale;

                    // Quick fix - review that
                    // Quick fix 2 - use item.Schema in that case
                    if (p.Scale > p.Precision)
                        p.Scale = 4;
                }

                p.Value = value == null ? DBNull.Value : value;
            }
            else if (value == null || value == DBNull.Value)
                p.Value = DBNull.Value;
            else
            {
                p.DbType = TypeMap.TryGetNullable(value.GetType()) ?? DbType.String;

                if (p.DbType == DbType.String)
                    p.Size = value.ToString().Length > 4000 ? -1 : 4000;

                p.Value = value;
            }

            cmd.Parameters.Add(p);

            return cmd;
        }

        /// <summary>Extension for adding single parameter determining only type of object.</summary>
        /// <param name="cmd">Command to handle.</param>
        /// <param name="builder">Query builder containing schema.</param>
        /// <param name="item">Column item to add.</param>
        /// <returns>Returns edited <see cref="System.Data.IDbCommand"/> instance.</returns>
        public static IDbCommand AddParameter(this IDbCommand cmd, IDynamicQueryBuilder builder, DynamicColumn item)
        {
            var p = cmd.CreateParameter();
            p.ParameterName = builder.Database.GetParameterName(cmd.Parameters.Count);

            var col = item.Schema ?? (builder as DynamicQueryBuilder)
                .NullOr(b => b.GetColumnFromSchema(item.ColumnName),
                    builder.Tables.FirstOrDefault()
                        .NullOr(t => t.Schema
                            .NullOr(s => s.TryGetNullable(item.ColumnName.ToLower()), null), null));

            if (col.HasValue)
            {
                p.DbType = col.Value.Type;

                if (builder.SupportSchema)
                {
                    p.Size = col.Value.Size;
                    p.Precision = col.Value.Precision;
                    p.Scale = col.Value.Scale;

                    // Quick fix - review that
                    // Quick fix 2 - use item.Schema in that case
                    if (p.Scale > p.Precision)
                        p.Scale = 4;
                }

                p.Value = item.Value == null ? DBNull.Value : item.Value;
            }
            else if (item.Value == null || item.Value == DBNull.Value)
                p.Value = DBNull.Value;
            else
            {
                p.DbType = TypeMap.TryGetNullable(item.Value.GetType()) ?? DbType.String;

                if (p.DbType == DbType.String)
                    p.Size = item.Value.ToString().Length > 4000 ? -1 : 4000;

                p.Value = item.Value;
            }

            cmd.Parameters.Add(p);

            return cmd;
        }

        /// <summary>Add <see cref="System.Data.IDataParameter"/> to <see cref="System.Data.IDbCommand"/> on the fly.</summary>
        /// <param name="command"><see cref="System.Data.IDbCommand"/> to which parameter will be added.</param>
        /// <param name="parameterName">The name of the <see cref="System.Data.IDataParameter"/>.</param>
        /// <param name="parameterDirection">Value indicating whether the parameter is input-only, output-only, bidirectional, or a stored procedure return value <see cref="System.Data.IDataParameter"/>.</param>
        /// <param name="databaseType">The <see cref="System.Data.DbType"/> of the <see cref="System.Data.IDataParameter"/>.</param>
        /// <param name="size">The size of the parameter.</param>
        /// <param name="precision">Indicates the precision of numeric parameters.</param>
        /// <param name="scale">Indicates the scale of numeric parameters.</param>
        /// <param name="value">The value of the <see cref="System.Data.IDataParameter"/>.</param>
        /// <returns>Returns edited <see cref="System.Data.IDbCommand"/> instance.</returns>
        public static IDbCommand AddParameter(this IDbCommand command, string parameterName, ParameterDirection parameterDirection, DbType databaseType, int size, byte precision, byte scale, object value)
        {
            IDbDataParameter param = command.CreateParameter();
            param.ParameterName = parameterName;
            param.Direction = parameterDirection;
            param.DbType = databaseType;
            param.Size = size;
            param.Precision = precision;
            param.Scale = scale;
            param.Value = value;
            command.Parameters.Add(param);

            return command;
        }

        /// <summary>Add <see cref="System.Data.IDataParameter"/> to <see cref="System.Data.IDbCommand"/> on the fly.</summary>
        /// <param name="command"><see cref="System.Data.IDbCommand"/> to which parameter will be added.</param>
        /// <param name="parameterName">The name of the <see cref="System.Data.IDataParameter"/>.</param>
        /// <param name="parameterDirection">Value indicating whether the parameter is input-only, output-only, bidirectional, or a stored procedure return value <see cref="System.Data.IDataParameter"/>.</param>
        /// <param name="databaseType">The <see cref="System.Data.DbType"/> of the <see cref="System.Data.IDataParameter"/>.</param>
        /// <param name="size">The size of the parameter.</param>
        /// <param name="precision">Indicates the precision of numeric parameters.</param>
        /// <param name="scale">Indicates the scale of numeric parameters.</param>
        /// <returns>Returns edited <see cref="System.Data.IDbCommand"/> instance.</returns>
        public static IDbCommand AddParameter(this IDbCommand command, string parameterName, ParameterDirection parameterDirection, DbType databaseType, int size, byte precision, byte scale)
        {
            IDbDataParameter param = command.CreateParameter();
            param.ParameterName = parameterName;
            param.Direction = parameterDirection;
            param.DbType = databaseType;
            param.Size = size;
            param.Precision = precision;
            param.Scale = scale;
            command.Parameters.Add(param);

            return command;
        }

        /// <summary>Add <see cref="System.Data.IDataParameter"/> to <see cref="System.Data.IDbCommand"/> on the fly.</summary>
        /// <param name="command"><see cref="System.Data.IDbCommand"/> to which parameter will be added.</param>
        /// <param name="parameterName">The name of the <see cref="System.Data.IDataParameter"/>.</param>
        /// <param name="parameterDirection">Value indicating whether the parameter is input-only, output-only, bidirectional, or a stored procedure return value <see cref="System.Data.IDataParameter"/>.</param>
        /// <param name="databaseType">The <see cref="System.Data.DbType"/> of the <see cref="System.Data.IDataParameter"/>.</param>
        /// <param name="precision">Indicates the precision of numeric parameters.</param>
        /// <param name="scale">Indicates the scale of numeric parameters.</param>
        /// <param name="value">The value of the <see cref="System.Data.IDataParameter"/>.</param>
        /// <returns>Returns edited <see cref="System.Data.IDbCommand"/> instance.</returns>
        public static IDbCommand AddParameter(this IDbCommand command, string parameterName, ParameterDirection parameterDirection, DbType databaseType, byte precision, byte scale, object value)
        {
            IDbDataParameter param = command.CreateParameter();
            param.ParameterName = parameterName;
            param.Direction = parameterDirection;
            param.DbType = databaseType;
            param.Precision = precision;
            param.Scale = scale;
            param.Value = value;
            command.Parameters.Add(param);

            return command;
        }

        /// <summary>Add <see cref="System.Data.IDataParameter"/> to <see cref="System.Data.IDbCommand"/> on the fly.</summary>
        /// <param name="command"><see cref="System.Data.IDbCommand"/> to which parameter will be added.</param>
        /// <param name="parameterName">The name of the <see cref="System.Data.IDataParameter"/>.</param>
        /// <param name="databaseType">The <see cref="System.Data.DbType"/> of the <see cref="System.Data.IDataParameter"/>.</param>
        /// <param name="precision">Indicates the precision of numeric parameters.</param>
        /// <param name="scale">Indicates the scale of numeric parameters.</param>
        /// <param name="value">The value of the <see cref="System.Data.IDataParameter"/>.</param>
        /// <returns>Returns edited <see cref="System.Data.IDbCommand"/> instance.</returns>
        public static IDbCommand AddParameter(this IDbCommand command, string parameterName, DbType databaseType, byte precision, byte scale, object value)
        {
            IDbDataParameter param = command.CreateParameter();
            param.ParameterName = parameterName;
            param.DbType = databaseType;
            param.Precision = precision;
            param.Scale = scale;
            param.Value = value;
            command.Parameters.Add(param);

            return command;
        }

        /// <summary>Add <see cref="System.Data.IDataParameter"/> to <see cref="System.Data.IDbCommand"/> on the fly.</summary>
        /// <param name="command"><see cref="System.Data.IDbCommand"/> to which parameter will be added.</param>
        /// <param name="parameterName">The name of the <see cref="System.Data.IDataParameter"/>.</param>
        /// <param name="databaseType">The <see cref="System.Data.DbType"/> of the <see cref="System.Data.IDataParameter"/>.</param>
        /// <param name="precision">Indicates the precision of numeric parameters.</param>
        /// <param name="scale">Indicates the scale of numeric parameters.</param>
        /// <returns>Returns edited <see cref="System.Data.IDbCommand"/> instance.</returns>
        public static IDbCommand AddParameter(this IDbCommand command, string parameterName, DbType databaseType, byte precision, byte scale)
        {
            IDbDataParameter param = command.CreateParameter();
            param.ParameterName = parameterName;
            param.DbType = databaseType;
            param.Precision = precision;
            param.Scale = scale;
            command.Parameters.Add(param);

            return command;
        }

        /// <summary>Add <see cref="System.Data.IDataParameter"/> to <see cref="System.Data.IDbCommand"/> on the fly.</summary>
        /// <param name="command"><see cref="System.Data.IDbCommand"/> to which parameter will be added.</param>
        /// <param name="parameterName">The name of the <see cref="System.Data.IDataParameter"/>.</param>
        /// <param name="parameterDirection">Value indicating whether the parameter is input-only, output-only, bidirectional, or a stored procedure return value <see cref="System.Data.IDataParameter"/>.</param>
        /// <param name="databaseType">The <see cref="System.Data.DbType"/> of the <see cref="System.Data.IDataParameter"/>.</param>
        /// <param name="size">The size of the parameter.</param>
        /// <param name="value">The value of the <see cref="System.Data.IDataParameter"/>.</param>
        /// <returns>Returns edited <see cref="System.Data.IDbCommand"/> instance.</returns>
        public static IDbCommand AddParameter(this IDbCommand command, string parameterName, ParameterDirection parameterDirection, DbType databaseType, int size, object value)
        {
            IDbDataParameter param = command.CreateParameter();
            param.ParameterName = parameterName;
            param.Direction = parameterDirection;
            param.DbType = databaseType;
            param.Size = size;
            param.Value = value;
            command.Parameters.Add(param);

            return command;
        }

        /// <summary>Add <see cref="System.Data.IDataParameter"/> to <see cref="System.Data.IDbCommand"/> on the fly.</summary>
        /// <param name="command"><see cref="System.Data.IDbCommand"/> to which parameter will be added.</param>
        /// <param name="parameterName">The name of the <see cref="System.Data.IDataParameter"/>.</param>
        /// <param name="databaseType">The <see cref="System.Data.DbType"/> of the <see cref="System.Data.IDataParameter"/>.</param>
        /// <param name="size">The size of the parameter.</param>
        /// <param name="value">The value of the <see cref="System.Data.IDataParameter"/>.</param>
        /// <returns>Returns edited <see cref="System.Data.IDbCommand"/> instance.</returns>
        public static IDbCommand AddParameter(this IDbCommand command, string parameterName, DbType databaseType, int size, object value)
        {
            IDbDataParameter param = command.CreateParameter();
            param.ParameterName = parameterName;
            param.DbType = databaseType;
            param.Size = size;
            param.Value = value;
            command.Parameters.Add(param);

            return command;
        }

        /// <summary>Add <see cref="System.Data.IDataParameter"/> to <see cref="System.Data.IDbCommand"/> on the fly.</summary>
        /// <param name="command"><see cref="System.Data.IDbCommand"/> to which parameter will be added.</param>
        /// <param name="parameterName">The name of the <see cref="System.Data.IDataParameter"/>.</param>
        /// <param name="databaseType">The <see cref="System.Data.DbType"/> of the <see cref="System.Data.IDataParameter"/>.</param>
        /// <param name="value">The value of the <see cref="System.Data.IDataParameter"/>.</param>
        /// <returns>Returns edited <see cref="System.Data.IDbCommand"/> instance.</returns>
        public static IDbCommand AddParameter(this IDbCommand command, string parameterName, DbType databaseType, object value)
        {
            IDbDataParameter param = command.CreateParameter();
            param.ParameterName = parameterName;
            param.DbType = databaseType;
            param.Value = value;
            command.Parameters.Add(param);

            return command;
        }

        /// <summary>Add <see cref="System.Data.IDataParameter"/> to <see cref="System.Data.IDbCommand"/> on the fly.</summary>
        /// <param name="command"><see cref="System.Data.IDbCommand"/> to which parameter will be added.</param>
        /// <param name="parameterName">The name of the <see cref="System.Data.IDataParameter"/>.</param>
        /// <param name="databaseType">The <see cref="System.Data.DbType"/> of the <see cref="System.Data.IDataParameter"/>.</param>
        /// <param name="size">The size of the parameter.</param>
        /// <returns>Returns edited <see cref="System.Data.IDbCommand"/> instance.</returns>
        public static IDbCommand AddParameter(this IDbCommand command, string parameterName, DbType databaseType, int size)
        {
            IDbDataParameter param = command.CreateParameter();
            param.ParameterName = parameterName;
            param.DbType = databaseType;
            param.Size = size;
            command.Parameters.Add(param);

            return command;
        }

        /// <summary>Add <see cref="System.Data.IDataParameter"/> to <see cref="System.Data.IDbCommand"/> on the fly.</summary>
        /// <param name="command"><see cref="System.Data.IDbCommand"/> to which parameter will be added.</param>
        /// <param name="parameterName">The name of the <see cref="System.Data.IDataParameter"/>.</param>
        /// <param name="databaseType">The <see cref="System.Data.DbType"/> of the <see cref="System.Data.IDataParameter"/>.</param>
        /// <returns>Returns edited <see cref="System.Data.IDbCommand"/> instance.</returns>
        public static IDbCommand AddParameter(this IDbCommand command, string parameterName, DbType databaseType)
        {
            IDbDataParameter param = command.CreateParameter();
            param.ParameterName = parameterName;
            param.DbType = databaseType;
            command.Parameters.Add(param);

            return command;
        }

        #endregion AddParameter

        #region SetParameter

        /// <summary>Set <see cref="System.Data.IDataParameter"/> value for <see cref="System.Data.IDbCommand"/> on the fly.</summary>
        /// <param name="command"><see cref="System.Data.IDbCommand"/> to which parameter will be added.</param>
        /// <param name="parameterName">The name of the <see cref="System.Data.IDataParameter"/>.</param>
        /// <param name="value">Value to set on this parameter.</param>
        /// <returns>Returns edited <see cref="System.Data.IDbCommand"/> instance.</returns>
        public static IDbCommand SetParameter(this IDbCommand command, string parameterName, object value)
        {
            ((IDbDataParameter)command.Parameters[parameterName]).Value = value;

            return command;
        }

        /// <summary>Set <see cref="System.Data.IDataParameter"/> value for <see cref="System.Data.IDbCommand"/> on the fly.</summary>
        /// <param name="command"><see cref="System.Data.IDbCommand"/> to which parameter will be added.</param>
        /// <param name="index">Index of the <see cref="System.Data.IDataParameter"/>.</param>
        /// <param name="value">Value to set on this parameter.</param>
        /// <returns>Returns edited <see cref="System.Data.IDbCommand"/> instance.</returns>
        public static IDbCommand SetParameter(this IDbCommand command, int index, object value)
        {
            ((IDbDataParameter)command.Parameters[index]).Value = value;

            return command;
        }

        #endregion SetParameter

        #region Generic Execution

#if !DYNAMORM_OMMIT_GENERICEXECUTION && !DYNAMORM_OMMIT_TRYPARSE

        /// <summary>Execute scalar and return string if possible.</summary>
        /// <typeparam name="T">Type to parse to.</typeparam>
        /// <param name="command"><see cref="System.Data.IDbCommand"/> which will be executed.</param>
        /// <returns>Returns resulting instance of T from query.</returns>
        public static T ExecuteScalarAs<T>(this IDbCommand command)
        {
            return ExecuteScalarAs<T>(command, default(T), null);
        }

        /// <summary>Execute scalar and return string if possible.</summary>
        /// <typeparam name="T">Type to parse to.</typeparam>
        /// <param name="command"><see cref="System.Data.IDbCommand"/> which will be executed.</param>
        /// <param name="handler">Handler of a try parse method.</param>
        /// <returns>Returns resulting instance of T from query.</returns>
        public static T ExecuteScalarAs<T>(this IDbCommand command, DynamicExtensions.TryParseHandler<T> handler)
        {
            return ExecuteScalarAs<T>(command, default(T), handler);
        }

        /// <summary>Execute scalar and return string if possible.</summary>
        /// <typeparam name="T">Type to parse to.</typeparam>
        /// <param name="command"><see cref="System.Data.IDbCommand"/> which will be executed.</param>
        /// <param name="defaultValue">Default result value.</param>
        /// <returns>Returns resulting instance of T from query.</returns>
        public static T ExecuteScalarAs<T>(this IDbCommand command, T defaultValue)
        {
            return ExecuteScalarAs<T>(command, defaultValue, null);
        }

        /// <summary>Execute scalar and return string if possible.</summary>
        /// <typeparam name="T">Type to parse to.</typeparam>
        /// <param name="command"><see cref="System.Data.IDbCommand"/> which will be executed.</param>
        /// <param name="defaultValue">Default result value.</param>
        /// <param name="handler">Handler of a try parse method.</param>
        /// <returns>Returns resulting instance of T from query.</returns>
        public static T ExecuteScalarAs<T>(this IDbCommand command, T defaultValue, DynamicExtensions.TryParseHandler<T> handler)
        {
            T ret = defaultValue;

            object o = command.ExecuteScalar();

            if (o is T)
                return (T)o;
            else if (o != DBNull.Value && o != null)
            {
                var method = typeof(T).GetMethod(
                    "TryParse",
                    new[]
                    {
                        typeof(string),
                        Type.GetType(string.Format("{0}&", typeof(T).FullName))
                    });

                if (handler != null)
                    ret = o.ToString().TryParseDefault<T>(defaultValue, handler);
                else if (method != null)
                    ret = o.ToString().TryParseDefault<T>(defaultValue, delegate(string v, out T r)
                    {
                        r = defaultValue;
                        return (bool)method.Invoke(null, new object[] { v, r });
                    });
                else if (typeof(T) == typeof(string))
                    ret = (T)(o.ToString() as object);
                else if (typeof(T) == typeof(object))
                    ret = (T)o;
                else
                    throw new InvalidOperationException("Provided type can't be parsed using generic approach.");
            }

            return ret;
        }

        /// <summary>Execute enumerator of specified type.</summary>
        /// <typeparam name="T">Type to parse to.</typeparam>
        /// <param name="command"><see cref="System.Data.IDbCommand"/> which will be executed.</param>
        /// <param name="defaultValue">Default result value.</param>
        /// <param name="handler">Handler of a try parse method.</param>
        /// <returns>Returns enumerator of specified type from query.</returns>
        public static IEnumerable<T> ExecuteEnumeratorOf<T>(this IDbCommand command, T defaultValue, DynamicExtensions.TryParseHandler<T> handler) where T : struct
        {
            using (IDataReader reader = command.ExecuteReader())
            {
                var method = typeof(T).GetMethod(
                    "TryParse",
                    new[]
                    {
                        typeof(string),
                        Type.GetType(string.Format("{0}&", typeof(T).FullName))
                    });

                while (reader.Read())
                {
                    T ret = defaultValue;

                    if (!reader.IsDBNull(0))
                    {
                        object o = reader.GetValue(0);

                        if (o is T)
                            ret = (T)o;
                        else if (o != DBNull.Value)
                        {
                            if (handler != null)
                                ret = o.ToString().TryParseDefault<T>(defaultValue, handler);
                            else if (method != null)
                                ret = o.ToString().TryParseDefault<T>(defaultValue, delegate(string v, out T r)
                                {
                                    r = defaultValue;
                                    return (bool)method.Invoke(null, new object[] { v, r });
                                });
                            else if (typeof(T) == typeof(string))
                                ret = (T)(o.ToString() as object);
                            else if (typeof(T) == typeof(object))
                                ret = (T)o;
                            else
                                throw new InvalidOperationException("Provided type can't be parsed using generic approach.");
                        }
                    }

                    yield return ret;
                }
            }
        }

#endif

        #endregion Generic Execution

        /// <summary>Dump command into text writer.</summary>
        /// <param name="command">Command to dump.</param>
        /// <param name="buider">Builder to which write output.</param>
        /// <returns>Returns dumped <see cref="System.Data.IDbCommand"/> instance.</returns>
        public static IDbCommand Dump(this IDbCommand command, StringBuilder buider)
        {
            using (StringWriter sw = new StringWriter(buider))
                return command.Dump(sw);
        }

        /// <summary>Dump command into text writer.</summary>
        /// <param name="command">Command to dump.</param>
        /// <param name="writer">Writer to which write output.</param>
        /// <returns>Returns dumped <see cref="System.Data.IDbCommand"/> instance.</returns>
        public static IDbCommand Dump(this IDbCommand command, TextWriter writer)
        {
            writer.WriteLine("Type: {0}; Timeout: {1}; Query: {2}", command.CommandType, command.CommandTimeout, command.CommandText);

            if (command.Parameters.Count > 0)
            {
                writer.WriteLine("Parameters:");

                foreach (IDbDataParameter param in command.Parameters)
                {
                    writer.WriteLine(" '{0}' ({1} (s:{2} p:{3} s:{4})) = '{5}' ({6});",
                        param.ParameterName,
                        param.DbType,
                        param.Scale,
                        param.Precision,
                        param.Scale,
                        param.Value is byte[] ? ConvertByteArrayToHexString((byte[])param.Value) : param.Value ?? "NULL",
                        param.Value != null ? param.Value.GetType().Name : "DBNull");
                }

                writer.WriteLine();
            }

            return command;
        }

        /// <summary>Convert byte array to hex formatted string without separators.</summary>
        /// <param name="data">Byte Array Data.</param>
        /// <returns>Hex string representation of byte array.</returns>
        private static string ConvertByteArrayToHexString(byte[] data)
        {
            return ConvertByteArrayToHexString(data, 0);
        }

        /// <summary>Convert byte array to hex formatted string.</summary>
        /// <param name="data">Byte Array Data.</param>
        /// <param name="separatorEach">Put '-' each <c>separatorEach</c> characters.</param>
        /// <returns>Hex string representation of byte array.</returns>
        private static string ConvertByteArrayToHexString(byte[] data, int separatorEach)
        {
            int len = data.Length * 2;
            System.Text.StringBuilder sb = new System.Text.StringBuilder();
            for (int i = 0; i < len; i++)
            {
                sb.AppendFormat("{0:X}", data[(i / 2)] >> (((i % 2) == 0) ? 4 : 0) & 0x0F);
                if ((separatorEach > 0) && ((i + 1) % separatorEach == 0))
                    sb.AppendFormat("-");
            }

            return sb.ToString();
        }

        #endregion Command extensions

        #region Dynamic builders extensions

        /// <summary>Turns an <see cref="IDynamicSelectQueryBuilder"/> to a Dynamic list of things.</summary>
        /// <param name="b">Ready to execute builder.</param>
        /// <returns>List of things.</returns>
        public static List<dynamic> ToList(this IDynamicSelectQueryBuilder b)
        {
            return b.Execute().ToList();
        }

        /// <summary>Sets the on create temporary parameter action.</summary>
        /// <typeparam name="T">Class implementing <see cref="IDynamicQueryBuilder"/> interface.</typeparam>
        /// <param name="b">The builder on which set delegate.</param>
        /// <param name="a">Action to invoke.</param>
        /// <returns>Returns instance of builder on which action is set.</returns>
        public static T CreateTemporaryParameterAction<T>(this T b, Action<IParameter> a) where T : IDynamicQueryBuilder
        {
            if (a == null)
                b.OnCreateTemporaryParameter = null;
            else
            {
                if (b.OnCreateTemporaryParameter == null)
                    b.OnCreateTemporaryParameter = new List<Action<IParameter>>();

                b.OnCreateTemporaryParameter.Add(a);
            }

            return b;
        }

        /// <summary>Sets the on create real parameter action.</summary>
        /// <typeparam name="T">Class implementing <see cref="IDynamicQueryBuilder"/> interface.</typeparam>
        /// <param name="b">The builder on which set delegate.</param>
        /// <param name="a">Action to invoke.</param>
        /// <returns>Returns instance of builder on which action is set.</returns>
        public static T CreateParameterAction<T>(this T b, Action<IParameter, IDbDataParameter> a) where T : IDynamicQueryBuilder
        {
            if (a == null)
                b.OnCreateParameter = null;
            else
            {
                if (b.OnCreateParameter == null)
                    b.OnCreateParameter = new List<Action<IParameter, IDbDataParameter>>();

                b.OnCreateParameter.Add(a);
            }

            return b;
        }

        /// <summary>Sets the virtual mode on builder.</summary>
        /// <typeparam name="T">Class implementing <see cref="IDynamicQueryBuilder"/> interface.</typeparam>
        /// <param name="b">The builder on which set virtual mode.</param>
        /// <param name="virtualMode">Virtual mode.</param>
        /// <returns>Returns instance of builder on which virtual mode is set.</returns>
        public static T SetVirtualMode<T>(this T b, bool virtualMode) where T : IDynamicQueryBuilder
        {
            b.VirtualMode = virtualMode;
            return b;
        }

        /// <summary>Creates sub query that can be used inside of from/join/expressions.</summary>
        /// <typeparam name="T">Class implementing <see cref="IDynamicQueryBuilder"/> interface.</typeparam>
        /// <param name="b">The builder that will be parent of new sub query.</param>
        /// <returns>Instance of sub query.</returns>
        public static IDynamicSelectQueryBuilder SubQuery<T>(this T b) where T : IDynamicQueryBuilder
        {
            return new DynamicSelectQueryBuilder(b.Database, b as DynamicQueryBuilder);
        }

        /// <summary>Creates sub query that can be used inside of from/join/expressions.</summary>
        /// <typeparam name="T">Class implementing <see cref="IDynamicQueryBuilder"/> interface.</typeparam>
        /// <param name="b">The builder that will be parent of new sub query.</param>
        /// <param name="fn">The specification for sub query.</param>
        /// <param name="func">The specification for sub query.</param>
        /// <returns>Instance of sub query.</returns>
        public static IDynamicSelectQueryBuilder SubQuery<T>(this T b, Func<dynamic, object> fn, params Func<dynamic, object>[] func) where T : IDynamicQueryBuilder
        {
            return new DynamicSelectQueryBuilder(b.Database, b as DynamicQueryBuilder).From(fn, func);
        }

        /// <summary>Creates sub query that can be used inside of from/join/expressions.</summary>
        /// <typeparam name="T">Class implementing <see cref="IDynamicQueryBuilder"/> interface.</typeparam>
        /// <param name="b">The builder that will be parent of new sub query.</param>
        /// <param name="subquery">First argument is parent query, second one is a sub query.</param>
        /// <returns>This instance to permit chaining.</returns>
        public static T SubQuery<T>(this T b, Action<T, IDynamicSelectQueryBuilder> subquery) where T : IDynamicQueryBuilder
        {
            var sub = b.SubQuery();

            subquery(b, sub);

            (b as DynamicQueryBuilder).ParseCommand(sub as DynamicQueryBuilder, b.Parameters);

            return b;
        }

        /// <summary>Creates sub query that can be used inside of from/join/expressions.</summary>
        /// <typeparam name="T">Class implementing <see cref="IDynamicQueryBuilder"/> interface.</typeparam>
        /// <param name="b">The builder that will be parent of new sub query.</param>
        /// <param name="subquery">First argument is parent query, second one is a sub query.</param>
        /// <param name="fn">The specification for sub query.</param>
        /// <param name="func">The specification for sub query.</param>
        /// <returns>This instance to permit chaining.</returns>
        public static T SubQuery<T>(this T b, Action<T, IDynamicSelectQueryBuilder> subquery, Func<dynamic, object> fn, params Func<dynamic, object>[] func) where T : IDynamicQueryBuilder
        {
            var sub = b.SubQuery(fn, func);

            subquery(b, sub);

            (b as DynamicQueryBuilder).ParseCommand(sub as DynamicQueryBuilder, b.Parameters);

            return b;
        }

        #endregion Dynamic builders extensions

        #region Dynamic extensions

        /// <summary>Turns an <see cref="IDataReader"/> to a Dynamic list of things.</summary>
        /// <param name="r">Reader from which read data.</param>
        /// <returns>List of things.</returns>
        public static List<dynamic> ToList(this IDataReader r)
        {
            var result = new List<dynamic>();

            while (r.Read())
                result.Add(r.RowToDynamic());

            return result;
        }

        /// <summary>Turns an <see cref="IDynamicSelectQueryBuilder"/> to a Dynamic list of things with specified type.</summary>
        /// <typeparam name="T">Type of object to map on.</typeparam>
        /// <param name="b">Ready to execute builder.</param>
        /// <returns>List of things.</returns>
        public static List<T> ToList<T>(this IDynamicSelectQueryBuilder b) where T : class
        {
            return b.Execute<T>().ToList();
        }

        /// <summary>Turns the dictionary into an ExpandoObject.</summary>
        /// <param name="d">Dictionary to convert.</param>
        /// <returns>Converted dictionary.</returns>
        public static dynamic ToDynamic(this IDictionary<string, object> d)
        {
            var result = new ExpandoObject();
            var dict = result as IDictionary<string, object>;

            foreach (var prop in d)
                dict.Add(prop.Key, prop.Value);

            return result;
        }

        /// <summary>Turns the object into an ExpandoObject.</summary>
        /// <param name="o">Object to convert.</param>
        /// <returns>Converted object.</returns>
        public static dynamic ToDynamic(this object o)
        {
            var result = new ExpandoObject();
            var dict = result as IDictionary<string, object>;
            var ot = o.GetType();

            if (ot == typeof(ExpandoObject))
                return o;

            if (o is IDictionary<string, object>)
                ((IDictionary<string, object>)o)
                    .ToList()
                    .ForEach(kvp => dict.Add(kvp.Key, kvp.Value));
            else if (ot == typeof(NameValueCollection) || ot.IsSubclassOf(typeof(NameValueCollection)))
            {
                var nameValue = (NameValueCollection)o;
                nameValue.Cast<string>()
                    .Select(key => new KeyValuePair<string, object>(key, nameValue[key]))
                    .ToList()
                    .ForEach(i => dict.Add(i));
            }
            else
            {
                var mapper = DynamicMapperCache.GetMapper(ot);

                if (mapper != null)
                {
                    foreach (var item in mapper.ColumnsMap.Values)
                        if (item.Get != null)
                            dict.Add(item.Name, item.Get(o));
                }
                else
                {
                    var props = ot.GetProperties();

                    foreach (var item in props)
                        if (item.CanRead)
                            dict.Add(item.Name, item.GetValue(o, null));
                }
            }

            return result;
        }

        /// <summary>Convert data row row into dynamic object.</summary>
        /// <param name="r">DataRow from which read.</param>
        /// <returns>Generated dynamic object.</returns>
        public static dynamic RowToDynamic(this DataRow r)
        {
            dynamic e = new ExpandoObject();
            var d = e as IDictionary<string, object>;

            int len = r.Table.Columns.Count;

            for (int i = 0; i < len; i++)
                d.Add(r.Table.Columns[i].ColumnName, r.IsNull(i) ? null : r[i]);

            return e;
        }

        /// <summary>Convert data row row into dynamic object (upper case key).</summary>
        /// <param name="r">DataRow from which read.</param>
        /// <returns>Generated dynamic object.</returns>
        public static dynamic RowToDynamicUpper(this DataRow r)
        {
            dynamic e = new ExpandoObject();
            var d = e as IDictionary<string, object>;

            int len = r.Table.Columns.Count;

            for (int i = 0; i < len; i++)
                d.Add(r.Table.Columns[i].ColumnName.ToUpper(), r.IsNull(i) ? null : r[i]);

            return e;
        }

        /// <summary>Convert reader row into dynamic object.</summary>
        /// <param name="r">Reader from which read.</param>
        /// <returns>Generated dynamic object.</returns>
        public static dynamic RowToDynamic(this IDataReader r)
        {
            dynamic e = new ExpandoObject();
            var d = e as IDictionary<string, object>;

            int c = r.FieldCount;
            for (int i = 0; i < c; i++)
                try
                {
                    d.Add(r.GetName(i), r.IsDBNull(i) ? null : r[i]);
                }
                catch (ArgumentException argex)
                {
                    throw new ArgumentException(
                        string.Format("Field '{0}' is defined more than once in a query.", r.GetName(i)), "Column name or alias", argex);
                }

            return e;
        }

        /// <summary>Turns the object into a Dictionary.</summary>
        /// <param name="o">Object to convert.</param>
        /// <returns>Resulting dictionary.</returns>
        public static IDictionary<string, object> ToDictionary(this ExpandoObject o)
        {
            return (IDictionary<string, object>)o;
        }

        /// <summary>Turns the object into a Dictionary.</summary>
        /// <param name="o">Object to convert.</param>
        /// <returns>Resulting dictionary.</returns>
        public static IDictionary<string, object> ToDictionary(this object o)
        {
            return o is IDictionary<string, object> ?
                (IDictionary<string, object>)o :
                (IDictionary<string, object>)o.ToDynamic();
        }

        #endregion Dynamic extensions

        #region Type extensions

        /// <summary>Check if type is anonymous. </summary>
        /// <param name="type">Type to test.</param>
        /// <returns>Returns <c>true</c> if type is anonymous.</returns>
        public static bool IsAnonymous(this Type type)
        {
            // HACK: The only way to detect anonymous types right now.
            return Attribute.IsDefined(type, typeof(CompilerGeneratedAttribute), false)
                && type.IsGenericType
                && (type.Name.Contains("AnonymousType") || type.Name.Contains("AnonType"))
                && (type.Name.StartsWith("<>") || type.Name.StartsWith("VB$"))
                && (type.Attributes & TypeAttributes.NotPublic) == TypeAttributes.NotPublic;
        }

        /// <summary>Check if type implements IEnumerable&lt;&gt; interface.</summary>
        /// <param name="type">Type to check.</param>
        /// <returns>Returns <c>true</c> if it does.</returns>
        public static bool IsGenericEnumerable(this Type type)
        {
            return type.IsGenericType && type.GetInterfaces().Any(t => t.GetGenericTypeDefinition() == typeof(IEnumerable<>));
        }

        /// <summary>Check if type implements IEnumerable&lt;&gt; interface.</summary>
        /// <param name="type">Type to check.</param>
        /// <returns>Returns <c>true</c> if it does.</returns>
        public static bool IsNullableType(this Type type)
        {
            Type generic = type.IsGenericType ? type.GetGenericTypeDefinition() : null;

            if (generic != null && generic.Equals(typeof(Nullable<>)) && type.IsClass)
                return true;

            return false;
        }

        /// <summary>Check if type is collection of any kind.</summary>
        /// <param name="type">Type to check.</param>
        /// <returns>Returns <c>true</c> if it is.</returns>
        public static bool IsCollection(this Type type)
        {
            if (!type.IsArray)
                return type.IsGenericEnumerable();

            return true;
        }

        /// <summary>Check if type is collection of value types like int.</summary>
        /// <param name="type">Type to check.</param>
        /// <returns>Returns <c>true</c> if it is.</returns>
        public static bool IsCollectionOfValueTypes(this Type type)
        {
            if (type.IsArray)
                return type.GetElementType().IsValueType;
            else
            {
                if (type.IsGenericType && type.GetInterfaces().Any(t => t.GetGenericTypeDefinition() == typeof(IEnumerable<>)))
                {
                    Type[] gt = type.GetGenericArguments();

                    return (gt.Length == 1) && gt[0].IsValueType;
                }
            }

            return false;
        }

        /// <summary>Gets <see cref="System.Data.DbType"/> corresponding to the
        /// provided <see cref="System.Type"/>.</summary>
        /// <param name="t">The type to be converted.</param>
        /// <returns>Returns <see cref="System.Data.DbType"/> corresponding to the
        /// provided <see cref="System.Type"/>.</returns>
        public static DbType ToDbType(this Type t)
        {
            return TypeMap.TryGetNullable(t) ?? DbType.Object;
        }

        /// <summary>Gets <see cref="System.Type"/> corresponding to the
        /// provided <see cref="System.Data.DbType"/>.</summary>
        /// <param name="dbt">The type to be converted.</param>
        /// <returns>Returns <see cref="System.Type"/> corresponding to the
        /// provided <see cref="System.Data.DbType"/>.</returns>
        public static Type ToType(this DbType dbt)
        {
            foreach (var tdbt in TypeMap)
                if (tdbt.Value == dbt)
                    return tdbt.Key;

            return typeof(object);
        }

        #endregion Type extensions

        #region IDictionary extensions

        /// <summary>Gets the value associated with the specified key.</summary>
        /// <typeparam name="TKey">The type of keys in the dictionary.</typeparam>
        /// <typeparam name="TValue">The type of values in the dictionary.</typeparam>
        /// <param name="dict">Dictionary to probe.</param>
        /// <param name="key">The key whose value to get.</param>
        /// <returns>Nullable type containing value or null if key was not found.</returns>
        public static TValue? TryGetNullable<TKey, TValue>(this IDictionary<TKey, TValue> dict, TKey key) where TValue : struct
        {
            TValue val;

            if (key != null && dict.TryGetValue(key, out val))
                return val;

            return null;
        }

        /// <summary>Gets the value associated with the specified key.</summary>
        /// <typeparam name="TKey">The type of keys in the dictionary.</typeparam>
        /// <typeparam name="TValue">The type of values in the dictionary.</typeparam>
        /// <param name="dict">Dictionary to probe.</param>
        /// <param name="key">The key whose value to get.</param>
        /// <returns>Instance of object or null if not found.</returns>
        public static TValue TryGetValue<TKey, TValue>(this IDictionary<TKey, TValue> dict, TKey key) where TValue : class
        {
            TValue val;

            if (key != null && dict.TryGetValue(key, out val))
                return val;

            return default(TValue);
        }

        /// <summary>Adds element to dictionary and returns added value.</summary>
        /// <typeparam name="TKey">The type of keys in the dictionary.</typeparam>
        /// <typeparam name="TValue">The type of values in the dictionary.</typeparam>
        /// <param name="dict">Dictionary to which add value.</param>
        /// <param name="key">The key under which value value will be added.</param>
        /// <param name="value">Value to add.</param>
        /// <returns>Instance of object or null if not found.</returns>
        public static TValue AddAndPassValue<TKey, TValue>(this IDictionary<TKey, TValue> dict, TKey key, TValue value)
        {
            dict.Add(key, value);
            return value;
        }

        #endregion IDictionary extensions

        #region IDataReader extensions

        /// <summary>Gets the <see cref="System.Data.DbType"/> information corresponding
        /// to the type of <see cref="System.Object"/> that would be returned from
        /// <see cref="System.Data.IDataRecord.GetValue(System.Int32)"/>.</summary>
        /// <param name="r">The data reader.</param>
        /// <param name="i">The index of the field to find.</param>
        /// <returns>The <see cref="System.Data.DbType"/> information corresponding to the
        /// type of <see cref="System.Object"/> that would be returned from
        /// <see cref="System.Data.IDataRecord.GetValue(System.Int32)"/>.</returns>
        public static DbType GetFieldDbType(this IDataReader r, int i)
        {
            return TypeMap.TryGetNullable(r.GetFieldType(i)) ?? DbType.String;
        }

        #endregion IDataReader extensions

        #region Mapper extensions

        /// <summary>MapEnumerable object enumerator into specified type.</summary>
        /// <typeparam name="T">Type to which columnMap results.</typeparam>
        /// <param name="enumerable">Source enumerator.</param>
        /// <returns>Enumerator of specified type.</returns>
        public static IEnumerable<T> MapEnumerable<T>(this IEnumerable<object> enumerable)
        {
            var mapper = DynamicMapperCache.GetMapper<T>();

            if (mapper == null)
                throw new InvalidOperationException("Type can't be mapped for unknown reason.");

            foreach (var item in enumerable)
                yield return (T)mapper.Create(item);
        }

        /// <summary>MapEnumerable object item into specified type.</summary>
        /// <typeparam name="T">Type to which columnMap results.</typeparam>
        /// <param name="item">Source object.</param>
        /// <returns>Item of specified type.</returns>
        public static T Map<T>(this object item)
        {
            var mapper = DynamicMapperCache.GetMapper<T>();

            if (mapper == null)
                throw new InvalidOperationException("Type can't be mapped for unknown reason.");

            return (T)mapper.Create(item);
        }

        /// <summary>Fill object of specified type with data from source object.</summary>
        /// <typeparam name="T">Type to which columnMap results.</typeparam>
        /// <param name="item">Item to which columnMap data.</param>
        /// <param name="source">Item from which extract data.</param>
        /// <returns>Filled item.</returns>
        public static T Fill<T>(this T item, object source)
        {
            var mapper = DynamicMapperCache.GetMapper<T>();

            if (mapper == null)
                throw new InvalidOperationException("Type can't be mapped for unknown reason.");

            mapper.Map(item, source);

            return item;
        }

        /// <summary>MapEnumerable object enumerator into specified type.</summary>
        /// <param name="enumerable">Source enumerator.</param>
        /// <param name="type">Type to which columnMap results.</param>
        /// <returns>Enumerator of specified type.</returns>
        public static IEnumerable<object> MapEnumerable(this IEnumerable<object> enumerable, Type type)
        {
            var mapper = DynamicMapperCache.GetMapper(type);

            if (mapper == null)
                throw new InvalidOperationException("Type can't be mapped for unknown reason.");

            foreach (var item in enumerable)
                yield return mapper.Create(item);
        }

        /// <summary>MapEnumerable object item into specified type.</summary>
        /// <param name="item">Source object.</param>
        /// <param name="type">Type to which columnMap results.</param>
        /// <returns>Item of specified type.</returns>
        public static object Map(this object item, Type type)
        {
            var mapper = DynamicMapperCache.GetMapper(type);

            if (mapper == null)
                throw new InvalidOperationException("Type can't be mapped for unknown reason.");

            return mapper.Create(item);
        }

        /// <summary>Converts the elements of an <see cref="System.Collections.IEnumerable"/>
        /// to the specified type.</summary>
        /// <typeparam name="T">The type to convert the elements of source to.</typeparam>
        /// <param name="enumerator">The <see cref="System.Collections.IEnumerable"/> that
        /// contains the elements to be converted.</param>
        /// <returns>An enumerator that contains each element of
        /// the source sequence converted to the specified type.</returns>
        /// <exception cref="System.ArgumentNullException">Thrown when
        /// <c>source</c> is null.</exception>
        /// <exception cref="System.InvalidCastException">An element in the
        /// sequence cannot be cast to type <c>T</c> or <c>enumerator</c>
        /// is not <see cref="System.Collections.IEnumerable"/>.</exception>
        public static IEnumerable<T> CastEnumerable<T>(this object enumerator)
        {
            return (enumerator as System.Collections.IEnumerable).Cast<T>();
        }

        #endregion Mapper extensions

        #region TryParse extensions

#if !DYNAMORM_OMMIT_TRYPARSE

        /// <summary>Generic try parse.</summary>
        /// <typeparam name="T">Type to parse to.</typeparam>
        /// <param name="value">Value to parse.</param>
        /// <param name="handler">Handler of a try parse method.</param>
        /// <returns>Returns <c>true</c> if conversion was successful.</returns>
        public static T? TryParse<T>(this string value, TryParseHandler<T> handler) where T : struct
        {
            if (String.IsNullOrEmpty(value))
                return null;

            T result;

            if (handler(value, out result))
                return result;

            return null;
        }

        /// <summary>Generic try parse with default value.</summary>
        /// <typeparam name="T">Type to parse to.</typeparam>
        /// <param name="value">Value to parse.</param>
        /// <param name="defaultValue">Default value of a result.</param>
        /// <param name="handler">Handler of a try parse method.</param>
        /// <returns>Returns <c>true</c> if conversion was successful.</returns>
        public static T TryParseDefault<T>(this string value, T defaultValue, TryParseHandler<T> handler)
        {
            if (String.IsNullOrEmpty(value))
                return defaultValue;

            T result;

            if (handler(value, out result))
                return result;

            return defaultValue;
        }

        /// <summary>Delegate fro try parse function of a type.</summary>
        /// <typeparam name="T">Type which implements this function.</typeparam>
        /// <param name="value">Value to parse.</param>
        /// <param name="result">Resulting value.</param>
        /// <returns>Returns <c>true</c> if conversion was successful.</returns>
        public delegate bool TryParseHandler<T>(string value, out T result);

#endif

        #endregion TryParse extensions

        #region Coalesce - besicaly not an extensions

        /// <summary>Select first not null value.</summary>
        /// <typeparam name="T">Type to return.</typeparam>
        /// <param name="vals">Values to check.</param>
        /// <returns>First not null or default value.</returns>
        public static T Coalesce<T>(params T[] vals) where T : class
        {
            return vals.FirstOrDefault(v => v != null);
        }

        /// <summary>Select first not null value.</summary>
        /// <typeparam name="T">Type to return.</typeparam>
        /// <param name="vals">Values to check.</param>
        /// <returns>First not null or default value.</returns>
        public static T? CoalesceNullable<T>(params T?[] vals) where T : struct
        {
            return vals.FirstOrDefault(v => v != null);
        }

        #endregion Coalesce - besicaly not an extensions
    }

    /// <summary>Dynamic query exception.</summary>
    public class DynamicQueryException : Exception, ISerializable
    {
        /// <summary>Gets the dumped command which failed.</summary>
        public string Command { get; private set; }

        /// <summary>Initializes a new instance of the
        /// <see cref="DynamicQueryException"/> class.</summary>
        /// <param name="command">The command which failed.</param>
        public DynamicQueryException(IDbCommand command = null)
            : base("Error executing command.")
        {
            if (command != null)
            {
                var sb = new StringBuilder();
                command.Dump(sb);
                Command = sb.ToString();
            }
        }

        /// <summary>Initializes a new instance of the
        /// <see cref="DynamicQueryException"/> class.</summary>
        /// <param name="message">The message.</param>
        /// <param name="command">The command which failed.</param>
        public DynamicQueryException(string message, IDbCommand command = null)
            : base(message)
        {
            SetCommand(command);
        }

        /// <summary>Initializes a new instance of the
        /// <see cref="DynamicQueryException"/> class.</summary>
        /// <param name="innerException">The inner exception.</param>
        /// <param name="command">The command which failed.</param>
        public DynamicQueryException(Exception innerException, IDbCommand command = null)
            : base("Error executing command.", innerException)
        {
            SetCommand(command);
        }

        /// <summary>Initializes a new instance of the
        /// <see cref="DynamicQueryException"/> class.</summary>
        /// <param name="message">The message.</param>
        /// <param name="innerException">The inner exception.</param>
        /// <param name="command">The command which failed.</param>
        public DynamicQueryException(string message, Exception innerException, IDbCommand command = null)
            : base(message, innerException)
        {
            SetCommand(command);
        }

        /// <summary>Initializes a new instance of the
        /// <see cref="DynamicQueryException"/> class.</summary>
        /// <param name="info">The <see cref="T:System.Runtime.Serialization.SerializationInfo" />
        /// that holds the serialized object data about the exception being thrown.</param>
        /// <param name="context">The <see cref="T:System.Runtime.Serialization.StreamingContext" />
        /// that contains contextual information about the source or destination.</param>
        public DynamicQueryException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
            Command = info.GetString("Command");
        }

        /// <summary>When overridden in a derived class, sets the
        /// <see cref="T:System.Runtime.Serialization.SerializationInfo" />
        /// with information about the exception.</summary>
        /// <param name="info">The <see cref="T:System.Runtime.Serialization.SerializationInfo" />
        /// that holds the serialized object data about the exception being thrown.</param>
        /// <param name="context">The <see cref="T:System.Runtime.Serialization.StreamingContext" />
        /// that contains contextual information about the source or destination.</param>
        /// <PermissionSet>
        ///   <IPermission class="System.Security.Permissions.FileIOPermission, mscorlib, Version=2.0.3600.0, Culture=neutral, PublicKeyToken=b77a5c561934e089" version="1" Read="*AllFiles*" PathDiscovery="*AllFiles*" />
        ///   <IPermission class="System.Security.Permissions.SecurityPermission, mscorlib, Version=2.0.3600.0, Culture=neutral, PublicKeyToken=b77a5c561934e089" version="1" Flags="SerializationFormatter" />
        /// </PermissionSet>
        public override void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            base.GetObjectData(info, context);

            if (!string.IsNullOrEmpty(Command))
                info.AddValue("Command", Command);
        }

        private void SetCommand(IDbCommand command)
        {
            if (command != null && (!(command is DynamicCommand) || ((command is DynamicCommand) && !(command as DynamicCommand).IsDisposed)))
            {
                var sb = new StringBuilder();
                command.Dump(sb);
                Command = sb.ToString();
            }
        }
    }

    /// <summary>Stores information about column from database schema.</summary>
    public struct DynamicSchemaColumn
    {
        /// <summary>Gets or sets column name.</summary>
        public string Name { get; set; }

        /// <summary>Gets or sets column type.</summary>
        public DbType Type { get; set; }

        /// <summary>Gets or sets a value indicating whether column is a key.</summary>
        public bool IsKey { get; set; }

        /// <summary>Gets or sets a value indicating whether column should have unique value.</summary>
        public bool IsUnique { get; set; }

        /// <summary>Gets or sets column size.</summary>
        public int Size { get; set; }

        /// <summary>Gets or sets column precision.</summary>
        public byte Precision { get; set; }

        /// <summary>Gets or sets column scale.</summary>
        public byte Scale { get; set; }
    }

#if !DYNAMORM_OMMIT_OLDSYNTAX

    /// <summary>Dynamic table is a simple ORM using dynamic objects.</summary>
    /// <example>
    /// <para>Assume that we have a table representing Users class.</para>
    /// <para>
    /// <para>Let's take a look at <c>Query</c> possibilities. Assume we want
    /// to get enumerator for all records in database, mapped to our class
    /// instead of dynamic type we can use following syntax.</para>
    /// <para>Approach first. Use dynamic <c>Query</c> method and just set type
    /// then just cast it to user class. Remember that you must cast result
    /// of <c>Query</c>to <c>IEnumerable&lt;object&gt;</c>. because from
    /// point of view of runtime you are operating on <c>object</c> type.</para>
    /// <code>(db.Table&lt;User&gt;().Query(type: typeof(User)) as IEnumerable&lt;object&gt;).Cast&lt;User&gt;();</code>
    /// <para>Second approach is similar. We ask database using dynamic
    /// <c>Query</c> method. The difference is that we use extension method of
    /// <c>IEnumerable&lt;object&gt;</c> (to which we must cast to) to map
    /// object.</para>
    /// <code>(db.Table&lt;User&gt;().Query(columns: "*") as IEnumerable&lt;object&gt;).MapEnumerable&lt;User&gt;();</code>
    /// You can also use generic approach. But be careful this method is currently available thanks to framework hack.
    /// <code>(db.Table&lt;User&gt;().Query&lt;User&gt;() as IEnumerable&lt;object&gt;).Cast&lt;User&gt;()</code>
    /// <para>Another approach uses existing methods, but still requires a
    /// cast, because <c>Query</c> also returns dynamic object enumerator.</para>
    /// <code>(db.Table&lt;User&gt;().Query().Execute() as IEnumerable&lt;object&gt;).MapEnumerable&lt;User&gt;();</code>
    /// </para>
    /// <para>Below you can find various invocations of dynamic and non dynamic
    /// methods of this class. <c>x</c> variable is a class instance.
    /// First various selects:</para>
    /// <code>x.Count(columns: "id");</code>
    /// <code>x.Count(last: new DynamicColumn
    /// {
    ///     Operator = DynamicColumn.CompareOperator.In,
    ///     Value = new object[] { "Hendricks", "Goodwin", "Freeman" }.Take(3)
    /// });</code>
    /// <code>x.Count(last: new DynamicColumn
    /// {
    ///     Operator = DynamicColumn.CompareOperator.In,
    ///     Value = new object[] { "Hendricks", "Goodwin", "Freeman" }
    /// });</code>
    /// <code>x.First(columns: "id").id;</code>
    /// <code>x.Last(columns: "id").id;</code>
    /// <code>x.Count(first: "Ori");</code>
    /// <code>x.Min(columns: "id");</code>
    /// <code>x.Max(columns: "id");</code>
    /// <code>x.Avg(columns: "id");</code>
    /// <code>x.Sum(columns: "id");</code>
    /// <code>x.Scalar(columns: "first", id: 19);</code>
    /// <code>x.Scalar(columns: "first:first:group_concat", id: new DynamicColumn { Operator = DynamicColumn.CompareOperator.Lt, Value = 20 });</code>
    /// <code>x.Scalar(columns: "group_concat(first):first", id: new DynamicColumn { Operator = DynamicColumn.CompareOperator.Lt, Value = 20 });</code>
    /// <code>var v = (x.Query(columns: "first,first:occurs:count", group: "first", order: ":desc:2") as IEnumerable&lt;dynamic&gt;).ToList();</code>
    /// <code>x.Scalar(columns: @"length(""login""):len:avg");</code>
    /// <code>x.Avg(columns: @"length(""email""):len");</code>
    /// <code>x.Count(condition1:
    ///     new DynamicColumn()
    ///     {
    ///         ColumnName = "email",
    ///         Aggregate = "length",
    ///         Operator = DynamicColumn.CompareOperator.Gt,
    ///         Value = 27
    ///     });</code>
    /// <code>var o = x.Single(columns: "id,first,last", id: 19);</code>
    /// <code>x.Single(where: new DynamicColumn("id").Eq(100)).login;</code>
    /// <code>x.Count(where: new DynamicColumn("id").Not(100));</code>
    /// <code>x.Single(where: new DynamicColumn("login").Like("Hoyt.%")).id;</code>
    /// <code>x.Count(where: new DynamicColumn("login").NotLike("Hoyt.%"));</code>
    /// <code>x.Count(where: new DynamicColumn("id").Greater(100));</code>
    /// <code>x.Count(where: new DynamicColumn("id").GreaterOrEqual(100));</code>
    /// <code>x.Count(where: new DynamicColumn("id").Less(100));</code>
    /// <code>x.Count(where: new DynamicColumn("id").LessOrEqual(100));</code>
    /// <code>x.Count(where: new DynamicColumn("id").Between(75, 100));</code>
    /// <code>x.Count(where: new DynamicColumn("id").In(75, 99, 100));</code>
    /// <code>x.Count(where: new DynamicColumn("id").In(new[] { 75, 99, 100 }));</code>
    /// Inserts:
    /// <code>x.Insert(code: 201, first: "Juri", last: "Gagarin", email: "juri.gagarin@megacorp.com", quote: "bla, bla, bla");</code>
    /// <code>x.Insert(values: new { code = 202, first = "Juri", last = "Gagarin", email = "juri.gagarin@megacorp.com", quote = "bla, bla, bla" });</code>
    /// <code>x.Insert(values: new Users
    /// {
    ///     Id = u.Max(columns: "id") + 1,
    ///     Code = "203",
    ///     First = "Juri",
    ///     Last = "Gagarin",
    ///     Email = "juri.gagarin@megacorp.com",
    ///     Quote = "bla, bla, bla"
    /// });</code>
    /// <code>x.Insert(values: new users
    /// {
    ///     id = u.Max(columns: "id") + 1,
    ///     code = "204",
    ///     first = "Juri",
    ///     last = "Gagarin",
    ///     email = "juri.gagarin@megacorp.com",
    ///     quote = "bla, bla, bla"
    /// });</code>
    /// <code>x.Update(id: 1, code: 201, first: "Juri", last: "Gagarin", email: "juri.gagarin@megacorp.com", quote: "bla, bla, bla");</code>
    /// <code>x.Update(update: new { id = 2, code = 202, first = "Juri", last = "Gagarin", email = "juri.gagarin@megacorp.com", quote = "bla, bla, bla" });</code>
    /// Updates:
    /// <code>x.Update(update: new Users
    /// {
    ///     Id = 3,
    ///     Code = "203",
    ///     First = "Juri",
    ///     Last = "Gagarin",
    ///     Email = "juri.gagarin@megacorp.com",
    ///     Quote = "bla, bla, bla"
    /// });</code>
    /// <code>x.Update(update: new users
    /// {
    ///     id = 4,
    ///     code = "204",
    ///     first = "Juri",
    ///     last = "Gagarin",
    ///     email = "juri.gagarin@megacorp.com",
    ///     quote = "bla, bla, bla"
    /// });</code>
    /// <code>x.Update(values: new { code = 205, first = "Juri", last = "Gagarin", email = "juri.gagarin@megacorp.com", quote = "bla, bla, bla" }, where: new { id = 5 });</code>
    /// <code>x.Update(values: new Users
    /// {
    ///     Id = 6,
    ///     Code = "206",
    ///     First = "Juri",
    ///     Last = "Gagarin",
    ///     Email = "juri.gagarin@megacorp.com",
    ///     Quote = "bla, bla, bla"
    /// }, id: 6);</code>
    /// <code>x.Update(values: new users
    /// {
    ///     id = 7,
    ///     code = "207",
    ///     first = "Juri",
    ///     last = "Gagarin",
    ///     email = "juri.gagarin@megacorp.com",
    ///     quote = "bla, bla, bla"
    /// }, id: 7);</code>
    /// Delete:
    /// <code>x.Delete(code: 10);</code>
    /// <code>x.Delete(delete: new { id = 11, code = 11, first = "Juri", last = "Gagarin", email = "juri.gagarin@megacorp.com", quote = "bla, bla, bla" });</code>
    /// <code>x.Delete(delete: new Users
    /// {
    ///     Id = 12,
    ///     Code = "12",
    ///     First = "Juri",
    ///     Last = "Gagarin",
    ///     Email = "juri.gagarin@megacorp.com",
    ///     Quote = "bla, bla, bla"
    /// });</code>
    /// <code>x.Delete(delete: new users
    /// {
    ///     id = 13,
    ///     code = "13",
    ///     first = "Juri",
    ///     last = "Gagarin",
    ///     email = "juri.gagarin@megacorp.com",
    ///     quote = "bla, bla, bla"
    /// });</code>
    /// <code>x.Delete(where: new { id = 14, code = 14 });</code>
    /// </example>
    public class DynamicTable : DynamicObject, IExtendedDisposable, ICloneable
    {
        private static HashSet<string> _allowedCommands = new HashSet<string>
        {
            "Insert", "Update", "Delete",
            "Query", "Single", "Where",
            "First", "Last", "Get",
            "Count", "Sum", "Avg",
            "Min", "Max", "Scalar"
        };

        /// <summary>Gets dynamic database.</summary>
        internal DynamicDatabase Database { get; private set; }

        /// <summary>Gets type of table (for coning and schema building).</summary>
        internal Type TableType { get; private set; }

        /// <summary>Gets name of table.</summary>
        public virtual string TableName { get; private set; }

        /// <summary>Gets name of owner.</summary>
        public virtual string OwnerName { get; private set; }

        /// <summary>Gets full name of table containing owner and table name.</summary>
        public virtual string FullName
        {
            get
            {
                return string.IsNullOrEmpty(TableName) ? null : string.IsNullOrEmpty(OwnerName) ?
                    Database.DecorateName(TableName) :
                    string.Format("{0}.{1}", Database.DecorateName(OwnerName), Database.DecorateName(TableName));
            }
        }

        /// <summary>Gets table schema.</summary>
        /// <remarks>If database doesn't support schema, only key columns are listed here.</remarks>
        public virtual Dictionary<string, DynamicSchemaColumn> Schema { get; private set; }

        private DynamicTable()
        {
        }

        /// <summary>Initializes a new instance of the <see cref="DynamicTable" /> class.</summary>
        /// <param name="database">Database and connection management.</param>
        /// <param name="table">Table name.</param>
        /// <param name="owner">Owner of the table.</param>
        /// <param name="keys">Override keys in schema.</param>
        public DynamicTable(DynamicDatabase database, string table = "", string owner = "", string[] keys = null)
        {
            IsDisposed = false;
            Database = database;
            TableName = Database.StripName(table);
            OwnerName = Database.StripName(owner);
            TableType = null;

            BuildAndCacheSchema(keys);
        }

        /// <summary>Initializes a new instance of the <see cref="DynamicTable" /> class.</summary>
        /// <param name="database">Database and connection management.</param>
        /// <param name="type">Type describing table.</param>
        /// <param name="keys">Override keys in schema.</param>
        public DynamicTable(DynamicDatabase database, Type type, string[] keys = null)
        {
            if (type == null)
                throw new ArgumentNullException("type", "Type can't be null.");

            IsDisposed = false;

            Database = database;
            TableType = type;

            var mapper = DynamicMapperCache.GetMapper(type);

            if (mapper != null)
            {
                TableName = mapper.Table == null || string.IsNullOrEmpty(mapper.Table.Name) ?
                    type.Name : mapper.Table.Name;
                OwnerName = mapper.Table == null || string.IsNullOrEmpty(mapper.Table.Name) ?
                    type.Name : mapper.Table.Name;
            }

            BuildAndCacheSchema(keys);
        }

        #region Schema

        private void BuildAndCacheSchema(string[] keys)
        {
            Dictionary<string, DynamicSchemaColumn> schema = null;

            schema = Database.GetSchema(TableType) ??
                Database.GetSchema(TableName);

            #region Fill currrent table schema

            if (keys == null && TableType != null)
            {
                var mapper = DynamicMapperCache.GetMapper(TableType);

                if (mapper != null)
                {
                    var k = mapper.ColumnsMap.Where(p => p.Value.Column != null && p.Value.Column.IsKey).Select(p => p.Key);
                    if (k.Count() > 0)
                        keys = k.ToArray();
                }
            }

            if (schema != null)
            {
                if (keys == null)
                    Schema = new Dictionary<string, DynamicSchemaColumn>(schema);
                else
                {
                    // TODO: Make this.... nicer
                    List<string> ks = keys.Select(k => k.ToLower()).ToList();

                    Schema = schema.ToDictionary(k => k.Key, (v) =>
                    {
                        DynamicSchemaColumn dsc = v.Value;
                        dsc.IsKey = ks.Contains(v.Key);
                        return dsc;
                    });
                }
            }

            #endregion Fill currrent table schema

            #region Build ad-hock schema

            if (keys != null && Schema == null)
            {
                Schema = keys.Select(k => k.ToLower()).ToList()
                    .ToDictionary(k => k, k => new DynamicSchemaColumn { Name = k, IsKey = true });
            }

            #endregion Build ad-hock schema
        }

        #endregion Schema

        #region Basic Queries

        /// <summary>Enumerate the reader and yield the result.</summary>
        /// <param name="sql">SQL query containing numbered parameters in format provided by
        /// <see cref="DynamicDatabase.GetParameterName(object)"/> methods. Also names should be formatted with
        /// <see cref="DynamicDatabase.DecorateName(string)"/> method.</param>
        /// <param name="args">Arguments (parameters).</param>
        /// <returns>Enumerator of objects expanded from query.</returns>
        public virtual IEnumerable<dynamic> Query(string sql, params object[] args)
        {
            using (var con = Database.Open())
            using (var cmd = con.CreateCommand())
            using (var rdr = cmd
                .SetCommand(sql)
                .AddParameters(Database, args)
                .ExecuteReader())
                while (rdr.Read())
                {
                    dynamic val = null;

                    // Work around to avoid yield being in try...catch block:
                    // http://stackoverflow.com/questions/346365/why-cant-yield-return-appear-inside-a-try-block-with-a-catch
                    try
                    {
                        val = rdr.RowToDynamic();
                    }
                    catch (ArgumentException argex)
                    {
                        var sb = new StringBuilder();
                        cmd.Dump(sb);

                        throw new ArgumentException(string.Format("{0}{1}{2}", argex.Message, Environment.NewLine, sb),
                            argex.InnerException.NullOr(a => a, argex));
                    }

                    yield return val;
                }
        }

        /// <summary>Enumerate the reader and yield the result.</summary>
        /// <param name="builder">Command builder.</param>
        /// <returns>Enumerator of objects expanded from query.</returns>
        public virtual IEnumerable<dynamic> Query(IDynamicQueryBuilder builder)
        {
            using (var con = Database.Open())
            using (var cmd = con.CreateCommand())
            using (var rdr = cmd
                .SetCommand(builder)
                .ExecuteReader())
                while (rdr.Read())
                {
                    dynamic val = null;

                    // Work around to avoid yield being in try...catch block:
                    // http://stackoverflow.com/questions/346365/why-cant-yield-return-appear-inside-a-try-block-with-a-catch
                    try
                    {
                        val = rdr.RowToDynamic();
                    }
                    catch (ArgumentException argex)
                    {
                        var sb = new StringBuilder();
                        cmd.Dump(sb);

                        throw new ArgumentException(string.Format("{0}{1}{2}", argex.Message, Environment.NewLine, sb),
                            argex.InnerException.NullOr(a => a, argex));
                    }

                    yield return val;
                }
        }

        /// <summary>Create new <see cref="DynamicSelectQueryBuilder"/>.</summary>
        /// <returns>New <see cref="DynamicSelectQueryBuilder"/> instance.</returns>
        public virtual IDynamicSelectQueryBuilder Query()
        {
            var builder = new DynamicSelectQueryBuilder(this.Database);

            var name = this.FullName;
            if (!string.IsNullOrEmpty(name))
                builder.From(x => name);

            return builder;
        }

        /// <summary>Returns a single result.</summary>
        /// <param name="sql">SQL query containing numbered parameters in format provided by
        /// <see cref="DynamicDatabase.GetParameterName(object)"/> methods. Also names should be formatted with
        /// <see cref="DynamicDatabase.DecorateName(string)"/> method.</param>
        /// <param name="args">Arguments (parameters).</param>
        /// <returns>Result of a query.</returns>
        public virtual object Scalar(string sql, params object[] args)
        {
            using (var con = Database.Open())
            using (var cmd = con.CreateCommand())
            {
                return cmd
                    .SetCommand(sql).AddParameters(Database, args)
                    .ExecuteScalar();
            }
        }

        /// <summary>Returns a single result.</summary>
        /// <param name="builder">Command builder.</param>
        /// <returns>Result of a query.</returns>
        public virtual object Scalar(IDynamicQueryBuilder builder)
        {
            using (var con = Database.Open())
            using (var cmd = con.CreateCommand())
            {
                return cmd
                    .SetCommand(builder)
                    .ExecuteScalar();
            }
        }

        /// <summary>Execute stored procedure.</summary>
        /// <param name="procName">Name of stored procedure to execute.</param>
        /// <param name="args">Arguments (parameters) in form of expando object.</param>
        /// <returns>Number of affected rows.</returns>
        public virtual int Procedure(string procName, ExpandoObject args = null)
        {
            if ((Database.Options & DynamicDatabaseOptions.SupportStoredProcedures) != DynamicDatabaseOptions.SupportStoredProcedures)
                throw new InvalidOperationException("Database connection desn't support stored procedures.");

            using (var con = Database.Open())
            using (var cmd = con.CreateCommand())
            {
                return cmd
                    .SetCommand(CommandType.StoredProcedure, procName).AddParameters(Database, args)
                    .ExecuteNonQuery();
            }
        }

        /// <summary>Execute non query.</summary>
        /// <param name="sql">SQL query containing numbered parameters in format provided by
        /// <see cref="DynamicDatabase.GetParameterName(object)"/> methods. Also names should be formatted with
        /// <see cref="DynamicDatabase.DecorateName(string)"/> method.</param>
        /// <param name="args">Arguments (parameters).</param>
        /// <returns>Number of affected rows.</returns>
        public virtual int Execute(string sql, params object[] args)
        {
            using (var con = Database.Open())
            using (var cmd = con.CreateCommand())
            {
                return cmd
                    .SetCommand(sql).AddParameters(Database, args)
                    .ExecuteNonQuery();
            }
        }

        /// <summary>Execute non query.</summary>
        /// <param name="builder">Command builder.</param>
        /// <returns>Number of affected rows.</returns>
        public virtual int Execute(IDynamicQueryBuilder builder)
        {
            using (var con = Database.Open())
            using (var cmd = con.CreateCommand())
            {
                return cmd
                    .SetCommand(builder)
                    .ExecuteNonQuery();
            }
        }

        /// <summary>Execute non query.</summary>
        /// <param name="builers">Command builders.</param>
        /// <returns>Number of affected rows.</returns>
        public virtual int Execute(IDynamicQueryBuilder[] builers)
        {
            int ret = 0;

            using (var con = Database.Open())
            {
                using (var trans = con.BeginTransaction())
                {
                    foreach (var builder in builers)
                    {
                        using (var cmd = con.CreateCommand())
                        {
                            ret += cmd
                               .SetCommand(builder)
                               .ExecuteNonQuery();
                        }
                    }

                    trans.Commit();
                }
            }

            return ret;
        }

        #endregion Basic Queries

        #region Insert

        /// <summary>Create new <see cref="DynamicInsertQueryBuilder"/>.</summary>
        /// <returns>New <see cref="DynamicInsertQueryBuilder"/> instance.</returns>
        public dynamic Insert()
        {
            return new DynamicProxy<IDynamicInsertQueryBuilder>(new DynamicInsertQueryBuilder(this.Database, this.FullName));
        }

        /// <summary>Adds a record to the database. You can pass in an Anonymous object, an <see cref="ExpandoObject"/>,
        /// A regular old POCO, or a NameValueCollection from a Request.Form or Request.QueryString.</summary>
        /// <param name="o">Anonymous object, an <see cref="ExpandoObject"/>, a regular old POCO, or a NameValueCollection
        /// from a Request.Form or Request.QueryString, containing fields to update.</param>
        /// <returns>Number of updated rows.</returns>
        public virtual int Insert(object o)
        {
            return Insert()
                .Insert(o)
                .Execute();
        }

        #endregion Insert

        #region Update

        /// <summary>Create new <see cref="DynamicUpdateQueryBuilder"/>.</summary>
        /// <returns>New <see cref="DynamicUpdateQueryBuilder"/> instance.</returns>
        public dynamic Update()
        {
            return new DynamicProxy<IDynamicUpdateQueryBuilder>(new DynamicUpdateQueryBuilder(this.Database, this.FullName));
        }

        /// <summary>Updates a record in the database. You can pass in an Anonymous object, an ExpandoObject,
        /// a regular old POCO, or a NameValueCollection from a Request.Form or Request.QueryString.</summary>
        /// <param name="o">Anonymous object, an ExpandoObject, a regular old POCO, or a NameValueCollection
        /// from a Request.Form or Request.QueryString, containing fields to update.</param>
        /// <param name="key">Anonymous object, an <see cref="ExpandoObject"/>, a regular old POCO, or a NameValueCollection
        /// from a Request.Form or Request.QueryString, containing fields with conditions.</param>
        /// <returns>Number of updated rows.</returns>
        public virtual int Update(object o, object key)
        {
            return Update()
                .Values(o)
                .Where(key)
                .Execute();
        }

        /// <summary>Updates a record in the database using schema. You can pass in an Anonymous object, an ExpandoObject,
        /// a regular old POCO, or a NameValueCollection from a Request.Form or Request.QueryString.</summary>
        /// <param name="o">Anonymous object, an <see cref="ExpandoObject"/>, a regular old POCO, or a NameValueCollection
        /// from a Request.Form or Request.QueryString, containing fields to update and conditions.</param>
        /// <returns>Number of updated rows.</returns>
        public virtual int Update(object o)
        {
            return Update()
                .Update(o)
                .Execute();
        }

        #endregion Update

        #region Delete

        /// <summary>Create new <see cref="DynamicDeleteQueryBuilder"/>.</summary>
        /// <returns>New <see cref="DynamicDeleteQueryBuilder"/> instance.</returns>
        public dynamic Delete()
        {
            return new DynamicProxy<IDynamicDeleteQueryBuilder>(new DynamicDeleteQueryBuilder(this.Database, this.FullName));
        }

        /// <summary>Removes a record from the database. You can pass in an Anonymous object, an <see cref="ExpandoObject"/>,
        /// A regular old POCO, or a NameValueCollection from a Request.Form or Request.QueryString.</summary>
        /// <param name="o">Anonymous object, an <see cref="ExpandoObject"/>, a regular old POCO, or a NameValueCollection
        /// from a Request.Form or Request.QueryString, containing fields with where conditions.</param>
        /// <param name="schema">If <c>true</c> use schema to determine key columns and ignore those which
        /// aren't keys.</param>
        /// <returns>Number of updated rows.</returns>
        public virtual int Delete(object o, bool schema = true)
        {
            return Delete()
                .Where(o, schema)
                .Execute();
        }

        #endregion Delete

        #region Universal Dynamic Invoker

        /// <summary>This is where the magic begins.</summary>
        /// <param name="binder">Binder to invoke.</param>
        /// <param name="args">Binder arguments.</param>
        /// <param name="result">Binder invoke result.</param>
        /// <returns>Returns <c>true</c> if invoke was performed.</returns>
        public override bool TryInvokeMember(InvokeMemberBinder binder, object[] args, out object result)
        {
            // parse the method
            var info = binder.CallInfo;

            // Get generic types
            var types = binder.GetGenericTypeArguments();

            // accepting named args only... SKEET!
            if (info.ArgumentNames.Count != args.Length)
                throw new InvalidOperationException("Please use named arguments for this type of query - the column name, orderby, columns, etc");

            var op = binder.Name;

            // Avoid strange things
            if (!_allowedCommands.Contains(op))
                throw new InvalidOperationException(string.Format("Dynamic method '{0}' is not supported.", op));

            switch (op)
            {
                case "Insert":
                    result = DynamicInsert(args, info, types);
                    break;

                case "Update":
                    result = DynamicUpdate(args, info, types);
                    break;

                case "Delete":
                    result = DynamicDelete(args, info, types);
                    break;

                default:
                    result = DynamicQuery(args, info, op, types);
                    break;
            }

            return true;
        }

        private object DynamicInsert(object[] args, CallInfo info, IList<Type> types)
        {
            var builder = new DynamicInsertQueryBuilder(this.Database);

            if (types != null && types.Count == 1)
                HandleTypeArgument<DynamicInsertQueryBuilder>(null, info, ref types, builder, 0);

            if (!string.IsNullOrEmpty(this.TableName) && builder.Tables.Count == 0)
                builder.Table(this.TableName, this.Schema);

            // loop the named args - see if we have order, columns and constraints
            if (info.ArgumentNames.Count > 0)
            {
                for (int i = 0; i < args.Length; i++)
                {
                    var fullName = info.ArgumentNames[i];
                    var name = fullName.ToLower();

                    switch (name)
                    {
                        case "table":
                            if (args[i] is string)
                                builder.Table(args[i].ToString());
                            else goto default;
                            break;

                        case "values":
                            builder.Insert(args[i]);
                            break;

                        case "type":
                            if (types == null || types.Count == 0)
                                HandleTypeArgument<DynamicInsertQueryBuilder>(args, info, ref types, builder, i);
                            else goto default;
                            break;

                        default:
                            builder.Insert(fullName, args[i]);
                            break;
                    }
                }
            }

            // Execute
            return Execute(builder);
        }

        private object DynamicUpdate(object[] args, CallInfo info, IList<Type> types)
        {
            var builder = new DynamicUpdateQueryBuilder(this.Database);

            if (types != null && types.Count == 1)
                HandleTypeArgument<DynamicUpdateQueryBuilder>(null, info, ref types, builder, 0);

            if (!string.IsNullOrEmpty(this.TableName) && builder.Tables.Count == 0)
                builder.Table(this.TableName, this.Schema);

            // loop the named args - see if we have order, columns and constraints
            if (info.ArgumentNames.Count > 0)
            {
                for (int i = 0; i < args.Length; i++)
                {
                    var fullName = info.ArgumentNames[i];
                    var name = fullName.ToLower();

                    switch (name)
                    {
                        case "table":
                            if (args[i] is string)
                                builder.Table(args[i].ToString());
                            else goto default;
                            break;

                        case "update":
                            builder.Update(args[i]);
                            break;

                        case "values":
                            builder.Values(args[i]);
                            break;

                        case "where":
                            builder.Where(args[i]);
                            break;

                        case "type":
                            if (types == null || types.Count == 0)
                                HandleTypeArgument(args, info, ref types, builder, i);
                            else goto default;
                            break;

                        default:
                            builder.Update(fullName, args[i]);
                            break;
                    }
                }
            }

            // Execute
            return Execute(builder);
        }

        private object DynamicDelete(object[] args, CallInfo info, IList<Type> types)
        {
            var builder = new DynamicDeleteQueryBuilder(this.Database);

            if (types != null && types.Count == 1)
                HandleTypeArgument<DynamicDeleteQueryBuilder>(null, info, ref types, builder, 0);

            if (!string.IsNullOrEmpty(this.TableName) && builder.Tables.Count == 0)
                builder.Table(this.TableName, this.Schema);

            // loop the named args - see if we have order, columns and constraints
            if (info.ArgumentNames.Count > 0)
            {
                for (int i = 0; i < args.Length; i++)
                {
                    var fullName = info.ArgumentNames[i];
                    var name = fullName.ToLower();

                    switch (name)
                    {
                        case "table":
                            if (args[i] is string)
                                builder.Table(args[i].ToString());
                            else goto default;
                            break;

                        case "where":
                            builder.Where(args[i], false);
                            break;

                        case "delete":
                            builder.Where(args[i], true);
                            break;

                        case "type":
                            if (types == null || types.Count == 0)
                                HandleTypeArgument<DynamicDeleteQueryBuilder>(args, info, ref types, builder, i);
                            else goto default;
                            break;

                        default:
                            builder.Where(fullName, args[i]);
                            break;
                    }
                }
            }

            // Execute
            return Execute(builder);
        }

        private object DynamicQuery(object[] args, CallInfo info, string op, IList<Type> types)
        {
            object result;
            var builder = new DynamicSelectQueryBuilder(this.Database);

            if (types != null && types.Count == 1)
                HandleTypeArgument<DynamicSelectQueryBuilder>(null, info, ref types, builder, 0);

            if (!string.IsNullOrEmpty(this.TableName) && builder.Tables.Count == 0)
                builder.From(x => this.TableName);

            // loop the named args - see if we have order, columns and constraints
            if (info.ArgumentNames.Count > 0)
            {
                for (int i = 0; i < args.Length; i++)
                {
                    var fullName = info.ArgumentNames[i];
                    var name = fullName.ToLower();

                    // TODO: Make this nicer
                    switch (name)
                    {
                        case "order":
                            if (args[i] is string)
                                builder.OrderByColumn(((string)args[i]).Split(','));
                            else if (args[i] is string[])
                                builder.OrderByColumn(args[i] as string);
                            else if (args[i] is DynamicColumn[])
                                builder.OrderByColumn((DynamicColumn[])args[i]);
                            else if (args[i] is DynamicColumn)
                                builder.OrderByColumn((DynamicColumn)args[i]);
                            else goto default;
                            break;

                        case "group":
                            if (args[i] is string)
                                builder.GroupByColumn(((string)args[i]).Split(','));
                            else if (args[i] is string[])
                                builder.GroupByColumn(args[i] as string);
                            else if (args[i] is DynamicColumn[])
                                builder.GroupByColumn((DynamicColumn[])args[i]);
                            else if (args[i] is DynamicColumn)
                                builder.GroupByColumn((DynamicColumn)args[i]);
                            else goto default;
                            break;

                        case "columns":
                            {
                                var agregate = (op == "Sum" || op == "Max" || op == "Min" || op == "Avg" || op == "Count") ?
                                    op.ToUpper() : null;

                                if (args[i] is string || args[i] is string[])
                                    builder.SelectColumn((args[i] as String).NullOr(s => s.Split(','), args[i] as String[])
                                        .Select(c =>
                                        {
                                            var col = DynamicColumn.ParseSelectColumn(c);
                                            if (string.IsNullOrEmpty(col.Aggregate))
                                                col.Aggregate = agregate;

                                            return col;
                                        }).ToArray());
                                else if (args[i] is DynamicColumn || args[i] is DynamicColumn[])
                                    builder.SelectColumn((args[i] as DynamicColumn).NullOr(c => new DynamicColumn[] { c }, args[i] as DynamicColumn[])
                                        .Select(c =>
                                        {
                                            if (string.IsNullOrEmpty(c.Aggregate))
                                                c.Aggregate = agregate;

                                            return c;
                                        }).ToArray());
                                else goto default;
                            }

                            break;

                        case "where":
                            builder.Where(args[i]);
                            break;

                        case "table":
                            if (args[i] is string)
                                builder.From(x => args[i].ToString());
                            else goto default;
                            break;

                        case "type":
                            if (types == null || types.Count == 0)
                                HandleTypeArgument<DynamicSelectQueryBuilder>(args, info, ref types, builder, i);
                            else goto default;
                            break;

                        default:
                            builder.Where(fullName, args[i]);
                            break;
                    }
                }
            }

            if (op == "Count" && !builder.HasSelectColumns)
            {
                result = Scalar(builder.Select(x => x.Count()));

                if (result is long)
                    result = (int)(long)result;
            }
            else if (op == "Sum" || op == "Max" ||
                op == "Min" || op == "Avg" || op == "Count")
            {
                if (!builder.HasSelectColumns)
                    throw new InvalidOperationException("You must select one column to agregate.");

                result = Scalar(builder);

                if (op == "Count" && result is long)
                    result = (int)(long)result;
                else if (result == DBNull.Value)
                    result = null;
            }
            else
            {
                // build the SQL
                var justOne = op == "First" || op == "Last" || op == "Get" || op == "Single";

                // Be sure to sort by DESC on selected columns
                /*if (op == "Last")
                {
                    if (builder.Order.Count > 0)
                        foreach (var o in builder.Order)
                            o.Order = o.Order == DynamicColumn.SortOrder.Desc ?
                                DynamicColumn.SortOrder.Asc : DynamicColumn.SortOrder.Desc;
                }*/

                if (justOne && !(op == "Last"))
                {
                    if ((Database.Options & DynamicDatabaseOptions.SupportLimitOffset) == DynamicDatabaseOptions.SupportLimitOffset)
                        builder.Limit(1);
                    else if ((Database.Options & DynamicDatabaseOptions.SupportTop) == DynamicDatabaseOptions.SupportTop)
                        builder.Top(1);
                }

                if (op == "Scalar")
                {
                    if (!builder.HasSelectColumns)
                        throw new InvalidOperationException("You must select one column in scalar statement.");

                    result = Scalar(builder);
                }
                else
                {
                    if (justOne)
                    {
                        if (op == "Last")
                            result = Query(builder).LastOrDefault(); // Last record fallback
                        else
                            result = Query(builder).FirstOrDefault(); // return a single record
                    }
                    else
                        result = Query(builder); // return lots

                    // MapEnumerable to specified result (still needs to be casted after that)
                    if (types != null)
                    {
                        if (types.Count == 1)
                            result = justOne ?
                                result.Map(types[0]) :
                                ((IEnumerable<object>)result).MapEnumerable(types[0]);

                        // TODO: Dictionaries
                    }
                }
            }

            return result;
        }

        private void HandleTypeArgument<T>(object[] args, CallInfo info, ref IList<Type> types, T builder, int i) where T : DynamicQueryBuilder
        {
            if (args != null)
            {
                if (args[i] is Type[])
                    types = new List<Type>((Type[])args[i]);
                else if (args[i] is Type)
                    types = new List<Type>(new Type[] { (Type)args[i] });
            }

            if (types != null && types.Count == 1 && !info.ArgumentNames.Any(a => a.ToLower() == "table"))
                builder.Table(types[0]);
        }

        #endregion Universal Dynamic Invoker

        #region IExtendedDisposable Members

        /// <summary>Performs application-defined tasks associated with
        /// freeing, releasing, or resetting unmanaged resources.</summary>
        public void Dispose()
        {
            // Lose reference but don't kill it.
            if (Database != null)
            {
                Database.RemoveFromCache(this);
                Database = null;
            }

            IsDisposed = true;
        }

        /// <summary>Gets a value indicating whether this instance is disposed.</summary>
        public bool IsDisposed { get; private set; }

        #endregion IExtendedDisposable Members

        #region ICloneable Members

        /// <summary>Creates a new object that is a copy of the current
        /// instance.</summary>
        /// <returns>A new object that is a copy of this instance.</returns>
        public object Clone()
        {
            return new DynamicTable()
            {
                Database = this.Database,
                Schema = this.Schema,
                TableName = this.TableName,
                TableType = this.TableType
            };
        }

        #endregion ICloneable Members
    }

#endif

    /// <summary>Helper class to easy manage transaction.</summary>
    public class DynamicTransaction : IDbTransaction, IExtendedDisposable
    {
        private DynamicDatabase _db;
        private DynamicConnection _con;
        private bool _singleTransaction;
        private Action _disposed;
        private bool _operational = false;

        /// <summary>Initializes a new instance of the <see cref="DynamicTransaction" /> class.</summary>
        /// <param name="db">Database connection manager.</param>
        /// <param name="con">Active connection.</param>
        /// <param name="singleTransaction">Are we using single transaction mode? I so... act correctly.</param>
        /// <param name="il">One of the <see cref="System.Data.IsolationLevel"/> values.</param>
        /// <param name="disposed">This action is invoked when transaction is disposed.</param>
        /// <param name="customParams">Pass custom transaction parameters.</param>
        internal DynamicTransaction(DynamicDatabase db, DynamicConnection con, bool singleTransaction, IsolationLevel? il, Action disposed, object customParams)
        {
            _db = db;
            _con = con;
            _singleTransaction = singleTransaction;
            _disposed = disposed;

            lock (_db.SyncLock)
            {
                if (!_db.TransactionPool.ContainsKey(_con.Connection))
                    throw new InvalidOperationException("Can't create transaction using disposed connection.");
                else if (_singleTransaction && _db.TransactionPool[_con.Connection].Count > 0)
                    _operational = false;
                else
                {
                    if (customParams != null)
                    {
                        var mi = _con.Connection.GetType().GetMethods().Where(m => m.GetParameters().Count() == 1 && m.GetParameters().First().ParameterType == customParams.GetType()).FirstOrDefault();
                        if (mi != null)
                            _db.TransactionPool[_con.Connection].Push((IDbTransaction)mi.Invoke(_con.Connection, new object[] { customParams, }));
                        else
                            throw new MissingMethodException(string.Format("Method 'BeginTransaction' accepting parameter of type '{0}' in '{1}' not found.",
                                customParams.GetType().FullName, _con.Connection.GetType().FullName));
                    }
                    else
                        _db.TransactionPool[_con.Connection]
                            .Push(il.HasValue ? _con.Connection.BeginTransaction(il.Value) : _con.Connection.BeginTransaction());

                    _db.PoolStamp = DateTime.Now.Ticks;
                    _operational = true;
                }
            }
        }

        /// <summary>Commits the database transaction.</summary>
        public void Commit()
        {
            lock (_db.SyncLock)
            {
                if (_operational)
                {
                    var t = _db.TransactionPool.TryGetValue(_con.Connection);

                    if (t != null && t.Count > 0)
                    {
                        IDbTransaction trans = t.Pop();

                        _db.PoolStamp = DateTime.Now.Ticks;

                        trans.Commit();
                        trans.Dispose();
                    }

                    _operational = false;
                }
            }
        }

        /// <summary>Rolls back a transaction from a pending state.</summary>
        public void Rollback()
        {
            lock (_db.SyncLock)
            {
                if (_operational)
                {
                    var t = _db.TransactionPool.TryGetValue(_con.Connection);

                    if (t != null && t.Count > 0)
                    {
                        IDbTransaction trans = t.Pop();

                        _db.PoolStamp = DateTime.Now.Ticks;

                        trans.Rollback();
                        trans.Dispose();
                    }

                    _operational = false;
                }
            }
        }

        /// <summary>Gets connection object to associate with the transaction.</summary>
        public IDbConnection Connection
        {
            get { return _con; }
        }

        /// <summary>Gets <see cref="System.Data.IsolationLevel"/> for this transaction.</summary>
        public IsolationLevel IsolationLevel { get; private set; }

        #region IExtendedDisposable Members

        /// <summary>Performs application-defined tasks associated with
        /// freeing, releasing, or resetting unmanaged resources.</summary>
        public void Dispose()
        {
            Rollback();

            if (_disposed != null)
                _disposed();
        }

        /// <summary>Gets a value indicating whether this instance is disposed.</summary>
        public bool IsDisposed { get { return !_operational; } }

        #endregion IExtendedDisposable Members
    }

    namespace Builders
    {
        /// <summary>Dynamic delete query builder interface.</summary>
        /// <remarks>This interface it publically available. Implementation should be hidden.</remarks>
        public interface IDynamicDeleteQueryBuilder : IDynamicQueryBuilder
        {
            /// <summary>Execute this builder.</summary>
            /// <returns>Result of an execution..</returns>
            int Execute();

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
            IDynamicDeleteQueryBuilder Where(Func<dynamic, object> func);

            /// <summary>Add where condition.</summary>
            /// <param name="column">Condition column with operator and value.</param>
            /// <returns>Builder instance.</returns>
            IDynamicDeleteQueryBuilder Where(DynamicColumn column);

            /// <summary>Add where condition.</summary>
            /// <param name="column">Condition column.</param>
            /// <param name="op">Condition operator.</param>
            /// <param name="value">Condition value.</param>
            /// <returns>Builder instance.</returns>
            IDynamicDeleteQueryBuilder Where(string column, DynamicColumn.CompareOperator op, object value);

            /// <summary>Add where condition.</summary>
            /// <param name="column">Condition column.</param>
            /// <param name="value">Condition value.</param>
            /// <returns>Builder instance.</returns>
            IDynamicDeleteQueryBuilder Where(string column, object value);

            /// <summary>Add where condition.</summary>
            /// <param name="conditions">Set conditions as properties and values of an object.</param>
            /// <param name="schema">If <c>true</c> use schema to determine key columns and ignore those which
            /// aren't keys.</param>
            /// <returns>Builder instance.</returns>
            IDynamicDeleteQueryBuilder Where(object conditions, bool schema = false);
        }

        /// <summary>Dynamic insert query builder interface.</summary>
        /// <remarks>This interface it publically available. Implementation should be hidden.</remarks>
        public interface IDynamicInsertQueryBuilder : IDynamicQueryBuilder
        {
            /// <summary>Execute this builder.</summary>
            /// <returns>Result of an execution..</returns>
            int Execute();

            /// <summary>
            /// Specifies the columns to insert using the dynamic lambda expressions given. Each expression correspond to one
            /// column, and can:
            /// <para>- Resolve to a string, in this case a '=' must appear in the string.</para>
            /// <para>- Resolve to a expression with the form: 'x => x.Column = Value'.</para>
            /// </summary>
            /// <param name="fn">The specifications.</param>
            /// <param name="func">The specifications.</param>
            /// <returns>This instance to permit chaining.</returns>
            IDynamicInsertQueryBuilder Values(Func<dynamic, object> fn, params Func<dynamic, object>[] func);

            /// <summary>Add insert fields.</summary>
            /// <param name="column">Insert column.</param>
            /// <param name="value">Insert value.</param>
            /// <returns>Builder instance.</returns>
            IDynamicInsertQueryBuilder Insert(string column, object value);

            /// <summary>Add insert fields.</summary>
            /// <param name="o">Set insert value as properties and values of an object.</param>
            /// <returns>Builder instance.</returns>
            IDynamicInsertQueryBuilder Insert(object o);
        }

        /// <summary>Dynamic query builder base interface.</summary>
        /// <remarks>This interface it publically available. Implementation should be hidden.</remarks>
        public interface IDynamicQueryBuilder
        {
            /// <summary>Gets <see cref="DynamicDatabase"/> instance.</summary>
            DynamicDatabase Database { get; }

            /// <summary>Gets tables information.</summary>
            IList<ITableInfo> Tables { get; }

            /// <summary>Gets the tables used in this builder.</summary>
            IDictionary<string, IParameter> Parameters { get; }

            /// <summary>Gets or sets a value indicating whether add virtual parameters.</summary>
            bool VirtualMode { get; set; }

            /// <summary>Gets a value indicating whether database supports standard schema.</summary>
            bool SupportSchema { get; }

            /// <summary>Fill command with query.</summary>
            /// <param name="command">Command to fill.</param>
            /// <returns>Filled instance of <see cref="IDbCommand"/>.</returns>
            IDbCommand FillCommand(IDbCommand command);

            /// <summary>
            /// Generates the text this command will execute against the underlying database.
            /// </summary>
            /// <returns>The text to execute against the underlying database.</returns>
            /// <remarks>This method must be override by derived classes.</remarks>
            string CommandText();

            /// <summary>Gets or sets the on create temporary parameter actions.</summary>
            /// <remarks>This is exposed to allow setting schema of column.</remarks>
            List<Action<IParameter>> OnCreateTemporaryParameter { get; set; }

            /// <summary>Gets or sets the on create real parameter actions.</summary>
            /// <remarks>This is exposed to allow modification of parameter.</remarks>
            List<Action<IParameter, IDbDataParameter>> OnCreateParameter { get; set; }
        }

        /// <summary>Dynamic select query builder interface.</summary>
        /// <remarks>This interface it publically available. Implementation should be hidden.</remarks>
        public interface IDynamicSelectQueryBuilder : IDynamicQueryBuilder ////, IEnumerable<object>
        {
            /// <summary>Execute this builder.</summary>
            /// <returns>Enumerator of objects expanded from query.</returns>
            IEnumerable<dynamic> Execute();

            /// <summary>Execute this builder and map to given type.</summary>
            /// <typeparam name="T">Type of object to map on.</typeparam>
            /// <returns>Enumerator of objects expanded from query.</returns>
            IEnumerable<T> Execute<T>() where T : class;

            /// <summary>Execute this builder as a data reader.</summary>
            /// <param name="reader">Action containing reader.</param>
            void ExecuteDataReader(Action<IDataReader> reader);

            /// <summary>Returns a single result.</summary>
            /// <returns>Result of a query.</returns>
            object Scalar();

            #region From/Join

            /// <summary>
            /// Adds to the 'From' clause the contents obtained by parsing the dynamic lambda expressions given. The supported
            /// formats are:
            /// <para>- Resolve to a string: 'x => "Table AS Alias', where the alias part is optional.</para>
            /// <para>- Resolve to an expression: 'x => x.Table.As( x.Alias )', where the alias part is optional.</para>
            /// <para>- Generic expression: 'x => x( expression ).As( x.Alias )', where the alias part is mandatory. In this
            /// case the alias is not annotated.</para>
            /// </summary>
            /// <param name="fn">The specification.</param>
            /// <param name="func">The specification.</param>
            /// <returns>This instance to permit chaining.</returns>
            IDynamicSelectQueryBuilder From(Func<dynamic, object> fn, params Func<dynamic, object>[] func);

            /// <summary>
            /// Adds to the 'Join' clause the contents obtained by parsing the dynamic lambda expressions given. The supported
            /// formats are:
            /// <para>- Resolve to a string: 'x => "Table AS Alias ON Condition', where the alias part is optional.</para>
            /// <para>- Resolve to an expression: 'x => x.Table.As( x.Alias ).On( condition )', where the alias part is optional.</para>
            /// <para>- Generic expression: 'x => x( expression ).As( x.Alias ).On( condition )', where the alias part is mandatory.
            /// In this case the alias is not annotated.</para>
            /// The expression might be prepended by a method that, in this case, is used to specify the specific join type you
            /// want to perform, as in: 'x => x.Left()...". Two considerations apply:
            /// <para>- If a 'false' argument is used when no 'Join' part appears in its name, then no 'Join' suffix is added
            /// with a space in between.</para>
            /// <para>- If a 'false' argument is used when a 'Join' part does appear, then no split is performed to separate the
            /// 'Join' part.</para>
            /// </summary>
            /// <param name="func">The specification.</param>
            /// <returns>This instance to permit chaining.</returns>
            IDynamicSelectQueryBuilder Join(params Func<dynamic, object>[] func);

            #endregion From/Join

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
            IDynamicSelectQueryBuilder Where(Func<dynamic, object> func);

            /// <summary>Add where condition.</summary>
            /// <param name="column">Condition column with operator and value.</param>
            /// <returns>Builder instance.</returns>
            IDynamicSelectQueryBuilder Where(DynamicColumn column);

            /// <summary>Add where condition.</summary>
            /// <param name="column">Condition column.</param>
            /// <param name="op">Condition operator.</param>
            /// <param name="value">Condition value.</param>
            /// <returns>Builder instance.</returns>
            IDynamicSelectQueryBuilder Where(string column, DynamicColumn.CompareOperator op, object value);

            /// <summary>Add where condition.</summary>
            /// <param name="column">Condition column.</param>
            /// <param name="value">Condition value.</param>
            /// <returns>Builder instance.</returns>
            IDynamicSelectQueryBuilder Where(string column, object value);

            /// <summary>Add where condition.</summary>
            /// <param name="conditions">Set conditions as properties and values of an object.</param>
            /// <param name="schema">If <c>true</c> use schema to determine key columns and ignore those which
            /// aren't keys.</param>
            /// <returns>Builder instance.</returns>
            IDynamicSelectQueryBuilder Where(object conditions, bool schema = false);

            #endregion Where

            #region Select

            /// <summary>
            /// Adds to the 'Select' clause the contents obtained by parsing the dynamic lambda expressions given. The supported
            /// formats are:
            /// <para>- Resolve to a string: 'x => "Table.Column AS Alias', where the alias part is optional.</para>
            /// <para>- Resolve to an expression: 'x => x.Table.Column.As( x.Alias )', where the alias part is optional.</para>
            /// <para>- Select all columns from a table: 'x => x.Table.All()'.</para>
            /// <para>- Generic expression: 'x => x( expression ).As( x.Alias )', where the alias part is mandatory. In this case
            /// the alias is not annotated.</para>
            /// </summary>
            /// <param name="fn">The specification.</param>
            /// <param name="func">The specification.</param>
            /// <returns>This instance to permit chaining.</returns>
            IDynamicSelectQueryBuilder Select(Func<dynamic, object> fn, params Func<dynamic, object>[] func);

            /// <summary>Add select columns.</summary>
            /// <param name="columns">Columns to add to object.</param>
            /// <returns>Builder instance.</returns>
            IDynamicSelectQueryBuilder SelectColumn(params DynamicColumn[] columns);

            /// <summary>Add select columns.</summary>
            /// <param name="columns">Columns to add to object.</param>
            /// <remarks>Column format consist of <c>Column Name</c>, <c>Alias</c> and
            /// <c>Aggregate function</c> in this order separated by '<c>:</c>'.</remarks>
            /// <returns>Builder instance.</returns>
            IDynamicSelectQueryBuilder SelectColumn(params string[] columns);

            #endregion Select

            #region GroupBy

            /// <summary>
            /// Adds to the 'Group By' clause the contents obtained from from parsing the dynamic lambda expression given.
            /// </summary>
            /// <param name="fn">The specification.</param>
            /// <param name="func">The specification.</param>
            /// <returns>This instance to permit chaining.</returns>
            IDynamicSelectQueryBuilder GroupBy(Func<dynamic, object> fn, params Func<dynamic, object>[] func);

            /// <summary>Add select columns.</summary>
            /// <param name="columns">Columns to group by.</param>
            /// <returns>Builder instance.</returns>
            IDynamicSelectQueryBuilder GroupByColumn(params DynamicColumn[] columns);

            /// <summary>Add select columns.</summary>
            /// <param name="columns">Columns to group by.</param>
            /// <remarks>Column format consist of <c>Column Name</c> and
            /// <c>Alias</c> in this order separated by '<c>:</c>'.</remarks>
            /// <returns>Builder instance.</returns>
            IDynamicSelectQueryBuilder GroupByColumn(params string[] columns);

            #endregion GroupBy

            #region OrderBy

            /// <summary>
            /// Adds to the 'Order By' clause the contents obtained from from parsing the dynamic lambda expression given. It
            /// accepts a multipart column specification followed by an optional <code>Ascending()</code> or <code>Descending()</code> virtual methods
            /// to specify the direction. If no virtual method is used, the default is ascending order. You can also use the
            /// shorter versions <code>Asc()</code> and <code>Desc()</code>.
            /// </summary>
            /// <param name="fn">The specification.</param>
            /// <param name="func">The specification.</param>
            /// <returns>This instance to permit chaining.</returns>
            IDynamicSelectQueryBuilder OrderBy(Func<dynamic, object> fn, params Func<dynamic, object>[] func);

            /// <summary>Add select columns.</summary>
            /// <param name="columns">Columns to order by.</param>
            /// <returns>Builder instance.</returns>
            IDynamicSelectQueryBuilder OrderByColumn(params DynamicColumn[] columns);

            /// <summary>Add select columns.</summary>
            /// <param name="columns">Columns to order by.</param>
            /// <remarks>Column format consist of <c>Column Name</c> and
            /// <c>Alias</c> in this order separated by '<c>:</c>'.</remarks>
            /// <returns>Builder instance.</returns>
            IDynamicSelectQueryBuilder OrderByColumn(params string[] columns);

            #endregion OrderBy

            #region Top/Limit/Offset/Distinct

            /// <summary>Set top if database support it.</summary>
            /// <param name="top">How many objects select.</param>
            /// <returns>Builder instance.</returns>
            IDynamicSelectQueryBuilder Top(int? top);

            /// <summary>Set top if database support it.</summary>
            /// <param name="limit">How many objects select.</param>
            /// <returns>Builder instance.</returns>
            IDynamicSelectQueryBuilder Limit(int? limit);

            /// <summary>Set top if database support it.</summary>
            /// <param name="offset">How many objects skip selecting.</param>
            /// <returns>Builder instance.</returns>
            IDynamicSelectQueryBuilder Offset(int? offset);

            /// <summary>Set distinct mode.</summary>
            /// <param name="distinct">Distinct mode.</param>
            /// <returns>Builder instance.</returns>
            IDynamicSelectQueryBuilder Distinct(bool distinct = true);

            #endregion Top/Limit/Offset/Distinct
        }

        /// <summary>Dynamic update query builder interface.</summary>
        /// <remarks>This interface it publically available. Implementation should be hidden.</remarks>
        public interface IDynamicUpdateQueryBuilder : IDynamicQueryBuilder
        {
            /// <summary>Execute this builder.</summary>
            /// <returns>Result of an execution..</returns>
            int Execute();

            #region Update

            /// <summary>Add update value or where condition using schema.</summary>
            /// <param name="column">Update or where column name.</param>
            /// <param name="value">Column value.</param>
            /// <returns>Builder instance.</returns>
            IDynamicUpdateQueryBuilder Update(string column, object value);

            /// <summary>Add update values and where condition columns using schema.</summary>
            /// <param name="conditions">Set values or conditions as properties and values of an object.</param>
            /// <returns>Builder instance.</returns>
            IDynamicUpdateQueryBuilder Update(object conditions);

            #endregion Update

            #region Values

            /// <summary>
            /// Specifies the columns to update using the dynamic lambda expressions given. Each expression correspond to one
            /// column, and can:
            /// <para>- Resolve to a string, in this case a '=' must appear in the string.</para>
            /// <para>- Resolve to a expression with the form: 'x => x.Column = Value'.</para>
            /// </summary>
            /// <param name="func">The specifications.</param>
            /// <returns>This instance to permit chaining.</returns>
            IDynamicUpdateQueryBuilder Set(params Func<dynamic, object>[] func);

            /// <summary>Add insert fields.</summary>
            /// <param name="column">Insert column.</param>
            /// <param name="value">Insert value.</param>
            /// <returns>Builder instance.</returns>
            IDynamicUpdateQueryBuilder Values(string column, object value);

            /// <summary>Add insert fields.</summary>
            /// <param name="o">Set insert value as properties and values of an object.</param>
            /// <returns>Builder instance.</returns>
            IDynamicUpdateQueryBuilder Values(object o);

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
            IDynamicUpdateQueryBuilder Where(Func<dynamic, object> func);

            /// <summary>Add where condition.</summary>
            /// <param name="column">Condition column with operator and value.</param>
            /// <returns>Builder instance.</returns>
            IDynamicUpdateQueryBuilder Where(DynamicColumn column);

            /// <summary>Add where condition.</summary>
            /// <param name="column">Condition column.</param>
            /// <param name="op">Condition operator.</param>
            /// <param name="value">Condition value.</param>
            /// <returns>Builder instance.</returns>
            IDynamicUpdateQueryBuilder Where(string column, DynamicColumn.CompareOperator op, object value);

            /// <summary>Add where condition.</summary>
            /// <param name="column">Condition column.</param>
            /// <param name="value">Condition value.</param>
            /// <returns>Builder instance.</returns>
            IDynamicUpdateQueryBuilder Where(string column, object value);

            /// <summary>Add where condition.</summary>
            /// <param name="conditions">Set conditions as properties and values of an object.</param>
            /// <param name="schema">If <c>true</c> use schema to determine key columns and ignore those which
            /// aren't keys.</param>
            /// <returns>Builder instance.</returns>
            IDynamicUpdateQueryBuilder Where(object conditions, bool schema = false);

            #endregion Where
        }

        /// <summary>Interface describing parameter info.</summary>
        public interface IParameter
        {
            /// <summary>Gets the parameter position in command.</summary>
            /// <remarks>Available after filling the command.</remarks>
            int Ordinal { get; }

            /// <summary>Gets the parameter temporary name.</summary>
            string Name { get; }

            /// <summary>Gets or sets the parameter value.</summary>
            object Value { get; set; }

            /// <summary>Gets or sets a value indicating whether name of temporary parameter is well known.</summary>
            bool WellKnown { get; set; }

            /// <summary>Gets or sets a value indicating whether this <see cref="IParameter"/> is virtual.</summary>
            bool Virtual { get; set; }

            /// <summary>Gets or sets the parameter schema information.</summary>
            DynamicSchemaColumn? Schema { get; set; }
        }

        /// <summary>Interface describing table information.</summary>
        public interface ITableInfo
        {
            /// <summary>Gets table owner name.</summary>
            string Owner { get; }

            /// <summary>Gets table name.</summary>
            string Name { get; }

            /// <summary>Gets table alias.</summary>
            string Alias { get; }

            /// <summary>Gets table schema.</summary>
            Dictionary<string, DynamicSchemaColumn> Schema { get; }
        }

        namespace Extensions
        {
            internal static class DynamicModifyBuilderExtensions
            {
                internal static T Table<T>(this T builder, Func<dynamic, object> func) where T : DynamicModifyBuilder
                {
                    if (func == null)
                        throw new ArgumentNullException("Function cannot be null.");

                    using (var parser = DynamicParser.Parse(func))
                    {
                        var result = parser.Result;

                        // If the expression result is string.
                        if (result is string)
                            return builder.Table((string)result);
                        else if (result is Type)
                            return builder.Table((Type)result);
                        else if (result is DynamicParser.Node)
                        {
                            // Or if it resolves to a dynamic node
                            var node = (DynamicParser.Node)result;

                            string owner = null;
                            string main = null;

                            while (true)
                            {
                                // Deny support for the AS() virtual method...
                                if (node is DynamicParser.Node.Method && ((DynamicParser.Node.Method)node).Name.ToUpper() == "AS")
                                    throw new ArgumentException(string.Format("Alias is not supported on modification builders. (Parsing: {0})", result));

                                // Support for table specifications...
                                if (node is DynamicParser.Node.GetMember)
                                {
                                    if (owner != null)
                                        throw new ArgumentException(string.Format("Owner '{0}.{1}' is already set when parsing '{2}'.", owner, main, result));

                                    if (main != null)
                                        owner = ((DynamicParser.Node.GetMember)node).Name;
                                    else
                                        main = ((DynamicParser.Node.GetMember)node).Name;

                                    node = node.Host;
                                    continue;
                                }

                                // Support for generic sources...
                                if (node is DynamicParser.Node.Invoke)
                                {
                                    if (owner == null && main == null)
                                    {
                                        var invoke = (DynamicParser.Node.Invoke)node;

                                        if (invoke.Arguments.Length == 1 && invoke.Arguments[0] is Type)
                                            return builder.Table((Type)invoke.Arguments[0]);
                                        else if (invoke.Arguments.Length == 1 && invoke.Arguments[0] is String)
                                            return builder.Table((string)invoke.Arguments[0]);
                                        else
                                            throw new ArgumentException(string.Format("Invalid argument count or type when parsing '{2}'. Invoke supports only one argument of type Type or String", owner, main, result));
                                    }
                                    else if (owner != null)
                                        throw new ArgumentException(string.Format("Owner '{0}.{1}' is already set when parsing '{2}'.", owner, main, result));
                                    else if (main != null)
                                        throw new ArgumentException(string.Format("Main '{0}' is already set when parsing '{1}'.", main, result));
                                }

                                if (!string.IsNullOrEmpty(main))
                                    return builder.Table(string.Format("{0}{1}",
                                        string.IsNullOrEmpty(owner) ? string.Empty : string.Format("{0}.", owner),
                                        main));
                            }
                        }

                        throw new ArgumentException(string.Format("Unable to set table parsing '{0}'", result));
                    }
                }

                internal static T Table<T>(this T builder, string tableName, Dictionary<string, DynamicSchemaColumn> schema = null) where T : DynamicModifyBuilder
                {
                    var tuple = tableName.Validated("Table Name").SplitSomethingAndAlias();

                    if (!string.IsNullOrEmpty(tuple.Item2))
                        throw new ArgumentException(string.Format("Can not use aliases in INSERT steatement. ({0})", tableName), "tableName");

                    var parts = tuple.Item1.Split('.');

                    if (parts.Length > 2)
                        throw new ArgumentException(string.Format("Table name can consist only from name or owner and name. ({0})", tableName), "tableName");

                    builder.Tables.Clear();
                    builder.Tables.Add(new DynamicQueryBuilder.TableInfo(builder.Database,
                         builder.Database.StripName(parts.Last()).Validated("Table"), null,
                         parts.Length == 2 ? builder.Database.StripName(parts.First()).Validated("Owner", canbeNull: true) : null));

                    if (schema != null)
                        (builder.Tables[0] as DynamicQueryBuilder.TableInfo).Schema = schema;

                    return builder;
                }

                internal static T Table<T>(this T builder, Type type) where T : DynamicQueryBuilder
                {
                    if (type.IsAnonymous())
                        throw new InvalidOperationException(string.Format("Cant assign anonymous type as a table ({0}).", type.FullName));

                    var mapper = DynamicMapperCache.GetMapper(type);

                    if (mapper == null)
                        throw new InvalidOperationException("Cant assign unmapable type as a table.");

                    if (builder is DynamicModifyBuilder)
                    {
                        builder.Tables.Clear();
                        builder.Tables.Add(new DynamicQueryBuilder.TableInfo(builder.Database, type));
                    }
                    else if (builder is DynamicSelectQueryBuilder)
                        (builder as DynamicSelectQueryBuilder).From(x => x(type));

                    return builder;
                }
            }

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

                    using (var parser = DynamicParser.Parse(func))
                    {
                        string condition = null;
                        bool and = true;

                        var result = parser.Result;
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
                                var node = (DynamicParser.Node.Method)result;
                                var name = node.Name.ToUpper();
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
                        var v = (DynamicColumn)value;

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

                    var dict = conditions.ToDictionary();
                    var mapper = DynamicMapperCache.GetMapper(conditions.GetType());
                    var table = dict.TryGetValue("_table").NullOr(x => x.ToString(), string.Empty);

                    foreach (var condition in dict)
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

        namespace Implementation
        {
            /// <summary>Implementation of dynamic delete query builder.</summary>
            internal class DynamicDeleteQueryBuilder : DynamicModifyBuilder, IDynamicDeleteQueryBuilder, DynamicQueryBuilder.IQueryWithWhere
            {
                /// <summary>
                /// Initializes a new instance of the <see cref="DynamicDeleteQueryBuilder"/> class.
                /// </summary>
                /// <param name="db">The database.</param>
                internal DynamicDeleteQueryBuilder(DynamicDatabase db)
                    : base(db)
                {
                }

                /// <summary>
                /// Initializes a new instance of the <see cref="DynamicDeleteQueryBuilder"/> class.
                /// </summary>
                /// <param name="db">The database.</param>
                /// <param name="tableName">Name of the table.</param>
                public DynamicDeleteQueryBuilder(DynamicDatabase db, string tableName)
                    : base(db, tableName)
                {
                }

                /// <summary>Generates the text this command will execute against the underlying database.</summary>
                /// <returns>The text to execute against the underlying database.</returns>
                /// <remarks>This method must be override by derived classes.</remarks>
                public override string CommandText()
                {
                    var info = Tables.Single();
                    return string.Format("DELETE FROM {0}{1}{2}{3}",
                        string.IsNullOrEmpty(info.Owner) ? string.Empty : string.Format("{0}.", Database.DecorateName(info.Owner)),
                        Database.DecorateName(info.Name),
                        string.IsNullOrEmpty(WhereCondition) ? string.Empty : " WHERE ",
                        WhereCondition);
                }

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
                public virtual IDynamicDeleteQueryBuilder Where(Func<dynamic, object> func)
                {
                    return this.InternalWhere(func);
                }

                /// <summary>Add where condition.</summary>
                /// <param name="column">Condition column with operator and value.</param>
                /// <returns>Builder instance.</returns>
                public virtual IDynamicDeleteQueryBuilder Where(DynamicColumn column)
                {
                    return this.InternalWhere(column);
                }

                /// <summary>Add where condition.</summary>
                /// <param name="column">Condition column.</param>
                /// <param name="op">Condition operator.</param>
                /// <param name="value">Condition value.</param>
                /// <returns>Builder instance.</returns>
                public virtual IDynamicDeleteQueryBuilder Where(string column, DynamicColumn.CompareOperator op, object value)
                {
                    return this.InternalWhere(column, op, value);
                }

                /// <summary>Add where condition.</summary>
                /// <param name="column">Condition column.</param>
                /// <param name="value">Condition value.</param>
                /// <returns>Builder instance.</returns>
                public virtual IDynamicDeleteQueryBuilder Where(string column, object value)
                {
                    return this.InternalWhere(column, value);
                }

                /// <summary>Add where condition.</summary>
                /// <param name="conditions">Set conditions as properties and values of an object.</param>
                /// <param name="schema">If <c>true</c> use schema to determine key columns and ignore those which
                /// aren't keys.</param>
                /// <returns>Builder instance.</returns>
                public virtual IDynamicDeleteQueryBuilder Where(object conditions, bool schema = false)
                {
                    return this.InternalWhere(conditions, schema);
                }

                #endregion Where
            }

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
                            value = Parse(node.Value, ref col, pars: Parameters, nulls: true);

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
                        string value = Parse(column.Value, ref col, pars: Parameters, nulls: true);

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

            /// <summary>Base query builder for insert/update/delete statements.</summary>
            internal abstract class DynamicModifyBuilder : DynamicQueryBuilder
            {
                /// <summary>
                /// Initializes a new instance of the <see cref="DynamicModifyBuilder"/> class.
                /// </summary>
                /// <param name="db">The database.</param>
                public DynamicModifyBuilder(DynamicDatabase db)
                    : base(db)
                {
                    VirtualMode = false;
                }

                /// <summary>
                /// Initializes a new instance of the <see cref="DynamicModifyBuilder" /> class.
                /// </summary>
                /// <param name="db">The database.</param>
                /// <param name="tableName">Name of the table.</param>
                public DynamicModifyBuilder(DynamicDatabase db, string tableName)
                    : this(db)
                {
                    VirtualMode = false;
                    this.Table(tableName);
                }

                /// <summary>Execute this builder.</summary>
                /// <returns>Result of an execution..</returns>
                public virtual int Execute()
                {
                    using (var con = Database.Open())
                    using (var cmd = con.CreateCommand())
                    {
                        return cmd
                            .SetCommand(this)
                            .ExecuteNonQuery();
                    }
                }
            }

            /// <summary>Implementation of dynamic query builder base interface.</summary>
            internal abstract class DynamicQueryBuilder : IDynamicQueryBuilder
            {
                /// <summary>Empty interface to allow where query builder implementation use universal approach.</summary>
                internal interface IQueryWithWhere
                {
                    /// <summary>Gets or sets the where condition.</summary>
                    string WhereCondition { get; set; }

                    /// <summary>Gets or sets the amount of not closed brackets in where statement.</summary>
                    int OpenBracketsCount { get; set; }
                }

                private DynamicQueryBuilder _parent = null;

                #region TableInfo

                /// <summary>Table information.</summary>
                internal class TableInfo : ITableInfo
                {
                    /// <summary>
                    /// Initializes a new instance of the <see cref="TableInfo"/> class.
                    /// </summary>
                    internal TableInfo()
                    {
                    }

                    /// <summary>
                    /// Initializes a new instance of the <see cref="TableInfo" /> class.
                    /// </summary>
                    /// <param name="db">The database.</param>
                    /// <param name="name">The name of table.</param>
                    /// <param name="alias">The table alias.</param>
                    /// <param name="owner">The table owner.</param>
                    public TableInfo(DynamicDatabase db, string name, string alias = null, string owner = null)
                    {
                        Name = name;
                        Alias = alias;
                        Owner = owner;

                        if (!name.ContainsAny(StringExtensions.InvalidMemberChars))
                            Schema = db.GetSchema(name, owner: owner);
                    }

                    /// <summary>
                    /// Initializes a new instance of the <see cref="TableInfo" /> class.
                    /// </summary>
                    /// <param name="db">The database.</param>
                    /// <param name="type">The type which can be mapped to database.</param>
                    /// <param name="alias">The table alias.</param>
                    /// <param name="owner">The table owner.</param>
                    public TableInfo(DynamicDatabase db, Type type, string alias = null, string owner = null)
                    {
                        var mapper = DynamicMapperCache.GetMapper(type);

                        Name = mapper.Table == null || string.IsNullOrEmpty(mapper.Table.Name) ?
                            mapper.Type.Name : mapper.Table.Name;

                        Owner = (mapper.Table != null) ? mapper.Table.Owner : owner;
                        Alias = alias;

                        Schema = db.GetSchema(type);
                    }

                    /// <summary>Gets or sets table owner name.</summary>
                    public string Owner { get; internal set; }

                    /// <summary>Gets or sets table name.</summary>
                    public string Name { get; internal set; }

                    /// <summary>Gets or sets table alias.</summary>
                    public string Alias { get; internal set; }

                    /// <summary>Gets or sets table schema.</summary>
                    public Dictionary<string, DynamicSchemaColumn> Schema { get; internal set; }
                }

                /// <summary>Generic based table information.</summary>
                /// <typeparam name="T">Type of class that is represented in database.</typeparam>
                internal class TableInfo<T> : TableInfo
                {
                    /// <summary>
                    /// Initializes a new instance of the <see cref="TableInfo{T}" /> class.
                    /// </summary>
                    /// <param name="db">The database.</param>
                    /// <param name="alias">The table alias.</param>
                    /// <param name="owner">The table owner.</param>
                    public TableInfo(DynamicDatabase db, string alias = null, string owner = null)
                        : base(db, typeof(T), alias, owner)
                    {
                    }
                }

                #endregion TableInfo

                #region Parameter

                /// <summary>Interface describing parameter info.</summary>
                internal class Parameter : IParameter
                {
                    /// <summary>Gets or sets the parameter position in command.</summary>
                    /// <remarks>Available after filling the command.</remarks>
                    public int Ordinal { get; internal set; }

                    /// <summary>Gets or sets the parameter temporary name.</summary>
                    public string Name { get; internal set; }

                    /// <summary>Gets or sets the parameter value.</summary>
                    public object Value { get; set; }

                    /// <summary>Gets or sets a value indicating whether name of temporary parameter is well known.</summary>
                    public bool WellKnown { get; set; }

                    /// <summary>Gets or sets a value indicating whether this <see cref="Parameter"/> is virtual.</summary>
                    public bool Virtual { get; set; }

                    /// <summary>Gets or sets the parameter schema information.</summary>
                    public DynamicSchemaColumn? Schema { get; set; }
                }

                #endregion Parameter

                #region Constructor

                /// <summary>
                /// Initializes a new instance of the <see cref="DynamicQueryBuilder"/> class.
                /// </summary>
                /// <param name="db">The database.</param>
                public DynamicQueryBuilder(DynamicDatabase db)
                {
                    VirtualMode = false;
                    Tables = new List<ITableInfo>();
                    Parameters = new Dictionary<string, IParameter>();

                    WhereCondition = null;
                    OpenBracketsCount = 0;

                    Database = db;
                    SupportSchema = (db.Options & DynamicDatabaseOptions.SupportSchema) == DynamicDatabaseOptions.SupportSchema;
                }

                /// <summary>Initializes a new instance of the <see cref="DynamicQueryBuilder"/> class.</summary>
                /// <param name="db">The database.</param>
                /// <param name="parent">The parent query.</param>
                internal DynamicQueryBuilder(DynamicDatabase db, DynamicQueryBuilder parent)
                    : this(db)
                {
                    _parent = parent;
                }

                #endregion Constructor

                #region IQueryWithWhere

                /// <summary>Gets or sets the where condition.</summary>
                public string WhereCondition { get; set; }

                /// <summary>Gets or sets the amount of not closed brackets in where statement.</summary>
                public int OpenBracketsCount { get; set; }

                #endregion IQueryWithWhere

                #region IDynamicQueryBuilder

                /// <summary>Gets <see cref="DynamicDatabase"/> instance.</summary>
                public DynamicDatabase Database { get; private set; }

                /// <summary>Gets the tables used in this builder.</summary>
                public IList<ITableInfo> Tables { get; private set; }

                /// <summary>Gets the tables used in this builder.</summary>
                public IDictionary<string, IParameter> Parameters { get; private set; }

                /// <summary>Gets or sets a value indicating whether add virtual parameters.</summary>
                public bool VirtualMode { get; set; }

                /// <summary>Gets or sets the on create temporary parameter actions.</summary>
                /// <remarks>This is exposed to allow setting schema of column.</remarks>
                public List<Action<IParameter>> OnCreateTemporaryParameter { get; set; }

                /// <summary>Gets or sets the on create real parameter actions.</summary>
                /// <remarks>This is exposed to allow modification of parameter.</remarks>
                public List<Action<IParameter, IDbDataParameter>> OnCreateParameter { get; set; }

                /// <summary>Gets a value indicating whether database supports standard schema.</summary>
                public bool SupportSchema { get; private set; }

                /// <summary>
                /// Generates the text this command will execute against the underlying database.
                /// </summary>
                /// <returns>The text to execute against the underlying database.</returns>
                /// <remarks>This method must be override by derived classes.</remarks>
                public abstract string CommandText();

                /// <summary>Fill command with query.</summary>
                /// <param name="command">Command to fill.</param>
                /// <returns>Filled instance of <see cref="IDbCommand"/>.</returns>
                public virtual IDbCommand FillCommand(IDbCommand command)
                {
                    // End not ended where statement
                    if (this is IQueryWithWhere)
                    {
                        while (OpenBracketsCount > 0)
                        {
                            WhereCondition += ")";
                            OpenBracketsCount--;
                        }
                    }

                    return command.SetCommand(CommandText()
                        .FillStringWithVariables(s =>
                        {
                            return Parameters.TryGetValue(s).NullOr(p =>
                            {
                                IDbDataParameter param = (IDbDataParameter)command
                                    .AddParameter(this, p.Schema, p.Value)
                                    .Parameters[command.Parameters.Count - 1];

                                (p as Parameter).Ordinal = command.Parameters.Count - 1;

                                if (OnCreateParameter != null)
                                    OnCreateParameter.ForEach(x => x(p, param));

                                return param.ParameterName;
                            }, s);
                        }));
                }

                #endregion IDynamicQueryBuilder

                #region Parser

                /// <summary>Parses the arbitrary object given and translates it into a string with the appropriate
                /// syntax for the database this parser is specific to.</summary>
                /// <param name="node">The object to parse and translate. It can be any arbitrary object, including null values (if
                /// permitted) and dynamic lambda expressions.</param>
                /// <param name="pars">If not null, the parameters' list where to store the parameters extracted by the parsing.</param>
                /// <param name="rawstr">If true, literal (raw) string are allowed. If false and the node is a literal then, as a
                /// security measure, an exception is thrown.</param>
                /// <param name="nulls">True to accept null values and translate them into the appropriate syntax accepted by the
                /// database. If false and the value is null, then an exception is thrown.</param>
                /// <param name="decorate">If set to <c>true</c> decorate element.</param>
                /// <param name="isMultiPart">If set parse argument as alias. This is workaround for AS method.</param>
                /// <returns>A string containing the result of the parsing, along with the parameters extracted in the
                /// <paramref name="pars" /> instance if such is given.</returns>
                /// <exception cref="System.ArgumentNullException">Null nodes are not accepted.</exception>
                internal virtual string Parse(object node, IDictionary<string, IParameter> pars = null, bool rawstr = false, bool nulls = false, bool decorate = true, bool isMultiPart = true)
                {
                    DynamicSchemaColumn? c = null;

                    return Parse(node, ref c, pars, rawstr, nulls, decorate, isMultiPart);
                }

                /// <summary>Parses the arbitrary object given and translates it into a string with the appropriate
                /// syntax for the database this parser is specific to.</summary>
                /// <param name="node">The object to parse and translate. It can be any arbitrary object, including null values (if
                /// permitted) and dynamic lambda expressions.</param>
                /// <param name="columnSchema">This parameter is used to determine type of parameter used in query.</param>
                /// <param name="pars">If not null, the parameters' list where to store the parameters extracted by the parsing.</param>
                /// <param name="rawstr">If true, literal (raw) string are allowed. If false and the node is a literal then, as a
                /// security measure, an exception is thrown.</param>
                /// <param name="nulls">True to accept null values and translate them into the appropriate syntax accepted by the
                /// database. If false and the value is null, then an exception is thrown.</param>
                /// <param name="decorate">If set to <c>true</c> decorate element.</param>
                /// <param name="isMultiPart">If set parse argument as alias. This is workaround for AS method.</param>
                /// <returns>A string containing the result of the parsing, along with the parameters extracted in the
                /// <paramref name="pars" /> instance if such is given.</returns>
                /// <exception cref="System.ArgumentNullException">Null nodes are not accepted.</exception>
                internal virtual string Parse(object node, ref DynamicSchemaColumn? columnSchema, IDictionary<string, IParameter> pars = null, bool rawstr = false, bool nulls = false, bool decorate = true, bool isMultiPart = true)
                {
                    // Null nodes are accepted or not depending upon the "nulls" flag...
                    if (node == null)
                    {
                        if (!nulls)
                            throw new ArgumentNullException("node", "Null nodes are not accepted.");

                        return Dispatch(node, ref columnSchema, pars, decorate);
                    }

                    // Nodes that are strings are parametrized or not depending the "rawstr" flag...
                    if (node is string)
                    {
                        if (rawstr) return (string)node;
                        else return Dispatch(node, ref columnSchema, pars, decorate);
                    }

                    // If node is a delegate, parse it to create the logical tree...
                    if (node is Delegate)
                    {
                        node = DynamicParser.Parse((Delegate)node).Result;
                        return Parse(node, ref columnSchema, pars, rawstr, decorate: decorate); // Intercept containers as in (x => "string")
                    }

                    return Dispatch(node, ref columnSchema, pars, decorate, isMultiPart);
                }

                private string Dispatch(object node, ref DynamicSchemaColumn? columnSchema, IDictionary<string, IParameter> pars = null, bool decorate = true, bool isMultiPart = true)
                {
                    if (node != null)
                    {
                        if (node is DynamicQueryBuilder) return ParseCommand((DynamicQueryBuilder)node, pars);
                        else if (node is DynamicParser.Node.Argument) return ParseArgument((DynamicParser.Node.Argument)node, isMultiPart);
                        else if (node is DynamicParser.Node.GetMember) return ParseGetMember((DynamicParser.Node.GetMember)node, ref columnSchema, pars, decorate, isMultiPart);
                        else if (node is DynamicParser.Node.SetMember) return ParseSetMember((DynamicParser.Node.SetMember)node, ref columnSchema, pars, decorate, isMultiPart);
                        else if (node is DynamicParser.Node.Unary) return ParseUnary((DynamicParser.Node.Unary)node, pars);
                        else if (node is DynamicParser.Node.Binary) return ParseBinary((DynamicParser.Node.Binary)node, pars);
                        else if (node is DynamicParser.Node.Method) return ParseMethod((DynamicParser.Node.Method)node, pars);
                        else if (node is DynamicParser.Node.Invoke) return ParseInvoke((DynamicParser.Node.Invoke)node, ref columnSchema, pars);
                        else if (node is DynamicParser.Node.Convert) return ParseConvert((DynamicParser.Node.Convert)node, pars);
                    }

                    // All other cases are considered constant parameters...
                    return ParseConstant(node, pars, columnSchema);
                }

                internal virtual string ParseCommand(DynamicQueryBuilder node, IDictionary<string, IParameter> pars = null)
                {
                    // Getting the command's text...
                    string str = node.CommandText(); // Avoiding spurious "OUTPUT XXX" statements

                    // If there are parameters to transform, but cannot store them, it is an error
                    if (node.Parameters.Count != 0 && pars == null)
                        throw new InvalidOperationException(string.Format("The parameters in this command '{0}' cannot be added to a null collection.", node.Parameters));

                    // Copy parameters to new comand
                    foreach (var parameter in node.Parameters)
                        pars.Add(parameter.Key, parameter.Value);

                    return string.Format("({0})", str);
                }

                protected virtual string ParseArgument(DynamicParser.Node.Argument node, bool isMultiPart = true, bool isOwner = false)
                {
                    if (!string.IsNullOrEmpty(node.Name) && (isOwner || (isMultiPart && IsTableAlias(node.Name))))
                        return node.Name;

                    return null;
                }

                protected virtual string ParseGetMember(DynamicParser.Node.GetMember node, ref DynamicSchemaColumn? columnSchema, IDictionary<string, IParameter> pars = null, bool decorate = true, bool isMultiPart = true)
                {
                    if (node.Host is DynamicParser.Node.Argument && IsTableAlias(node.Name))
                    {
                        decorate = false;
                        isMultiPart = false;
                    }

                    // This hack allows to use argument as alias, but when it is not nesesary use other column.
                    // Let say we hace a table Users with alias usr, and we join to table with alias ua which also has a column Users
                    // This allow use of usr => usr.ua.Users to result in ua."Users" instead of "Users" or usr."ua"."Users", se tests for examples.
                    string parent = null;
                    if (node.Host != null)
                    {
                        if (isMultiPart && node.Host is DynamicParser.Node.GetMember && IsTable(node.Host.Name, null))
                        {
                            if (node.Host.Host != null && node.Host.Host is DynamicParser.Node.GetMember && IsTable(node.Host.Name, node.Host.Host.Name))
                                parent = string.Format("{0}.{1}", Parse(node.Host.Host, pars, isMultiPart: false), Parse(node.Host, pars, isMultiPart: false));
                            else
                                parent = Parse(node.Host, pars, isMultiPart: false);
                        }
                        else if (isMultiPart)
                            parent = Parse(node.Host, pars, isMultiPart: isMultiPart);
                    }

                    ////string parent = node.Host == null || !isMultiPart ? null : Parse(node.Host, pars, isMultiPart: !IsTable(node.Name, node.Host.Name));
                    string name = parent == null ?
                        decorate ? Database.DecorateName(node.Name) : node.Name :
                        string.Format("{0}.{1}", parent, decorate ? Database.DecorateName(node.Name) : node.Name);

                    columnSchema = GetColumnFromSchema(name);

                    return name;
                }

                protected virtual string ParseSetMember(DynamicParser.Node.SetMember node, ref DynamicSchemaColumn? columnSchema, IDictionary<string, IParameter> pars = null, bool decorate = true, bool isMultiPart = true)
                {
                    if (node.Host is DynamicParser.Node.Argument && IsTableAlias(node.Name))
                    {
                        decorate = false;
                        isMultiPart = false;
                    }

                    string parent = null;
                    if (node.Host != null)
                    {
                        if (isMultiPart && node.Host is DynamicParser.Node.GetMember && IsTable(node.Host.Name, null))
                        {
                            if (node.Host.Host != null && node.Host.Host is DynamicParser.Node.GetMember && IsTable(node.Name, node.Host.Name))
                                parent = string.Format("{0}.{1}", Parse(node.Host.Host, pars, isMultiPart: false), Parse(node.Host, pars, isMultiPart: false));
                            else
                                parent = Parse(node.Host, pars, isMultiPart: false);
                        }
                        else if (isMultiPart)
                            parent = Parse(node.Host, pars, isMultiPart: isMultiPart);
                    }

                    ////string parent = node.Host == null || !isMultiPart ? null : Parse(node.Host, pars, isMultiPart: !IsTable(node.Name, node.Host.Name));
                    string name = parent == null ?
                        decorate ? Database.DecorateName(node.Name) : node.Name :
                        string.Format("{0}.{1}", parent, decorate ? Database.DecorateName(node.Name) : node.Name);

                    columnSchema = GetColumnFromSchema(name);

                    string value = Parse(node.Value, ref columnSchema, pars, nulls: true);
                    return string.Format("{0} = ({1})", name, value);
                }

                protected virtual string ParseUnary(DynamicParser.Node.Unary node, IDictionary<string, IParameter> pars = null)
                {
                    switch (node.Operation)
                    {
                        // Artifacts from the DynamicParser class that are not usefull here...
                        case ExpressionType.IsFalse:
                        case ExpressionType.IsTrue: return Parse(node.Target, pars);

                        // Unary supported operations...
                        case ExpressionType.Not: return string.Format("(NOT {0})", Parse(node.Target, pars));
                        case ExpressionType.Negate: return string.Format("!({0})", Parse(node.Target, pars));
                    }

                    throw new ArgumentException("Not supported unary operation: " + node);
                }

                protected virtual string ParseBinary(DynamicParser.Node.Binary node, IDictionary<string, IParameter> pars = null)
                {
                    string op = string.Empty;

                    switch (node.Operation)
                    {
                        // Arithmetic binary operations...
                        case ExpressionType.Add: op = "+"; break;
                        case ExpressionType.Subtract: op = "-"; break;
                        case ExpressionType.Multiply: op = "*"; break;
                        case ExpressionType.Divide: op = "/"; break;
                        case ExpressionType.Modulo: op = "%"; break;
                        case ExpressionType.Power: op = "^"; break;

                        case ExpressionType.And: op = "AND"; break;
                        case ExpressionType.Or: op = "OR"; break;

                        // Logical comparisons...
                        case ExpressionType.GreaterThan: op = ">"; break;
                        case ExpressionType.GreaterThanOrEqual: op = ">="; break;
                        case ExpressionType.LessThan: op = "<"; break;
                        case ExpressionType.LessThanOrEqual: op = "<="; break;

                        // Comparisons against 'NULL' require the 'IS' or 'IS NOT' operator instead the numeric ones...
                        case ExpressionType.Equal: op = node.Right == null && !VirtualMode ? "IS" : "="; break;
                        case ExpressionType.NotEqual: op = node.Right == null && !VirtualMode ? "IS NOT" : "<>"; break;

                        default: throw new ArgumentException("Not supported operator: '" + node.Operation);
                    }

                    DynamicSchemaColumn? columnSchema = null;
                    string left = Parse(node.Left, ref columnSchema, pars); // Not nulls: left is assumed to be an object
                    string right = Parse(node.Right, ref columnSchema, pars, nulls: true);
                    return string.Format("({0} {1} {2})", left, op, right);
                }

                protected virtual string ParseMethod(DynamicParser.Node.Method node, IDictionary<string, IParameter> pars = null)
                {
                    string method = node.Name.ToUpper();
                    string parent = node.Host == null ? null : Parse(node.Host, pars: pars);
                    string item = null;

                    // Root-level methods...
                    if (node.Host == null)
                    {
                        switch (method)
                        {
                            case "NOT":
                                if (node.Arguments == null || node.Arguments.Length != 1) throw new ArgumentNullException("NOT method expects one argument: " + node.Arguments.Sketch());
                                item = Parse(node.Arguments[0], pars: pars);
                                return string.Format("(NOT {0})", item);
                        }
                    }

                    // Column-level methods...
                    if (node.Host != null)
                    {
                        switch (method)
                        {
                            case "BETWEEN":
                                {
                                    if (node.Arguments == null || node.Arguments.Length == 0)
                                        throw new ArgumentException("BETWEEN method expects at least one argument: " + node.Arguments.Sketch());

                                    if (node.Arguments.Length > 2)
                                        throw new ArgumentException("BETWEEN method expects at most two arguments: " + node.Arguments.Sketch());

                                    var arguments = node.Arguments;

                                    if (arguments.Length == 1 && (arguments[0] is IEnumerable<object> || arguments[0] is Array) && !(arguments[0] is byte[]))
                                    {
                                        var vals = arguments[0] as IEnumerable<object>;

                                        if (vals == null && arguments[0] is Array)
                                            vals = ((Array)arguments[0]).Cast<object>() as IEnumerable<object>;

                                        if (vals != null)
                                            arguments = vals.ToArray();
                                        else
                                            throw new ArgumentException("BETWEEN method expects single argument to be enumerable of exactly two elements: " + node.Arguments.Sketch());
                                    }

                                    return string.Format("{0} BETWEEN {1} AND {2}", parent, Parse(arguments[0], pars: pars), Parse(arguments[1], pars: pars));
                                }

                            case "IN":
                                {
                                    if (node.Arguments == null || node.Arguments.Length == 0)
                                        throw new ArgumentException("IN method expects at least one argument: " + node.Arguments.Sketch());

                                    bool firstParam = true;
                                    StringBuilder sbin = new StringBuilder();
                                    foreach (var arg in node.Arguments)
                                    {
                                        if (!firstParam)
                                            sbin.Append(", ");

                                        if ((arg is IEnumerable<object> || arg is Array) && !(arg is byte[]))
                                        {
                                            var vals = arg as IEnumerable<object>;

                                            if (vals == null && arg is Array)
                                                vals = ((Array)arg).Cast<object>() as IEnumerable<object>;

                                            if (vals != null)
                                                foreach (var val in vals)
                                                {
                                                    if (!firstParam)
                                                        sbin.Append(", ");
                                                    else
                                                        firstParam = false;

                                                    sbin.Append(Parse(val, pars: pars));
                                                }
                                            else
                                                sbin.Append(Parse(arg, pars: pars));
                                        }
                                        else
                                            sbin.Append(Parse(arg, pars: pars));

                                        firstParam = false;
                                    }

                                    return string.Format("{0} IN({1})", parent, sbin.ToString());
                                }

                            case "LIKE":
                                if (node.Arguments == null || node.Arguments.Length != 1)
                                    throw new ArgumentException("LIKE method expects one argument: " + node.Arguments.Sketch());

                                return string.Format("{0} LIKE {1}", parent, Parse(node.Arguments[0], pars: pars));

                            case "NOTLIKE":
                                if (node.Arguments == null || node.Arguments.Length != 1)
                                    throw new ArgumentException("NOT LIKE method expects one argument: " + node.Arguments.Sketch());

                                return string.Format("{0} NOT LIKE {1}", parent, Parse(node.Arguments[0], pars: pars));

                            case "AS":
                                if (node.Arguments == null || node.Arguments.Length != 1)
                                    throw new ArgumentException("AS method expects one argument: " + node.Arguments.Sketch());

                                item = Parse(node.Arguments[0], pars: null, rawstr: true, isMultiPart: false); // pars=null to avoid to parameterize aliases
                                item = item.Validated("Alias"); // Intercepting null and empty aliases
                                return string.Format("{0} AS {1}", parent, item);

                            case "COUNT":
                                if (node.Arguments != null && node.Arguments.Length > 1)
                                    throw new ArgumentException("COUNT method expects one or none argument: " + node.Arguments.Sketch());

                                if (node.Arguments == null || node.Arguments.Length == 0)
                                    return "COUNT(*)";

                                return string.Format("COUNT({0})", Parse(node.Arguments[0], pars: Parameters, nulls: true));
                        }
                    }

                    // Default case: parsing the method's name along with its arguments...
                    method = parent == null ? node.Name : string.Format("{0}.{1}", parent, node.Name);
                    StringBuilder sb = new StringBuilder();
                    sb.AppendFormat("{0}(", method);

                    if (node.Arguments != null && node.Arguments.Length != 0)
                    {
                        bool first = true;

                        foreach (object argument in node.Arguments)
                        {
                            if (!first)
                                sb.Append(", ");
                            else
                                first = false;

                            sb.Append(Parse(argument, pars, nulls: true)); // We don't accept raw strings here!!!
                        }
                    }

                    sb.Append(")");
                    return sb.ToString();
                }

                protected virtual string ParseInvoke(DynamicParser.Node.Invoke node, ref DynamicSchemaColumn? columnSchema, IDictionary<string, IParameter> pars = null)
                {
                    // This is used as an especial syntax to merely concatenate its arguments. It is used as a way to extend the supported syntax without the need of treating all the possible cases...
                    if (node.Arguments == null || node.Arguments.Length == 0)
                        return string.Empty;

                    StringBuilder sb = new StringBuilder();
                    foreach (object arg in node.Arguments)
                    {
                        if (arg is string)
                        {
                            sb.Append((string)arg);

                            if (node.Arguments.Length == 1 && !columnSchema.HasValue)
                                columnSchema = GetColumnFromSchema((string)arg);
                        }
                        else
                            sb.Append(Parse(arg, ref columnSchema, pars, rawstr: true, nulls: true));
                    }

                    return sb.ToString();
                }

                protected virtual string ParseConvert(DynamicParser.Node.Convert node, IDictionary<string, IParameter> pars = null)
                {
                    // The cast mechanism is left for the specific database implementation, that should override this method
                    // as needed...
                    string r = Parse(node.Target, pars);
                    return r;
                }

                protected virtual string ParseConstant(object node, IDictionary<string, IParameter> pars = null, DynamicSchemaColumn? columnSchema = null)
                {
                    if (node == null && !VirtualMode)
                        return ParseNull();

                    if (pars != null)
                    {
                        bool wellKnownName = VirtualMode && node is String && ((String)node).StartsWith("[$") && ((String)node).EndsWith("]") && ((String)node).Length > 4;

                        // If we have a list of parameters to store it, let's parametrize it
                        var par = new Parameter()
                        {
                            Name = wellKnownName ? ((String)node).Substring(2, ((String)node).Length - 3) : Guid.NewGuid().ToString(),
                            Value = wellKnownName ? null : node,
                            WellKnown = wellKnownName,
                            Virtual = VirtualMode,
                            Schema = columnSchema,
                        };

                        // If we are adding parameter we inform external sources about this.
                        if (OnCreateTemporaryParameter != null)
                            OnCreateTemporaryParameter.ForEach(x => x(par));

                        pars.Add(par.Name, par);

                        return string.Format("[${0}]", par.Name);
                    }

                    return node.ToString(); // Last resort case
                }

                protected virtual string ParseNull()
                {
                    return "NULL"; // Override if needed
                }

                #endregion Parser

                #region Helpers

                internal bool IsTableAlias(string name)
                {
                    DynamicQueryBuilder builder = this;

                    while (builder != null)
                    {
                        if (builder.Tables.Any(t => t.Alias == name))
                            return true;

                        builder = builder._parent;
                    }

                    return false;
                }

                internal bool IsTable(string name, string owner)
                {
                    DynamicQueryBuilder builder = this;

                    while (builder != null)
                    {
                        if ((string.IsNullOrEmpty(owner) && builder.Tables.Any(t => t.Name.ToLower() == name.ToLower())) ||
                            (!string.IsNullOrEmpty(owner) && builder.Tables.Any(t => t.Name.ToLower() == name.ToLower() &&
                                !string.IsNullOrEmpty(t.Owner) && t.Owner.ToLower() == owner.ToLower())))
                            return true;

                        builder = builder._parent;
                    }

                    return false;
                }

                internal string FixObjectName(string main, bool onlyColumn = false)
                {
                    if (main.IndexOf("(") > 0 && main.IndexOf(")") > 0)
                        return main.FillStringWithVariables(f => string.Format("({0})", FixObjectNamePrivate(f, onlyColumn)), "(", ")");
                    else
                        return FixObjectNamePrivate(main, onlyColumn);
                }

                private string FixObjectNamePrivate(string f, bool onlyColumn = false)
                {
                    var objects = f.Split('.')
                        .Select(x => Database.StripName(x));

                    if (onlyColumn || objects.Count() == 1)
                        f = Database.DecorateName(objects.Last());
                    else if (!IsTableAlias(objects.First()))
                        f = string.Join(".", objects.Select(o => Database.DecorateName(o)));
                    else
                        f = string.Format("{0}.{1}", objects.First(), string.Join(".", objects.Skip(1).Select(o => Database.DecorateName(o))));

                    return f;
                }

                internal DynamicSchemaColumn? GetColumnFromSchema(string colName, DynamicTypeMap mapper = null, string table = null)
                {
                    // This is tricky and will not always work unfortunetly.
                    ////if (colName.ContainsAny(StringExtensions.InvalidMultipartMemberChars))
                    ////    return null;

                    // First we need to get real column name and it's owner if exist.
                    var parts = colName.Split('.')
                        .Select(c => Database.StripName(c))
                        .ToArray();

                    var columnName = parts.Last();

                    // Get table name from mapper
                    string tableName = table;

                    if (string.IsNullOrEmpty(tableName))
                    {
                        tableName = (mapper != null && mapper.Table != null) ? mapper.Table.Name : string.Empty;

                        if (parts.Length > 1 && string.IsNullOrEmpty(tableName))
                        {
                            // OK, we have a multi part identifier, that's good, we can get table name
                            tableName = string.Join(".", parts.Take(parts.Length - 1));
                        }
                    }

                    // Try to get table info from cache
                    var tableInfo = !string.IsNullOrEmpty(tableName) ?
                        Tables.FirstOrDefault(x => !string.IsNullOrEmpty(x.Alias) && x.Alias.ToLower() == tableName) ??
                        Tables.FirstOrDefault(x => x.Name.ToLower() == tableName.ToLower()) ?? Tables.FirstOrDefault() :
                        this is DynamicModifyBuilder ? Tables.FirstOrDefault() : null;

                    // Try to get column from schema
                    if (tableInfo != null && tableInfo.Schema != null)
                        return tableInfo.Schema.TryGetNullable(columnName.ToLower());

                    // Well, we failed to find a column
                    return null;
                }

                #endregion Helpers
            }

            /// <summary>Implementation of dynamic select query builder.</summary>
            internal class DynamicSelectQueryBuilder : DynamicQueryBuilder, IDynamicSelectQueryBuilder, DynamicQueryBuilder.IQueryWithWhere
            {
                private int? _top = null;
                private int? _limit = null;
                private int? _offset = null;
                private bool _distinct = false;

                private string _select;
                private string _from;
                private string _join;
                private string _groupby;
                private string _orderby;

                /// <summary>
                /// Gets a value indicating whether this instance has select columns.
                /// </summary>
                public bool HasSelectColumns { get { return !string.IsNullOrEmpty(_select); } }

                /// <summary>
                /// Initializes a new instance of the <see cref="DynamicSelectQueryBuilder"/> class.
                /// </summary>
                /// <param name="db">The database.</param>
                public DynamicSelectQueryBuilder(DynamicDatabase db)
                    : base(db)
                {
                }

                /// <summary>
                /// Initializes a new instance of the <see cref="DynamicSelectQueryBuilder"/> class.
                /// </summary>
                /// <param name="db">The database.</param>
                /// <param name="parent">The parent query.</param>
                internal DynamicSelectQueryBuilder(DynamicDatabase db, DynamicQueryBuilder parent)
                    : base(db, parent)
                {
                }

                /// <summary>Generates the text this command will execute against the underlying database.</summary>
                /// <returns>The text to execute against the underlying database.</returns>
                public override string CommandText()
                {
                    StringBuilder sb = new StringBuilder("SELECT");
                    if (_distinct) sb.AppendFormat(" DISTINCT");
                    if (_top.HasValue) sb.AppendFormat(" TOP {0}", _top);
                    if (_select != null) sb.AppendFormat(" {0}", _select); else sb.Append(" *");
                    if (_from != null) sb.AppendFormat(" FROM {0}", _from);
                    if (_join != null) sb.AppendFormat(" {0}", _join);
                    if (WhereCondition != null) sb.AppendFormat(" WHERE {0}", WhereCondition);
                    if (_groupby != null) sb.AppendFormat(" GROUP BY {0}", _groupby);
                    if (_orderby != null) sb.AppendFormat(" ORDER BY {0}", _orderby);
                    if (_limit.HasValue) sb.AppendFormat(" LIMIT {0}", _limit);
                    if (_offset.HasValue) sb.AppendFormat(" OFFSET {0}", _offset);

                    return sb.ToString();
                }

                #region Execution

                /*/// <summary>Execute this builder.</summary>

                /// <returns>Enumerator of objects expanded from query.</returns>
                public virtual IEnumerator<dynamic> GetEnumerator()
                {
                    using (var con = Database.Open())
                    using (var cmd = con.CreateCommand())
                    {
                        using (var rdr = cmd
                            .SetCommand(this)
                            .ExecuteReader())
                            while (rdr.Read())
                            {
                                dynamic val = null;

                                // Work around to avoid yield being in try...catchblock:
                                // http://stackoverflow.com/questions/346365/why-cant-yield-return-appear-inside-a-try-block-with-a-catch
                                try
                                {
                                    val = rdr.RowToDynamic();
                                }
                                catch (ArgumentException argex)
                                {
                                    var sb = new StringBuilder();
                                    cmd.Dump(sb);

                                    throw new ArgumentException(string.Format("{0}{1}{2}", argex.Message, Environment.NewLine, sb),
                                        argex.InnerException.NullOr(a => a, argex));
                                }

                                yield return val;
                            }
                    }
                }

                /// <summary>Execute this builder.</summary>
                /// <returns>Enumerator of objects expanded from query.</returns>
                System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
                {
                    return GetEnumerator();
                }*/

                /// <summary>Execute this builder.</summary>
                /// <returns>Enumerator of objects expanded from query.</returns>
                public virtual IEnumerable<dynamic> Execute()
                {
                    using (var con = Database.Open())
                    using (var cmd = con.CreateCommand())
                    using (var rdr = cmd
                        .SetCommand(this)
                        .ExecuteReader())
                        while (rdr.Read())
                        {
                            dynamic val = null;

                            // Work around to avoid yield being in try...catchblock:
                            // http://stackoverflow.com/questions/346365/why-cant-yield-return-appear-inside-a-try-block-with-a-catch
                            try
                            {
                                val = rdr.RowToDynamic();
                            }
                            catch (ArgumentException argex)
                            {
                                var sb = new StringBuilder();
                                cmd.Dump(sb);

                                throw new ArgumentException(string.Format("{0}{1}{2}", argex.Message, Environment.NewLine, sb),
                                    argex.InnerException.NullOr(a => a, argex));
                            }

                            yield return val;
                        }
                }

                /// <summary>Execute this builder and map to given type.</summary>
                /// <typeparam name="T">Type of object to map on.</typeparam>
                /// <returns>Enumerator of objects expanded from query.</returns>
                public virtual IEnumerable<T> Execute<T>() where T : class
                {
                    var mapper = DynamicMapperCache.GetMapper<T>();

                    if (mapper == null)
                        throw new InvalidOperationException("Type can't be mapped for unknown reason.");

                    using (var con = Database.Open())
                    using (var cmd = con.CreateCommand())
                    {
                        using (var rdr = cmd
                            .SetCommand(this)
                            .ExecuteReader())
                            while (rdr.Read())
                            {
                                dynamic val = null;

                                // Work around to avoid yield being in try...catchblock:
                                // http://stackoverflow.com/questions/346365/why-cant-yield-return-appear-inside-a-try-block-with-a-catch
                                try
                                {
                                    val = rdr.RowToDynamic();
                                }
                                catch (ArgumentException argex)
                                {
                                    var sb = new StringBuilder();
                                    cmd.Dump(sb);

                                    throw new ArgumentException(string.Format("{0}{1}{2}", argex.Message, Environment.NewLine, sb),
                                        argex.InnerException.NullOr(a => a, argex));
                                }

                                yield return mapper.Create(val) as T;
                            }
                    }
                }

                /// <summary>Execute this builder as a data reader.</summary>
                /// <param name="reader">Action containing reader.</param>
                public virtual void ExecuteDataReader(Action<IDataReader> reader)
                {
                    using (var con = Database.Open())
                    using (var cmd = con.CreateCommand())
                    using (var rdr = cmd
                        .SetCommand(this)
                        .ExecuteReader())
                        reader(rdr);
                }

                /// <summary>Returns a single result.</summary>
                /// <returns>Result of a query.</returns>
                public virtual object Scalar()
                {
                    using (var con = Database.Open())
                    using (var cmd = con.CreateCommand())
                    {
                        return cmd
                            .SetCommand(this)
                            .ExecuteScalar();
                    }
                }

                #endregion Execution

                #region From/Join

                /// <summary>
                /// Adds to the 'From' clause the contents obtained by parsing the dynamic lambda expressions given. The supported
                /// formats are:
                /// <para>- Resolve to a string: 'x => "Table AS Alias', where the alias part is optional.</para>
                /// <para>- Resolve to an expression: 'x => x.Table.As( x.Alias )', where the alias part is optional.</para>
                /// <para>- Generic expression: 'x => x( expression ).As( x.Alias )', where the alias part is mandatory. In this
                /// case the alias is not annotated.</para>
                /// </summary>
                /// <param name="fn">The specification.</param>
                /// <param name="func">The specification.</param>
                /// <returns>This instance to permit chaining.</returns>
                public virtual IDynamicSelectQueryBuilder From(Func<dynamic, object> fn, params Func<dynamic, object>[] func)
                {
                    if (fn == null)
                        throw new ArgumentNullException("Array of functions cannot be or contain null.");

                    int index = FromFunc(-1, fn);
                    foreach (var f in func)
                        index = FromFunc(index, f);

                    return this;
                }

                private int FromFunc(int index, Func<dynamic, object> f)
                {
                    if (f == null)
                        throw new ArgumentNullException("Array of functions cannot be or contain null.");

                    index++;
                    ITableInfo tableInfo = null;
                    using (var parser = DynamicParser.Parse(f))
                    {
                        var result = parser.Result;

                        // If the expression result is string.
                        if (result is string)
                        {
                            var node = (string)result;
                            var tuple = node.SplitSomethingAndAlias();
                            var parts = tuple.Item1.Split('.');
                            tableInfo = new TableInfo(Database,
                                Database.StripName(parts.Last()).Validated("Table"),
                                tuple.Item2.Validated("Alias", canbeNull: true),
                                parts.Length == 2 ? Database.StripName(parts.First()).Validated("Owner", canbeNull: true) : null);
                        }
                        else if (result is Type)
                        {
                            Type type = (Type)result;
                            if (type.IsAnonymous())
                                throw new InvalidOperationException(string.Format("Cant assign anonymous type as a table ({0}). Parsing {1}", type.FullName, result));

                            var mapper = DynamicMapperCache.GetMapper(type);

                            if (mapper == null)
                                throw new InvalidOperationException(string.Format("Cant assign unmapable type as a table ({0}). Parsing {1}", type.FullName, result));

                            tableInfo = new TableInfo(Database, type);
                        }
                        else if (result is DynamicParser.Node)
                        {
                            // Or if it resolves to a dynamic node
                            var node = (DynamicParser.Node)result;

                            string owner = null;
                            string main = null;
                            string alias = null;
                            Type type = null;

                            while (true)
                            {
                                // Support for the AS() virtual method...
                                if (node is DynamicParser.Node.Method && ((DynamicParser.Node.Method)node).Name.ToUpper() == "AS")
                                {
                                    if (alias != null)
                                        throw new ArgumentException(string.Format("Alias '{0}' is already set when parsing '{1}'.", alias, result));

                                    object[] args = ((DynamicParser.Node.Method)node).Arguments;

                                    if (args == null)
                                        throw new ArgumentNullException("arg", "AS() is not a parameterless method.");

                                    if (args.Length != 1)
                                        throw new ArgumentException("AS() requires one and only one parameter: " + args.Sketch());

                                    alias = Parse(args[0], rawstr: true, decorate: false).Validated("Alias");

                                    node = node.Host;
                                    continue;
                                }

                                /*if (node is DynamicParser.Node.Method && ((DynamicParser.Node.Method)node).Name.ToUpper() == "subquery")
                                {
                                    main = Parse(this.SubQuery(((DynamicParser.Node.Method)node).Arguments.Where(p => p is Func<dynamic, object>).Cast<Func<dynamic, object>>().ToArray()), Parameters);
                                    continue;
                                }*/

                                // Support for table specifications...
                                if (node is DynamicParser.Node.GetMember)
                                {
                                    if (owner != null)
                                        throw new ArgumentException(string.Format("Owner '{0}.{1}' is already set when parsing '{2}'.", owner, main, result));

                                    if (main != null)
                                        owner = ((DynamicParser.Node.GetMember)node).Name;
                                    else
                                        main = ((DynamicParser.Node.GetMember)node).Name;

                                    node = node.Host;
                                    continue;
                                }

                                // Support for generic sources...
                                if (node is DynamicParser.Node.Invoke)
                                {
                                    if (owner != null)
                                        throw new ArgumentException(string.Format("Owner '{0}.{1}' is already set when parsing '{2}'.", owner, main, result));

                                    if (main != null)
                                        owner = string.Format("{0}", Parse(node, rawstr: true, pars: Parameters));
                                    else
                                    {
                                        var invoke = (DynamicParser.Node.Invoke)node;
                                        if (invoke.Arguments.Length == 1 && invoke.Arguments[0] is Type)
                                        {
                                            type = (Type)invoke.Arguments[0];
                                            if (type.IsAnonymous())
                                                throw new InvalidOperationException(string.Format("Cant assign anonymous type as a table ({0}). Parsing {1}", type.FullName, result));

                                            var mapper = DynamicMapperCache.GetMapper(type);

                                            if (mapper == null)
                                                throw new InvalidOperationException(string.Format("Cant assign unmapable type as a table ({0}). Parsing {1}", type.FullName, result));

                                            main = mapper.Table == null || string.IsNullOrEmpty(mapper.Table.Name) ?
                                                mapper.Type.Name : mapper.Table.Name;

                                            owner = (mapper.Table != null) ? mapper.Table.Owner : owner;
                                        }
                                        else
                                            main = string.Format("{0}", Parse(node, rawstr: true, pars: Parameters));
                                    }

                                    node = node.Host;
                                    continue;
                                }

                                // Just finished the parsing...
                                if (node is DynamicParser.Node.Argument) break;

                                // All others are assumed to be part of the main element...
                                if (main != null)
                                    main = Parse(node, pars: Parameters);
                                else
                                    main = Parse(node, pars: Parameters);

                                break;
                            }

                            if (!string.IsNullOrEmpty(main))
                                tableInfo = type == null ? new TableInfo(Database, main, alias, owner) : new TableInfo(Database, type, alias, owner);
                            else
                                throw new ArgumentException(string.Format("Specification #{0} is invalid: {1}", index, result));
                        }

                        // Or it is a not supported expression...
                        if (tableInfo == null)
                            throw new ArgumentException(string.Format("Specification #{0} is invalid: {1}", index, result));

                        Tables.Add(tableInfo);

                        // We finally add the contents...
                        StringBuilder sb = new StringBuilder();

                        if (!string.IsNullOrEmpty(tableInfo.Owner))
                            sb.AppendFormat("{0}.", Database.DecorateName(tableInfo.Owner));

                        sb.Append(tableInfo.Name.ContainsAny(StringExtensions.InvalidMemberChars) ? tableInfo.Name : Database.DecorateName(tableInfo.Name));

                        if (!string.IsNullOrEmpty(tableInfo.Alias))
                            sb.AppendFormat(" AS {0}", tableInfo.Alias);

                        _from = string.IsNullOrEmpty(_from) ? sb.ToString() : string.Format("{0}, {1}", _from, sb.ToString());
                    }

                    return index;
                }

                /// <summary>
                /// Adds to the 'Join' clause the contents obtained by parsing the dynamic lambda expressions given. The supported
                /// formats are:
                /// <para>- Resolve to a string: 'x => "Table AS Alias ON Condition', where the alias part is optional.</para>
                /// <para>- Resolve to an expression: 'x => x.Table.As( x.Alias ).On( condition )', where the alias part is optional.</para>
                /// <para>- Generic expression: 'x => x( expression ).As( x.Alias ).On( condition )', where the alias part is mandatory.
                /// In this case the alias is not annotated.</para>
                /// The expression might be prepended by a method that, in this case, is used to specify the specific join type you
                /// want to perform, as in: 'x => x.Left()...". Two considerations apply:
                /// <para>- If a 'false' argument is used when no 'Join' part appears in its name, then no 'Join' suffix is added
                /// with a space in between.</para>
                /// <para>- If a 'false' argument is used when a 'Join' part does appear, then no split is performed to separate the
                /// 'Join' part.</para>
                /// </summary>
                /// <param name="func">The specification.</param>
                /// <returns>This instance to permit chaining.</returns>
                public virtual IDynamicSelectQueryBuilder Join(params Func<dynamic, object>[] func)
                {
                    // We need to do two passes to add aliases first.
                    return JoinInternal(true, func).JoinInternal(false, func);
                }

                /// <summary>
                /// Adds to the 'Join' clause the contents obtained by parsing the dynamic lambda expressions given. The supported
                /// formats are:
                /// <para>- Resolve to a string: 'x => "Table AS Alias ON Condition', where the alias part is optional.</para>
                /// <para>- Resolve to an expression: 'x => x.Table.As( x.Alias ).On( condition )', where the alias part is optional.</para>
                /// <para>- Generic expression: 'x => x( expression ).As( x.Alias ).On( condition )', where the alias part is mandatory.
                /// In this case the alias is not annotated.</para>
                /// The expression might be prepended by a method that, in this case, is used to specify the specific join type you
                /// want to perform, as in: 'x => x.Left()...". Two considerations apply:
                /// <para>- If a 'false' argument is used when no 'Join' part appears in its name, then no 'Join' suffix is added
                /// with a space in between.</para>
                /// <para>- If a 'false' argument is used when a 'Join' part does appear, then no split is performed to separate the
                /// 'Join' part.</para>
                /// </summary>
                /// <param name="justAddTables">If <c>true</c> just pass by to locate tables and aliases, otherwise create rules.</param>
                /// <param name="func">The specification.</param>
                /// <returns>This instance to permit chaining.</returns>
                protected virtual DynamicSelectQueryBuilder JoinInternal(bool justAddTables, params Func<dynamic, object>[] func)
                {
                    if (func == null) throw new ArgumentNullException("Array of functions cannot be null.");

                    int index = -1;

                    foreach (var f in func)
                    {
                        index++;
                        ITableInfo tableInfo = null;

                        if (f == null)
                            throw new ArgumentNullException(string.Format("Specification #{0} cannot be null.", index));

                        using (var parser = DynamicParser.Parse(f))
                        {
                            var result = parser.Result;
                            if (result == null) throw new ArgumentException(string.Format("Specification #{0} resolves to null.", index));

                            string type = null;
                            string main = null;
                            string owner = null;
                            string alias = null;
                            string condition = null;
                            Type tableType = null;

                            // If the expression resolves to a string...
                            if (result is string)
                            {
                                var node = (string)result;

                                int n = node.ToUpper().IndexOf("JOIN ");

                                if (n < 0)
                                    main = node;
                                else
                                {
                                    // For strings we only accept 'JOIN' as a suffix
                                    type = node.Substring(0, n + 4);
                                    main = node.Substring(n + 4);
                                }

                                n = main.ToUpper().IndexOf("ON");

                                if (n >= 0)
                                {
                                    condition = main.Substring(n + 3);
                                    main = main.Substring(0, n).Trim();
                                }

                                var tuple = main.SplitSomethingAndAlias(); // In this case we split on the remaining 'main'
                                var parts = tuple.Item1.Split('.');
                                main = Database.StripName(parts.Last()).Validated("Table");
                                owner = parts.Length == 2 ? Database.StripName(parts.First()).Validated("Owner", canbeNull: true) : null;
                                alias = tuple.Item2.Validated("Alias", canbeNull: true);
                            }
                            else if (result is DynamicParser.Node)
                            {
                                // Or if it resolves to a dynamic node...
                                var node = (DynamicParser.Node)result;
                                while (true)
                                {
                                    // Support for the ON() virtual method...
                                    if (node is DynamicParser.Node.Method && ((DynamicParser.Node.Method)node).Name.ToUpper() == "ON")
                                    {
                                        if (condition != null)
                                            throw new ArgumentException(string.Format("Condition '{0}' is already set when parsing '{1}'.", alias, result));

                                        object[] args = ((DynamicParser.Node.Method)node).Arguments;
                                        if (args == null)
                                            throw new ArgumentNullException("arg", "ON() is not a parameterless method.");

                                        if (args.Length != 1)
                                            throw new ArgumentException("ON() requires one and only one parameter: " + args.Sketch());

                                        condition = Parse(args[0], rawstr: true, pars: justAddTables ? null : Parameters);

                                        node = node.Host;
                                        continue;
                                    }

                                    // Support for the AS() virtual method...
                                    if (node is DynamicParser.Node.Method && ((DynamicParser.Node.Method)node).Name.ToUpper() == "AS")
                                    {
                                        if (alias != null)
                                            throw new ArgumentException(string.Format("Alias '{0}' is already set when parsing '{1}'.", alias, result));

                                        object[] args = ((DynamicParser.Node.Method)node).Arguments;

                                        if (args == null)
                                            throw new ArgumentNullException("arg", "AS() is not a parameterless method.");

                                        if (args.Length != 1)
                                            throw new ArgumentException("AS() requires one and only one parameter: " + args.Sketch());

                                        alias = Parse(args[0], rawstr: true, decorate: false, isMultiPart: false).Validated("Alias");

                                        node = node.Host;
                                        continue;
                                    }

                                    // Support for table specifications...
                                    if (node is DynamicParser.Node.GetMember)
                                    {
                                        if (owner != null)
                                            throw new ArgumentException(string.Format("Owner '{0}.{1}' is already set when parsing '{2}'.", owner, main, result));

                                        if (main != null)
                                            owner = ((DynamicParser.Node.GetMember)node).Name;
                                        else
                                            main = ((DynamicParser.Node.GetMember)node).Name;

                                        node = node.Host;
                                        continue;
                                    }

                                    // Support for Join Type specifications...
                                    if (node is DynamicParser.Node.Method && (node.Host is DynamicParser.Node.Argument || node.Host is DynamicParser.Node.Invoke))
                                    {
                                        if (type != null) throw new ArgumentException(string.Format("Join type '{0}' is already set when parsing '{1}'.", main, result));
                                        type = ((DynamicParser.Node.Method)node).Name;

                                        bool avoid = false;
                                        object[] args = ((DynamicParser.Node.Method)node).Arguments;

                                        if (args != null && args.Length > 0)
                                        {
                                            avoid = args[0] is bool && !((bool)args[0]);
                                            var proposedType = args.FirstOrDefault(a => a is string) as string;
                                            if (!string.IsNullOrEmpty(proposedType))
                                                type = proposedType;
                                        }

                                        type = type.ToUpper(); // Normalizing, and stepping out the trivial case...
                                        if (type != "JOIN")
                                        {
                                            // Special cases
                                            // x => x.LeftOuter() / x => x.RightOuter()...
                                            type = type.Replace("OUTER", " OUTER ")
                                                .Replace("  ", " ")
                                                .Trim(' ');

                                            // x => x.Left()...
                                            int n = type.IndexOf("JOIN");

                                            if (n < 0 && !avoid)
                                                type += " JOIN";

                                            // x => x.InnerJoin() / x => x.JoinLeft() ...
                                            else
                                            {
                                                if (!avoid)
                                                {
                                                    if (n == 0) type = type.Replace("JOIN", "JOIN ");
                                                    else type = type.Replace("JOIN", " JOIN");
                                                }
                                            }
                                        }

                                        node = node.Host;
                                        continue;
                                    }

                                    // Support for generic sources...
                                    if (node is DynamicParser.Node.Invoke)
                                    {
                                        if (owner != null)
                                            throw new ArgumentException(string.Format("Owner '{0}.{1}' is already set when parsing '{2}'.", owner, main, result));

                                        if (main != null)
                                            owner = string.Format("{0}", Parse(node, rawstr: true, pars: justAddTables ? null : Parameters));
                                        else
                                        {
                                            var invoke = (DynamicParser.Node.Invoke)node;
                                            if (invoke.Arguments.Length == 1 && invoke.Arguments[0] is Type)
                                            {
                                                tableType = (Type)invoke.Arguments[0];
                                                var mapper = DynamicMapperCache.GetMapper(tableType);

                                                if (mapper == null)
                                                    throw new InvalidOperationException(string.Format("Cant assign unmapable type as a table ({0}).", tableType.FullName));

                                                main = mapper.Table == null || string.IsNullOrEmpty(mapper.Table.Name) ?
                                                    mapper.Type.Name : mapper.Table.Name;

                                                owner = (mapper.Table != null) ? mapper.Table.Owner : owner;
                                            }
                                            else
                                                main = string.Format("{0}", Parse(node, rawstr: true, pars: justAddTables ? null : Parameters));
                                        }

                                        node = node.Host;
                                        continue;
                                    }

                                    // Just finished the parsing...
                                    if (node is DynamicParser.Node.Argument) break;
                                    throw new ArgumentException(string.Format("Specification #{0} is invalid: {1}", index, result));
                                }
                            }
                            else
                            {
                                // Or it is a not supported expression...
                                throw new ArgumentException(string.Format("Specification #{0} is invalid: {1}", index, result));
                            }

                            // We annotate the aliases being conservative...
                            main = main.Validated("Main");

                            if (justAddTables)
                            {
                                if (!string.IsNullOrEmpty(main))
                                    tableInfo = tableType == null ? new TableInfo(Database, main, alias, owner) : new TableInfo(Database, tableType, alias, owner);
                                else
                                    throw new ArgumentException(string.Format("Specification #{0} is invalid: {1}", index, result));

                                Tables.Add(tableInfo);
                            }
                            else
                            {
                                // Get cached table info
                                tableInfo = string.IsNullOrEmpty(alias) ?
                                    Tables.SingleOrDefault(t => t.Name == main && string.IsNullOrEmpty(t.Alias)) :
                                    Tables.SingleOrDefault(t => t.Alias == alias);

                                // We finally add the contents if we can...
                                StringBuilder sb = new StringBuilder();
                                if (string.IsNullOrEmpty(type))
                                    type = "JOIN";

                                sb.AppendFormat("{0} ", type);

                                if (!string.IsNullOrEmpty(tableInfo.Owner))
                                    sb.AppendFormat("{0}.", Database.DecorateName(tableInfo.Owner));

                                sb.Append(tableInfo.Name.ContainsAny(StringExtensions.InvalidMemberChars) ? tableInfo.Name : Database.DecorateName(tableInfo.Name));

                                if (!string.IsNullOrEmpty(tableInfo.Alias))
                                    sb.AppendFormat(" AS {0}", tableInfo.Alias);

                                if (!string.IsNullOrEmpty(condition))
                                    sb.AppendFormat(" ON {0}", condition);

                                _join = string.IsNullOrEmpty(_join) ? sb.ToString() : string.Format("{0} {1}", _join, sb.ToString()); // No comma in this case
                            }
                        }
                    }

                    return this;
                }

                #endregion From/Join

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
                public virtual IDynamicSelectQueryBuilder Where(Func<dynamic, object> func)
                {
                    return this.InternalWhere(func);
                }

                /// <summary>Add where condition.</summary>
                /// <param name="column">Condition column with operator and value.</param>
                /// <returns>Builder instance.</returns>
                public virtual IDynamicSelectQueryBuilder Where(DynamicColumn column)
                {
                    return this.InternalWhere(column);
                }

                /// <summary>Add where condition.</summary>
                /// <param name="column">Condition column.</param>
                /// <param name="op">Condition operator.</param>
                /// <param name="value">Condition value.</param>
                /// <returns>Builder instance.</returns>
                public virtual IDynamicSelectQueryBuilder Where(string column, DynamicColumn.CompareOperator op, object value)
                {
                    return this.InternalWhere(column, op, value);
                }

                /// <summary>Add where condition.</summary>
                /// <param name="column">Condition column.</param>
                /// <param name="value">Condition value.</param>
                /// <returns>Builder instance.</returns>
                public virtual IDynamicSelectQueryBuilder Where(string column, object value)
                {
                    return this.InternalWhere(column, value);
                }

                /// <summary>Add where condition.</summary>
                /// <param name="conditions">Set conditions as properties and values of an object.</param>
                /// <param name="schema">If <c>true</c> use schema to determine key columns and ignore those which
                /// aren't keys.</param>
                /// <returns>Builder instance.</returns>
                public virtual IDynamicSelectQueryBuilder Where(object conditions, bool schema = false)
                {
                    return this.InternalWhere(conditions, schema);
                }

                #endregion Where

                #region Select

                /// <summary>
                /// Adds to the 'Select' clause the contents obtained by parsing the dynamic lambda expressions given. The supported
                /// formats are:
                /// <para>- Resolve to a string: 'x => "Table.Column AS Alias', where the alias part is optional.</para>
                /// <para>- Resolve to an expression: 'x => x.Table.Column.As( x.Alias )', where the alias part is optional.</para>
                /// <para>- Select all columns from a table: 'x => x.Table.All()'.</para>
                /// <para>- Generic expression: 'x => x( expression ).As( x.Alias )', where the alias part is mandatory. In this case
                /// the alias is not annotated.</para>
                /// </summary>
                /// <param name="fn">The specification.</param>
                /// <param name="func">The specification.</param>
                /// <returns>This instance to permit chaining.</returns>
                public virtual IDynamicSelectQueryBuilder Select(Func<dynamic, object> fn, params Func<dynamic, object>[] func)
                {
                    if (fn == null)
                        throw new ArgumentNullException("Array of specifications cannot be null.");

                    int index = SelectFunc(-1, fn);
                    if (func != null)
                        foreach (var f in func)
                            index = SelectFunc(index, f);

                    return this;
                }

                private int SelectFunc(int index, Func<dynamic, object> f)
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
                        string alias = null;
                        bool all = false;
                        bool anon = false;

                        // If the expression resolves to a string...
                        if (result is string)
                        {
                            var node = (string)result;
                            var tuple = node.SplitSomethingAndAlias();
                            main = tuple.Item1.Validated("Table and/or Column");

                            main = FixObjectName(main);

                            alias = tuple.Item2.Validated("Alias", canbeNull: true);
                        }
                        else if (result is DynamicParser.Node)
                        {
                            // Or if it resolves to a dynamic node...
                            ParseSelectNode(result, ref main, ref alias, ref all);
                        }
                        else if (result.GetType().IsAnonymous())
                        {
                            anon = true;

                            foreach (var prop in result.ToDictionary())
                            {
                                if (prop.Value is string)
                                {
                                    var node = (string)prop.Value;
                                    var tuple = node.SplitSomethingAndAlias();
                                    main = FixObjectName(tuple.Item1.Validated("Table and/or Column"));

                                    ////alias = tuple.Item2.Validated("Alias", canbeNull: true);
                                }
                                else if (prop.Value is DynamicParser.Node)
                                {
                                    // Or if it resolves to a dynamic node...
                                    ParseSelectNode(prop.Value, ref main, ref alias, ref all);
                                }
                                else
                                {
                                    // Or it is a not supported expression...
                                    throw new ArgumentException(string.Format("Specification #{0} in anonymous type is invalid: {1}", index, prop.Value));
                                }

                                alias = Database.DecorateName(prop.Key);
                                ParseSelectAddColumn(main, alias, all);
                            }
                        }
                        else
                        {
                            // Or it is a not supported expression...
                            throw new ArgumentException(string.Format("Specification #{0} is invalid: {1}", index, result));
                        }

                        if (!anon)
                            ParseSelectAddColumn(main, alias, all);
                    }

                    return index;
                }

                /// <summary>Add select columns.</summary>
                /// <param name="columns">Columns to add to object.</param>
                /// <returns>Builder instance.</returns>
                public virtual IDynamicSelectQueryBuilder SelectColumn(params DynamicColumn[] columns)
                {
                    foreach (var col in columns)
                        Select(x => col.ToSQLSelectColumn(Database));

                    return this;
                }

                /// <summary>Add select columns.</summary>
                /// <param name="columns">Columns to add to object.</param>
                /// <remarks>Column format consist of <c>Column Name</c>, <c>Alias</c> and
                /// <c>Aggregate function</c> in this order separated by '<c>:</c>'.</remarks>
                /// <returns>Builder instance.</returns>
                public virtual IDynamicSelectQueryBuilder SelectColumn(params string[] columns)
                {
                    return SelectColumn(columns.Select(c => DynamicColumn.ParseSelectColumn(c)).ToArray());
                }

                #endregion Select

                #region GroupBy

                /// <summary>
                /// Adds to the 'Group By' clause the contents obtained from from parsing the dynamic lambda expression given.
                /// </summary>
                /// <param name="fn">The specification.</param>
                /// <param name="func">The specification.</param>
                /// <returns>This instance to permit chaining.</returns>
                public virtual IDynamicSelectQueryBuilder GroupBy(Func<dynamic, object> fn, params Func<dynamic, object>[] func)
                {
                    if (fn == null)
                        throw new ArgumentNullException("Array of specifications cannot be null.");

                    int index = GroupByFunc(-1, fn);

                    if (func != null)
                        foreach (var f in func)
                            index = GroupByFunc(index, f);

                    return this;
                }

                private int GroupByFunc(int index, Func<dynamic, object> f)
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

                        if (result is string)
                            main = FixObjectName(result as string);
                        else
                            main = Parse(result, pars: Parameters);

                        main = main.Validated("Group By");
                        if (_groupby == null)
                            _groupby = main;
                        else
                            _groupby = string.Format("{0}, {1}", _groupby, main);
                    }

                    return index;
                }

                /// <summary>Add select columns.</summary>
                /// <param name="columns">Columns to group by.</param>
                /// <returns>Builder instance.</returns>
                public virtual IDynamicSelectQueryBuilder GroupByColumn(params DynamicColumn[] columns)
                {
                    foreach (var col in columns)
                        GroupBy(x => col.ToSQLGroupByColumn(Database));

                    return this;
                }

                /// <summary>Add select columns.</summary>
                /// <param name="columns">Columns to group by.</param>
                /// <remarks>Column format consist of <c>Column Name</c> and
                /// <c>Alias</c> in this order separated by '<c>:</c>'.</remarks>
                /// <returns>Builder instance.</returns>
                public virtual IDynamicSelectQueryBuilder GroupByColumn(params string[] columns)
                {
                    return GroupByColumn(columns.Select(c => DynamicColumn.ParseSelectColumn(c)).ToArray());
                }

                #endregion GroupBy

                #region OrderBy

                /// <summary>
                /// Adds to the 'Order By' clause the contents obtained from from parsing the dynamic lambda expression given. It
                /// accepts a multipart column specification followed by an optional <code>Ascending()</code> or <code>Descending()</code> virtual methods
                /// to specify the direction. If no virtual method is used, the default is ascending order. You can also use the
                /// shorter versions <code>Asc()</code> and <code>Desc()</code>.
                /// </summary>
                /// <param name="fn">The specification.</param>
                /// <param name="func">The specification.</param>
                /// <returns>This instance to permit chaining.</returns>
                public virtual IDynamicSelectQueryBuilder OrderBy(Func<dynamic, object> fn, params Func<dynamic, object>[] func)
                {
                    if (fn == null)
                        throw new ArgumentNullException("Array of specifications cannot be null.");

                    int index = OrderByFunc(-1, fn);

                    if (func != null)
                        foreach (var f in func)
                            index = OrderByFunc(index, f);

                    return this;
                }

                private int OrderByFunc(int index, Func<dynamic, object> f)
                {
                    index++;
                    if (f == null)
                        throw new ArgumentNullException(string.Format("Specification #{0} cannot be null.", index));

                    using (var parser = DynamicParser.Parse(f))
                    {
                        var result = parser.Result;
                        if (result == null) throw new ArgumentException(string.Format("Specification #{0} resolves to null.", index));

                        string main = null;
                        bool ascending = true;

                        if (result is int)
                            main = result.ToString();
                        else if (result is string)
                        {
                            var parts = ((string)result).Split(' ');
                            main = Database.StripName(parts.First());

                            int colNo;
                            if (!Int32.TryParse(main, out colNo))
                                main = FixObjectName(main);

                            ascending = parts.Length != 2 || parts.Last().ToUpper() == "ASCENDING" || parts.Last().ToUpper() == "ASC";
                        }
                        else
                        {
                            // Intercepting trailing 'Ascending' or 'Descending' virtual methods...
                            if (result is DynamicParser.Node.Method)
                            {
                                var node = (DynamicParser.Node.Method)result;
                                var name = node.Name.ToUpper();
                                if (name == "ASCENDING" || name == "ASC" || name == "DESCENDING" || name == "DESC")
                                {
                                    object[] args = node.Arguments;
                                    if (args != null && !(node.Host is DynamicParser.Node.Argument))
                                        throw new ArgumentException(string.Format("{0} must be a parameterless method, but found: {1}.", name, args.Sketch()));
                                    else if ((args == null || args.Length != 1) && node.Host is DynamicParser.Node.Argument)
                                        throw new ArgumentException(string.Format("{0} requires one numeric parameter, but found: {1}.", name, args.Sketch()));

                                    ascending = (name == "ASCENDING" || name == "ASC") ? true : false;

                                    if (args != null && args.Length == 1)
                                    {
                                        int col = -1;
                                        if (args[0] is int)
                                            main = args[0].ToString();
                                        else if (args[0] is string)
                                        {
                                            if (Int32.TryParse(args[0].ToString(), out col))
                                                main = col.ToString();
                                            else
                                                main = FixObjectName(args[0].ToString());
                                        }
                                        else
                                            main = Parse(args[0], pars: Parameters);
                                    }

                                    result = node.Host;
                                }
                            }

                            // Just parsing the contents...
                            if (!(result is DynamicParser.Node.Argument))
                                main = Parse(result, pars: Parameters);
                        }

                        main = main.Validated("Order By");
                        main = string.Format("{0} {1}", main, ascending ? "ASC" : "DESC");

                        if (_orderby == null)
                            _orderby = main;
                        else
                            _orderby = string.Format("{0}, {1}", _orderby, main);
                    }

                    return index;
                }

                /// <summary>Add select columns.</summary>
                /// <param name="columns">Columns to order by.</param>
                /// <returns>Builder instance.</returns>
                public virtual IDynamicSelectQueryBuilder OrderByColumn(params DynamicColumn[] columns)
                {
                    foreach (var col in columns)
                        OrderBy(x => col.ToSQLOrderByColumn(Database));

                    return this;
                }

                /// <summary>Add select columns.</summary>
                /// <param name="columns">Columns to order by.</param>
                /// <remarks>Column format consist of <c>Column Name</c> and
                /// <c>Alias</c> in this order separated by '<c>:</c>'.</remarks>
                /// <returns>Builder instance.</returns>
                public virtual IDynamicSelectQueryBuilder OrderByColumn(params string[] columns)
                {
                    return OrderByColumn(columns.Select(c => DynamicColumn.ParseOrderByColumn(c)).ToArray());
                }

                #endregion OrderBy

                #region Top/Limit/Offset/Distinct

                /// <summary>Set top if database support it.</summary>
                /// <param name="top">How many objects select.</param>
                /// <returns>Builder instance.</returns>
                public virtual IDynamicSelectQueryBuilder Top(int? top)
                {
                    if ((Database.Options & DynamicDatabaseOptions.SupportTop) != DynamicDatabaseOptions.SupportTop)
                        throw new NotSupportedException("Database doesn't support TOP clause.");

                    _top = top;
                    return this;
                }

                /// <summary>Set top if database support it.</summary>
                /// <param name="limit">How many objects select.</param>
                /// <returns>Builder instance.</returns>
                public virtual IDynamicSelectQueryBuilder Limit(int? limit)
                {
                    if ((Database.Options & DynamicDatabaseOptions.SupportLimitOffset) != DynamicDatabaseOptions.SupportLimitOffset)
                        throw new NotSupportedException("Database doesn't support LIMIT clause.");

                    _limit = limit;
                    return this;
                }

                /// <summary>Set top if database support it.</summary>
                /// <param name="offset">How many objects skip selecting.</param>
                /// <returns>Builder instance.</returns>
                public virtual IDynamicSelectQueryBuilder Offset(int? offset)
                {
                    if ((Database.Options & DynamicDatabaseOptions.SupportLimitOffset) != DynamicDatabaseOptions.SupportLimitOffset)
                        throw new NotSupportedException("Database doesn't support OFFSET clause.");

                    _offset = offset;
                    return this;
                }

                /// <summary>Set distinct mode.</summary>
                /// <param name="distinct">Distinct mode.</param>
                /// <returns>Builder instance.</returns>
                public virtual IDynamicSelectQueryBuilder Distinct(bool distinct = true)
                {
                    _distinct = distinct;
                    return this;
                }

                #endregion Top/Limit/Offset/Distinct

                #region Helpers

                private void ParseSelectAddColumn(string main, string alias, bool all)
                {
                    // We annotate the aliases being conservative...
                    main = main.Validated("Main");

                    ////if (alias != null && !main.ContainsAny(StringExtensions.InvalidMemberChars)) TableAliasList.Add(new KTableAlias(main, alias));

                    // If all columns are requested...
                    if (all)
                        main += ".*";

                    // We finally add the contents...
                    string str = (alias == null || all) ? main : string.Format("{0} AS {1}", main, alias);
                    _select = _select == null ? str : string.Format("{0}, {1}", _select, str);
                }

                private void ParseSelectNode(object result, ref string column, ref string alias, ref bool all)
                {
                    string main = null;

                    var node = (DynamicParser.Node)result;
                    while (true)
                    {
                        // Support for the AS() virtual method...
                        if (node is DynamicParser.Node.Method && ((DynamicParser.Node.Method)node).Name.ToUpper() == "AS")
                        {
                            if (alias != null)
                                throw new ArgumentException(string.Format("Alias '{0}' is already set when parsing '{1}'.", alias, result));

                            object[] args = ((DynamicParser.Node.Method)node).Arguments;

                            if (args == null)
                                throw new ArgumentNullException("arg", "AS() is not a parameterless method.");

                            if (args.Length != 1)
                                throw new ArgumentException("AS() requires one and only one parameter: " + args.Sketch());

                            // Yes, we decorate columns
                            alias = Parse(args[0], rawstr: true, decorate: true, isMultiPart: false).Validated("Alias");

                            node = node.Host;
                            continue;
                        }

                        // Support for the ALL() virtual method...
                        if (node is DynamicParser.Node.Method && ((DynamicParser.Node.Method)node).Name.ToUpper() == "ALL")
                        {
                            if (all)
                                throw new ArgumentException(string.Format("Flag to select all columns is already set when parsing '{0}'.", result));

                            object[] args = ((DynamicParser.Node.Method)node).Arguments;

                            if (args != null)
                                throw new ArgumentException("ALL() must be a parameterless virtual method, but found: " + args.Sketch());

                            all = true;

                            node = node.Host;
                            continue;
                        }

                        // Support for table and/or column specifications...
                        if (node is DynamicParser.Node.GetMember)
                        {
                            if (main != null)
                                throw new ArgumentException(string.Format("Main '{0}' is already set when parsing '{1}'.", main, result));

                            main = ((DynamicParser.Node.GetMember)node).Name;

                            if (node.Host is DynamicParser.Node.GetMember)
                            {
                                // If leaf then decorate
                                main = Database.DecorateName(main);

                                // Supporting multipart specifications...
                                node = node.Host;

                                // Get table/alias name
                                var table = ((DynamicParser.Node.GetMember)node).Name;
                                bool isAlias = node.Host is DynamicParser.Node.Argument && IsTableAlias(table);

                                if (isAlias)
                                    main = string.Format("{0}.{1}", table, main);
                                else if (node.Host is DynamicParser.Node.GetMember)
                                {
                                    node = node.Host;
                                    main = string.Format("{0}.{1}.{2}",
                                        Database.DecorateName(((DynamicParser.Node.GetMember)node).Name),
                                        Database.DecorateName(table), main);
                                }
                                else
                                    main = string.Format("{0}.{1}", Database.DecorateName(table), main);
                            }
                            else if (node.Host is DynamicParser.Node.Argument)
                            {
                                var table = ((DynamicParser.Node.Argument)node.Host).Name;

                                if (IsTableAlias(table))
                                    main = string.Format("{0}.{1}", table, Database.DecorateName(main));
                                else if (!IsTableAlias(main))
                                    main = Database.DecorateName(main);
                            }
                            else if (!(node.Host is DynamicParser.Node.Argument && IsTableAlias(main)))
                                main = Database.DecorateName(main);

                            node = node.Host;

                            continue;
                        }

                        // Support for generic sources...
                        if (node is DynamicParser.Node.Invoke)
                        {
                            if (main != null)
                                throw new ArgumentException(string.Format("Main '{0}' is already set when parsing '{1}'.", main, result));

                            main = string.Format("{0}", Parse(node, rawstr: true, pars: Parameters));

                            node = node.Host;
                            continue;
                        }

                        // Just finished the parsing...
                        if (node is DynamicParser.Node.Argument)
                        {
                            if (string.IsNullOrEmpty(main) && IsTableAlias(node.Name))
                                main = node.Name;

                            break;
                        }

                        // All others are assumed to be part of the main element...
                        if (main != null) throw new ArgumentException(string.Format("Main '{0}' is already set when parsing '{1}'.", main, result));
                        main = Parse(node, pars: Parameters);

                        break;
                    }

                    column = main;
                }

                #endregion Helpers
            }

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
                            value = Parse(node.Value, ref col, pars: Parameters, nulls: true);

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
                        string value = Parse(column.Value, ref col, pars: Parameters, nulls: true);

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
    }

    namespace Helpers
    {
        /// <summary>Defines methods to support the comparison of collections for equality.</summary>
        /// <typeparam name="T">The type of collection to compare.</typeparam>
        public class CollectionComparer<T> : IEqualityComparer<IEnumerable<T>>
        {
            /// <summary>Determines whether the specified objects are equal.</summary>
            /// <param name="first">The first object of type T to compare.</param>
            /// <param name="second">The second object of type T to compare.</param>
            /// <returns>Returns <c>true</c> if the specified objects are equal; otherwise, <c>false</c>.</returns>
            bool IEqualityComparer<IEnumerable<T>>.Equals(IEnumerable<T> first, IEnumerable<T> second)
            {
                return Equals(first, second);
            }

            /// <summary>Returns a hash code for the specified object.</summary>
            /// <param name="enumerable">The enumerable for which a hash code is to be returned.</param>
            /// <returns>A hash code for the specified object.</returns>
            int IEqualityComparer<IEnumerable<T>>.GetHashCode(IEnumerable<T> enumerable)
            {
                return GetHashCode(enumerable);
            }

            /// <summary>Returns a hash code for the specified object.</summary>
            /// <param name="enumerable">The enumerable for which a hash code is to be returned.</param>
            /// <returns>A hash code for the specified object.</returns>
            public static int GetHashCode(IEnumerable<T> enumerable)
            {
                int hash = 17;

                foreach (T val in enumerable.OrderBy(x => x))
                    hash = (hash * 23) + val.GetHashCode();

                return hash;
            }

            /// <summary>Determines whether the specified objects are equal.</summary>
            /// <param name="first">The first object of type T to compare.</param>
            /// <param name="second">The second object of type T to compare.</param>
            /// <returns>Returns <c>true</c> if the specified objects are equal; otherwise, <c>false</c>.</returns>
            public static bool Equals(IEnumerable<T> first, IEnumerable<T> second)
            {
                if ((first == null) != (second == null))
                    return false;

                if (!object.ReferenceEquals(first, second) && (first != null))
                {
                    if (first.Count() != second.Count())
                        return false;

                    if ((first.Count() != 0) && HaveMismatchedElement(first, second))
                        return false;
                }

                return true;
            }

            private static bool HaveMismatchedElement(IEnumerable<T> first, IEnumerable<T> second)
            {
                int firstCount;
                int secondCount;

                var firstElementCounts = GetElementCounts(first, out firstCount);
                var secondElementCounts = GetElementCounts(second, out secondCount);

                if (firstCount != secondCount)
                    return true;

                foreach (var kvp in firstElementCounts)
                    if (kvp.Value != (secondElementCounts.TryGetNullable(kvp.Key) ?? 0))
                        return true;

                return false;
            }

            private static Dictionary<T, int> GetElementCounts(IEnumerable<T> enumerable, out int nullCount)
            {
                var dictionary = new Dictionary<T, int>();
                nullCount = 0;

                foreach (T element in enumerable)
                {
                    if (element == null)
                        nullCount++;
                    else
                    {
                        int count = dictionary.TryGetNullable(element) ?? 0;
                        dictionary[element] = ++count;
                    }
                }

                return dictionary;
            }
        }

        /// <summary>Framework detection and specific implementations.</summary>
        public static class FrameworkTools
        {
            #region Mono or .NET Framework detection

            /// <summary>This is pretty simple trick.</summary>
            private static bool _isMono = Type.GetType("Mono.Runtime") != null;

            /// <summary>Gets a value indicating whether application is running under mono runtime.</summary>
            public static bool IsMono { get { return _isMono; } }

            #endregion Mono or .NET Framework detection

            static FrameworkTools()
            {
                _frameworkTypeArgumentsGetter = CreateTypeArgumentsGetter();
            }

            #region GetGenericTypeArguments

            private static Func<InvokeMemberBinder, IList<Type>> _frameworkTypeArgumentsGetter = null;

            private static Func<InvokeMemberBinder, IList<Type>> CreateTypeArgumentsGetter()
            {
                // HACK: Creating binders assuming types are correct... this may fail.
                if (IsMono)
                {
                    var binderType = typeof(Microsoft.CSharp.RuntimeBinder.RuntimeBinderException).Assembly.GetType("Microsoft.CSharp.RuntimeBinder.CSharpInvokeMemberBinder");

                    if (binderType != null)
                    {
                        ParameterExpression param = Expression.Parameter(typeof(InvokeMemberBinder), "o");

                        return Expression.Lambda<Func<InvokeMemberBinder, IList<Type>>>(
                            Expression.TypeAs(
                                Expression.Field(
                                    Expression.TypeAs(param, binderType), "typeArguments"),
                                typeof(IList<Type>)), param).Compile();
                    }
                }
                else
                {
                    var inter = typeof(Microsoft.CSharp.RuntimeBinder.RuntimeBinderException).Assembly.GetType("Microsoft.CSharp.RuntimeBinder.ICSharpInvokeOrInvokeMemberBinder");

                    if (inter != null)
                    {
                        var prop = inter.GetProperty("TypeArguments");

                        if (!prop.CanRead)
                            return null;

                        var objParm = Expression.Parameter(typeof(InvokeMemberBinder), "o");

                        return Expression.Lambda<Func<InvokeMemberBinder, IList<Type>>>(
                            Expression.TypeAs(
                                Expression.Property(
                                    Expression.TypeAs(objParm, inter), prop.Name),
                                typeof(IList<Type>)), objParm).Compile();
                    }
                }

                return null;
            }

            /// <summary>Extension method allowing to easily extract generic type
            /// arguments from <see cref="InvokeMemberBinder"/> assuming that it
            /// inherits from
            /// <c>Microsoft.CSharp.RuntimeBinder.ICSharpInvokeOrInvokeMemberBinder</c>
            /// in .NET Framework or
            /// <c>Microsoft.CSharp.RuntimeBinder.CSharpInvokeMemberBinder</c>
            /// under Mono.</summary>
            /// <param name="binder">Binder from which get type arguments.</param>
            /// <remarks>This is generally a bad solution, but there is no other
            /// currently so we have to go with it.</remarks>
            /// <returns>List of types passed as generic parameters.</returns>
            public static IList<Type> GetGenericTypeArguments(this InvokeMemberBinder binder)
            {
                // First try to use delegate if exist
                if (_frameworkTypeArgumentsGetter != null)
                    return _frameworkTypeArgumentsGetter(binder);

                if (_isMono)
                {
                    // HACK: Using Reflection
                    // In mono this is trivial.

                    // First we get field info.
                    var field = binder.GetType().GetField("typeArguments", BindingFlags.Instance |
                        BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static);

                    // If this was a success get and return it's value
                    if (field != null)
                        return field.GetValue(binder) as IList<Type>;
                }
                else
                {
                    // HACK: Using Reflection
                    // In this case, we need more aerobic :D

                    // First, get the interface
                    var inter = binder.GetType().GetInterface("Microsoft.CSharp.RuntimeBinder.ICSharpInvokeOrInvokeMemberBinder");

                    if (inter != null)
                    {
                        // Now get property.
                        var prop = inter.GetProperty("TypeArguments");

                        // If we have a property, return it's value
                        if (prop != null)
                            return prop.GetValue(binder, null) as IList<Type>;
                    }
                }

                // Sadly return null if failed.
                return null;
            }

            #endregion GetGenericTypeArguments
        }

        /// <summary>Extends <see cref="IDisposable"/> interface.</summary>
        public interface IExtendedDisposable : IDisposable
        {
            /// <summary>
            /// Gets a value indicating whether this instance is disposed.
            /// </summary>
            /// <value>
            /// <c>true</c> if this instance is disposed; otherwise, <c>false</c>.
            /// </value>
            bool IsDisposed { get; }
        }

        /// <summary>Class containing useful string extensions.</summary>
        internal static class StringExtensions
        {
            static StringExtensions()
            {
                InvalidMultipartMemberChars = _InvalidMultipartMemberChars.ToCharArray();
                InvalidMemberChars = _InvalidMemberChars.ToCharArray();
            }

            private static readonly string _InvalidMultipartMemberChars = " +-*/^%[]{}()!\"\\&=?¿";
            private static readonly string _InvalidMemberChars = "." + _InvalidMultipartMemberChars;

            /// <summary>
            /// Gets an array with some invalid characters that cannot be used with multipart names for class members.
            /// </summary>
            public static char[] InvalidMultipartMemberChars { get; private set; }

            /// <summary>
            /// Gets an array with some invalid characters that cannot be used with names for class members.
            /// </summary>
            public static char[] InvalidMemberChars { get; private set; }

            /// <summary>
            /// Provides with an alternate and generic way to obtain an alternate string representation for this instance,
            /// applying the following rules:
            /// <para>- Null values are returned as with the <paramref name="nullString"/> value, or a null object.</para>
            /// <para>- Enum values are translated into their string representation.</para>
            /// <para>- If the type has override the 'ToString' method then it is used.</para>
            /// <para>- If it is a dictionary, then a collection of key/value pairs where the value part is also translated.</para>
            /// <para>- If it is a collection, then a collection of value items also translated.</para>
            /// <para>- If it has public public properties (or if not, if it has public fields), the collection of name/value
            /// pairs, with the values translated.</para>
            /// <para>- Finally it falls back to the standard 'type.FullName' mechanism.</para>
            /// </summary>
            /// <param name="obj">The object to obtain its alternate string representation from.</param>
            /// <param name="brackets">The brackets to use if needed. If not null it must be at least a 2-chars' array containing
            /// the opening and closing brackets.</param>
            /// <param name="nullString">Representation of null string..</param>
            /// <returns>The alternate string representation of this object.</returns>
            public static string Sketch(this object obj, char[] brackets = null, string nullString = "(null)")
            {
                if (obj == null) return nullString;
                if (obj is string) return (string)obj;

                Type type = obj.GetType();
                if (type.IsEnum) return obj.ToString();

                // If the ToString() method has been overriden (by the type itself, or by its parents), let's use it...
                MethodInfo method = type.GetMethod("ToString", Type.EmptyTypes);
                if (method.DeclaringType != typeof(object)) return obj.ToString();

                // For alll other cases...
                StringBuilder sb = new StringBuilder();
                bool first = true;

                // Dictionaries...
                if (obj is IDictionary)
                {
                    if (brackets == null || brackets.Length < 2)
                        brackets = "[]".ToCharArray();

                    sb.AppendFormat("{0}", brackets[0]); first = true; foreach (DictionaryEntry kvp in (IDictionary)obj)
                    {
                        if (!first) sb.Append(", "); else first = false;
                        sb.AppendFormat("'{0}'='{1}'", kvp.Key.Sketch(), kvp.Value.Sketch());
                    }

                    sb.AppendFormat("{0}", brackets[1]);
                    return sb.ToString();
                }

                // IEnumerables...
                IEnumerator ator = null;
                if (obj is IEnumerable)
                    ator = ((IEnumerable)obj).GetEnumerator();
                else
                {
                    method = type.GetMethod("GetEnumerator", Type.EmptyTypes);
                    if (method != null)
                        ator = (IEnumerator)method.Invoke(obj, null);
                }

                if (ator != null)
                {
                    if (brackets == null || brackets.Length < 2) brackets = "[]".ToCharArray();
                    sb.AppendFormat("{0}", brackets[0]); first = true; while (ator.MoveNext())
                    {
                        if (!first) sb.Append(", "); else first = false;
                        sb.AppendFormat("{0}", ator.Current.Sketch());
                    }

                    sb.AppendFormat("{0}", brackets[1]);

                    if (ator is IDisposable)
                        ((IDisposable)ator).Dispose();

                    return sb.ToString();
                }

                // As a last resort, using the public properties (or fields if needed, or type name)...
                BindingFlags flags = BindingFlags.Public | BindingFlags.Instance | BindingFlags.FlattenHierarchy;
                PropertyInfo[] props = type.GetProperties(flags);
                FieldInfo[] infos = type.GetFields(flags);

                if (props.Length == 0 && infos.Length == 0) sb.Append(type.FullName); // Fallback if needed
                else
                {
                    if (brackets == null || brackets.Length < 2) brackets = "{}".ToCharArray();
                    sb.AppendFormat("{0}", brackets[0]);
                    first = true;

                    if (props.Length != 0)
                    {
                        foreach (var prop in props)
                        {
                            if (!first) sb.Append(", "); else first = false;
                            sb.AppendFormat("{0}='{1}'", prop.Name, prop.GetValue(obj, null).Sketch());
                        }
                    }
                    else
                    {
                        if (infos.Length != 0)
                        {
                            foreach (var info in infos)
                            {
                                if (!first) sb.Append(", "); else first = false;
                                sb.AppendFormat("{0}='{1}'", info.Name, info.GetValue(obj).Sketch());
                            }
                        }
                    }

                    sb.AppendFormat("{0}", brackets[1]);
                }

                // And returning...
                return sb.ToString();
            }

            /// <summary>
            /// Returns true if the target string contains any of the characters given.
            /// </summary>
            /// <param name="source">The target string. It cannot be null.</param>
            /// <param name="items">An array containing the characters to test. It cannot be null. If empty false is returned.</param>
            /// <returns>True if the target string contains any of the characters given, false otherwise.</returns>
            public static bool ContainsAny(this string source, char[] items)
            {
                if (source == null) throw new ArgumentNullException("source", "Source string cannot be null.");
                if (items == null) throw new ArgumentNullException("items", "Array of characters to test cannot be null.");

                if (items.Length == 0) return false; // No characters to validate
                int ix = source.IndexOfAny(items);
                return ix >= 0 ? true : false;
            }

            /// <summary>
            /// Returns a new validated string using the rules given.
            /// </summary>
            /// <param name="source">The source string.</param>
            /// <param name="desc">A description of the source string to build errors and exceptions if needed.</param>
            /// <param name="canbeNull">True if the returned string can be null.</param>
            /// <param name="canbeEmpty">True if the returned string can be empty.</param>
            /// <param name="trim">True to trim the returned string.</param>
            /// <param name="trimStart">True to left-trim the returned string.</param>
            /// <param name="trimEnd">True to right-trim the returned string.</param>
            /// <param name="minLen">If >= 0, the min valid length for the returned string.</param>
            /// <param name="maxLen">If >= 0, the max valid length for the returned string.</param>
            /// <param name="padLeft">If not '\0', the character to use to left-pad the returned string if needed.</param>
            /// <param name="padRight">If not '\0', the character to use to right-pad the returned string if needed.</param>
            /// <param name="invalidChars">If not null, an array containing invalid chars that must not appear in the returned
            /// string.</param>
            /// <param name="validChars">If not null, an array containing the only characters that are considered valid for the
            /// returned string.</param>
            /// <returns>A new validated string.</returns>
            public static string Validated(this string source, string desc = null,
                bool canbeNull = false, bool canbeEmpty = false,
                bool trim = true, bool trimStart = false, bool trimEnd = false,
                int minLen = -1, int maxLen = -1, char padLeft = '\0', char padRight = '\0',
                char[] invalidChars = null, char[] validChars = null)
            {
                // Assuring a valid descriptor...
                if (string.IsNullOrWhiteSpace(desc)) desc = "Source";

                // Validating if null sources are accepted...
                if (source == null)
                {
                    if (!canbeNull) throw new ArgumentNullException(desc, string.Format("{0} cannot be null.", desc));
                    return null;
                }

                // Trimming if needed...
                if (trim && !(trimStart || trimEnd)) source = source.Trim();
                else
                {
                    if (trimStart) source = source.TrimStart(' ');
                    if (trimEnd) source = source.TrimEnd(' ');
                }

                // Adjusting lenght...
                if (minLen > 0)
                {
                    if (padLeft != '\0') source = source.PadLeft(minLen, padLeft);
                    if (padRight != '\0') source = source.PadRight(minLen, padRight);
                }

                if (maxLen > 0)
                {
                    if (padLeft != '\0') source = source.PadLeft(maxLen, padLeft);
                    if (padRight != '\0') source = source.PadRight(maxLen, padRight);
                }

                // Validating emptyness and lenghts...
                if (source.Length == 0)
                {
                    if (!canbeEmpty) throw new ArgumentException(string.Format("{0} cannot be empty.", desc));
                    return string.Empty;
                }

                if (minLen >= 0 && source.Length < minLen) throw new ArgumentException(string.Format("Lenght of {0} '{1}' is lower than '{2}'.", desc, source, minLen));
                if (maxLen >= 0 && source.Length > maxLen) throw new ArgumentException(string.Format("Lenght of {0} '{1}' is bigger than '{2}'.", desc, source, maxLen));

                // Checking invalid chars...
                if (invalidChars != null)
                {
                    int n = source.IndexOfAny(invalidChars);
                    if (n >= 0) throw new ArgumentException(string.Format("Invalid character '{0}' found in {1} '{2}'.", source[n], desc, source));
                }

                // Checking valid chars...
                if (validChars != null)
                {
                    int n = validChars.ToString().IndexOfAny(source.ToCharArray());
                    if (n >= 0) throw new ArgumentException(string.Format("Invalid character '{0}' found in {1} '{2}'.", validChars.ToString()[n], desc, source));
                }

                return source;
            }

            /// <summary>
            /// Splits the given string with the 'something AS alias' format, returning a tuple containing its 'something' and 'alias' parts.
            /// If no alias is detected, then its component in the tuple returned is null and all the contents from the source
            /// string are considered as the 'something' part.
            /// </summary>
            /// <param name="source">The source string.</param>
            /// <returns>A tuple containing the 'something' and 'alias' parts.</returns>
            public static Tuple<string, string> SplitSomethingAndAlias(this string source)
            {
                source = source.Validated("[Something AS Alias]");

                string something = null;
                string alias = null;
                int n = source.LastIndexOf(" AS ", StringComparison.OrdinalIgnoreCase);

                if (n < 0)
                    something = source;
                else
                {
                    something = source.Substring(0, n);
                    alias = source.Substring(n + 4);
                }

                return new Tuple<string, string>(something, alias);
            }

            /// <summary>Allows to replace parameters inside of string.</summary>
            /// <param name="stringToFill">String containing parameters in format <c>[$ParameterName]</c>.</param>
            /// <param name="getValue">Function that should return value that will be placed in string in place of placed parameter.</param>
            /// <param name="prefix">Prefix of the parameter. This value can't be null or empty, default value <code>[$</code>.</param>
            /// <param name="sufix">Suffix of the parameter. This value can't be null or empty, default value <code>]</code>.</param>
            /// <returns>Parsed string.</returns>
            public static string FillStringWithVariables(this string stringToFill, Func<string, string> getValue, string prefix = "[$", string sufix = "]")
            {
                int startPos = 0, endPos = 0;
                prefix.Validated();
                sufix.Validated();

                startPos = stringToFill.IndexOf(prefix, startPos);
                while (startPos >= 0)
                {
                    endPos = stringToFill.IndexOf(sufix, startPos + prefix.Length);
                    int nextStartPos = stringToFill.IndexOf(prefix, startPos + prefix.Length);

                    if (endPos > startPos + prefix.Length + 1 && (nextStartPos > endPos || nextStartPos == -1))
                    {
                        string paramName = stringToFill.Substring(startPos + prefix.Length, endPos - (startPos + prefix.Length));

                        stringToFill = stringToFill
                            .Remove(startPos, (endPos - startPos) + sufix.Length)
                            .Insert(startPos, getValue(paramName));
                    }

                    startPos = stringToFill.IndexOf(prefix, startPos + prefix.Length);
                }

                return stringToFill;
            }
        }

        /// <summary>Class contains unclassified extensions.</summary>
        internal static class UnclassifiedExtensions
        {
            /// <summary>Easy way to use conditional value.</summary>
            /// <remarks>Includes <see cref="DBNull.Value"/>.</remarks>
            /// <typeparam name="T">Input object type to check.</typeparam>
            /// <typeparam name="R">Result type.</typeparam>
            /// <param name="obj">The object to check.</param>
            /// <param name="func">The select function.</param>
            /// <param name="elseValue">The else value.</param>
            /// <returns>Selected value or default value.</returns>
            /// <example>It lets you do this:
            /// <code>var lname = thingy.NullOr(t => t.Name).NullOr(n => n.ToLower());</code>
            /// which is more fluent and (IMO) easier to read than this:
            /// <code>var lname = (thingy != null ? thingy.Name : null) != null ? thingy.Name.ToLower() : null;</code>
            /// </example>
            public static R NullOr<T, R>(this T obj, Func<T, R> func, R elseValue = default(R)) where T : class
            {
                return obj != null && obj != DBNull.Value ?
                    func(obj) : elseValue;
            }

            /// <summary>Easy way to use conditional value.</summary>
            /// <remarks>Includes <see cref="DBNull.Value"/>.</remarks>
            /// <typeparam name="T">Input object type to check.</typeparam>
            /// <typeparam name="R">Result type.</typeparam>
            /// <param name="obj">The object to check.</param>
            /// <param name="func">The select function.</param>
            /// <param name="elseFunc">The else value function.</param>
            /// <returns>Selected value or default value.</returns>
            /// <example>It lets you do this:
            /// <code>var lname = thingy.NullOr(t => t.Name).NullOr(n => n.ToLower());</code>
            /// which is more fluent and (IMO) easier to read than this:
            /// <code>var lname = (thingy != null ? thingy.Name : null) != null ? thingy.Name.ToLower() : null;</code>
            /// </example>
            public static R NullOrFn<T, R>(this T obj, Func<T, R> func, Func<R> elseFunc = null) where T : class
            {
                // Old if to avoid recurency.
                return obj != null && obj != DBNull.Value ?
                    func(obj) : elseFunc != null ? elseFunc() : default(R);
            }
        }

        namespace Dynamics
        {
            /// <summary>
            /// Class able to parse dynamic lambda expressions. Allows to create dynamic logic.
            /// </summary>
            public class DynamicParser : IExtendedDisposable
            {
                #region Node

                /// <summary>
                /// Generic bindable operation where some of its operands is a dynamic argument, or a dynamic member or
                /// a method of that argument.
                /// </summary>
                [Serializable]
                public class Node : IDynamicMetaObjectProvider, IExtendedDisposable, ISerializable
                {
                    #region MetaNode

                    /// <summary>
                    /// Represents the dynamic binding and a binding logic of
                    /// an object participating in the dynamic binding.
                    /// </summary>
                    internal class MetaNode : DynamicMetaObject
                    {
                        /// <summary>
                        /// Initializes a new instance of the <see cref="MetaNode"/> class.
                        /// </summary>
                        /// <param name="parameter">The parameter.</param>
                        /// <param name="rest">The restrictions.</param>
                        /// <param name="value">The value.</param>
                        public MetaNode(Expression parameter, BindingRestrictions rest, object value)
                            : base(parameter, rest, value)
                        {
                        }

                        private DynamicMetaObject GetBinder(Func<Node, Node> newNodeFunc)
                        {
                            var o = (Node)this.Value;
                            var node = newNodeFunc(o);
                            o.Parser.Last = node;

                            var p = Expression.Variable(typeof(Node), "ret");
                            var exp = Expression.Block(new ParameterExpression[] { p }, Expression.Assign(p, Expression.Constant(node)));

                            return new MetaNode(exp, this.Restrictions, node);
                        }

                        /// <summary>
                        /// Performs the binding of the dynamic get member operation.
                        /// </summary>
                        /// <param name="binder">An instance of the <see cref="T:System.Dynamic.GetMemberBinder" /> that represents the details of the dynamic operation.</param>
                        /// <returns>
                        /// The new <see cref="T:System.Dynamic.DynamicMetaObject" /> representing the result of the binding.
                        /// </returns>
                        public override DynamicMetaObject BindGetMember(GetMemberBinder binder)
                        {
                            return GetBinder(x => new GetMember(x, binder.Name) { Parser = x.Parser });
                        }

                        /// <summary>
                        /// Performs the binding of the dynamic set member operation.
                        /// </summary>
                        /// <param name="binder">An instance of the <see cref="T:System.Dynamic.SetMemberBinder" /> that represents the details of the dynamic operation.</param>
                        /// <param name="value">The <see cref="T:System.Dynamic.DynamicMetaObject" /> representing the value for the set member operation.</param>
                        /// <returns>
                        /// The new <see cref="T:System.Dynamic.DynamicMetaObject" /> representing the result of the binding.
                        /// </returns>
                        public override DynamicMetaObject BindSetMember(SetMemberBinder binder, DynamicMetaObject value)
                        {
                            return GetBinder(x => new SetMember(x, binder.Name, value.Value) { Parser = x.Parser });
                        }

                        /// <summary>
                        /// Performs the binding of the dynamic get index operation.
                        /// </summary>
                        /// <param name="binder">An instance of the <see cref="T:System.Dynamic.GetIndexBinder" /> that represents the details of the dynamic operation.</param>
                        /// <param name="indexes">An array of <see cref="T:System.Dynamic.DynamicMetaObject" /> instances - indexes for the get index operation.</param>
                        /// <returns>
                        /// The new <see cref="T:System.Dynamic.DynamicMetaObject" /> representing the result of the binding.
                        /// </returns>
                        public override DynamicMetaObject BindGetIndex(GetIndexBinder binder, DynamicMetaObject[] indexes)
                        {
                            return GetBinder(x => new GetIndex(x, indexes.Select(m => m.Value).ToArray()) { Parser = x.Parser });
                        }

                        /// <summary>
                        /// Performs the binding of the dynamic set index operation.
                        /// </summary>
                        /// <param name="binder">An instance of the <see cref="T:System.Dynamic.SetIndexBinder" /> that represents the details of the dynamic operation.</param>
                        /// <param name="indexes">An array of <see cref="T:System.Dynamic.DynamicMetaObject" /> instances - indexes for the set index operation.</param>
                        /// <param name="value">The <see cref="T:System.Dynamic.DynamicMetaObject" /> representing the value for the set index operation.</param>
                        /// <returns>
                        /// The new <see cref="T:System.Dynamic.DynamicMetaObject" /> representing the result of the binding.
                        /// </returns>
                        public override DynamicMetaObject BindSetIndex(SetIndexBinder binder, DynamicMetaObject[] indexes, DynamicMetaObject value)
                        {
                            return GetBinder(x => new SetIndex(x, indexes.Select(m => m.Value).ToArray(), value.Value) { Parser = x.Parser });
                        }

                        /// <summary>
                        /// Performs the binding of the dynamic invoke operation.
                        /// </summary>
                        /// <param name="binder">An instance of the <see cref="T:System.Dynamic.InvokeBinder" /> that represents the details of the dynamic operation.</param>
                        /// <param name="args">An array of <see cref="T:System.Dynamic.DynamicMetaObject" /> instances - arguments to the invoke operation.</param>
                        /// <returns>
                        /// The new <see cref="T:System.Dynamic.DynamicMetaObject" /> representing the result of the binding.
                        /// </returns>
                        public override DynamicMetaObject BindInvoke(InvokeBinder binder, DynamicMetaObject[] args)
                        {
                            return GetBinder(x => new Invoke(x, args.Select(m => m.Value).ToArray()) { Parser = x.Parser });
                        }

                        /// <summary>
                        /// Performs the binding of the dynamic invoke member operation.
                        /// </summary>
                        /// <param name="binder">An instance of the <see cref="T:System.Dynamic.InvokeMemberBinder" /> that represents the details of the dynamic operation.</param>
                        /// <param name="args">An array of <see cref="T:System.Dynamic.DynamicMetaObject" /> instances - arguments to the invoke member operation.</param>
                        /// <returns>
                        /// The new <see cref="T:System.Dynamic.DynamicMetaObject" /> representing the result of the binding.
                        /// </returns>
                        public override DynamicMetaObject BindInvokeMember(InvokeMemberBinder binder, DynamicMetaObject[] args)
                        {
                            return GetBinder(x => new Method(x, binder.Name, args.Select(m => m.Value).ToArray()) { Parser = x.Parser });
                        }

                        /// <summary>
                        /// Performs the binding of the dynamic binary operation.
                        /// </summary>
                        /// <param name="binder">An instance of the <see cref="T:System.Dynamic.BinaryOperationBinder" /> that represents the details of the dynamic operation.</param>
                        /// <param name="arg">An instance of the <see cref="T:System.Dynamic.DynamicMetaObject" /> representing the right hand side of the binary operation.</param>
                        /// <returns>
                        /// The new <see cref="T:System.Dynamic.DynamicMetaObject" /> representing the result of the binding.
                        /// </returns>
                        public override DynamicMetaObject BindBinaryOperation(BinaryOperationBinder binder, DynamicMetaObject arg)
                        {
                            return GetBinder(x => new Binary(x, binder.Operation, arg.Value) { Parser = x.Parser });
                        }

                        /// <summary>
                        /// Performs the binding of the dynamic unary operation.
                        /// </summary>
                        /// <param name="binder">An instance of the <see cref="T:System.Dynamic.UnaryOperationBinder" /> that represents the details of the dynamic operation.</param>
                        /// <returns>
                        /// The new <see cref="T:System.Dynamic.DynamicMetaObject" /> representing the result of the binding.
                        /// </returns>
                        public override DynamicMetaObject BindUnaryOperation(UnaryOperationBinder binder)
                        {
                            var o = (Node)this.Value;
                            var node = new Unary(o, binder.Operation) { Parser = o.Parser };
                            o.Parser.Last = node;

                            // If operation is 'IsTrue' or 'IsFalse', we will return false to keep the engine working...
                            object ret = node;
                            if (binder.Operation == ExpressionType.IsTrue) ret = (object)false;
                            if (binder.Operation == ExpressionType.IsFalse) ret = (object)false;

                            var p = Expression.Variable(ret.GetType(), "ret"); // the type is now obtained from "ret"
                            var exp = Expression.Block(
                                new ParameterExpression[] { p },
                                Expression.Assign(p, Expression.Constant(ret))); // the expression is now obtained from "ret"

                            return new MetaNode(exp, this.Restrictions, node);
                        }

                        /// <summary>
                        /// Performs the binding of the dynamic conversion operation.
                        /// </summary>
                        /// <param name="binder">An instance of the <see cref="T:System.Dynamic.ConvertBinder" /> that represents the details of the dynamic operation.</param>
                        /// <returns>
                        /// The new <see cref="T:System.Dynamic.DynamicMetaObject" /> representing the result of the binding.
                        /// </returns>
                        public override DynamicMetaObject BindConvert(ConvertBinder binder)
                        {
                            var o = (Node)this.Value;
                            var node = new Convert(o, binder.ReturnType) { Parser = o.Parser };
                            o.Parser.Last = node;

                            // Reducing the object to return if this is an assignment node...
                            object ret = o;
                            bool done = false;

                            while (!done)
                            {
                                if (ret is SetMember)
                                    ret = ((SetMember)o).Value;
                                else if (ret is SetIndex)
                                    ret = ((SetIndex)o).Value;
                                else
                                    done = true;
                            }

                            // Creating an instance...
                            if (binder.ReturnType == typeof(string)) ret = ret.ToString();
                            else
                            {
                                try
                                {
                                    if (binder.ReturnType.IsNullableType())
                                        ret = null; // to avoid cast exceptions
                                    else
                                        ret = Activator.CreateInstance(binder.ReturnType, true); // true to allow non-public ctor as well
                                }
                                catch
                                {
                                    // as the last resort scenario
                                    ret = new object();
                                }
                            }

                            var p = Expression.Variable(binder.ReturnType, "ret");
                            var exp = Expression.Block(
                                new ParameterExpression[] { p },
                                Expression.Assign(p, Expression.Constant(ret, binder.ReturnType))); // specifying binder.ReturnType

                            return new MetaNode(exp, this.Restrictions, node);
                        }
                    }

                    #endregion MetaNode

                    #region Argument

                    /// <summary>
                    /// Describe a dynamic argument used in a dynamic lambda expression.
                    /// </summary>
                    [Serializable]
                    public class Argument : Node, ISerializable
                    {
                        /// <summary>
                        /// Initializes a new instance of the <see cref="Argument"/> class.
                        /// </summary>
                        /// <param name="name">The name.</param>
                        public Argument(string name)
                            : base(name)
                        {
                        }

                        /// <summary>
                        /// Initializes a new instance of the <see cref="Argument"/> class.
                        /// </summary>
                        /// <param name="info">The info.</param>
                        /// <param name="context">The context.</param>
                        protected Argument(SerializationInfo info, StreamingContext context)
                            : base(info, context)
                        {
                        }

                        /// <summary>Returns a <see cref="System.String" /> that represents this instance.</summary>
                        /// <returns>A <see cref="System.String" /> that represents this instance.</returns>
                        public override string ToString()
                        {
                            if (IsDisposed)
                                return "{DynamicParser::Node::Argument::Disposed}";
                            return Name;
                        }
                    }

                    #endregion Argument

                    #region GetMember

                    /// <summary>
                    /// Describe a 'get member' operation, as in 'x => x.Member'.
                    /// </summary>
                    [Serializable]
                    public class GetMember : Node, ISerializable
                    {
                        /// <summary>
                        /// Initializes a new instance of the <see cref="GetMember"/> class.
                        /// </summary>
                        /// <param name="host">The host.</param>
                        /// <param name="name">The name.</param>
                        public GetMember(Node host, string name)
                            : base(host, name)
                        {
                        }

                        /// <summary>
                        /// Initializes a new instance of the <see cref="GetMember"/> class.
                        /// </summary>
                        /// <param name="info">The info.</param>
                        /// <param name="context">The context.</param>
                        protected GetMember(SerializationInfo info, StreamingContext context)
                            : base(info, context)
                        {
                        }

                        /// <summary>Returns a <see cref="System.String" /> that represents this instance.</summary>
                        /// <returns>A <see cref="System.String" /> that represents this instance.</returns>
                        public override string ToString()
                        {
                            if (IsDisposed)
                                return "{DynamicParser::Node::GetMember::Disposed}";
                            return string.Format("{0}.{1}", Host.Sketch(), Name.Sketch());
                        }
                    }

                    #endregion GetMember

                    #region SetMember

                    /// <summary>
                    /// Describe a 'set member' operation, as in 'x => x.Member = y'.
                    /// </summary>
                    [Serializable]
                    public class SetMember : Node, ISerializable
                    {
                        /// <summary>
                        /// Gets the value that has been (virtually) assigned to this member. It might be null if the null value has been
                        /// assigned to this instance, or if this instance is disposed.
                        /// </summary>
                        public object Value { get; private set; }

                        /// <summary>
                        /// Initializes a new instance of the <see cref="SetMember"/> class.
                        /// </summary>
                        /// <param name="host">The host.</param>
                        /// <param name="name">The name.</param>
                        /// <param name="value">The value.</param>
                        public SetMember(Node host, string name, object value)
                            : base(host, name)
                        {
                            Value = value;
                        }

                        /// <summary>
                        /// Initializes a new instance of the <see cref="SetMember"/> class.
                        /// </summary>
                        /// <param name="info">The info.</param>
                        /// <param name="context">The context.</param>
                        protected SetMember(SerializationInfo info, StreamingContext context)
                            : base(info, context)
                        {
                            string type = info.GetString("MemberType");
                            Value = type == "NULL" ? null : info.GetValue("MemberValue", Type.GetType(type));
                        }

                        /// <summary>
                        /// Gets the object data.
                        /// </summary>
                        /// <param name="info">The info.</param>
                        /// <param name="context">The context.</param>
                        public override void GetObjectData(SerializationInfo info, StreamingContext context)
                        {
                            info.AddValue("MemberType", Value == null ? "NULL" : Value.GetType().AssemblyQualifiedName);
                            if (Value != null)
                                info.AddValue("MemberValue", Value);

                            base.GetObjectData(info, context);
                        }

                        /// <summary>Returns a <see cref="System.String" /> that represents this instance.</summary>
                        /// <returns>A <see cref="System.String" /> that represents this instance.</returns>
                        public override string ToString()
                        {
                            if (IsDisposed)
                                return "{DynamicParser::Node::SetMember::Disposed}";
                            return string.Format("({0}.{1} = {2})", Host.Sketch(), Name.Sketch(), Value.Sketch());
                        }
                    }

                    #endregion SetMember

                    #region GetIndex

                    /// <summary>
                    /// Describe a 'get indexed' operation, as in 'x => x.Member[...]'.
                    /// </summary>
                    [Serializable]
                    public class GetIndex : Node, ISerializable
                    {
                        /// <summary>Gets the indexes.</summary>
                        public object[] Indexes { get; internal set; }

                        /// <summary>
                        /// Initializes a new instance of the <see cref="GetIndex"/> class.
                        /// </summary>
                        /// <param name="host">The host.</param>
                        /// <param name="indexes">The indexes.</param>
                        /// <exception cref="System.ArgumentNullException">Indexes array cannot be null.</exception>
                        /// <exception cref="System.ArgumentException">Indexes array cannot be empty.</exception>
                        public GetIndex(Node host, object[] indexes)
                            : base(host)
                        {
                            if (indexes == null)
                                throw new ArgumentNullException("indexes", "Indexes array cannot be null.");
                            if (indexes.Length == 0)
                                throw new ArgumentException("Indexes array cannot be empty.");

                            Indexes = indexes;
                        }

                        /// <summary>
                        /// Initializes a new instance of the <see cref="GetIndex"/> class.
                        /// </summary>
                        /// <param name="info">The info.</param>
                        /// <param name="context">The context.</param>
                        protected GetIndex(SerializationInfo info, StreamingContext context)
                            : base(info, context)
                        {
                            int count = (int)info.GetValue("IndexCount", typeof(int));

                            if (count != 0)
                            {
                                Indexes = new object[count]; for (int i = 0; i < count; i++)
                                {
                                    string typeName = info.GetString("IndexType" + i);
                                    object obj = typeName == "NULL" ? null : info.GetValue("IndexValue" + i, Type.GetType(typeName));
                                    Indexes[i] = obj;
                                }
                            }
                        }

                        /// <summary>
                        /// Gets the object data.
                        /// </summary>
                        /// <param name="info">The info.</param>
                        /// <param name="context">The context.</param>
                        public override void GetObjectData(SerializationInfo info, StreamingContext context)
                        {
                            int count = Indexes == null ? 0 : Indexes.Length; info.AddValue("IndexCount", count);
                            for (int i = 0; i < count; i++)
                            {
                                info.AddValue("IndexType" + i, Indexes[i] == null ? "NULL" : Indexes[i].GetType().AssemblyQualifiedName);
                                if (Indexes[i] != null) info.AddValue("IndexValue" + i, Indexes[i]);
                            }

                            base.GetObjectData(info, context);
                        }

                        /// <summary>Returns a <see cref="System.String" /> that represents this instance.</summary>
                        /// <returns>A <see cref="System.String" /> that represents this instance.</returns>
                        public override string ToString()
                        {
                            if (IsDisposed)
                                return "{DynamicParser::Node::GetIndex::Disposed}";

                            return string.Format("{0}{1}", Host.Sketch(), Indexes == null ? "[empty]" : Indexes.Sketch());
                        }
                    }

                    #endregion GetIndex

                    #region SetIndex

                    /// <summary>
                    /// Describe a 'set indexed' operation, as in 'x => x.Member[...] = Value'.
                    /// </summary>
                    [Serializable]
                    public class SetIndex : GetIndex, ISerializable
                    {
                        /// <summary>
                        /// Gets the value that has been (virtually) assigned to this member. It might be null if the null value has been
                        /// assigned to this instance, or if this instance is disposed.
                        /// </summary>
                        public object Value { get; private set; }

                        /// <summary>
                        /// Initializes a new instance of the <see cref="SetIndex"/> class.
                        /// </summary>
                        /// <param name="host">The host.</param>
                        /// <param name="indexes">The indexes.</param>
                        /// <param name="value">The value.</param>
                        public SetIndex(Node host, object[] indexes, object value)
                            : base(host, indexes)
                        {
                            Value = value;
                        }

                        /// <summary>
                        /// Initializes a new instance of the <see cref="SetIndex"/> class.
                        /// </summary>
                        /// <param name="info">The info.</param>
                        /// <param name="context">The context.</param>
                        protected SetIndex(SerializationInfo info, StreamingContext context)
                            : base(info, context)
                        {
                            string type = info.GetString("MemberType");
                            Value = type == "NULL" ? null : info.GetValue("MemberValue", Type.GetType(type));
                        }

                        /// <summary>
                        /// Gets the object data.
                        /// </summary>
                        /// <param name="info">The info.</param>
                        /// <param name="context">The context.</param>
                        public override void GetObjectData(SerializationInfo info, StreamingContext context)
                        {
                            info.AddValue("MemberType", Value == null ? "NULL" : Value.GetType().AssemblyQualifiedName);
                            if (Value != null) info.AddValue("MemberValue", Value);

                            base.GetObjectData(info, context);
                        }

                        /// <summary>Returns a <see cref="System.String" /> that represents this instance.</summary>
                        /// <returns>A <see cref="System.String" /> that represents this instance.</returns>
                        public override string ToString()
                        {
                            if (IsDisposed)
                                return "{DynamicParser::Node::SetIndex::Disposed}";

                            return string.Format("({0}{1} = {2})", Host.Sketch(), Indexes == null ? "[empty]" : Indexes.Sketch(), Value.Sketch());
                        }
                    }

                    #endregion SetIndex

                    #region Invoke

                    /// <summary>
                    /// Describe a method invocation operation, as in 'x => x.Method(...)".
                    /// </summary>
                    [Serializable]
                    public class Invoke : Node, ISerializable
                    {
                        /// <summary>Gets the arguments.</summary>
                        public object[] Arguments { get; internal set; }

                        /// <summary>
                        /// Initializes a new instance of the <see cref="Invoke"/> class.
                        /// </summary>
                        /// <param name="host">The host.</param>
                        /// <param name="arguments">The arguments.</param>
                        public Invoke(Node host, object[] arguments)
                            : base(host)
                        {
                            Arguments = arguments == null || arguments.Length == 0 ? null : arguments;
                        }

                        /// <summary>
                        /// Initializes a new instance of the <see cref="Invoke"/> class.
                        /// </summary>
                        /// <param name="info">The info.</param>
                        /// <param name="context">The context.</param>
                        protected Invoke(SerializationInfo info, StreamingContext context)
                            : base(info, context)
                        {
                            int count = (int)info.GetValue("ArgumentCount", typeof(int));

                            if (count != 0)
                            {
                                Arguments = new object[count]; for (int i = 0; i < count; i++)
                                {
                                    string typeName = info.GetString("ArgumentType" + i);
                                    object obj = typeName == "NULL" ? null : info.GetValue("ArgumentValue" + i, Type.GetType(typeName));
                                    Arguments[i] = obj;
                                }
                            }
                        }

                        /// <summary>
                        /// Gets the object data.
                        /// </summary>
                        /// <param name="info">The info.</param>
                        /// <param name="context">The context.</param>
                        public override void GetObjectData(SerializationInfo info, StreamingContext context)
                        {
                            int count = Arguments == null ? 0 : Arguments.Length; info.AddValue("ArgumentCount", count);
                            for (int i = 0; i < count; i++)
                            {
                                info.AddValue("ArgumentType" + i, Arguments[i] == null ? "NULL" : Arguments[i].GetType().AssemblyQualifiedName);
                                if (Arguments[i] != null) info.AddValue("ArgumentValue" + i, Arguments[i]);
                            }

                            base.GetObjectData(info, context);
                        }

                        /// <summary>Returns a <see cref="System.String" /> that represents this instance.</summary>
                        /// <returns>A <see cref="System.String" /> that represents this instance.</returns>
                        public override string ToString()
                        {
                            if (IsDisposed)
                                return "{DynamicParser::Node::Invoke::Disposed}";

                            return string.Format("{0}{1}", Host.Sketch(), Arguments == null ? "()" : Arguments.Sketch(brackets: "()".ToCharArray()));
                        }
                    }

                    #endregion Invoke

                    #region Method

                    /// <summary>
                    /// Describe a method invocation operation, as in 'x => x.Method(...)".
                    /// </summary>
                    [Serializable]
                    public class Method : Node, ISerializable
                    {
                        /// <summary>Gets the arguments.</summary>
                        public object[] Arguments { get; internal set; }

                        /// <summary>
                        /// Initializes a new instance of the <see cref="Method"/> class.
                        /// </summary>
                        /// <param name="host">The host.</param>
                        /// <param name="name">The name.</param>
                        /// <param name="arguments">The arguments.</param>
                        public Method(Node host, string name, object[] arguments)
                            : base(host, name)
                        {
                            Arguments = arguments == null || arguments.Length == 0 ? null : arguments;
                        }

                        /// <summary>
                        /// Initializes a new instance of the <see cref="Method"/> class.
                        /// </summary>
                        /// <param name="info">The info.</param>
                        /// <param name="context">The context.</param>
                        protected Method(SerializationInfo info, StreamingContext context)
                            : base(info, context)
                        {
                            int count = (int)info.GetValue("ArgumentCount", typeof(int));

                            if (count != 0)
                            {
                                Arguments = new object[count]; for (int i = 0; i < count; i++)
                                {
                                    string typeName = info.GetString("ArgumentType" + i);
                                    object obj = typeName == "NULL" ? null : info.GetValue("ArgumentValue" + i, Type.GetType(typeName));
                                    Arguments[i] = obj;
                                }
                            }
                        }

                        /// <summary>
                        /// Gets the object data.
                        /// </summary>
                        /// <param name="info">The info.</param>
                        /// <param name="context">The context.</param>
                        public override void GetObjectData(SerializationInfo info, StreamingContext context)
                        {
                            int count = Arguments == null ? 0 : Arguments.Length; info.AddValue("ArgumentCount", count);
                            for (int i = 0; i < count; i++)
                            {
                                info.AddValue("ArgumentType" + i, Arguments[i] == null ? "NULL" : Arguments[i].GetType().AssemblyQualifiedName);
                                if (Arguments[i] != null) info.AddValue("ArgumentValue" + i, Arguments[i]);
                            }

                            base.GetObjectData(info, context);
                        }

                        /// <summary>Returns a <see cref="System.String" /> that represents this instance.</summary>
                        /// <returns>A <see cref="System.String" /> that represents this instance.</returns>
                        public override string ToString()
                        {
                            if (IsDisposed)
                                return "{DynamicParser::Node::Method::Disposed}";

                            return string.Format("{0}.{1}{2}", Host.Sketch(), Name.Sketch(), Arguments == null ? "()" : Arguments.Sketch(brackets: "()".ToCharArray()));
                        }
                    }

                    #endregion Method

                    #region Binary

                    /// <summary>
                    /// Represents a binary operation between a dynamic element and an arbitrary object, including null ones, as in
                    /// 'x =&gt; (x &amp;&amp; null)'. The left operand must be an instance of <see cref="Node"/>, whereas the right one
                    /// can be any object, including null values.
                    /// </summary>
                    [Serializable]
                    public class Binary : Node, ISerializable
                    {
                        /// <summary>Gets the operation.</summary>
                        public ExpressionType Operation { get; private set; }

                        /// <summary>Gets host of the <see cref="Node"/>.</summary>
                        public Node Left { get { return Host; } }

                        /// <summary>Gets the right side value.</summary>
                        public object Right { get; private set; }

                        /// <summary>
                        /// Initializes a new instance of the <see cref="Binary"/> class.
                        /// </summary>
                        /// <param name="left">The left.</param>
                        /// <param name="operation">The operation.</param>
                        /// <param name="right">The right.</param>
                        public Binary(Node left, ExpressionType operation, object right)
                            : base(left)
                        {
                            Operation = operation;
                            Right = right;
                        }

                        /// <summary>
                        /// Initializes a new instance of the <see cref="Binary"/> class.
                        /// </summary>
                        /// <param name="info">The info.</param>
                        /// <param name="context">The context.</param>
                        protected Binary(SerializationInfo info, StreamingContext context)
                            : base(info, context)
                        {
                            Operation = (ExpressionType)info.GetValue("Operation", typeof(ExpressionType));

                            string type = info.GetString("RightType");
                            Right = type == "NULL" ? null : (Node)info.GetValue("RightItem", Type.GetType(type));
                        }

                        /// <summary>
                        /// Gets the object data.
                        /// </summary>
                        /// <param name="info">The info.</param>
                        /// <param name="context">The context.</param>
                        public override void GetObjectData(SerializationInfo info, StreamingContext context)
                        {
                            info.AddValue("Operation", Operation);

                            info.AddValue("RightType", Right == null ? "NULL" : Right.GetType().AssemblyQualifiedName);
                            if (Right != null)
                                info.AddValue("RightItem", Right);

                            base.GetObjectData(info, context);
                        }

                        /// <summary>Returns a <see cref="System.String" /> that represents this instance.</summary>
                        /// <returns>A <see cref="System.String" /> that represents this instance.</returns>
                        public override string ToString()
                        {
                            if (IsDisposed)
                                return "{DynamicParser::Node::Binary::Disposed}";

                            return string.Format("({0} {1} {2})", Host.Sketch(), Operation, Right.Sketch());
                        }
                    }

                    #endregion Binary

                    #region Unary

                    /// <summary>
                    /// Represents an unary operation, as in 'x => !x'. The target must be a <see cref="Node"/> instance. There
                    /// is no distinction between pre- and post- version of the same operation.
                    /// </summary>
                    [Serializable]
                    public class Unary : Node, ISerializable
                    {
                        /// <summary>Gets the operation.</summary>
                        public ExpressionType Operation { get; private set; }

                        /// <summary>Gets host of the <see cref="Node"/>.</summary>
                        public Node Target { get { return Host; } }

                        /// <summary>
                        /// Initializes a new instance of the <see cref="Unary"/> class.
                        /// </summary>
                        /// <param name="target">The target.</param>
                        /// <param name="operation">The operation.</param>
                        public Unary(Node target, ExpressionType operation)
                            : base(target)
                        {
                            Operation = operation;
                        }

                        /// <summary>
                        /// Initializes a new instance of the <see cref="Unary"/> class.
                        /// </summary>
                        /// <param name="info">The info.</param>
                        /// <param name="context">The context.</param>
                        protected Unary(SerializationInfo info, StreamingContext context)
                            : base(info, context)
                        {
                            Operation = (ExpressionType)info.GetValue("Operation", typeof(ExpressionType));
                        }

                        /// <summary>
                        /// Gets the object data.
                        /// </summary>
                        /// <param name="info">The info.</param>
                        /// <param name="context">The context.</param>
                        public override void GetObjectData(SerializationInfo info, StreamingContext context)
                        {
                            info.AddValue("Operation", Operation);

                            base.GetObjectData(info, context);
                        }

                        /// <summary>Returns a <see cref="System.String" /> that represents this instance.</summary>
                        /// <returns>A <see cref="System.String" /> that represents this instance.</returns>
                        public override string ToString()
                        {
                            if (IsDisposed)
                                return "{DynamicParser::Node::Binary::Disposed}";

                            return string.Format("({0} {1})", Operation, Host.Sketch());
                        }
                    }

                    #endregion Unary

                    #region Convert

                    /// <summary>
                    /// Represents a conversion operation, as in 'x => (string)x'.
                    /// </summary>
                    [Serializable]
                    public class Convert : Node, ISerializable
                    {
                        /// <summary>Gets the new type to which value will be converted.</summary>
                        public Type NewType { get; private set; }

                        /// <summary>Gets host of the <see cref="Node"/>.</summary>
                        public Node Target { get { return Host; } }

                        /// <summary>
                        /// Initializes a new instance of the <see cref="Convert"/> class.
                        /// </summary>
                        /// <param name="target">The target.</param>
                        /// <param name="newType">The new type.</param>
                        public Convert(Node target, Type newType)
                            : base(target)
                        {
                            NewType = newType;
                        }

                        /// <summary>
                        /// Initializes a new instance of the <see cref="Convert"/> class.
                        /// </summary>
                        /// <param name="info">The info.</param>
                        /// <param name="context">The context.</param>
                        protected Convert(SerializationInfo info, StreamingContext context)
                            : base(info, context)
                        {
                            NewType = (Type)info.GetValue("NewType", typeof(Type));
                        }

                        /// <summary>
                        /// Gets the object data.
                        /// </summary>
                        /// <param name="info">The info.</param>
                        /// <param name="context">The context.</param>
                        public override void GetObjectData(SerializationInfo info, StreamingContext context)
                        {
                            info.AddValue("NewType", NewType);

                            base.GetObjectData(info, context);
                        }
                    }

                    #endregion Convert

                    /// <summary>
                    /// Gets the name of the member. It might be null if this instance is disposed.
                    /// </summary>
                    public string Name { get; internal set; }

                    /// <summary>Gets host of the <see cref="Node"/>.</summary>
                    public Node Host { get; internal set; }

                    /// <summary>Gets reference to the parser.</summary>
                    public DynamicParser Parser { get; internal set; }

                    /// <summary>
                    /// Initializes a new instance of the <see cref="Node"/> class.
                    /// </summary>
                    internal Node()
                    {
                        IsDisposed = false;
                    }

                    /// <summary>
                    /// Initializes a new instance of the <see cref="Node"/> class.
                    /// </summary>
                    /// <param name="host">The host.</param>
                    internal Node(Node host)
                        : this()
                    {
                        if (host == null)
                            throw new ArgumentNullException("host", "Host cannot be null.");

                        Host = host;
                    }

                    /// <summary>
                    /// Initializes a new instance of the <see cref="Node"/> class.
                    /// </summary>
                    /// <param name="name">The name.</param>
                    internal Node(string name)
                        : this()
                    {
                        Name = name.Validated("Name");
                    }

                    /// <summary>
                    /// Initializes a new instance of the <see cref="Node"/> class.
                    /// </summary>
                    /// <param name="host">The host.</param>
                    /// <param name="name">The name.</param>
                    /// <exception cref="System.ArgumentNullException">Host cannot be null.</exception>
                    internal Node(Node host, string name)
                        : this(host)
                    {
                        Name = name.Validated("Name");
                    }

                    /// <summary>
                    /// Initializes a new instance of the <see cref="Node"/> class.
                    /// </summary>
                    /// <param name="info">The info.</param>
                    /// <param name="context">The context.</param>
                    protected Node(SerializationInfo info, StreamingContext context)
                    {
                        Name = info.GetString("MemberName");

                        string type = info.GetString("HostType");
                        Host = type == "NULL" ? null : (Node)info.GetValue("HostItem", Type.GetType(type));
                    }

                    /// <summary>Returns a <see cref="System.String" /> that represents this instance.</summary>
                    /// <returns>A <see cref="System.String" /> that represents this instance.</returns>
                    public override string ToString()
                    {
                        if (IsDisposed)
                            return "{DynamicParser::Node::Disposed}";

                        return "{DynamicParser::Node::Empty}";
                    }

                    #region Implementation of IDynamicMetaObjectProvider

                    /// <summary>Returns the <see cref="T:System.Dynamic.DynamicMetaObject" /> responsible
                    /// for binding operations performed on this object.</summary>
                    /// <param name="parameter">The expression tree representation of the runtime value.</param>
                    /// <returns>The <see cref="T:System.Dynamic.DynamicMetaObject" /> to bind this object.</returns>
                    /// <exception cref="System.ObjectDisposedException">Thrown if this instance is disposed.</exception>
                    public DynamicMetaObject GetMetaObject(System.Linq.Expressions.Expression parameter)
                    {
                        if (IsDisposed)
                            throw new ObjectDisposedException("DynamicParser.Node");

                        return new MetaNode(
                            parameter,
                            BindingRestrictions.GetInstanceRestriction(parameter, this),
                            this);
                    }

                    #endregion Implementation of IDynamicMetaObjectProvider

                    #region Implementation of IExtendedDisposable

                    /// <summary>Gets a value indicating whether this instance is disposed.</summary>
                    public bool IsDisposed { get; private set; }

                    /// <summary>
                    /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
                    /// </summary>
                    public void Dispose()
                    {
                        IsDisposed = true;
                    }

                    #endregion Implementation of IExtendedDisposable

                    #region Implementation of ISerializable

                    /// <summary>
                    /// Populates a <see cref="T:System.Runtime.Serialization.SerializationInfo" /> with the data needed to serialize the target object.
                    /// </summary>
                    /// <param name="info">The <see cref="T:System.Runtime.Serialization.SerializationInfo" /> to populate with data.</param>
                    /// <param name="context">The destination (see <see cref="T:System.Runtime.Serialization.StreamingContext" />) for this serialization.</param>
                    public virtual void GetObjectData(SerializationInfo info, StreamingContext context)
                    {
                        if (!string.IsNullOrEmpty(Name))
                            info.AddValue("MemberName", Name);

                        info.AddValue("HostType", Host == null ? "NULL" : Host.GetType().AssemblyQualifiedName);
                        if (Host != null)
                            info.AddValue("HostItem", Host);
                    }

                    #endregion Implementation of ISerializable
                }

                #endregion Node

                #region Data

                private List<Node.Argument> _arguments = new List<Node.Argument>();
                private object _uncertainResult;

                #endregion Data

                #region Properties

                /// <summary>Gets the last node (root of the tree).</summary>
                public Node Last { get; internal set; }

                /// <summary>
                /// Gets an enumeration containing the dynamic arguments used in the dynamic lambda expression parsed.
                /// </summary>
                public IEnumerable<Node.Argument> Arguments
                {
                    get
                    {
                        List<Node.Argument> list = new List<Node.Argument>();
                        if (!IsDisposed && _arguments != null)
                            list.AddRange(_arguments);

                        foreach (var arg in list)
                            yield return arg;

                        list.Clear();
                        list = null;
                    }
                }

                /// <summary>
                /// Gets the number of dynamic arguments used in the dynamic lambda expression parsed.
                /// </summary>
                public int Count
                {
                    get { return _arguments == null ? 0 : _arguments.Count; }
                }

                /// <summary>
                /// Gets the result of the parsing of the dynamic lambda expression. This result can be either an arbitrary object,
                /// including null, if the expression resolves to it, or an instance of the <see cref="Node"/> class that
                /// contains the last logic expression evaluated when parsing the dynamic lambda expression.
                /// </summary>
                public object Result
                {
                    get { return _uncertainResult ?? Last; }
                }

                #endregion Properties

                private DynamicParser(Delegate f)
                {
                    foreach (var p in f.Method.GetParameters())
                    {
                        if (p.GetCustomAttributes(typeof(DynamicAttribute), true).Any())
                            this._arguments.Add(new Node.Argument(p.Name) { Parser = this });
                        else
                            throw new ArgumentException(string.Format("Argument '{0}' must be dynamic.", p.Name));
                    }

                    _uncertainResult = f.DynamicInvoke(_arguments.ToArray());
                }

                /// <summary>
                /// Parses the dynamic lambda expression given in the form of a delegate, and returns a new instance of the
                /// <see cref="DynamicParser"/> class that holds the dynamic arguments used in the dynamic lambda expression, and
                /// the result of the parsing.
                /// </summary>
                /// <param name="f">The dynamic lambda expression to parse.</param>
                /// <returns>A new instance of <see cref="DynamicParser"/>.</returns>
                public static DynamicParser Parse(Delegate f)
                {
                    return new DynamicParser(f);
                }

                /// <summary>Returns a <see cref="System.String" /> that represents this instance.</summary>
                /// <returns>A <see cref="System.String" /> that represents this instance.</returns>
                public override string ToString()
                {
                    if (IsDisposed)
                        return "{DynamicParser::Disposed}";

                    StringBuilder sb = new StringBuilder();

                    sb.Append("(");
                    bool first = true;

                    if (_arguments != null)
                    {
                        foreach (var arg in _arguments)
                        {
                            if (!first) sb.Append(", "); else first = false;
                            sb.Append(arg);
                        }
                    }

                    sb.Append(")");

                    sb.AppendFormat(" => {0}", Result.Sketch());

                    return sb.ToString();
                }

                #region Implementation of IExtendedDisposable

                /// <summary>Gets a value indicating whether this instance is disposed.</summary>
                public bool IsDisposed { get; private set; }

                /// <summary>
                /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
                /// </summary>
                public void Dispose()
                {
                    IsDisposed = true;
                }

                #endregion Implementation of IExtendedDisposable
            }

            /// <summary>Class that allows to use interfaces as dynamic objects.</summary>
            /// <typeparam name="T">Type of class to proxy.</typeparam>
            /// <remarks>This is temporary solution. Which allows to use builders as a dynamic type.</remarks>
            public class DynamicProxy<T> : DynamicObject
            {
                private T _proxy;
                private Type _type;
                private Dictionary<string, DynamicPropertyInvoker> _properties;
                private Dictionary<MethodInfo, Delegate> _methods;

                /// <summary>
                /// Initializes a new instance of the <see cref="DynamicProxy{T}" /> class.
                /// </summary>
                /// <param name="proxiedObject">The object to which proxy should be created.</param>
                /// <exception cref="System.ArgumentNullException">The object to which proxy should be created is null.</exception>
                public DynamicProxy(T proxiedObject)
                {
                    if (proxiedObject == null)
                        throw new ArgumentNullException("proxiedObject");

                    _proxy = proxiedObject;
                    _type = typeof(T);

                    var members = GetAllMembers(_type);

                    _properties = members
                        .Where(x => x is PropertyInfo)
                        .ToDictionary(
                            k => k.Name,
                            v => new DynamicPropertyInvoker((PropertyInfo)v, null));

                    _methods = members
                        .Where(x => x is MethodInfo)
                        .Cast<MethodInfo>()
                        .Where(m => !((m.Name.StartsWith("set_") && m.ReturnType == typeof(void)) || m.Name.StartsWith("get_")))
                        .Where(m => !m.IsStatic && !m.IsGenericMethod)
                        .ToDictionary(
                            k => k,
                            v =>
                            {
                                try
                                {
                                    return Delegate.CreateDelegate(Expression.GetDelegateType(v.GetParameters().Select(t => t.ParameterType).Concat(new[] { v.ReflectedType }).ToArray()), _proxy, v.Name);
                                }
                                catch (ArgumentException)
                                {
                                    return null;
                                }
                            });
                }

                /// <summary>Provides implementation for type conversion operations.
                /// Classes derived from the <see cref="T:System.Dynamic.DynamicObject" />
                /// class can override this method to specify dynamic behavior for
                /// operations that convert an object from one type to another.</summary>
                /// <param name="binder">Provides information about the conversion operation.
                /// The binder.Type property provides the type to which the object must be
                /// converted. For example, for the statement (String)sampleObject in C#
                /// (CType(sampleObject, Type) in Visual Basic), where sampleObject is an
                /// instance of the class derived from the <see cref="T:System.Dynamic.DynamicObject" />
                /// class, binder.Type returns the <see cref="T:System.String" /> type.
                /// The binder.Explicit property provides information about the kind of
                /// conversion that occurs. It returns true for explicit conversion and
                /// false for implicit conversion.</param>
                /// <param name="result">The result of the type conversion operation.</param>
                /// <returns>Returns <c>true</c> if the operation is successful; otherwise, <c>false</c>.
                /// If this method returns false, the run-time binder of the language determines the
                /// behavior. (In most cases, a language-specific run-time exception is thrown).</returns>
                public override bool TryConvert(ConvertBinder binder, out object result)
                {
                    if (binder.Type == typeof(T))
                    {
                        result = _proxy;
                        return true;
                    }

                    if (_proxy != null &&
                        binder.Type.IsAssignableFrom(_proxy.GetType()))
                    {
                        result = _proxy;
                        return true;
                    }

                    return base.TryConvert(binder, out result);
                }

                /// <summary>Provides the implementation for operations that get member
                /// values. Classes derived from the <see cref="T:System.Dynamic.DynamicObject" />
                /// class can override this method to specify dynamic behavior for
                /// operations such as getting a value for a property.</summary>
                /// <param name="binder">Provides information about the object that
                /// called the dynamic operation. The binder.Name property provides
                /// the name of the member on which the dynamic operation is performed.
                /// For example, for the Console.WriteLine(sampleObject.SampleProperty)
                /// statement, where sampleObject is an instance of the class derived
                /// from the <see cref="T:System.Dynamic.DynamicObject" /> class,
                /// binder.Name returns "SampleProperty". The binder.IgnoreCase property
                /// specifies whether the member name is case-sensitive.</param>
                /// <param name="result">The result of the get operation. For example,
                /// if the method is called for a property, you can assign the property
                /// value to <paramref name="result" />.</param>
                /// <returns>Returns <c>true</c> if the operation is successful; otherwise,
                /// <c>false</c>. If this method returns false, the run-time binder of the
                /// language determines the behavior. (In most cases, a run-time exception
                /// is thrown).</returns>
                public override bool TryGetMember(GetMemberBinder binder, out object result)
                {
                    try
                    {
                        var prop = _properties.TryGetValue(binder.Name);

                        result = prop.NullOr(p => p.Get.NullOr(g => g(_proxy), null), null);

                        return prop != null && prop.Get != null;
                    }
                    catch (Exception ex)
                    {
                        throw new InvalidOperationException(string.Format("Cannot get member {0}", binder.Name), ex);
                    }
                }

                /// <summary>Provides the implementation for operations that set member
                /// values. Classes derived from the <see cref="T:System.Dynamic.DynamicObject" />
                /// class can override this method to specify dynamic behavior for operations
                /// such as setting a value for a property.</summary>
                /// <param name="binder">Provides information about the object that called
                /// the dynamic operation. The binder.Name property provides the name of
                /// the member to which the value is being assigned. For example, for the
                /// statement sampleObject.SampleProperty = "Test", where sampleObject is
                /// an instance of the class derived from the <see cref="T:System.Dynamic.DynamicObject" />
                /// class, binder.Name returns "SampleProperty". The binder.IgnoreCase
                /// property specifies whether the member name is case-sensitive.</param>
                /// <param name="value">The value to set to the member. For example, for
                /// sampleObject.SampleProperty = "Test", where sampleObject is an instance
                /// of the class derived from the <see cref="T:System.Dynamic.DynamicObject" />
                /// class, the <paramref name="value" /> is "Test".</param>
                /// <returns>Returns <c>true</c> if the operation is successful; otherwise,
                /// <c>false</c>. If this method returns false, the run-time binder of the
                /// language determines the behavior. (In most cases, a language-specific
                /// run-time exception is thrown).</returns>
                public override bool TrySetMember(SetMemberBinder binder, object value)
                {
                    try
                    {
                        var prop = _properties.TryGetValue(binder.Name);

                        if (prop != null && prop.Set != null)
                        {
                            prop.Set(_proxy, value);
                            return true;
                        }

                        return false;
                    }
                    catch (Exception ex)
                    {
                        throw new InvalidOperationException(string.Format("Cannot set member {0} to '{1}'", binder.Name, value), ex);
                    }
                }

                /// <summary>Provides the implementation for operations that invoke a member.
                /// Classes derived from the <see cref="T:System.Dynamic.DynamicObject" />
                /// class can override this method to specify dynamic behavior for
                /// operations such as calling a method.</summary>
                /// <param name="binder">Provides information about the dynamic operation.
                /// The binder.Name property provides the name of the member on which the
                /// dynamic operation is performed. For example, for the statement
                /// sampleObject.SampleMethod(100), where sampleObject is an instance of
                /// the class derived from the <see cref="T:System.Dynamic.DynamicObject" />
                /// class, binder.Name returns "SampleMethod". The binder.IgnoreCase property
                /// specifies whether the member name is case-sensitive.</param>
                /// <param name="args">The arguments that are passed to the object member
                /// during the invoke operation. For example, for the statement
                /// sampleObject.SampleMethod(100), where sampleObject is derived from the
                /// <see cref="T:System.Dynamic.DynamicObject" /> class,
                /// First element of <paramref name="args" /> is equal to 100.</param>
                /// <param name="result">The result of the member invocation.</param>
                /// <returns>Returns <c>true</c> if the operation is successful; otherwise,
                /// <c>false</c>. If this method returns false, the run-time binder of the
                /// language determines the behavior. (In most cases, a language-specific
                /// run-time exception is thrown).</returns>
                public override bool TryInvokeMember(InvokeMemberBinder binder, object[] args, out object result)
                {
                    return TryInvokeMethod(binder.Name, out result, args) || base.TryInvokeMember(binder, args, out result);
                }

                private bool TryInvokeMethod(string name, out object result, object[] args)
                {
                    result = null;

                    MethodInfo mi = _methods.Keys
                        .Where(m => m.Name == name)
                        .FirstOrDefault(m =>
                            CompareTypes(m.GetParameters().ToArray(),
                            args.Select(a => a.GetType()).ToArray()));

                    Delegate d = _methods.TryGetValue(mi);

                    if (d != null)
                    {
                        result = d.DynamicInvoke(CompleteArguments(mi.GetParameters().ToArray(), args));

                        if (d.Method.ReturnType == _type && result is T)
                            result = new DynamicProxy<T>((T)result);

                        return true;
                    }
                    else if (mi != null)
                    {
                        result = mi.Invoke(_proxy, CompleteArguments(mi.GetParameters().ToArray(), args));

                        if (mi.ReturnType == _type && result is T)
                            result = new DynamicProxy<T>((T)result);

                        return true;
                    }

                    return false;
                }

                private bool CompareTypes(ParameterInfo[] parameters, Type[] types)
                {
                    if (parameters.Length < types.Length || parameters.Count(p => !p.IsOptional) > types.Length)
                        return false;

                    for (int i = 0; i < types.Length; i++)
                        if (types[i] != parameters[i].ParameterType && !parameters[i].ParameterType.IsAssignableFrom(types[i]))
                            return false;

                    return true;
                }

                private object[] CompleteArguments(ParameterInfo[] parameters, object[] arguments)
                {
                    return arguments.Concat(parameters.Skip(arguments.Length).Select(p => p.DefaultValue)).ToArray();
                }

                private IEnumerable<MemberInfo> GetAllMembers(Type type)
                {
                    if (type.IsInterface)
                    {
                        var members = new List<MemberInfo>();

                        var considered = new List<Type>();
                        var queue = new Queue<Type>();

                        considered.Add(type);
                        queue.Enqueue(type);

                        while (queue.Count > 0)
                        {
                            var subType = queue.Dequeue();
                            foreach (var subInterface in subType.GetInterfaces())
                            {
                                if (considered.Contains(subInterface)) continue;

                                considered.Add(subInterface);
                                queue.Enqueue(subInterface);
                            }

                            var typeProperties = subType.GetMembers(
                                BindingFlags.FlattenHierarchy
                                | BindingFlags.Public
                                | BindingFlags.Instance);

                            var newPropertyInfos = typeProperties
                                .Where(x => !members.Contains(x));

                            members.InsertRange(0, newPropertyInfos);
                        }

                        return members;
                    }

                    return type.GetMembers(BindingFlags.FlattenHierarchy | BindingFlags.Public | BindingFlags.Instance);
                }
            }
        }
    }

    namespace Mapper
    {
        /// <summary>Allows to add table name to class.</summary>
        [AttributeUsage(AttributeTargets.Property)]
        public class ColumnAttribute : Attribute
        {
            /// <summary>Gets or sets name.</summary>
            public string Name { get; set; }

            /// <summary>Gets or sets column type.</summary>
            /// <remarks>Used when overriding schema.</remarks>
            public DbType? Type { get; set; }

            /// <summary>Gets or sets a value indicating whether column is a key.</summary>
            public bool IsKey { get; set; }

            /// <summary>Gets or sets a value indicating whether column should have unique value.</summary>
            /// <remarks>Used when overriding schema.</remarks>
            public bool? IsUnique { get; set; }

            /// <summary>Gets or sets column size.</summary>
            /// <remarks>Used when overriding schema.</remarks>
            public int? Size { get; set; }

            /// <summary>Gets or sets column precision.</summary>
            /// <remarks>Used when overriding schema.</remarks>
            public byte? Precision { get; set; }

            /// <summary>Gets or sets column scale.</summary>
            /// <remarks>Used when overriding schema.</remarks>
            public byte? Scale { get; set; }

            /// <summary>Gets or sets a value indicating whether this column is no allowed to be inserted.</summary>
            /// <remarks>This is only a suggestion to automated mapping.</remarks>
            public bool IsNoInsert { get; set; }

            /// <summary>Gets or sets a value indicating whether this column is no allowed to be updated.</summary>
            /// <remarks>This is only a suggestion to automated mapping.</remarks>
            public bool IsNoUpdate { get; set; }

            #region Constructors

            /// <summary>Initializes a new instance of the <see cref="ColumnAttribute" /> class.</summary>
            public ColumnAttribute()
            {
            }

            /// <summary>Initializes a new instance of the <see cref="ColumnAttribute" /> class.</summary>
            /// <param name="name">Name of column.</param>
            public ColumnAttribute(string name)
            {
                Name = name;
            }

            /// <summary>Initializes a new instance of the <see cref="ColumnAttribute" /> class.</summary>
            /// <param name="isKey">Set column as a key column.</param>
            public ColumnAttribute(bool isKey)
            {
                IsKey = isKey;
            }

            /// <summary>Initializes a new instance of the <see cref="ColumnAttribute" /> class.</summary>
            /// <param name="name">Name of column.</param>
            /// <param name="isKey">Set column as a key column.</param>
            public ColumnAttribute(string name, bool isKey)
                : this(name)
            {
                IsKey = isKey;
            }

            /// <summary>Initializes a new instance of the <see cref="ColumnAttribute" /> class.</summary>
            /// <param name="isKey">Set column as a key column.</param>
            /// <param name="type">Set column type.</param>
            public ColumnAttribute(bool isKey, DbType type)
                : this(isKey)
            {
                Type = type;
            }

            /// <summary>Initializes a new instance of the <see cref="ColumnAttribute" /> class.</summary>
            /// <param name="name">Name of column.</param>
            /// <param name="isKey">Set column as a key column.</param>
            /// <param name="type">Set column type.</param>
            public ColumnAttribute(string name, bool isKey, DbType type)
                : this(name, isKey)
            {
                Type = type;
            }

            /// <summary>Initializes a new instance of the <see cref="ColumnAttribute" /> class.</summary>
            /// <param name="name">Name of column.</param>
            /// <param name="isKey">Set column as a key column.</param>
            /// <param name="type">Set column type.</param>
            /// <param name="size">Set column value size.</param>
            public ColumnAttribute(string name, bool isKey, DbType type, int size)
                : this(name, isKey, type)
            {
                Size = size;
            }

            /// <summary>Initializes a new instance of the <see cref="ColumnAttribute" /> class.</summary>
            /// <param name="name">Name of column.</param>
            /// <param name="isKey">Set column as a key column.</param>
            /// <param name="type">Set column type.</param>
            /// <param name="precision">Set column value precision.</param>
            /// <param name="scale">Set column value scale.</param>
            public ColumnAttribute(string name, bool isKey, DbType type, byte precision, byte scale)
                : this(name, isKey, type)
            {
                Precision = precision;
                Scale = scale;
            }

            /// <summary>Initializes a new instance of the <see cref="ColumnAttribute" /> class.</summary>
            /// <param name="name">Name of column.</param>
            /// <param name="isKey">Set column as a key column.</param>
            /// <param name="type">Set column type.</param>
            /// <param name="size">Set column value size.</param>
            /// <param name="precision">Set column value precision.</param>
            /// <param name="scale">Set column value scale.</param>
            public ColumnAttribute(string name, bool isKey, DbType type, int size, byte precision, byte scale)
                : this(name, isKey, type, precision, scale)
            {
                Size = size;
            }

            /// <summary>Initializes a new instance of the <see cref="ColumnAttribute" /> class.</summary>
            /// <param name="name">Name of column.</param>
            /// <param name="isKey">Set column as a key column.</param>
            /// <param name="isUnique">Set column has unique value.</param>
            /// <param name="type">Set column type.</param>
            /// <param name="size">Set column value size.</param>
            /// <param name="precision">Set column value precision.</param>
            /// <param name="scale">Set column value scale.</param>
            public ColumnAttribute(string name, bool isKey, bool isUnique, DbType type, int size, byte precision, byte scale)
                : this(name, isKey, type, size, precision, scale)
            {
                IsUnique = isUnique;
            }

            #endregion Constructors
        }

        /// <summary>Class with mapper cache.</summary>
        public static class DynamicMapperCache
        {
            private static readonly object SyncLock = new object();
            private static Dictionary<Type, DynamicTypeMap> _cache = new Dictionary<Type, DynamicTypeMap>();

            /// <summary>Get type mapper.</summary>
            /// <typeparam name="T">Type of mapper.</typeparam>
            /// <returns>Type mapper.</returns>
            public static DynamicTypeMap GetMapper<T>()
            {
                return GetMapper(typeof(T));
            }

            /// <summary>Get type mapper.</summary>
            /// <param name="type">Type of mapper.</param>
            /// <returns>Type mapper.</returns>
            public static DynamicTypeMap GetMapper(Type type)
            {
                if (type == null)
                    return null;
                /*if (type.IsAnonymous())
                    return null;*/

                DynamicTypeMap mapper = null;

                lock (SyncLock)
                {
                    if (!_cache.TryGetValue(type, out mapper))
                    {
                        mapper = new DynamicTypeMap(type);

                        if (mapper != null)
                            _cache.Add(type, mapper);
                    }
                }

                return mapper;
            }
        }

        /// <summary>Dynamic property invoker.</summary>
        public class DynamicPropertyInvoker
        {
            internal class ParameterSpec
            {
                public string Name { get; set; }

                public DbType Type { get; set; }

                public int Ordinal { get; set; }
            }

            /// <summary>Gets the type of property.</summary>
            public Type Type { get; private set; }

            /// <summary>Gets value getter.</summary>
            public Func<object, object> Get { get; private set; }

            /// <summary>Gets value setter.</summary>
            public Action<object, object> Set { get; private set; }

            /// <summary>Gets name of property.</summary>
            public string Name { get; private set; }

            /// <summary>Gets type column description.</summary>
            public ColumnAttribute Column { get; private set; }

            /// <summary>Gets a value indicating whether this <see cref="DynamicPropertyInvoker"/> is ignored in some cases.</summary>
            public bool Ignore { get; private set; }

            /// <summary>Initializes a new instance of the <see cref="DynamicPropertyInvoker" /> class.</summary>
            /// <param name="property">Property info to be invoked in the future.</param>
            /// <param name="attr">Column attribute if exist.</param>
            public DynamicPropertyInvoker(PropertyInfo property, ColumnAttribute attr)
            {
                Name = property.Name;
                Type = property.PropertyType;

                var ignore = property.GetCustomAttributes(typeof(IgnoreAttribute), false);

                Ignore = ignore != null && ignore.Length > 0;

                Column = attr;

                if (property.CanRead)
                    Get = CreateGetter(property);

                if (property.CanWrite)
                    Set = CreateSetter(property);
            }

            private Func<object, object> CreateGetter(PropertyInfo property)
            {
                if (!property.CanRead)
                    return null;

                var objParm = Expression.Parameter(typeof(object), "o");

                return Expression.Lambda<Func<object, object>>(
                    Expression.Convert(
                        Expression.Property(
                            Expression.TypeAs(objParm, property.DeclaringType),
                            property.Name),
                        typeof(object)), objParm).Compile();
            }

            private Action<object, object> CreateSetter(PropertyInfo property)
            {
                if (!property.CanWrite)
                    return null;

                var objParm = Expression.Parameter(typeof(object), "o");
                var valueParm = Expression.Parameter(typeof(object), "value");

                return Expression.Lambda<Action<object, object>>(
                    Expression.Assign(
                        Expression.Property(
                            Expression.Convert(objParm, property.DeclaringType),
                            property.Name),
                        Expression.Convert(valueParm, property.PropertyType)),
                        objParm, valueParm).Compile();
            }

            #region Type command cache

            internal ParameterSpec InsertCommandParameter { get; set; }

            internal ParameterSpec UpdateCommandParameter { get; set; }

            internal ParameterSpec DeleteCommandParameter { get; set; }

            #endregion Type command cache
        }

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
                var ignored = new List<string>();

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

                    if (val.Ignore)
                        ignored.Add(pi.Name);
                }

                ColumnsMap = columnMap;
                PropertyMap = propertyMap;

                Ignored = ignored; ////columnMap.Where(i => i.Value.Ignore).Select(i => i.Value.Name).ToList();
            }

            private Func<object> CreateCreator()
            {
                if (Type.GetConstructor(Type.EmptyTypes) != null)
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

            /// <summary>Fill values from <c>source</c> to <see cref="DynamicTypeMap.Type"/> object in <c>destination</c>.</summary>
            /// <param name="source">Object containing values that will be mapped to newly created object.</param>
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

            #region Type command cache

            internal string InsertCommandText { get; set; }

            internal string UpdateCommandText { get; set; }

            internal string DeleteCommandText { get; set; }

            #endregion Type command cache
        }

        /// <summary>Allows to add ignore action to property.</summary>
        /// <remarks>Property still get's mapped from output.</remarks>
        [AttributeUsage(AttributeTargets.Property)]
        public class IgnoreAttribute : Attribute
        {
        }

        /// <summary>Allows to add table name to class.</summary>
        [AttributeUsage(AttributeTargets.Class)]
        public class TableAttribute : Attribute
        {
            /// <summary>Gets or sets table owner name.</summary>
            public string Owner { get; set; }

            /// <summary>Gets or sets name.</summary>
            public string Name { get; set; }

            /// <summary>Gets or sets a value indicating whether override database
            /// schema values.</summary>
            /// <remarks>If database doesn't support schema, you still have to
            /// set this to true to get schema from type.</remarks>
            public bool Override { get; set; }
        }
    }
}