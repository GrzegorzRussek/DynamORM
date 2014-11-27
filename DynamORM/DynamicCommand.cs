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
using System.Data;
using DynamORM.Helpers;

namespace DynamORM
{
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

                _command.Parameters.Clear();

                _command.Dispose();
                _command = null;
            }
        }

        /// <summary>Gets a value indicating whether this instance is disposed.</summary>
        public bool IsDisposed { get; private set; }

        #endregion IExtendedDisposable Members
    }
}