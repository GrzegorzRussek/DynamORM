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
}