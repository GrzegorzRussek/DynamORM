﻿/*
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

namespace DynamORM
{
    /// <summary>Helper class to easy manage transaction.</summary>
    public class DynamicTransaction : IDbTransaction, IDisposable
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
        internal DynamicTransaction(DynamicDatabase db, DynamicConnection con, bool singleTransaction, IsolationLevel? il, Action disposed)
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
                    _db.TransactionPool[_con.Connection].Push(_con.Connection.BeginTransaction());
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

        /// <summary>Performs application-defined tasks associated with
        /// freeing, releasing, or resetting unmanaged resources.</summary>
        public void Dispose()
        {
            Rollback();

            if (_disposed != null)
                _disposed();
        }
    }
}