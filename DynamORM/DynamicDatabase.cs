﻿/*
 * DynamORM - Dynamic Object-Relational Mapping library.
 * Copyright (c) 2012-2015, Grzegorz Russek (grzegorz.russek@gmail.com)
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
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Dynamic;
using System.Linq;
using System.Reflection;
using System.Text;
using DynamORM.Builders;
using DynamORM.Builders.Extensions;
using DynamORM.Builders.Implementation;
using DynamORM.Helpers;
using DynamORM.Mapper;

namespace DynamORM
{
    /// <summary>Dynamic database is a class responsible for managing database.</summary>
    public class DynamicDatabase : IExtendedDisposable
    {
        #region Internal fields and properties

        private DbProviderFactory _provider;
        private DynamicProcedureInvoker _proc;
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

        /// <summary>Gets active builders that weren't disposed.</summary>
        internal List<IDynamicQueryBuilder> RemainingBuilders { get; private set; }

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

        /// <summary>Gets the procedures invoker.</summary>
        public dynamic Procedures
        {
            get
            {
                if (_proc == null)
                {
                    if ((Options & DynamicDatabaseOptions.SupportStoredProcedures) != DynamicDatabaseOptions.SupportStoredProcedures)
                        throw new InvalidOperationException("Database connection desn't support stored procedures.");

                    _proc = new DynamicProcedureInvoker(this);
                }

                return _proc;
            }
        }

        /// <summary>Gets or sets a value indicating whether
        /// dump commands to console or not.</summary>
        public bool DumpCommands { get; set; }

        /// <summary>Gets or sets the dump command delegate.</summary>
        /// <value>The dump command delegate.</value>
        public Action<IDbCommand, string> DumpCommandDelegate { get; set; }

#if NETFRAMEWORK
        // https://github.com/dotnet/runtime/issues/26229
        /// <summary>Initializes a new instance of the <see cref="DynamicDatabase" /> class.</summary>
        /// <param name="provider">Database provider by name.</param>
        /// <param name="connectionString">Connection string to provided database.</param>
        /// <param name="options">Connection options.</param>
        public DynamicDatabase(string provider, string connectionString, DynamicDatabaseOptions options)
            : this(DbProviderFactories.GetFactory(provider), connectionString, options)
        {
        }
#endif

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

        private DbProviderFactory FindDbProviderFactoryFromConnection(Type t)
        {
            foreach (var type in t.Assembly.GetTypes().Where(x => x.IsSubclassOf(typeof(DbProviderFactory))))
            {
                DbProviderFactory provider = null;
                bool dispose = false;

                var pi = type.GetProperty("Instance", BindingFlags.Static | BindingFlags.Public | BindingFlags.GetProperty);
                if (pi != null)
                    provider = (DbProviderFactory)pi.GetValue(null, null);
                else
                {
                    var fi = type.GetField("Instance", BindingFlags.Static | BindingFlags.Public | BindingFlags.GetField);
                    if (fi != null)
                        provider = (DbProviderFactory)fi.GetValue(null);
                }

                if (provider == null)
                {
                    var ci = type.GetConstructor(Type.EmptyTypes);
                    if (ci != null)
                    {
                        provider = ci.Invoke(null) as DbProviderFactory;
                        dispose = true;
                    }
                }

                try
                {
                    if (provider != null)
                    {
                        using (var c = provider.CreateConnection())
                        {
                            if (c.GetType() == t)
                                return provider;
                        }
                    }
                }
                finally
                {
                    if (provider != null && dispose && provider is IDisposable)
                        ((IDisposable)provider).Dispose();
                }
            }

            return null;
        }

        /// <summary>Initializes a new instance of the <see cref="DynamicDatabase" /> class.</summary>
        /// <param name="connection">Active database connection.</param>
        /// <param name="options">Connection options. <see cref="DynamicDatabaseOptions.SingleConnection"/> required.</param>
        public DynamicDatabase(IDbConnection connection, DynamicDatabaseOptions options)
        {
            // Try to find correct provider if possible
            _provider = FindDbProviderFactoryFromConnection(connection.GetType());

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
            RemainingBuilders = new List<IDynamicQueryBuilder>();
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
            foreach (KeyValuePair<string, DynamicTable> item in TablesCache.Where(kvp => kvp.Value == dynamicTable).ToList())
                TablesCache.Remove(item.Key);
        }

#endif

        #endregion Table

        /// <summary>Adds cached builder.</summary>
        /// <param name="builder">New dynamic builder.</param>
        internal void AddToCache(IDynamicQueryBuilder builder)
        {
            lock (SyncLock)
                RemainingBuilders.Add(builder);
        }

        /// <summary>Removes cached builder.</summary>
        /// <param name="builder">Disposed dynamic builder.</param>
        internal void RemoveFromCache(IDynamicQueryBuilder builder)
        {
            lock (SyncLock)
                RemainingBuilders.Remove(builder);
        }

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
        /// <typeparam name="T">Type which can be represented in database.</typeparam>
        /// <param name="alias">Table alias.</param>
        /// <returns>This instance to permit chaining.</returns>
        public virtual IDynamicSelectQueryBuilder From<T>(string alias)
        {
            return new DynamicSelectQueryBuilder(this).From(x => x(typeof(T)).As(alias));
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
            return Insert(typeof(T), e);
        }

        /// <summary>Bulk insert objects into database.</summary>
        /// <param name="t">Type of objects to insert.</param>
        /// <param name="e">Enumerable containing instances of objects to insert.</param>
        /// <returns>Number of inserted rows.</returns>
        public virtual int Insert(Type t, IEnumerable e)
        {
            int affected = 0;
            DynamicTypeMap mapper = DynamicMapperCache.GetMapper(t);

            if (mapper != null)
            {
                using (IDbConnection con = Open())
                using (IDbTransaction tra = con.BeginTransaction())
                using (IDbCommand cmd = con.CreateCommand())
                {
                    try
                    {
                        Dictionary<IDbDataParameter, DynamicPropertyInvoker> parameters = new Dictionary<IDbDataParameter, DynamicPropertyInvoker>();

                        if (!string.IsNullOrEmpty(mapper.InsertCommandText))
                        {
                            cmd.CommandText = mapper.InsertCommandText;

                            foreach (DynamicPropertyInvoker col in mapper.ColumnsMap.Values
                                .Where(di => !di.Ignore && di.InsertCommandParameter != null)
                                .OrderBy(di => di.InsertCommandParameter.Ordinal))
                            {
                                IDbDataParameter para = cmd.CreateParameter();
                                para.ParameterName = col.InsertCommandParameter.Name;
                                para.DbType = col.InsertCommandParameter.Type;
                                cmd.Parameters.Add(para);

                                parameters[para] = col;
                            }
                        }
                        else
                            PrepareBatchInsert(t, mapper, cmd, parameters);

                        foreach (var o in e)
                        {
                            foreach (KeyValuePair<IDbDataParameter, DynamicPropertyInvoker> m in parameters)
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

                        StringBuilder problematicCommand = new StringBuilder();
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
            return Update(typeof(T), e);
        }

        /// <summary>Bulk update objects in database.</summary>
        /// <param name="t">Type of objects to update.</param>
        /// <param name="e">Enumerable containing instances of objects to update.</param>
        /// <returns>Number of updated rows.</returns>
        public virtual int Update(Type t, IEnumerable e)
        {
            int affected = 0;
            DynamicTypeMap mapper = DynamicMapperCache.GetMapper(t);

            if (mapper != null)
            {
                using (IDbConnection con = Open())
                using (IDbTransaction tra = con.BeginTransaction())
                using (IDbCommand cmd = con.CreateCommand())
                {
                    try
                    {
                        Dictionary<IDbDataParameter, DynamicPropertyInvoker> parameters = new Dictionary<IDbDataParameter, DynamicPropertyInvoker>();

                        if (!string.IsNullOrEmpty(mapper.UpdateCommandText))
                        {
                            cmd.CommandText = mapper.UpdateCommandText;

                            foreach (DynamicPropertyInvoker col in mapper.ColumnsMap.Values
                                .Where(di => !di.Ignore && di.UpdateCommandParameter != null)
                                .OrderBy(di => di.UpdateCommandParameter.Ordinal))
                            {
                                IDbDataParameter para = cmd.CreateParameter();
                                para.ParameterName = col.UpdateCommandParameter.Name;
                                para.DbType = col.UpdateCommandParameter.Type;
                                cmd.Parameters.Add(para);

                                parameters[para] = col;
                            }
                        }
                        else
                            PrepareBatchUpdate(t, mapper, cmd, parameters);

                        foreach (var o in e)
                        {
                            foreach (KeyValuePair<IDbDataParameter, DynamicPropertyInvoker> m in parameters)
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

                        StringBuilder problematicCommand = new StringBuilder();
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
            return UpdateOrInsert(typeof(T), e);
        }

        /// <summary>Bulk update or insert objects into database.</summary>
        /// <param name="t">Type of objects to update or insert.</param>
        /// <param name="e">Enumerable containing instances of objects to update or insert.</param>
        /// <returns>Number of updated or inserted rows.</returns>
        public virtual int UpdateOrInsert(Type t, IEnumerable e)
        {
            int affected = 0;
            DynamicTypeMap mapper = DynamicMapperCache.GetMapper(t);

            if (mapper != null)
            {
                using (IDbConnection con = Open())
                using (IDbTransaction tra = con.BeginTransaction())
                using (IDbCommand cmdUp = con.CreateCommand())
                using (IDbCommand cmdIn = con.CreateCommand())
                {
                    try
                    {
                        #region Update

                        Dictionary<IDbDataParameter, DynamicPropertyInvoker> parametersUp = new Dictionary<IDbDataParameter, DynamicPropertyInvoker>();

                        if (!string.IsNullOrEmpty(mapper.UpdateCommandText))
                        {
                            cmdUp.CommandText = mapper.UpdateCommandText;

                            foreach (DynamicPropertyInvoker col in mapper.ColumnsMap.Values
                                .Where(di => !di.Ignore && di.UpdateCommandParameter != null)
                                .OrderBy(di => di.UpdateCommandParameter.Ordinal))
                            {
                                IDbDataParameter para = cmdUp.CreateParameter();
                                para.ParameterName = col.UpdateCommandParameter.Name;
                                para.DbType = col.UpdateCommandParameter.Type;
                                cmdUp.Parameters.Add(para);

                                parametersUp[para] = col;
                            }
                        }
                        else
                            PrepareBatchUpdate(t, mapper, cmdUp, parametersUp);

                        #endregion Update

                        #region Insert

                        Dictionary<IDbDataParameter, DynamicPropertyInvoker> parametersIn = new Dictionary<IDbDataParameter, DynamicPropertyInvoker>();

                        if (!string.IsNullOrEmpty(mapper.InsertCommandText))
                        {
                            cmdIn.CommandText = mapper.InsertCommandText;

                            foreach (DynamicPropertyInvoker col in mapper.ColumnsMap.Values
                                .Where(di => !di.Ignore && di.InsertCommandParameter != null)
                                .OrderBy(di => di.InsertCommandParameter.Ordinal))
                            {
                                IDbDataParameter para = cmdIn.CreateParameter();
                                para.ParameterName = col.InsertCommandParameter.Name;
                                para.DbType = col.InsertCommandParameter.Type;
                                cmdIn.Parameters.Add(para);

                                parametersIn[para] = col;
                            }
                        }
                        else
                            PrepareBatchInsert(t, mapper, cmdIn, parametersIn);

                        #endregion Insert

                        foreach (var o in e)
                        {
                            foreach (KeyValuePair<IDbDataParameter, DynamicPropertyInvoker> m in parametersUp)
                                m.Key.Value = m.Value.Get(o);

                            int a = cmdUp.ExecuteNonQuery();
                            if (a == 0)
                            {
                                foreach (KeyValuePair<IDbDataParameter, DynamicPropertyInvoker> m in parametersIn)
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

                        StringBuilder problematicCommand = new StringBuilder();
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

        /// <summary>Adds to the <code>DELETE FROM</code> clause using <see cref="Type"/>.</summary>
        /// <param name="t">Type which can be represented in database.</param>
        /// <returns>This instance to permit chaining.</returns>
        public virtual IDynamicDeleteQueryBuilder Delete(Type t)
        {
            return new DynamicDeleteQueryBuilder(this).Table(t);
        }

        /// <summary>Bulk delete objects in database.</summary>
        /// <typeparam name="T">Type of objects to delete.</typeparam>
        /// <param name="e">Enumerable containing instances of objects to delete.</param>
        /// <returns>Number of deleted rows.</returns>
        public virtual int Delete<T>(IEnumerable<T> e) where T : class
        {
            return Delete(typeof(T), e);
        }

        /// <summary>Bulk delete objects in database.</summary>
        /// <param name="t">Type of objects to delete.</param>
        /// <param name="e">Enumerable containing instances of objects to delete.</param>
        /// <returns>Number of deleted rows.</returns>
        public virtual int Delete(Type t, IEnumerable e)
        {
            int affected = 0;
            DynamicTypeMap mapper = DynamicMapperCache.GetMapper(t);

            if (mapper != null)
            {
                using (IDbConnection con = Open())
                using (IDbTransaction tra = con.BeginTransaction())
                using (IDbCommand cmd = con.CreateCommand())
                {
                    try
                    {
                        Dictionary<IDbDataParameter, DynamicPropertyInvoker> parameters = new Dictionary<IDbDataParameter, DynamicPropertyInvoker>();

                        if (!string.IsNullOrEmpty(mapper.DeleteCommandText))
                        {
                            cmd.CommandText = mapper.DeleteCommandText;

                            foreach (DynamicPropertyInvoker col in mapper.ColumnsMap.Values
                                .Where(di => !di.Ignore && di.DeleteCommandParameter != null)
                                .OrderBy(di => di.DeleteCommandParameter.Ordinal))
                            {
                                IDbDataParameter para = cmd.CreateParameter();
                                para.ParameterName = col.DeleteCommandParameter.Name;
                                para.DbType = col.DeleteCommandParameter.Type;
                                cmd.Parameters.Add(para);

                                parameters[para] = col;
                            }
                        }
                        else
                            PrepareBatchDelete(t, mapper, cmd, parameters);

                        foreach (var o in e)
                        {
                            foreach (KeyValuePair<IDbDataParameter, DynamicPropertyInvoker> m in parameters)
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

                        StringBuilder problematicCommand = new StringBuilder();
                        cmd.Dump(problematicCommand);

                        throw new InvalidOperationException(problematicCommand.ToString(), ex);
                    }
                }
            }

            return affected;
        }

        private void PrepareBatchInsert<T>(DynamicTypeMap mapper, IDbCommand cmd, Dictionary<IDbDataParameter, DynamicPropertyInvoker> parameters)
        {
            PrepareBatchInsert(typeof(T), mapper, cmd, parameters);
        }

        private void PrepareBatchInsert(Type t, DynamicTypeMap mapper, IDbCommand cmd, Dictionary<IDbDataParameter, DynamicPropertyInvoker> parameters)
        {
            DynamicPropertyInvoker currentprop = null;
            Dictionary<string, DynamicPropertyInvoker> temp = new Dictionary<string, DynamicPropertyInvoker>();
            Dictionary<string, DynamicSchemaColumn> schema = this.GetSchema(t);
            int ord = 0;

            IDynamicInsertQueryBuilder ib = Insert(t)
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

            foreach (KeyValuePair<string, string> prop in mapper.PropertyMap)
                if (!mapper.Ignored.Contains(prop.Key))
                {
                    string col = mapper.PropertyMap.TryGetValue(prop.Key) ?? prop.Key;
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
            PrepareBatchUpdate(typeof(T), mapper, cmd, parameters);
        }

        private void PrepareBatchUpdate(Type t, DynamicTypeMap mapper, IDbCommand cmd, Dictionary<IDbDataParameter, DynamicPropertyInvoker> parameters)
        {
            DynamicPropertyInvoker currentprop = null;
            Dictionary<string, DynamicPropertyInvoker> temp = new Dictionary<string, DynamicPropertyInvoker>();
            Dictionary<string, DynamicSchemaColumn> schema = this.GetSchema(t);
            int ord = 0;

            IDynamicUpdateQueryBuilder ib = Update(t)
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

            foreach (KeyValuePair<string, string> prop in mapper.PropertyMap)
                if (!mapper.Ignored.Contains(prop.Key))
                {
                    string col = mapper.PropertyMap.TryGetValue(prop.Key) ?? prop.Key;
                    currentprop = mapper.ColumnsMap.TryGetValue(col.ToLower());

                    if (currentprop.Ignore)
                        continue;

                    if (currentprop.Get != null)
                    {
                        DynamicSchemaColumn? colS = schema == null ? null : schema.TryGetNullable(col.ToLower());

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
            PrepareBatchDelete(typeof(T), mapper, cmd, parameters);
        }

        private void PrepareBatchDelete(Type t, DynamicTypeMap mapper, IDbCommand cmd, Dictionary<IDbDataParameter, DynamicPropertyInvoker> parameters)
        {
            DynamicPropertyInvoker currentprop = null;
            Dictionary<string, DynamicPropertyInvoker> temp = new Dictionary<string, DynamicPropertyInvoker>();
            Dictionary<string, DynamicSchemaColumn> schema = this.GetSchema(t);
            int ord = 0;

            IDynamicDeleteQueryBuilder ib = Delete(t)
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

            foreach (KeyValuePair<string, string> prop in mapper.PropertyMap)
                if (!mapper.Ignored.Contains(prop.Key))
                {
                    string col = mapper.PropertyMap.TryGetValue(prop.Key) ?? prop.Key;
                    currentprop = mapper.ColumnsMap.TryGetValue(col.ToLower());

                    if (currentprop.Ignore)
                        continue;

                    if (currentprop.Get != null)
                    {
                        DynamicSchemaColumn? colS = schema == null ? null : schema.TryGetNullable(col.ToLower());

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

        #region Procedure

        /// <summary>Execute stored procedure.</summary>
        /// <param name="procName">Name of stored procedure to execute.</param>
        /// <returns>Number of affected rows.</returns>
        public virtual int Procedure(string procName)
        {
            return Procedure(procName, (DynamicExpando)null);
        }

        /// <summary>Execute stored procedure.</summary>
        /// <param name="procName">Name of stored procedure to execute.</param>
        /// <param name="args">Arguments (parameters) in form of expando object.</param>
        /// <returns>Number of affected rows.</returns>
        public virtual int Procedure(string procName, params object[] args)
        {
            if ((Options & DynamicDatabaseOptions.SupportStoredProcedures) != DynamicDatabaseOptions.SupportStoredProcedures)
                throw new InvalidOperationException("Database connection desn't support stored procedures.");

            using (IDbConnection con = Open())
            using (IDbCommand cmd = con.CreateCommand())
            {
                return cmd
                    .SetCommand(CommandType.StoredProcedure, procName)
                    .AddParameters(this, args)
                    .ExecuteNonQuery();
            }
        }

        /// <summary>Execute stored procedure.</summary>
        /// <param name="procName">Name of stored procedure to execute.</param>
        /// <param name="args">Arguments (parameters) in form of expando object.</param>
        /// <returns>Number of affected rows.</returns>
        public virtual int Procedure(string procName, DynamicExpando args)
        {
            if ((Options & DynamicDatabaseOptions.SupportStoredProcedures) != DynamicDatabaseOptions.SupportStoredProcedures)
                throw new InvalidOperationException("Database connection desn't support stored procedures.");

            using (IDbConnection con = Open())
            using (IDbCommand cmd = con.CreateCommand())
            {
                return cmd
                    .SetCommand(CommandType.StoredProcedure, procName)
                    .AddParameters(this, args)
                    .ExecuteNonQuery();
            }
        }

        /// <summary>Execute stored procedure.</summary>
        /// <param name="procName">Name of stored procedure to execute.</param>
        /// <param name="args">Arguments (parameters) in form of expando object.</param>
        /// <returns>Number of affected rows.</returns>
        public virtual int Procedure(string procName, ExpandoObject args)
        {
            if ((Options & DynamicDatabaseOptions.SupportStoredProcedures) != DynamicDatabaseOptions.SupportStoredProcedures)
                throw new InvalidOperationException("Database connection desn't support stored procedures.");

            using (IDbConnection con = Open())
            using (IDbCommand cmd = con.CreateCommand())
            {
                return cmd
                    .SetCommand(CommandType.StoredProcedure, procName)
                    .AddParameters(this, args)
                    .ExecuteNonQuery();
            }
        }

        #endregion Procedure

        #region Execute

        /// <summary>Execute non query.</summary>
        /// <param name="sql">SQL query containing numbered parameters in format provided by
        /// <see cref="DynamicDatabase.GetParameterName(object)"/> methods. Also names should be formatted with
        /// <see cref="DynamicDatabase.DecorateName(string)"/> method.</param>
        /// <param name="args">Arguments (parameters).</param>
        /// <returns>Number of affected rows.</returns>
        public virtual int Execute(string sql, params object[] args)
        {
            using (IDbConnection con = Open())
            using (IDbCommand cmd = con.CreateCommand())
            {
                return cmd
                    .SetCommand(sql).AddParameters(this, args)
                    .ExecuteNonQuery();
            }
        }

        /// <summary>Execute non query.</summary>
        /// <param name="builder">Command builder.</param>
        /// <returns>Number of affected rows.</returns>
        public virtual int Execute(IDynamicQueryBuilder builder)
        {
            using (IDbConnection con = Open())
            using (IDbCommand cmd = con.CreateCommand())
            {
                return cmd
                    .SetCommand(builder)
                    .ExecuteNonQuery();
            }
        }

        /// <summary>Execute non query.</summary>
        /// <param name="builders">Command builders.</param>
        /// <returns>Number of affected rows.</returns>
        public virtual int Execute(IDynamicQueryBuilder[] builders)
        {
            int ret = 0;

            using (IDbConnection con = Open())
            {
                using (IDbTransaction trans = con.BeginTransaction())
                {
                    foreach (IDynamicQueryBuilder builder in builders)
                        using (IDbCommand cmd = con.CreateCommand())
                            ret += cmd
                               .SetCommand(builder)
                               .ExecuteNonQuery();

                    trans.Commit();
                }
            }

            return ret;
        }

        #endregion Execute

        #region Scalar

        /// <summary>Returns a single result.</summary>
        /// <param name="sql">SQL query containing numbered parameters in format provided by
        /// <see cref="DynamicDatabase.GetParameterName(object)"/> methods. Also names should be formatted with
        /// <see cref="DynamicDatabase.DecorateName(string)"/> method.</param>
        /// <param name="args">Arguments (parameters).</param>
        /// <returns>Result of a query.</returns>
        public virtual object Scalar(string sql, params object[] args)
        {
            using (IDbConnection con = Open())
            using (IDbCommand cmd = con.CreateCommand())
            {
                return cmd
                    .SetCommand(sql).AddParameters(this, args)
                    .ExecuteScalar();
            }
        }

        /// <summary>Returns a single result.</summary>
        /// <param name="builder">Command builder.</param>
        /// <returns>Result of a query.</returns>
        public virtual object Scalar(IDynamicQueryBuilder builder)
        {
            using (IDbConnection con = Open())
            using (IDbCommand cmd = con.CreateCommand())
            {
                return cmd
                    .SetCommand(builder)
                    .ExecuteScalar();
            }
        }

#if !DYNAMORM_OMMIT_GENERICEXECUTION && !DYNAMORM_OMMIT_TRYPARSE

        /// <summary>Returns a single result.</summary>
        /// <typeparam name="T">What kind of result is expected.</typeparam>
        /// <param name="sql">SQL query containing numbered parameters in format provided by
        /// <see cref="DynamicDatabase.GetParameterName(object)"/> methods. Also names should be formatted with
        /// <see cref="DynamicDatabase.DecorateName(string)"/> method.</param>
        /// <param name="args">Arguments (parameters).</param>
        /// <returns>Result of a query.</returns>
        public virtual T ScalarAs<T>(string sql, params object[] args)
        {
            using (IDbConnection con = Open())
            using (IDbCommand cmd = con.CreateCommand())
            {
                return cmd
                    .SetCommand(sql).AddParameters(this, args)
                    .ExecuteScalarAs<T>();
            }
        }

        /// <summary>Returns a single result.</summary>
        /// <typeparam name="T">What kind of result is expected.</typeparam>
        /// <param name="builder">Command builder.</param>
        /// <param name="defaultValue">Default value.</param>
        /// <returns>Result of a query.</returns>
        public virtual T ScalarAs<T>(IDynamicQueryBuilder builder, T defaultValue = default(T))
        {
            using (IDbConnection con = Open())
            using (IDbCommand cmd = con.CreateCommand())
            {
                return cmd
                    .SetCommand(builder)
                    .ExecuteScalarAs<T>(defaultValue);
            }
        }

#endif

        #endregion Scalar

        #region Query

        /// <summary>Enumerate the reader and yield the result.</summary>
        /// <param name="sql">SQL query containing numbered parameters in format provided by
        /// <see cref="DynamicDatabase.GetParameterName(object)"/> methods. Also names should be formatted with
        /// <see cref="DynamicDatabase.DecorateName(string)"/> method.</param>
        /// <param name="args">Arguments (parameters).</param>
        /// <returns>Enumerator of objects expanded from query.</returns>
        public virtual IEnumerable<dynamic> Query(string sql, params object[] args)
        {
            DynamicCachedReader cache = null;
            using (IDbConnection con = Open())
            using (IDbCommand cmd = con.CreateCommand())
            {
                using (IDataReader rdr = cmd
                    .SetCommand(sql)
                    .AddParameters(this, args)
                    .ExecuteReader())
                    cache = new DynamicCachedReader(rdr);

                while (cache.Read())
                {
                    dynamic val = null;

                    // Work around to avoid yield being in try...catch block:
                    // http://stackoverflow.com/questions/346365/why-cant-yield-return-appear-inside-a-try-block-with-a-catch
                    try
                    {
                        val = cache.RowToDynamic();
                    }
                    catch (ArgumentException argex)
                    {
                        StringBuilder sb = new StringBuilder();
                        cmd.Dump(sb);

                        throw new ArgumentException(string.Format("{0}{1}{2}", argex.Message, Environment.NewLine, sb),
                            argex.InnerException.NullOr(a => a, argex));
                    }

                    yield return val;
                }
            }
        }

        /// <summary>Enumerate the reader and yield the result.</summary>
        /// <param name="builder">Command builder.</param>
        /// <returns>Enumerator of objects expanded from query.</returns>
        public virtual IEnumerable<dynamic> Query(IDynamicQueryBuilder builder)
        {
            DynamicCachedReader cache = null;
            using (IDbConnection con = Open())
            using (IDbCommand cmd = con.CreateCommand())
            {
                using (IDataReader rdr = cmd
                    .SetCommand(builder)
                    .ExecuteReader())
                    cache = new DynamicCachedReader(rdr);

                while (cache.Read())
                {
                    dynamic val = null;

                    // Work around to avoid yield being in try...catch block:
                    // http://stackoverflow.com/questions/346365/why-cant-yield-return-appear-inside-a-try-block-with-a-catch
                    try
                    {
                        val = cache.RowToDynamic();
                    }
                    catch (ArgumentException argex)
                    {
                        StringBuilder sb = new StringBuilder();
                        cmd.Dump(sb);

                        throw new ArgumentException(string.Format("{0}{1}{2}", argex.Message, Environment.NewLine, sb),
                            argex.InnerException.NullOr(a => a, argex));
                    }

                    yield return val;
                }
            }
        }

        #endregion Query

        #region CachedQuery

        /// <summary>Enumerate the reader and yield the result.</summary>
        /// <param name="sql">SQL query containing numbered parameters in format provided by
        /// <see cref="DynamicDatabase.GetParameterName(object)"/> methods. Also names should be formatted with
        /// <see cref="DynamicDatabase.DecorateName(string)"/> method.</param>
        /// <param name="args">Arguments (parameters).</param>
        /// <returns>Enumerator of objects expanded from query.</returns>
        public virtual DynamicCachedReader CachedQuery(string sql, params object[] args)
        {
            using (IDbConnection con = Open())
            using (IDbCommand cmd = con.CreateCommand())
            {
                using (IDataReader rdr = cmd
                    .SetCommand(sql)
                    .AddParameters(this, args)
                    .ExecuteReader())
                    return new DynamicCachedReader(rdr);
            }
        }

        /// <summary>Enumerate the reader and yield the result.</summary>
        /// <param name="builder">Command builder.</param>
        /// <returns>Enumerator of objects expanded from query.</returns>
        public virtual DynamicCachedReader CachedQuery(IDynamicQueryBuilder builder)
        {
            using (IDbConnection con = Open())
            using (IDbCommand cmd = con.CreateCommand())
            {
                using (IDataReader rdr = cmd
                    .SetCommand(builder)
                    .ExecuteReader())
                    return new DynamicCachedReader(rdr);
            }
        }

        #endregion Query

        #region Schema

        /// <summary>Builds query cache if necessary and returns it.</summary>
        /// <param name="builder">The builder containing query to read schema from.</param>
        /// <returns>Query schema.</returns>
        public Dictionary<string, DynamicSchemaColumn> GetQuerySchema(IDynamicSelectQueryBuilder builder)
        {
            using (IDbConnection con = Open())
            using (IDbCommand cmd = con.CreateCommand().SetCommand(builder))
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
            using (IDbConnection con = Open())
            using (IDbCommand cmd = con.CreateCommand().SetCommand(sql, args))
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
                {
                    if (Schema[table.FullName] != null)
                        Schema[table.FullName].Clear();
                    Schema.Remove(table.FullName);
                }
        }

        /// <summary>Clears the all schemas from cache.</summary>
        /// <remarks>Use this method to refresh all table information.</remarks>
        public void ClearSchema()
        {
            lock (SyncLock)
            {
                foreach (KeyValuePair<string, Dictionary<string, DynamicSchemaColumn>> s in Schema)
                    if (s.Value != null)
                        s.Value.Clear();

                Schema.Clear();
            }
        }

        /// <summary>Get schema describing objects from reader.</summary>
        /// <param name="table">Table from which extract column info.</param>
        /// <param name="owner">Owner of table from which extract column info.</param>
        /// <returns>List of <see cref="DynamicSchemaColumn"/> objects .
        /// If your database doesn't get those values in upper case (like most of the databases) you should override this method.</returns>
        protected virtual IList<DynamicSchemaColumn> ReadSchema(string table, string owner)
        {
            using (IDbConnection con = Open())
            using (IDbCommand cmd = con.CreateCommand()
                .SetCommand(string.Format("SELECT * FROM {0}{1} WHERE 1 = 0",
                    !string.IsNullOrEmpty(owner) ? string.Format("{0}.", DecorateName(owner)) : string.Empty,
                    DecorateName(table))))
                return ReadSchema(cmd)
                    .ToList();
        }

        /// <summary>Get schema describing objects from reader.</summary>
        /// <param name="cmd">Command containing query to execute.</param>
        /// <returns>List of <see cref="DynamicSchemaColumn"/> objects .
        /// If your database doesn't get those values in upper case (like most of the databases) you should override this method.</returns>
        public virtual IEnumerable<DynamicSchemaColumn> ReadSchema(IDbCommand cmd)
        {
            DataTable st = null;

            using (IDataReader rdr = cmd.ExecuteReader(CommandBehavior.SchemaOnly | CommandBehavior.KeyInfo))
                st = rdr.GetSchemaTable();

            using (st)
                foreach (DataRow col in st.Rows)
                {
                    dynamic c = col.RowToDynamicUpper();

                    yield return new DynamicSchemaColumn
                    {
                        Name = c.COLUMNNAME,
                        Type = ReadSchemaType(c),
                        IsKey = c.ISKEY ?? false,
                        IsUnique = c.ISUNIQUE ?? false,
                        AllowNull = c.ALLOWNULL ?? false,
                        Size = (int)(c.COLUMNSIZE ?? 0),
                        Precision = (byte)(c.NUMERICPRECISION ?? 0),
                        Scale = (byte)(c.NUMERICSCALE ?? 0)
                    };
                }
        }

        /// <summary>Reads the type of column from the schema.</summary>
        /// <param name="schema">The schema.</param>
        /// <returns>Generic parameter type.</returns>
        protected virtual DbType ReadSchemaType(dynamic schema)
        {
            Type type = (Type)schema.DATATYPE;

            // Small hack for SQL Server Provider
            if (type == typeof(string) && Provider != null && Provider.GetType().Name == "SqlClientFactory")
            {
                var map = schema as IDictionary<string, object>;
                string typeName = (map.TryGetValue("DATATYPENAME") ?? string.Empty).ToString();

                switch (typeName)
                {
                    case "varchar":
                        return DbType.AnsiString;

                    case "nvarchar":
                        return DbType.String;
                }
            }

            return DynamicExtensions.TypeMap.TryGetNullable(type) ?? DbType.String;
        }

        private Dictionary<string, DynamicSchemaColumn> BuildAndCacheSchema(string tableName, DynamicTypeMap mapper, string owner = null)
        {
            Dictionary<string, DynamicSchemaColumn> schema = null;

            if (mapper != null)
            {
                tableName = mapper.Table == null || string.IsNullOrEmpty(mapper.Table.Name) ?
                    mapper.Type.Name : mapper.Table.Name;
                owner = mapper.Table == null || string.IsNullOrEmpty(mapper.Table.Owner) ?
                    null : mapper.Table.Owner;
            }

            bool databaseSchemaSupport = !string.IsNullOrEmpty(tableName) &&
                (Options & DynamicDatabaseOptions.SupportSchema) == DynamicDatabaseOptions.SupportSchema;
            bool mapperSchema = mapper != null && mapper.Table != null && (mapper.Table.Override || !databaseSchemaSupport);

            #region Database schema

            if (databaseSchemaSupport && !Schema.ContainsKey(tableName.ToLower()))
            {
                schema = ReadSchema(tableName, owner)
                    .Where(x => x.Name != null)
                    .DistinctBy(x => x.Name.ToLower())
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
                            AllowNull = DynamicExtensions.CoalesceNullable<bool>(
                                v.Value.Column != null ? v.Value.Column.AllowNull : true,
                                col.HasValue ? col.Value.AllowNull : true).Value,
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
                            AllowNull = DynamicExtensions.CoalesceNullable<bool>(v.Value.Column != null ? v.Value.Column.AllowNull : true, true).Value,
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
            if (DumpCommandDelegate != null)
                DumpCommandDelegate(cmd, cmd.DumpToString());
            else
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
                    List<IDbCommand> tmp = CommandsPool[connection].ToList();
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
                connection = null;
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
                Stack<IDbTransaction> t = TransactionPool.TryGetValue(_tempConn.Connection);

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
                Stack<IDbTransaction> t = TransactionPool.TryGetValue(_tempConn.Connection);

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
                Stack<IDbTransaction> t = TransactionPool.TryGetValue(_tempConn.Connection);

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
            List<DynamicTable> tables = TablesCache.Values.ToList();
            TablesCache.Clear();

            tables.ForEach(t => t.Dispose());
            tables.Clear();
            tables = null;
#endif

            foreach (KeyValuePair<IDbConnection, Stack<IDbTransaction>> con in TransactionPool)
            {
                // Close all commands
                if (CommandsPool.ContainsKey(con.Key))
                {
                    List<IDbCommand> tmp = CommandsPool[con.Key].ToList();
                    tmp.ForEach(cmd => cmd.Dispose());

                    CommandsPool[con.Key].Clear();
                    tmp.Clear();
                    CommandsPool[con.Key] = tmp = null;
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

            while (RemainingBuilders.Count > 0)
                RemainingBuilders.First().Dispose();

            // Clear pools
            lock (SyncLock)
            {
                TransactionPool.Clear();
                CommandsPool.Clear();
                RemainingBuilders.Clear();

                TransactionPool = null;
                CommandsPool = null;
                RemainingBuilders = null;
            }

            ClearSchema();
            if (_proc != null)
                _proc.Dispose();

            IsDisposed = true;
        }

        /// <summary>Gets a value indicating whether this instance is disposed.</summary>
        public bool IsDisposed { get; private set; }

        #endregion IExtendedDisposable Members
    }
}