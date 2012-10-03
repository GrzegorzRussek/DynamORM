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
using System.Data.Common;
using System.Linq;
using System.Text;
using DynamORM.Mapper;

namespace DynamORM
{
    /// <summary>Dynamic database is a class responsible for managing database.</summary>
    public class DynamicDatabase : IDisposable
    {
        #region Internal fields and properties

        private DbProviderFactory _provider;
        private string _connectionString;
        private bool _singleConnection;
        private bool _singleTransaction;
        private string _leftDecorator = "\"";
        private string _rightDecorator = "\"";
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
        internal Dictionary<IDbConnection, List<IDbCommand>> CommandsPool { get; private set; }

        /// <summary>Gets schema columns cache.</summary>
        internal Dictionary<string, Dictionary<string, DynamicSchemaColumn>> Schema { get; private set; }

        /// <summary>Gets tables cache for this database instance.</summary>
        internal Dictionary<string, DynamicTable> TablesCache { get; private set; }

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
        /// <param name="provider">Database proider by name.</param>
        /// <param name="connectionString">Connection string to provided database.</param>
        /// <param name="options">Connection options.</param>
        public DynamicDatabase(string provider, string connectionString, DynamicDatabaseOptions options)
            : this(DbProviderFactories.GetFactory(provider), connectionString, options)
        {
        }

        /// <summary>Initializes a new instance of the <see cref="DynamicDatabase" /> class.</summary>
        /// <param name="provider">Database proider.</param>
        /// <param name="connectionString">Connection string to provided database.</param>
        /// <param name="options">Connection options.</param>
        public DynamicDatabase(DbProviderFactory provider, string connectionString, DynamicDatabaseOptions options)
        {
            _provider = provider;

            InitCommon(connectionString, options);
        }

        /// <summary>Initializes a new instance of the <see cref="DynamicDatabase" /> class.</summary>
        /// <param name="connection">Active database connection.</param>
        /// <param name="options">Connection options. <see cref="DynamicDatabaseOptions.SingleConnection"/> required.</param>
        public DynamicDatabase(IDbConnection connection, DynamicDatabaseOptions options)
        {
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

            TransactionPool = new Dictionary<IDbConnection, Stack<IDbTransaction>>();
            CommandsPool = new Dictionary<IDbConnection, List<IDbCommand>>();
            Schema = new Dictionary<string, Dictionary<string, DynamicSchemaColumn>>();
            TablesCache = new Dictionary<string, DynamicTable>();
        }

        #endregion Properties and Constructors

        #region Table

        /// <summary>Gets dynamic table which is a simple ORM using dynamic objects.</summary>
        /// <param name="action">The action with nstance of <see cref="DynamicTable"/> as parameter.</param>
        /// <param name="table">Table name.</param>
        /// <param name="keys">Override keys in schema.</param>
        public void Table(Action<dynamic> action, string table = "", string[] keys = null)
        {
            using (dynamic t = Table(table, keys))
                action(t);
        }

        /// <summary>Gets dynamic table which is a simple ORM using dynamic objects.</summary>
        /// <typeparam name="T">Type used to determine table name.</typeparam>
        /// <param name="action">The action with nstance of <see cref="DynamicTable"/> as parameter.</param>
        /// <param name="keys">Override keys in schema.</param>
        public void Table<T>(Action<dynamic> action, string[] keys = null)
        {
            using (dynamic t = Table<T>(keys))
                action(t);
        }

        /// <summary>Gets dynamic table which is a simple ORM using dynamic objects.</summary>
        /// <param name="table">Table name.</param>
        /// <param name="keys">Override keys in schema.</param>
        /// <returns>Instance of <see cref="DynamicTable"/>.</returns>
        public dynamic Table(string table = "", string[] keys = null)
        {
            string key = string.Concat(
                table == null ? string.Empty : table,
                keys == null ? string.Empty : string.Join("_|_", keys));

            DynamicTable dt = null;
            lock (SyncLock)
                dt = TablesCache.TryGetValue(key) ??
                    TablesCache.AddAndPassValue(key,
                        new DynamicTable(this, table, keys));

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

        #endregion Table

        #region Schema

        /// <summary>Builds table cache if nessesary and returns it.</summary>
        /// <param name="table">Name of table for which build schema.</param>
        /// <returns>Table chema.</returns>
        public Dictionary<string, DynamicSchemaColumn> GetSchema(string table)
        {
            Dictionary<string, DynamicSchemaColumn> schema = null;

            lock (SyncLock)
                schema = Schema.TryGetValue(table.ToLower()) ??
                    BuildAndCacheSchema(table, null);

            return schema;
        }

        /// <summary>Builds table cache if nessesary and returns it.</summary>
        /// <typeparam name="T">Type of table for which build schema.</typeparam>
        /// <returns>Table schema or null if type was anonymous.</returns>
        public Dictionary<string, DynamicSchemaColumn> GetSchema<T>()
        {
            if (typeof(T).IsAnonymous())
                return null;

            Dictionary<string, DynamicSchemaColumn> schema = null;

            lock (SyncLock)
                schema = Schema.TryGetValue(typeof(T).GetType().FullName) ??
                    BuildAndCacheSchema(null, DynamicMapperCache.GetMapper<T>());

            return schema;
        }

        /// <summary>Builds table cache if nessesary and returns it.</summary>
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

        /// <summary>Get schema describing objects from reader.</summary>
        /// <param name="table">Table from which extract column info.</param>
        /// <returns>List of <see cref="DynamicSchemaColumn"/> objects .
        /// If your database doesn't get those values in upper case (like most of the databases) you should override this method.</returns>
        protected virtual IEnumerable<DynamicSchemaColumn> ReadSchema(string table)
        {
            using (var con = Open())
            using (var cmd = con.CreateCommand())
            {
                using (var rdr = cmd
                    .SetCommand(string.Format("SELECT * FROM {0} WHERE 1 = 0", DecorateName(table)))
                    .ExecuteReader(CommandBehavior.SchemaOnly | CommandBehavior.KeyInfo))
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
        }

        private Dictionary<string, DynamicSchemaColumn> BuildAndCacheSchema(string tableName, DynamicTypeMap mapper)
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
                schema = ReadSchema(tableName)
                    .ToDictionary(k => k.Name.ToLower(), k => k);

                Schema[tableName.ToLower()] = schema;
            }

            #endregion Database schema

            #region Type schema

            if (mapperSchema && !Schema.ContainsKey(mapper.Type.FullName))
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
                                col.HasValue ? col.Value.Type : DbType.String).Value,
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
                            Type = DynamicExtensions.CoalesceNullable<DbType>(v.Value.Column != null ? v.Value.Column.Type : null, DbType.String).Value,
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
        public string LeftDecorator { get { return _leftDecorator; } set { _leftDecorator = value; } }

        /// <summary>Gets or sets right side decorator for database objects.</summary>
        public string RightDecorator { get { return _rightDecorator; } set { _rightDecorator = value; } }

        /// <summary>Gets or sets parameter name format.</summary>
        public string ParameterFormat { get { return _parameterFormat; } set { _parameterFormat = value; } }

        /// <summary>Decorate string representing name of database object.</summary>
        /// <param name="name">Name of database object.</param>
        /// <returns>Decorated name of database object.</returns>
        public string DecorateName(string name)
        {
            return String.Concat(_leftDecorator, name, _rightDecorator);
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

            lock (SyncLock)
            {
                if (!_singleConnection && connection != null && TransactionPool.ContainsKey(connection))
                {
                    // Close all commands
                    if (CommandsPool.ContainsKey(connection))
                    {
                        CommandsPool[connection].ForEach(cmd => cmd.Dispose());
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
                    TransactionPool.Remove(connection);
                    CommandsPool.Remove(connection);

                    // Set stamp
                    _poolStamp = DateTime.Now.Ticks;

                    // Dispose the corpse
                    connection.Dispose();
                }
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
        /// connection untill all transactions are finished.</remarks>
        /// <returns>Returns <see cref="DynamicTransaction"/> representation.</returns>
        public IDbTransaction BeginTransaction()
        {
            _tempConn = Open() as DynamicConnection;

            return _tempConn.BeginTransaction(null, () =>
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

        #region IDisposable Members

        /// <summary>Performs application-defined tasks associated with freeing,
        /// releasing, or resetting unmanaged resources.</summary>
        public void Dispose()
        {
            lock (SyncLock)
            {
                var tables = TablesCache.Values.ToList();
                TablesCache.Clear();

                tables.ForEach(t => t.Dispose());

                foreach (var con in TransactionPool)
                {
                    // Close all commands
                    if (CommandsPool.ContainsKey(con.Key))
                    {
                        CommandsPool[con.Key].ForEach(cmd => cmd.Dispose());
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
                TransactionPool.Clear();
                CommandsPool.Clear();
            }
        }

        #endregion IDisposable Members
    }
}