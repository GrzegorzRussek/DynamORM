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
using System.Dynamic;
using System.Linq;
using System.Text;
using DynamORM.Builders;
using DynamORM.Builders.Extensions;
using DynamORM.Builders.Implementation;
using DynamORM.Helpers;
using DynamORM.Helpers.Dynamics;
using DynamORM.Mapper;

namespace DynamORM
{
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
                .SetCommand(sql, args)
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
                    var name = info.ArgumentNames[i].ToLower();

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
                            builder.Insert(name, args[i]);
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
                    var name = info.ArgumentNames[i].ToLower();

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
                            builder.Update(name, args[i]);
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
                    var name = info.ArgumentNames[i].ToLower();

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
                            builder.Where(name, args[i]);
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
                    var name = info.ArgumentNames[i].ToLower();

                    // TODO: Make this nicer
                    switch (name)
                    {
                        case "order":
                            if (args[i] is string)
                                builder.OrderBy(((string)args[i]).Split(','));
                            else if (args[i] is string[])
                                builder.OrderBy(args[i] as string);
                            else if (args[i] is DynamicColumn[])
                                builder.OrderBy((DynamicColumn[])args[i]);
                            else if (args[i] is DynamicColumn)
                                builder.OrderBy((DynamicColumn)args[i]);
                            else goto default;
                            break;

                        case "group":
                            if (args[i] is string)
                                builder.GroupBy(((string)args[i]).Split(','));
                            else if (args[i] is string[])
                                builder.GroupBy(args[i] as string);
                            else if (args[i] is DynamicColumn[])
                                builder.GroupBy((DynamicColumn[])args[i]);
                            else if (args[i] is DynamicColumn)
                                builder.GroupBy((DynamicColumn)args[i]);
                            else goto default;
                            break;

                        case "columns":
                            {
                                var agregate = (op == "Sum" || op == "Max" || op == "Min" || op == "Avg" || op == "Count") ?
                                    op.ToUpper() : null;

                                if (args[i] is string || args[i] is string[])
                                    builder.Select((args[i] as String).NullOr(s => s.Split(','), args[i] as String[])
                                        .Select(c =>
                                        {
                                            var col = DynamicColumn.ParseSelectColumn(c);
                                            if (string.IsNullOrEmpty(col.Aggregate))
                                                col.Aggregate = agregate;

                                            return col;
                                        }).ToArray());
                                else if (args[i] is DynamicColumn || args[i] is DynamicColumn[])
                                    builder.Select((args[i] as DynamicColumn).NullOr(c => new DynamicColumn[] { c }, args[i] as DynamicColumn[])
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
                            builder.Where(name, args[i]);
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
}