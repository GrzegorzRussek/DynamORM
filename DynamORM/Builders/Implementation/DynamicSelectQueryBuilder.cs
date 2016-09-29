/*
 * DynamORM - Dynamic Object-Relational Mapping library.
 * Copyright (c) 2012-2015, Grzegorz Russek (grzegorz.russek@gmail.com)
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
using System.Data;
using System.Linq;
using System.Text;
using DynamORM.Builders.Extensions;
using DynamORM.Helpers;
using DynamORM.Helpers.Dynamics;
using DynamORM.Mapper;

namespace DynamORM.Builders.Implementation
{
    /// <summary>Implementation of dynamic select query builder.</summary>
    internal class DynamicSelectQueryBuilder : DynamicQueryBuilder, IDynamicSelectQueryBuilder, DynamicQueryBuilder.IQueryWithWhere
    {
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
            bool lused = false;
            bool oused = false;

            StringBuilder sb = new StringBuilder("SELECT");
            if (_distinct) sb.AppendFormat(" DISTINCT");

            if (_limit.HasValue)
            {
                if ((Database.Options & DynamicDatabaseOptions.SupportTop) == DynamicDatabaseOptions.SupportTop)
                {
                    sb.AppendFormat(" TOP {0}", _limit);
                    lused = true;
                }
                else if ((Database.Options & DynamicDatabaseOptions.SupportFirstSkip) == DynamicDatabaseOptions.SupportFirstSkip)
                {
                    sb.AppendFormat(" FIRST {0}", _limit);
                    lused = true;
                }
            }

            if (_offset.HasValue && (Database.Options & DynamicDatabaseOptions.SupportFirstSkip) == DynamicDatabaseOptions.SupportFirstSkip)
            {
                sb.AppendFormat(" SKIP {0}", _offset);
                oused = true;
            }

            if (_select != null) sb.AppendFormat(" {0}", _select); else sb.Append(" *");
            if (_from != null) sb.AppendFormat(" FROM {0}", _from);
            if (_join != null) sb.AppendFormat(" {0}", _join);
            if (WhereCondition != null) sb.AppendFormat(" WHERE {0}", WhereCondition);
            if (_groupby != null) sb.AppendFormat(" GROUP BY {0}", _groupby);
            if (_orderby != null) sb.AppendFormat(" ORDER BY {0}", _orderby);
            if (_limit.HasValue && !lused && (Database.Options & DynamicDatabaseOptions.SupportLimitOffset) == DynamicDatabaseOptions.SupportLimitOffset)
                sb.AppendFormat(" LIMIT {0}", _limit);
            if (_offset.HasValue && !oused && (Database.Options & DynamicDatabaseOptions.SupportLimitOffset) == DynamicDatabaseOptions.SupportLimitOffset)
                sb.AppendFormat(" OFFSET {0}", _offset);

            return sb.ToString();
        }

        #region Execution

        /// <summary>Execute this builder.</summary>
        /// <returns>Enumerator of objects expanded from query.</returns>
        public virtual IEnumerable<dynamic> Execute()
        {
            DynamicCachedReader cache = null;
            using (IDbConnection con = Database.Open())
            using (IDbCommand cmd = con.CreateCommand())
            {
                using (IDataReader rdr = cmd
                    .SetCommand(this)
                    .ExecuteReader())
                    cache = new DynamicCachedReader(rdr);

                while (cache.Read())
                {
                    dynamic val = null;

                    // Work around to avoid yield being in try...catchblock:
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

        /// <summary>Execute this builder and map to given type.</summary>
        /// <typeparam name="T">Type of object to map on.</typeparam>
        /// <returns>Enumerator of objects expanded from query.</returns>
        public virtual IEnumerable<T> Execute<T>() where T : class
        {
            DynamicCachedReader cache = null;
            DynamicTypeMap mapper = DynamicMapperCache.GetMapper<T>();

            if (mapper == null)
                throw new InvalidOperationException("Type can't be mapped for unknown reason.");

            using (IDbConnection con = Database.Open())
            using (IDbCommand cmd = con.CreateCommand())
            {
                using (IDataReader rdr = cmd
                    .SetCommand(this)
                    .ExecuteReader())
                    cache = new DynamicCachedReader(rdr);

                while (cache.Read())
                {
                    dynamic val = null;

                    // Work around to avoid yield being in try...catchblock:
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

                    yield return mapper.Create(val) as T;
                }
            }
        }

        /// <summary>Execute this builder as a data reader.</summary>
        /// <param name="reader">Action containing reader.</param>
        public virtual void ExecuteDataReader(Action<IDataReader> reader)
        {
            using (IDbConnection con = Database.Open())
            using (IDbCommand cmd = con.CreateCommand())
            using (IDataReader rdr = cmd
                .SetCommand(this)
                .ExecuteReader())
                reader(rdr);
        }

        /// <summary>Execute this builder as a data reader, but 
        /// first makes a full reader copy in memory.</summary>
        /// <param name="reader">Action containing reader.</param>
        public virtual void ExecuteCachedDataReader(Action<IDataReader> reader)
        {
            DynamicCachedReader cache = null;

            using (IDbConnection con = Database.Open())
            using (IDbCommand cmd = con.CreateCommand())
            using (IDataReader rdr = cmd
                .SetCommand(this)
                .ExecuteReader())
                cache = new DynamicCachedReader(rdr);

            reader(cache);
        }

        /// <summary>Returns a single result.</summary>
        /// <returns>Result of a query.</returns>
        public virtual object Scalar()
        {
            using (IDbConnection con = Database.Open())
            using (IDbCommand cmd = con.CreateCommand())
            {
                return cmd
                    .SetCommand(this)
                    .ExecuteScalar();
            }
        }

#if !DYNAMORM_OMMIT_GENERICEXECUTION && !DYNAMORM_OMMIT_TRYPARSE

        /// <summary>Returns a single result.</summary>
        /// <typeparam name="T">Type to parse to.</typeparam>
        /// <param name="defaultValue">Default value.</param>
        /// <returns>Result of a query.</returns>
        public virtual T ScalarAs<T>(T defaultValue = default(T))
        {
            using (IDbConnection con = Database.Open())
            using (IDbCommand cmd = con.CreateCommand())
            {
                return cmd
                    .SetCommand(this)
                    .ExecuteScalarAs<T>(defaultValue);
            }
        }

#endif

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
            foreach (Func<dynamic, object> f in func)
                index = FromFunc(index, f);

            return this;
        }

        private int FromFunc(int index, Func<dynamic, object> f)
        {
            if (f == null)
                throw new ArgumentNullException("Array of functions cannot be or contain null.");

            index++;
            ITableInfo tableInfo = null;
            using (DynamicParser parser = DynamicParser.Parse(f))
            {
                object result = parser.Result;

                // If the expression result is string.
                if (result is string)
                {
                    string node = (string)result;
                    Tuple<string, string> tuple = node.SplitSomethingAndAlias();
                    string[] parts = tuple.Item1.Split('.');
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

                    DynamicTypeMap mapper = DynamicMapperCache.GetMapper(type);

                    if (mapper == null)
                        throw new InvalidOperationException(string.Format("Cant assign unmapable type as a table ({0}). Parsing {1}", type.FullName, result));

                    tableInfo = new TableInfo(Database, type);
                }
                else if (result is DynamicParser.Node)
                {
                    // Or if it resolves to a dynamic node
                    DynamicParser.Node node = (DynamicParser.Node)result;

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
                                DynamicParser.Node.Invoke invoke = (DynamicParser.Node.Invoke)node;
                                if (invoke.Arguments.Length == 1 && invoke.Arguments[0] is Type)
                                {
                                    type = (Type)invoke.Arguments[0];
                                    if (type.IsAnonymous())
                                        throw new InvalidOperationException(string.Format("Cant assign anonymous type as a table ({0}). Parsing {1}", type.FullName, result));

                                    DynamicTypeMap mapper = DynamicMapperCache.GetMapper(type);

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

            foreach (Func<dynamic, object> f in func)
            {
                index++;
                ITableInfo tableInfo = null;

                if (f == null)
                    throw new ArgumentNullException(string.Format("Specification #{0} cannot be null.", index));

                using (DynamicParser parser = DynamicParser.Parse(f))
                {
                    object result = parser.Result;
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
                        string node = (string)result;

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

                        Tuple<string, string> tuple = main.SplitSomethingAndAlias(); // In this case we split on the remaining 'main'
                        string[] parts = tuple.Item1.Split('.');
                        main = Database.StripName(parts.Last()).Validated("Table");
                        owner = parts.Length == 2 ? Database.StripName(parts.First()).Validated("Owner", canbeNull: true) : null;
                        alias = tuple.Item2.Validated("Alias", canbeNull: true);
                    }
                    else if (result is DynamicParser.Node)
                    {
                        // Or if it resolves to a dynamic node...
                        DynamicParser.Node node = (DynamicParser.Node)result;
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
                                    string proposedType = args.FirstOrDefault(a => a is string) as string;
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
                                    DynamicParser.Node.Invoke invoke = (DynamicParser.Node.Invoke)node;
                                    if (invoke.Arguments.Length == 1 && invoke.Arguments[0] is Type)
                                    {
                                        tableType = (Type)invoke.Arguments[0];
                                        DynamicTypeMap mapper = DynamicMapperCache.GetMapper(tableType);

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
                foreach (Func<dynamic, object> f in func)
                    index = SelectFunc(index, f);

            return this;
        }

        private int SelectFunc(int index, Func<dynamic, object> f)
        {
            index++;
            if (f == null)
                throw new ArgumentNullException(string.Format("Specification #{0} cannot be null.", index));

            using (DynamicParser parser = DynamicParser.Parse(f))
            {
                object result = parser.Result;
                if (result == null)
                    throw new ArgumentException(string.Format("Specification #{0} resolves to null.", index));

                string main = null;
                string alias = null;
                bool all = false;
                bool anon = false;

                // If the expression resolves to a string...
                if (result is string)
                {
                    string node = (string)result;
                    Tuple<string, string> tuple = node.SplitSomethingAndAlias();
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

                    foreach (KeyValuePair<string, object> prop in result.ToDictionary())
                    {
                        if (prop.Value is string)
                        {
                            string node = (string)prop.Value;
                            Tuple<string, string> tuple = node.SplitSomethingAndAlias();
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
            foreach (DynamicColumn col in columns)
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
            DynamicColumn[] cols = new DynamicColumn[columns.Length];
            for (int i = 0; i < columns.Length; i++)
                cols[i] = DynamicColumn.ParseSelectColumn(columns[i]);

            return SelectColumn(cols);
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
                for (int i = 0; i < func.Length; i++)
                {
                    Func<dynamic, object> f = func[i];
                    index = GroupByFunc(index, f);
                }

            return this;
        }

        private int GroupByFunc(int index, Func<dynamic, object> f)
        {
            index++;
            if (f == null)
                throw new ArgumentNullException(string.Format("Specification #{0} cannot be null.", index));
            using (DynamicParser parser = DynamicParser.Parse(f))
            {
                object result = parser.Result;
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
            for (int i = 0; i < columns.Length; i++)
            {
                DynamicColumn col = columns[i];
                GroupBy(x => col.ToSQLGroupByColumn(Database));
            }

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
                for (int i = 0; i < func.Length; i++)
                {
                    Func<dynamic, object> f = func[i];
                    index = OrderByFunc(index, f);
                }

            return this;
        }

        private int OrderByFunc(int index, Func<dynamic, object> f)
        {
            index++;
            if (f == null)
                throw new ArgumentNullException(string.Format("Specification #{0} cannot be null.", index));

            using (DynamicParser parser = DynamicParser.Parse(f))
            {
                object result = parser.Result;
                if (result == null) throw new ArgumentException(string.Format("Specification #{0} resolves to null.", index));

                string main = null;
                bool ascending = true;

                if (result is int)
                    main = result.ToString();
                else if (result is string)
                {
                    string[] parts = ((string)result).Split(' ');
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
                        DynamicParser.Node.Method node = (DynamicParser.Node.Method)result;
                        string name = node.Name.ToUpper();
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
            for (int i = 0; i < columns.Length; i++)
            {
                DynamicColumn col = columns[i];
                OrderBy(x => col.ToSQLOrderByColumn(Database));
            }

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
            return Limit(top);
        }

        /// <summary>Set top if database support it.</summary>
        /// <param name="limit">How many objects select.</param>
        /// <returns>Builder instance.</returns>
        public virtual IDynamicSelectQueryBuilder Limit(int? limit)
        {
            if ((Database.Options & DynamicDatabaseOptions.SupportLimitOffset) != DynamicDatabaseOptions.SupportLimitOffset &&
                (Database.Options & DynamicDatabaseOptions.SupportFirstSkip) != DynamicDatabaseOptions.SupportFirstSkip &&
                (Database.Options & DynamicDatabaseOptions.SupportTop) != DynamicDatabaseOptions.SupportTop)
                throw new NotSupportedException("Database doesn't support LIMIT clause.");

            _limit = limit;
            return this;
        }

        /// <summary>Set top if database support it.</summary>
        /// <param name="offset">How many objects skip selecting.</param>
        /// <returns>Builder instance.</returns>
        public virtual IDynamicSelectQueryBuilder Offset(int? offset)
        {
            if ((Database.Options & DynamicDatabaseOptions.SupportLimitOffset) != DynamicDatabaseOptions.SupportLimitOffset &&
                (Database.Options & DynamicDatabaseOptions.SupportFirstSkip) != DynamicDatabaseOptions.SupportFirstSkip)
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

            DynamicParser.Node node = (DynamicParser.Node)result;
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
                        string table = ((DynamicParser.Node.GetMember)node).Name;
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
                        string table = ((DynamicParser.Node.Argument)node.Host).Name;

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

        #region IExtendedDisposable

        /// <summary>Performs application-defined tasks associated with
        /// freeing, releasing, or resetting unmanaged resources.</summary>
        public override void Dispose()
        {
            base.Dispose();

            _select = _from = _join = _groupby = _orderby = null;
        }

        #endregion IExtendedDisposable
    }
}