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
using System.Linq.Expressions;
using System.Text;
using DynamORM.Helpers;
using DynamORM.Helpers.Dynamics;
using DynamORM.Mapper;

namespace DynamORM.Builders.Implementation
{
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
                IsDisposed = false;
            }

            /// <summary>
            /// Initializes a new instance of the <see cref="TableInfo" /> class.
            /// </summary>
            /// <param name="db">The database.</param>
            /// <param name="name">The name of table.</param>
            /// <param name="alias">The table alias.</param>
            /// <param name="owner">The table owner.</param>
            public TableInfo(DynamicDatabase db, string name, string alias = null, string owner = null)
                : this()
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
                : this()
            {
                DynamicTypeMap mapper = DynamicMapperCache.GetMapper(type);

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

            /// <summary>Gets a value indicating whether this instance is disposed.</summary>
            public bool IsDisposed { get; private set; }

            /// <summary>Performs application-defined tasks associated with
            /// freeing, releasing, or resetting unmanaged resources.</summary>
            public virtual void Dispose()
            {
                IsDisposed = true;

                if (Schema != null)
                    Schema.Clear();

                Owner = Name = Alias = null;
                Schema = null;
            }
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
            /// <summary>Initializes a new instance of the
            /// <see cref="Parameter"/> class.</summary>
            public Parameter()
            {
                IsDisposed = false;
            }

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

            /// <summary>Gets a value indicating whether this instance is disposed.</summary>
            public bool IsDisposed { get; private set; }

            /// <summary>Performs application-defined tasks associated with
            /// freeing, releasing, or resetting unmanaged resources.</summary>
            public virtual void Dispose()
            {
                IsDisposed = true;

                Name = null;
                Schema = null;
            }
        }

        #endregion Parameter

        #region Constructor

        /// <summary>
        /// Initializes a new instance of the <see cref="DynamicQueryBuilder"/> class.
        /// </summary>
        /// <param name="db">The database.</param>
        public DynamicQueryBuilder(DynamicDatabase db)
        {
            IsDisposed = false;
            VirtualMode = false;
            Tables = new List<ITableInfo>();
            Parameters = new Dictionary<string, IParameter>();

            WhereCondition = null;
            OpenBracketsCount = 0;

            Database = db;
            if (Database != null)
                Database.AddToCache(this);

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
                using (DynamicParser p = DynamicParser.Parse((Delegate)node))
                    node = p.Result;

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
                else if (node is DynamicParser.Node.Method) return ParseMethod((DynamicParser.Node.Method)node, ref columnSchema, pars);
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
            foreach (KeyValuePair<string, IParameter> parameter in node.Parameters)
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

        protected virtual string ParseMethod(DynamicParser.Node.Method node, ref DynamicSchemaColumn? columnSchema, IDictionary<string, IParameter> pars = null)
        {
            string method = node.Name.ToUpper();
            string parent = node.Host == null ? null : Parse(node.Host, ref columnSchema, pars: pars);
            string item = null;

            // Root-level methods...
            if (node.Host == null)
            {
                switch (method)
                {
                    case "NOT":
                        if (node.Arguments == null || node.Arguments.Length != 1) throw new ArgumentNullException("NOT method expects one argument: " + node.Arguments.Sketch());
                        item = Parse(node.Arguments[0], ref columnSchema, pars: pars);
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

                            object[] arguments = node.Arguments;

                            if (arguments.Length == 1 && (arguments[0] is IEnumerable<object> || arguments[0] is Array) && !(arguments[0] is byte[]))
                            {
                                IEnumerable<object> vals = arguments[0] as IEnumerable<object>;

                                if (vals == null && arguments[0] is Array)
                                    vals = ((Array)arguments[0]).Cast<object>() as IEnumerable<object>;

                                if (vals != null)
                                    arguments = vals.ToArray();
                                else
                                    throw new ArgumentException("BETWEEN method expects single argument to be enumerable of exactly two elements: " + node.Arguments.Sketch());
                            }

                            return string.Format("{0} BETWEEN {1} AND {2}", parent, Parse(arguments[0], ref columnSchema, pars: pars), Parse(arguments[1], ref columnSchema, pars: pars));
                        }

                    case "IN":
                        {
                            if (node.Arguments == null || node.Arguments.Length == 0)
                                throw new ArgumentException("IN method expects at least one argument: " + node.Arguments.Sketch());

                            bool firstParam = true;
                            StringBuilder sbin = new StringBuilder();
                            foreach (object arg in node.Arguments)
                            {
                                if (!firstParam)
                                    sbin.Append(", ");

                                if ((arg is IEnumerable<object> || arg is Array) && !(arg is byte[]))
                                {
                                    IEnumerable<object> vals = arg as IEnumerable<object>;

                                    if (vals == null && arg is Array)
                                        vals = ((Array)arg).Cast<object>() as IEnumerable<object>;

                                    if (vals != null)
                                        foreach (object val in vals)
                                        {
                                            if (!firstParam)
                                                sbin.Append(", ");
                                            else
                                                firstParam = false;

                                            sbin.Append(Parse(val, ref columnSchema, pars: pars));
                                        }
                                    else
                                        sbin.Append(Parse(arg, ref columnSchema, pars: pars));
                                }
                                else
                                    sbin.Append(Parse(arg, ref columnSchema, pars: pars));

                                firstParam = false;
                            }

                            return string.Format("{0} IN({1})", parent, sbin.ToString());
                        }
                    
                    case "NOTIN":
                        {
                            if (node.Arguments == null || node.Arguments.Length == 0)
                                throw new ArgumentException("IN method expects at least one argument: " + node.Arguments.Sketch());

                            bool firstParam = true;
                            StringBuilder sbin = new StringBuilder();
                            foreach (object arg in node.Arguments)
                            {
                                if (!firstParam)
                                    sbin.Append(", ");

                                if ((arg is IEnumerable<object> || arg is Array) && !(arg is byte[]))
                                {
                                    IEnumerable<object> vals = arg as IEnumerable<object>;

                                    if (vals == null && arg is Array)
                                        vals = ((Array)arg).Cast<object>() as IEnumerable<object>;

                                    if (vals != null)
                                        foreach (object val in vals)
                                        {
                                            if (!firstParam)
                                                sbin.Append(", ");
                                            else
                                                firstParam = false;

                                            sbin.Append(Parse(val, ref columnSchema, pars: pars));
                                        }
                                    else
                                        sbin.Append(Parse(arg, ref columnSchema, pars: pars));
                                }
                                else
                                    sbin.Append(Parse(arg, ref columnSchema, pars: pars));

                                firstParam = false;
                            }

                            return string.Format("{0} NOT IN({1})", parent, sbin.ToString());
                        }

                    case "LIKE":
                        if (node.Arguments == null || node.Arguments.Length != 1)
                            throw new ArgumentException("LIKE method expects one argument: " + node.Arguments.Sketch());

                        return string.Format("{0} LIKE {1}", parent, Parse(node.Arguments[0], ref columnSchema, pars: pars));

                    case "NOTLIKE":
                        if (node.Arguments == null || node.Arguments.Length != 1)
                            throw new ArgumentException("NOT LIKE method expects one argument: " + node.Arguments.Sketch());

                        return string.Format("{0} NOT LIKE {1}", parent, Parse(node.Arguments[0], ref columnSchema, pars: pars));

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

                        return string.Format("COUNT({0})", Parse(node.Arguments[0], ref columnSchema, pars: Parameters, nulls: true));
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

                    sb.Append(Parse(argument, ref columnSchema, pars, nulls: true)); // We don't accept raw strings here!!!
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
                Parameter par = new Parameter()
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
            IEnumerable<string> objects = f.Split('.')
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
            string[] parts = colName.Split('.');
            for (int i = 0; i < parts.Length; i++)
                parts[i] = Database.StripName(parts[i]);

            string columnName = parts.Last();

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
            ITableInfo tableInfo = !string.IsNullOrEmpty(tableName) ?
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

        #region IExtendedDisposable

        /// <summary>Gets a value indicating whether this instance is disposed.</summary>
        public bool IsDisposed { get; private set; }

        /// <summary>Performs application-defined tasks associated with
        /// freeing, releasing, or resetting unmanaged resources.</summary>
        public virtual void Dispose()
        {
            IsDisposed = true;

            if (Database != null)
                Database.RemoveFromCache(this);

            if (Parameters != null)
            {
                foreach (KeyValuePair<string, IParameter> p in Parameters)
                    p.Value.Dispose();

                Parameters.Clear();
                Parameters = null;
            }

            if (Tables != null)
            {
                foreach (ITableInfo t in Tables)
                    if (t != null)
                        t.Dispose();

                Tables.Clear();
                Tables = null;
            }

            WhereCondition = null;
            Database = null;
        }

        #endregion IExtendedDisposable
    }
}