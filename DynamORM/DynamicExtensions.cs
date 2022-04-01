/*
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
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Data;
using System.Dynamic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;
using DynamORM.Builders;
using DynamORM.Builders.Implementation;
using DynamORM.Helpers;
using DynamORM.Mapper;

namespace DynamORM
{
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
                foreach (object item in args)
                {
                    if (item is DynamicExpando)
                        cmd.AddParameters(database, (DynamicExpando)item);
                    else if (item is ExpandoObject)
                        cmd.AddParameters(database, (ExpandoObject)item);
                    else
                        cmd.AddParameter(database, item);
                }

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
                foreach (KeyValuePair<string, object> item in args.ToDictionary())
                    cmd.AddParameter(database, item.Key, item.Value);

            return cmd;
        }

        /// <summary>Extension method for adding in a bunch of parameters.</summary>
        /// <param name="cmd">Command to handle.</param>
        /// <param name="database">Database object required to get proper formatting.</param>
        /// <param name="args">Items to add in an expando object.</param>
        /// <returns>Returns edited <see cref="System.Data.IDbCommand"/> instance.</returns>
        public static IDbCommand AddParameters(this IDbCommand cmd, DynamicDatabase database, DynamicExpando args)
        {
            if (args != null && args.Count() > 0)
                foreach (KeyValuePair<string, object> item in args.ToDictionary())
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
            IDbDataParameter p = cmd.CreateParameter();
            p.ParameterName = name;

            if (item == null || item == DBNull.Value)
                p.Value = DBNull.Value;
            else
            {
                Type type = item.GetType();

                p.DbType = TypeMap.TryGetNullable(type) ?? DbType.String;

                if (type == typeof(DynamicExpando) || type == typeof(ExpandoObject))
                    p.Value = ((IDictionary<string, object>)item).Values.FirstOrDefault();
                else
                    p.Value = item;

                if (p.DbType == DbType.String)
                    p.Size = item.ToString().Length > 4000 ? -1 : 4000;
                else if (p.DbType == DbType.AnsiString)
                    p.Size = item.ToString().Length > 8000 ? -1 : 8000;
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
            IDbDataParameter p = cmd.CreateParameter();
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

                if (p.DbType == DbType.AnsiString)
                    p.Size = value.ToString().Length > 8000 ? -1 : 8000;
                else if (p.DbType == DbType.String)
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
            IDbDataParameter p = cmd.CreateParameter();
            p.ParameterName = builder.Database.GetParameterName(cmd.Parameters.Count);

            DynamicSchemaColumn? col = item.Schema ?? (builder as DynamicQueryBuilder)
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

                if (p.DbType == DbType.AnsiString)
                    p.Size = item.Value.ToString().Length > 8000 ? -1 : 8000;
                else if (p.DbType == DbType.String)
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
            param.Value = value ?? DBNull.Value;
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
            param.Value = value ?? DBNull.Value;
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
            param.Value = value ?? DBNull.Value;
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
            try
            {
                ((IDbDataParameter)command.Parameters[parameterName]).Value = value;
            }
            catch (Exception ex)
            {
                throw new ArgumentException(string.Format("Error setting parameter {0} in command {1}", parameterName, command.CommandText ?? string.Empty), ex);
            }

            return command;
        }

        /// <summary>Set <see cref="System.Data.IDataParameter"/> value for <see cref="System.Data.IDbCommand"/> on the fly.</summary>
        /// <param name="command"><see cref="System.Data.IDbCommand"/> to which parameter will be added.</param>
        /// <param name="index">Index of the <see cref="System.Data.IDataParameter"/>.</param>
        /// <param name="value">Value to set on this parameter.</param>
        /// <returns>Returns edited <see cref="System.Data.IDbCommand"/> instance.</returns>
        public static IDbCommand SetParameter(this IDbCommand command, int index, object value)
        {
            try
            {
                ((IDbDataParameter)command.Parameters[index]).Value = value;
            }
            catch (Exception ex)
            {
                throw new ArgumentException(string.Format("Error setting parameter {0} in command {1}", index, command.CommandText ?? string.Empty), ex);
            }

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
                if (handler != null)
                    ret = o.ToString().TryParseDefault<T>(defaultValue, handler);
                else if (o is IConvertible && typeof(T).GetInterfaces().Any(i => i == typeof(IConvertible)))
                    ret = (T)Convert.ChangeType(o, typeof(T));
                else if (typeof(T) == typeof(Guid))
                {
                    if (o.GetType() == typeof(byte[]))
                        ret = (T)(object)new Guid((byte[])o);
                    else
                        ret = (T)(object)Guid.Parse(o.ToString());
                }
                else if (typeof(T) == typeof(string))
                    ret = (T)(o.ToString() as object);
                else if (typeof(T) == typeof(object))
                    ret = (T)o;
                else
                {
                    MethodInfo method = typeof(T).GetMethod(
                        "TryParse",
                        new Type[]
                        {
                            typeof(string),
                            Type.GetType(string.Format("{0}&", typeof(T).FullName))
                        });

                    if (method != null)
                        ret = o.ToString().TryParseDefault<T>(defaultValue, delegate(string v, out T r)
                        {
                            r = defaultValue;
                            return (bool)method.Invoke(null, new object[] { v, r });
                        });
                    else
                        throw new InvalidOperationException("Provided type can't be parsed using generic approach.");
                }
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
                MethodInfo method = typeof(T).GetMethod(
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
                            else if (o is IConvertible && typeof(T).GetInterfaces().Any(i => i == typeof(IConvertible)))
                                ret = (T)Convert.ChangeType(o, typeof(T));
                            else if (typeof(T) == typeof(Guid))
                            {
                                if (o.GetType() == typeof(byte[]))
                                    ret = (T)(object)new Guid((byte[])o);
                                else
                                    ret = (T)(object)Guid.Parse(o.ToString());
                            }
                            else if (typeof(T) == typeof(string))
                                ret = (T)(o.ToString() as object);
                            else if (typeof(T) == typeof(object))
                                ret = (T)o;
                            else if (method != null)
                                ret = o.ToString().TryParseDefault<T>(defaultValue, delegate(string v, out T r)
                                {
                                    r = defaultValue;
                                    return (bool)method.Invoke(null, new object[] { v, r });
                                });
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

        /// <summary>Dump command into string.</summary>
        /// <param name="command">Command to dump.</param>
        /// <returns>Returns dumped <see cref="System.Data.IDbCommand"/> instance in string form.</returns>
        public static string DumpToString(this IDbCommand command)
        {
            StringBuilder sb = new StringBuilder();
            command.Dump(sb);
            return sb.ToString();
        }

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
            try
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
            }
            catch (NullReferenceException)
            {
                writer.WriteLine("Command disposed.");
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

        /// <summary>Turns an <see cref="IDynamicSelectQueryBuilder"/> to a Dynamic list of things with specified type.</summary>
        /// <typeparam name="T">Type of object to map on.</typeparam>
        /// <param name="b">Ready to execute builder.</param>
        /// <returns>List of things.</returns>
        public static List<T> ToList<T>(this IDynamicSelectQueryBuilder b) where T : class
        {
            return b.Execute<T>().ToList();
        }

        /// <summary>Sets the on create temporary parameter action.</summary>
        /// <typeparam name="T">Class implementing <see cref="IDynamicQueryBuilder"/> interface.</typeparam>
        /// <param name="b">The builder on which set delegate.</param>
        /// <param name="a">Action to invoke.</param>
        /// <returns>Returns instance of builder on which action is set.</returns>
        public static T CreateTemporaryParameterAction<T>(this T b, Action<IParameter> a) where T : IDynamicQueryBuilder
        {
            if (a != null)
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
            if (a != null)
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
            IDynamicSelectQueryBuilder sub = b.SubQuery();

            subquery(b, sub);
            try
            {
                (b as DynamicQueryBuilder).ParseCommand(sub as DynamicQueryBuilder, b.Parameters);
            }
            catch (ArgumentException)
            {
                // This might occur if join was made to subquery
            }

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
            IDynamicSelectQueryBuilder sub = b.SubQuery(fn, func);

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
            List<dynamic> result = new List<dynamic>();
            
            while (r.Read())
                result.Add(r.RowToDynamic());

            return result;
        }

        /// <summary>Turns the dictionary into an ExpandoObject.</summary>
        /// <param name="d">Dictionary to convert.</param>
        /// <returns>Converted dictionary.</returns>
        public static dynamic ToDynamic(this IDictionary<string, object> d)
        {
            DynamicExpando result = new DynamicExpando();
            IDictionary<string, object> dict = (IDictionary<string, object>)result;

            foreach (KeyValuePair<string, object> prop in d)
                dict.Add(prop.Key, prop.Value);

            return result;
        }

        /// <summary>Turns the dictionary into an ExpandoObject.</summary>
        /// <param name="d">Dictionary to convert.</param>
        /// <returns>Converted dictionary.</returns>
        public static dynamic ToExpando(this IDictionary<string, object> d)
        {
            ExpandoObject result = new ExpandoObject();
            IDictionary<string, object> dict = (IDictionary<string, object>)result;

            foreach (KeyValuePair<string, object> prop in d)
                dict.Add(prop.Key, prop.Value);

            return result;
        }

        /// <summary>Turns the object into an ExpandoObject.</summary>
        /// <param name="o">Object to convert.</param>
        /// <returns>Converted object.</returns>
        public static dynamic ToDynamic(this object o)
        {
            Type ot = o.GetType();

            if (ot == typeof(DynamicExpando) || ot == typeof(ExpandoObject))
                return o;

            DynamicExpando result = new DynamicExpando();
            IDictionary<string, object> dict = (IDictionary<string, object>)result;

            if (o is IDictionary<string, object>)
                ((IDictionary<string, object>)o)
                    .ToList()
                    .ForEach(kvp => dict.Add(kvp.Key, kvp.Value));
            else if (ot == typeof(NameValueCollection) || ot.IsSubclassOf(typeof(NameValueCollection)))
            {
                NameValueCollection nameValue = (NameValueCollection)o;
                nameValue.Cast<string>()
                    .Select(key => new KeyValuePair<string, object>(key, nameValue[key]))
                    .ToList()
                    .ForEach(i => dict.Add(i));
            }
            else
            {
                DynamicTypeMap mapper = DynamicMapperCache.GetMapper(ot);

                if (mapper != null)
                {
                    foreach (DynamicPropertyInvoker item in mapper.ColumnsMap.Values)
                        if (item.Get != null)
                            dict.Add(item.Name, item.Get(o));
                }
                else
                {
                    PropertyInfo[] props = ot.GetProperties();

                    foreach (PropertyInfo item in props)
                        if (item.CanRead)
                            dict.Add(item.Name, item.GetValue(o, null));
                }
            }

            return result;
        }

        /// <summary>Turns the object into an ExpandoObject.</summary>
        /// <param name="o">Object to convert.</param>
        /// <returns>Converted object.</returns>
        public static dynamic ToExpando(this object o)
        {
            Type ot = o.GetType();

            if (ot == typeof(ExpandoObject) || ot == typeof(DynamicExpando))
                return o;

            ExpandoObject result = new ExpandoObject();
            IDictionary<string, object> dict = (IDictionary<string, object>)result;

            if (o is IDictionary<string, object>)
                ((IDictionary<string, object>)o)
                    .ToList()
                    .ForEach(kvp => dict.Add(kvp.Key, kvp.Value));
            else if (ot == typeof(NameValueCollection) || ot.IsSubclassOf(typeof(NameValueCollection)))
            {
                NameValueCollection nameValue = (NameValueCollection)o;
                nameValue.Cast<string>()
                    .Select(key => new KeyValuePair<string, object>(key, nameValue[key]))
                    .ToList()
                    .ForEach(i => dict.Add(i));
            }
            else
            {
                DynamicTypeMap mapper = DynamicMapperCache.GetMapper(ot);

                if (mapper != null)
                {
                    foreach (DynamicPropertyInvoker item in mapper.ColumnsMap.Values)
                        if (item.Get != null)
                            dict.Add(item.Name, item.Get(o));
                }
                else
                {
                    PropertyInfo[] props = ot.GetProperties();

                    foreach (PropertyInfo item in props)
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
            dynamic e = new DynamicExpando();

            int len = r.Table.Columns.Count;

            for (int i = 0; i < len; i++)
                ((IDictionary<string, object>)e).Add(r.Table.Columns[i].ColumnName, r.IsNull(i) ? null : r[i]);

            return e;
        }

        /// <summary>Convert data row row into dynamic object.</summary>
        /// <param name="r">DataRow from which read.</param>
        /// <returns>Generated dynamic object.</returns>
        public static dynamic RowToExpando(this DataRow r)
        {
            dynamic e = new ExpandoObject();

            int len = r.Table.Columns.Count;

            for (int i = 0; i < len; i++)
                ((IDictionary<string, object>)e).Add(r.Table.Columns[i].ColumnName, r.IsNull(i) ? null : r[i]);

            return e;
        }

        /// <summary>Convert data row row into dynamic object (upper case key).</summary>
        /// <param name="r">DataRow from which read.</param>
        /// <returns>Generated dynamic object.</returns>
        public static dynamic RowToDynamicUpper(this DataRow r)
        {
            dynamic e = new DynamicExpando();

            int len = r.Table.Columns.Count;

            for (int i = 0; i < len; i++)
                ((IDictionary<string, object>)e).Add(r.Table.Columns[i].ColumnName.ToUpper(), r.IsNull(i) ? null : r[i]);

            return e;
        }

        /// <summary>Convert data row row into dynamic object (upper case key).</summary>
        /// <param name="r">DataRow from which read.</param>
        /// <returns>Generated dynamic object.</returns>
        public static dynamic RowToExpandoUpper(this DataRow r)
        {
            // ERROR: Memory leak
            dynamic e = new ExpandoObject();

            int len = r.Table.Columns.Count;

            for (int i = 0; i < len; i++)
                ((IDictionary<string, object>)e).Add(r.Table.Columns[i].ColumnName.ToUpper(), r.IsNull(i) ? null : r[i]);

            return e;
        }

        internal static Dictionary<string, object> RowToDynamicUpperDict(this DataRow r)
        {
            dynamic e = new Dictionary<string, object>();

            int len = r.Table.Columns.Count;

            for (int i = 0; i < len; i++)
                e.Add(r.Table.Columns[i].ColumnName.ToUpper(), r.IsNull(i) ? null : r[i]);

            return e;
        }

        /// <summary>Convert reader row into dynamic object.</summary>
        /// <param name="r">Reader from which read.</param>
        /// <returns>Generated dynamic object.</returns>
        public static dynamic RowToDynamic(this IDataReader r)
        {
            dynamic e = new DynamicExpando();
            IDictionary<string, object> d = e as IDictionary<string, object>;

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

        /// <summary>Convert reader row into dynamic object.</summary>
        /// <param name="r">Reader from which read.</param>
        /// <returns>Generated dynamic object.</returns>
        public static dynamic RowToExpando(this IDataReader r)
        {
            dynamic e = new ExpandoObject();
            IDictionary<string, object> d = e as IDictionary<string, object>;

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
        public static IDictionary<string, object> ToDictionary(this DynamicExpando o)
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
            return type.IsGenericType && type.GetInterfaces().Any(t => t.IsGenericType && t.GetGenericTypeDefinition() == typeof(IEnumerable<>));
        }

        /// <summary>Check if type implements IEnumerable&lt;&gt; interface.</summary>
        /// <param name="type">Type to check.</param>
        /// <returns>Returns <c>true</c> if it does.</returns>
        public static bool IsNullableType(this Type type)
        {
            Type generic = type.IsGenericType ? type.GetGenericTypeDefinition() : null;

            if (generic != null && generic.Equals(typeof(Nullable<>)) && (type.IsClass || type.IsValueType || type.IsEnum))
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
            if (dbt == DbType.String)
                return typeof(string);

            foreach (KeyValuePair<Type, DbType> tdbt in TypeMap)
                if (tdbt.Value == dbt)
                    return tdbt.Key;

            return typeof(object);
        }

        /// <summary>Determines whether the specified value is has only ASCII chars.</summary>
        /// <param name="value">The value to check.</param>
        /// <returns>Returns <c>true</c> if the specified value has only ASCII cars; otherwise, <c>false</c>.</returns>
        public static bool IsASCII(this string value)
        {
            return Encoding.UTF8.GetByteCount(value) == value.Length;
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

            if (key != null && dict != null && dict.TryGetValue(key, out val))
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

        internal static IEnumerable<dynamic> EnumerateReader(this IDataReader r)
        {
            while (r.Read())
                yield return r.RowToDynamic();
        }

        /// <summary>Creates cached reader object from non cached reader.</summary>
        /// <param name="r">The reader to cache.</param>
        /// <param name="offset">The offset row.</param>
        /// <param name="limit">The limit to number of tows. -1 is no limit.</param>
        /// <param name="progress">The progress delegate.</param>
        /// <returns>Returns new instance of cached reader or current instance of a reader.</returns>
        public static IDataReader CachedReader(this IDataReader r, int offset = 0, int limit = -1, Func<DynamicCachedReader, int, bool> progress = null)
        {
            if (r is DynamicCachedReader)
                return r;

            return new DynamicCachedReader(r, offset, limit, progress);
        }

        #endregion IDataReader extensions

        #region Mapper extensions

        /// <summary>MapEnumerable object enumerator into specified type.</summary>
        /// <typeparam name="T">Type to which columnMap results.</typeparam>
        /// <param name="enumerable">Source enumerator.</param>
        /// <returns>Enumerator of specified type.</returns>
        public static IEnumerable<T> MapEnumerable<T>(this IEnumerable<object> enumerable)
        {
            DynamicTypeMap mapper = DynamicMapperCache.GetMapper<T>();

            if (mapper == null)
                throw new InvalidOperationException("Type can't be mapped for unknown reason.");

            foreach (object item in enumerable)
                yield return (T)mapper.Create(item);
        }

        /// <summary>MapEnumerable object item into specified type.</summary>
        /// <typeparam name="T">Type to which columnMap results.</typeparam>
        /// <param name="item">Source object.</param>
        /// <returns>Item of specified type.</returns>
        public static T Map<T>(this object item)
        {
            DynamicTypeMap mapper = DynamicMapperCache.GetMapper<T>();

            if (mapper == null)
                throw new InvalidOperationException("Type can't be mapped for unknown reason.");

            return (T)mapper.Create(item);
        }

        /// <summary>MapEnumerable object enumerator into specified type using property names.</summary>
        /// <typeparam name="T">Type to which columnMap results.</typeparam>
        /// <param name="enumerable">Source enumerator.</param>
        /// <returns>Enumerator of specified type.</returns>
        public static IEnumerable<T> MapEnumerableByProperty<T>(this IEnumerable<object> enumerable)
        {
            DynamicTypeMap mapper = DynamicMapperCache.GetMapper<T>();

            if (mapper == null)
                throw new InvalidOperationException("Type can't be mapped for unknown reason.");

            foreach (object item in enumerable)
                yield return (T)mapper.CreateByProperty(item);
        }

        /// <summary>MapEnumerable object item into specified type using property names.</summary>
        /// <typeparam name="T">Type to which columnMap results.</typeparam>
        /// <param name="item">Source object.</param>
        /// <returns>Item of specified type.</returns>
        public static T MapByProperty<T>(this object item)
        {
            DynamicTypeMap mapper = DynamicMapperCache.GetMapper<T>();

            if (mapper == null)
                throw new InvalidOperationException("Type can't be mapped for unknown reason.");

            return (T)mapper.CreateByProperty(item);
        }

        /// <summary>Fill object of specified type with data from source object.</summary>
        /// <typeparam name="T">Type to which columnMap results.</typeparam>
        /// <param name="item">Item to which columnMap data.</param>
        /// <param name="source">Item from which extract data.</param>
        /// <returns>Filled item.</returns>
        public static T Fill<T>(this T item, object source)
        {
            DynamicTypeMap mapper = DynamicMapperCache.GetMapper<T>();

            if (mapper == null)
                throw new InvalidOperationException("Type can't be mapped for unknown reason.");

            mapper.Map(source, item);

            return item;
        }

        /// <summary>Fill object of specified type with data from source object using property names.</summary>
        /// <typeparam name="T">Type to which columnMap results.</typeparam>
        /// <param name="item">Item to which columnMap data.</param>
        /// <param name="source">Item from which extract data.</param>
        /// <returns>Filled item.</returns>
        public static T FillByProperty<T>(this T item, object source)
        {
            DynamicTypeMap mapper = DynamicMapperCache.GetMapper<T>();

            if (mapper == null)
                throw new InvalidOperationException("Type can't be mapped for unknown reason.");

            mapper.MapByProperty(source, item);

            return item;
        }

        /// <summary>MapEnumerable object enumerator into specified type.</summary>
        /// <param name="enumerable">Source enumerator.</param>
        /// <param name="type">Type to which columnMap results.</param>
        /// <returns>Enumerator of specified type.</returns>
        public static IEnumerable<object> MapEnumerable(this IEnumerable<object> enumerable, Type type)
        {
            DynamicTypeMap mapper = DynamicMapperCache.GetMapper(type);

            if (mapper == null)
                throw new InvalidOperationException("Type can't be mapped for unknown reason.");

            foreach (object item in enumerable)
                yield return mapper.Create(item);
        }

        /// <summary>MapEnumerable object item into specified type.</summary>
        /// <param name="item">Source object.</param>
        /// <param name="type">Type to which columnMap results.</param>
        /// <returns>Item of specified type.</returns>
        public static object Map(this object item, Type type)
        {
            DynamicTypeMap mapper = DynamicMapperCache.GetMapper(type);

            if (mapper == null)
                throw new InvalidOperationException("Type can't be mapped for unknown reason.");

            return mapper.Create(item);
        }

        /// <summary>MapEnumerable object enumerator into specified type  using property names.</summary>
        /// <param name="enumerable">Source enumerator.</param>
        /// <param name="type">Type to which columnMap results.</param>
        /// <returns>Enumerator of specified type.</returns>
        public static IEnumerable<object> MapEnumerableByProperty(this IEnumerable<object> enumerable, Type type)
        {
            DynamicTypeMap mapper = DynamicMapperCache.GetMapper(type);

            if (mapper == null)
                throw new InvalidOperationException("Type can't be mapped for unknown reason.");

            foreach (object item in enumerable)
                yield return mapper.CreateByProperty(item);
        }

        /// <summary>MapEnumerable object item into specified type  using property names.</summary>
        /// <param name="item">Source object.</param>
        /// <param name="type">Type to which columnMap results.</param>
        /// <returns>Item of specified type.</returns>
        public static object MapByProperty(this object item, Type type)
        {
            DynamicTypeMap mapper = DynamicMapperCache.GetMapper(type);

            if (mapper == null)
                throw new InvalidOperationException("Type can't be mapped for unknown reason.");

            return mapper.CreateByProperty(item);
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

        /// <summary>Delegate from try parse function of a type.</summary>
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
}