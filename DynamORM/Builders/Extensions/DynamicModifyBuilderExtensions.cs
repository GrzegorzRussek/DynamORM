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
using System.Linq;
using DynamORM.Builders.Implementation;
using DynamORM.Helpers;
using DynamORM.Helpers.Dynamics;
using DynamORM.Mapper;

namespace DynamORM.Builders.Extensions
{
    internal static class DynamicModifyBuilderExtensions
    {
        internal static T Table<T>(this T builder, Func<dynamic, object> func) where T : DynamicModifyBuilder
        {
            if (func == null)
                throw new ArgumentNullException("Function cannot be null.");

            using (DynamicParser parser = DynamicParser.Parse(func))
            {
                object result = parser.Result;

                // If the expression result is string.
                if (result is string)
                    return builder.Table((string)result);
                else if (result is Type)
                    return builder.Table((Type)result);
                else if (result is DynamicParser.Node)
                {
                    // Or if it resolves to a dynamic node
                    DynamicParser.Node node = (DynamicParser.Node)result;

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
                                DynamicParser.Node.Invoke invoke = (DynamicParser.Node.Invoke)node;

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
            Tuple<string, string> tuple = tableName.Validated("Table Name").SplitSomethingAndAlias();

            if (!string.IsNullOrEmpty(tuple.Item2))
                throw new ArgumentException(string.Format("Can not use aliases in INSERT steatement. ({0})", tableName), "tableName");

            string[] parts = tuple.Item1.Split('.');

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

            DynamicTypeMap mapper = DynamicMapperCache.GetMapper(type);

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
}