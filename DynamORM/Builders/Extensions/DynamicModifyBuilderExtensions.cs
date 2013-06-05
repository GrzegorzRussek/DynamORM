/*
 * DynamORM - Dynamic Object-Relational Mapping library.
 * Copyright (c) 2012, Grzegorz Russek (grzegorz.russek@gmail.com)
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
using DynamORM.Mapper;

namespace DynamORM.Builders.Extensions
{
    internal static class DynamicModifyBuilderExtensions
    {
        internal static T Table<T>(this T builder, string tableName, Dictionary<string, DynamicSchemaColumn> schema = null) where T : DynamicModifyBuilder
        {
            var tuple = tableName.Validated("Table Name").SplitSomethingAndAlias();

            if (!string.IsNullOrEmpty(tuple.Item2))
                throw new ArgumentException(string.Format("Can not use aliases in INSERT steatement. ({0})", tableName), "tableName");

            var parts = tuple.Item1.Split('.');

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
            var mapper = DynamicMapperCache.GetMapper(type);

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