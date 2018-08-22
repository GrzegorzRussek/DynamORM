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
using System.Data;

namespace DynamORM.Builders
{
    /// <summary>Dynamic select query builder interface.</summary>
    /// <remarks>This interface it publicly available. Implementation should be hidden.</remarks>
    public interface IDynamicSelectQueryBuilder : IDynamicQueryBuilder ////, IEnumerable<object>
    {
        /// <summary>Execute this builder.</summary>
        /// <returns>Enumerator of objects expanded from query.</returns>
        IEnumerable<dynamic> Execute();

        /// <summary>Execute this builder and map to given type.</summary>
        /// <typeparam name="T">Type of object to map on.</typeparam>
        /// <returns>Enumerator of objects expanded from query.</returns>
        IEnumerable<T> Execute<T>() where T : class;

        /// <summary>Execute this builder as a data reader.</summary>
        /// <param name="reader">Action containing reader.</param>
        void ExecuteDataReader(Action<IDataReader> reader);

        /// <summary>Returns a single result.</summary>
        /// <returns>Result of a query.</returns>
        object Scalar();

#if !DYNAMORM_OMMIT_GENERICEXECUTION && !DYNAMORM_OMMIT_TRYPARSE

        /// <summary>Returns a single result.</summary>
        /// <typeparam name="T">Type to parse to.</typeparam>
        /// <param name="defaultValue">Default value.</param>
        /// <returns>Result of a query.</returns>
        T ScalarAs<T>(T defaultValue = default(T));

#endif

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
        IDynamicSelectQueryBuilder From(Func<dynamic, object> fn, params Func<dynamic, object>[] func);

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
        IDynamicSelectQueryBuilder Join(params Func<dynamic, object>[] func);

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
        IDynamicSelectQueryBuilder Where(Func<dynamic, object> func);

        /// <summary>Add where condition.</summary>
        /// <param name="column">Condition column with operator and value.</param>
        /// <returns>Builder instance.</returns>
        IDynamicSelectQueryBuilder Where(DynamicColumn column);

        /// <summary>Add where condition.</summary>
        /// <param name="column">Condition column.</param>
        /// <param name="op">Condition operator.</param>
        /// <param name="value">Condition value.</param>
        /// <returns>Builder instance.</returns>
        IDynamicSelectQueryBuilder Where(string column, DynamicColumn.CompareOperator op, object value);

        /// <summary>Add where condition.</summary>
        /// <param name="column">Condition column.</param>
        /// <param name="value">Condition value.</param>
        /// <returns>Builder instance.</returns>
        IDynamicSelectQueryBuilder Where(string column, object value);

        /// <summary>Add where condition.</summary>
        /// <param name="conditions">Set conditions as properties and values of an object.</param>
        /// <param name="schema">If <c>true</c> use schema to determine key columns and ignore those which
        /// aren't keys.</param>
        /// <returns>Builder instance.</returns>
        IDynamicSelectQueryBuilder Where(object conditions, bool schema = false);

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
        IDynamicSelectQueryBuilder Select(Func<dynamic, object> fn, params Func<dynamic, object>[] func);

        /// <summary>Add select columns.</summary>
        /// <param name="columns">Columns to add to object.</param>
        /// <returns>Builder instance.</returns>
        IDynamicSelectQueryBuilder SelectColumn(params DynamicColumn[] columns);

        /// <summary>Add select columns.</summary>
        /// <param name="columns">Columns to add to object.</param>
        /// <remarks>Column format consist of <c>Column Name</c>, <c>Alias</c> and
        /// <c>Aggregate function</c> in this order separated by '<c>:</c>'.</remarks>
        /// <returns>Builder instance.</returns>
        IDynamicSelectQueryBuilder SelectColumn(params string[] columns);

        #endregion Select

        #region GroupBy

        /// <summary>
        /// Adds to the 'Group By' clause the contents obtained from from parsing the dynamic lambda expression given.
        /// </summary>
        /// <param name="fn">The specification.</param>
        /// <param name="func">The specification.</param>
        /// <returns>This instance to permit chaining.</returns>
        IDynamicSelectQueryBuilder GroupBy(Func<dynamic, object> fn, params Func<dynamic, object>[] func);

        /// <summary>Add select columns.</summary>
        /// <param name="columns">Columns to group by.</param>
        /// <returns>Builder instance.</returns>
        IDynamicSelectQueryBuilder GroupByColumn(params DynamicColumn[] columns);

        /// <summary>Add select columns.</summary>
        /// <param name="columns">Columns to group by.</param>
        /// <remarks>Column format consist of <c>Column Name</c> and
        /// <c>Alias</c> in this order separated by '<c>:</c>'.</remarks>
        /// <returns>Builder instance.</returns>
        IDynamicSelectQueryBuilder GroupByColumn(params string[] columns);

        #endregion GroupBy

        #region Having

        /// <summary>
        /// Adds to the 'Having' clause the contents obtained from parsing the dynamic lambda expression given. The condition
        /// is parsed to the appropriate syntax, Having the specific customs virtual methods supported by the parser are used
        /// as needed.
        /// <para>- If several Having() methods are chained their contents are, by default, concatenated with an 'AND' operator.</para>
        /// <para>- The 'And()' and 'Or()' virtual method can be used to concatenate with an 'OR' or an 'AND' operator, as in:
        /// 'Having( x => x.Or( condition ) )'.</para>
        /// </summary>
        /// <param name="func">The specification.</param>
        /// <returns>This instance to permit chaining.</returns>
        IDynamicSelectQueryBuilder Having(Func<dynamic, object> func);

        /// <summary>Add Having condition.</summary>
        /// <param name="column">Condition column with operator and value.</param>
        /// <returns>Builder instance.</returns>
        IDynamicSelectQueryBuilder Having(DynamicColumn column);

        /// <summary>Add Having condition.</summary>
        /// <param name="column">Condition column.</param>
        /// <param name="op">Condition operator.</param>
        /// <param name="value">Condition value.</param>
        /// <returns>Builder instance.</returns>
        IDynamicSelectQueryBuilder Having(string column, DynamicColumn.CompareOperator op, object value);

        /// <summary>Add Having condition.</summary>
        /// <param name="column">Condition column.</param>
        /// <param name="value">Condition value.</param>
        /// <returns>Builder instance.</returns>
        IDynamicSelectQueryBuilder Having(string column, object value);

        /// <summary>Add Having condition.</summary>
        /// <param name="conditions">Set conditions as properties and values of an object.</param>
        /// <param name="schema">If <c>true</c> use schema to determine key columns and ignore those which
        /// aren't keys.</param>
        /// <returns>Builder instance.</returns>
        IDynamicSelectQueryBuilder Having(object conditions, bool schema = false);

        #endregion Having

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
        IDynamicSelectQueryBuilder OrderBy(Func<dynamic, object> fn, params Func<dynamic, object>[] func);

        /// <summary>Add select columns.</summary>
        /// <param name="columns">Columns to order by.</param>
        /// <returns>Builder instance.</returns>
        IDynamicSelectQueryBuilder OrderByColumn(params DynamicColumn[] columns);

        /// <summary>Add select columns.</summary>
        /// <param name="columns">Columns to order by.</param>
        /// <remarks>Column format consist of <c>Column Name</c> and
        /// <c>Alias</c> in this order separated by '<c>:</c>'.</remarks>
        /// <returns>Builder instance.</returns>
        IDynamicSelectQueryBuilder OrderByColumn(params string[] columns);

        #endregion OrderBy

        #region Top/Limit/Offset/Distinct

        /// <summary>Set top if database support it.</summary>
        /// <param name="top">How many objects select.</param>
        /// <returns>Builder instance.</returns>
        IDynamicSelectQueryBuilder Top(int? top);

        /// <summary>Set top if database support it.</summary>
        /// <param name="limit">How many objects select.</param>
        /// <returns>Builder instance.</returns>
        IDynamicSelectQueryBuilder Limit(int? limit);

        /// <summary>Set top if database support it.</summary>
        /// <param name="offset">How many objects skip selecting.</param>
        /// <returns>Builder instance.</returns>
        IDynamicSelectQueryBuilder Offset(int? offset);

        /// <summary>Set distinct mode.</summary>
        /// <param name="distinct">Distinct mode.</param>
        /// <returns>Builder instance.</returns>
        IDynamicSelectQueryBuilder Distinct(bool distinct = true);

        #endregion Top/Limit/Offset/Distinct
    }
}