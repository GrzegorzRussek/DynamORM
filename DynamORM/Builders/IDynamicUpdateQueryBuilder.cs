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

namespace DynamORM.Builders
{
    /// <summary>Dynamic update query builder interface.</summary>
    /// <remarks>This interface it publically available. Implementation should be hidden.</remarks>
    public interface IDynamicUpdateQueryBuilder : IDynamicQueryBuilder
    {
        /// <summary>Execute this builder.</summary>
        /// <returns>Result of an execution..</returns>
        int Execute();

        #region Update

        /// <summary>Add update value or where condition using schema.</summary>
        /// <param name="column">Update or where column name and value.</param>
        /// <returns>Builder instance.</returns>
        IDynamicUpdateQueryBuilder Update(DynamicColumn column);

        /// <summary>Add update value or where condition using schema.</summary>
        /// <param name="column">Update or where column name.</param>
        /// <param name="value">Column value.</param>
        /// <returns>Builder instance.</returns>
        IDynamicUpdateQueryBuilder Update(string column, object value);

        /// <summary>Add update values and where condition columns using schema.</summary>
        /// <param name="conditions">Set values or conditions as properties and values of an object.</param>
        /// <returns>Builder instance.</returns>
        IDynamicUpdateQueryBuilder Update(object conditions);

        #endregion Update

        #region Values

        /// <summary>
        /// Specifies the columns to update using the dynamic lambda expressions given. Each expression correspond to one
        /// column, and can:
        /// <para>- Resolve to a string, in this case a '=' must appear in the string.</para>
        /// <para>- Resolve to a expression with the form: 'x => x.Column = Value'.</para>
        /// </summary>
        /// <param name="func">The specifications.</param>
        /// <returns>This instance to permit chaining.</returns>
        IDynamicUpdateQueryBuilder Values(params Func<dynamic, object>[] func);

        /// <summary>Add insert fields.</summary>
        /// <param name="column">Insert column and value.</param>
        /// <returns>Builder instance.</returns>
        IDynamicUpdateQueryBuilder Values(DynamicColumn column);

        /// <summary>Add insert fields.</summary>
        /// <param name="column">Insert column.</param>
        /// <param name="value">Insert value.</param>
        /// <returns>Builder instance.</returns>
        IDynamicUpdateQueryBuilder Values(string column, object value);

        /// <summary>Add insert fields.</summary>
        /// <param name="o">Set insert value as properties and values of an object.</param>
        /// <returns>Builder instance.</returns>
        IDynamicUpdateQueryBuilder Values(object o);

        #endregion Values

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
        IDynamicUpdateQueryBuilder Where(Func<dynamic, object> func);

        /// <summary>Add where condition.</summary>
        /// <param name="column">Condition column with operator and value.</param>
        /// <returns>Builder instance.</returns>
        IDynamicUpdateQueryBuilder Where(DynamicColumn column);

        /// <summary>Add where condition.</summary>
        /// <param name="column">Condition column.</param>
        /// <param name="op">Condition operator.</param>
        /// <param name="value">Condition value.</param>
        /// <returns>Builder instance.</returns>
        IDynamicUpdateQueryBuilder Where(string column, DynamicColumn.CompareOperator op, object value);

        /// <summary>Add where condition.</summary>
        /// <param name="column">Condition column.</param>
        /// <param name="value">Condition value.</param>
        /// <returns>Builder instance.</returns>
        IDynamicUpdateQueryBuilder Where(string column, object value);

        /// <summary>Add where condition.</summary>
        /// <param name="conditions">Set conditions as properties and values of an object.</param>
        /// <param name="schema">If <c>true</c> use schema to determine key columns and ignore those which
        /// aren't keys.</param>
        /// <returns>Builder instance.</returns>
        IDynamicUpdateQueryBuilder Where(object conditions, bool schema = false);

        #endregion Where
    }
}