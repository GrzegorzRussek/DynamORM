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

namespace DynamORM.Builders
{
    /// <summary>Dynamic delete query builder interface.</summary>
    /// <remarks>This interface it publicly available. Implementation should be hidden.</remarks>
    public interface IDynamicDeleteQueryBuilder : IDynamicQueryBuilder
    {
        /// <summary>Execute this builder.</summary>
        /// <returns>Result of an execution..</returns>
        int Execute();

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
        IDynamicDeleteQueryBuilder Where(Func<dynamic, object> func);

        /// <summary>Add where condition.</summary>
        /// <param name="column">Condition column with operator and value.</param>
        /// <returns>Builder instance.</returns>
        IDynamicDeleteQueryBuilder Where(DynamicColumn column);

        /// <summary>Add where condition.</summary>
        /// <param name="column">Condition column.</param>
        /// <param name="op">Condition operator.</param>
        /// <param name="value">Condition value.</param>
        /// <returns>Builder instance.</returns>
        IDynamicDeleteQueryBuilder Where(string column, DynamicColumn.CompareOperator op, object value);

        /// <summary>Add where condition.</summary>
        /// <param name="column">Condition column.</param>
        /// <param name="value">Condition value.</param>
        /// <returns>Builder instance.</returns>
        IDynamicDeleteQueryBuilder Where(string column, object value);

        /// <summary>Add where condition.</summary>
        /// <param name="conditions">Set conditions as properties and values of an object.</param>
        /// <param name="schema">If <c>true</c> use schema to determine key columns and ignore those which
        /// aren't keys.</param>
        /// <returns>Builder instance.</returns>
        IDynamicDeleteQueryBuilder Where(object conditions, bool schema = false);
    }
}