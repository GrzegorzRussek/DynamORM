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

namespace DynamORM.Builders
{
    /// <summary>Dynamic query builder base interface.</summary>
    /// <remarks>This interface it publically available. Implementation should be hidden.</remarks>
    public interface IDynamicQueryBuilder
    {
        /// <summary>Gets <see cref="DynamicDatabase"/> instance.</summary>
        DynamicDatabase Database { get; }

        /// <summary>Gets tables information.</summary>
        IList<ITableInfo> Tables { get; }

        /// <summary>Gets the tables used in this builder.</summary>
        IDictionary<string, IParameter> Parameters { get; }

        /// <summary>Gets or sets a value indicating whether add virtual parameters.</summary>
        bool VirtualMode { get; set; }

        /// <summary>Gets a value indicating whether database supports standard schema.</summary>
        bool SupportSchema { get; }

        /// <summary>Fill command with query.</summary>
        /// <param name="command">Command to fill.</param>
        /// <returns>Filled instance of <see cref="IDbCommand"/>.</returns>
        IDbCommand FillCommand(IDbCommand command);

        /// <summary>
        /// Generates the text this command will execute against the underlying database.
        /// </summary>
        /// <returns>The text to execute against the underlying database.</returns>
        /// <remarks>This method must be override by derived classes.</remarks>
        string CommandText();

        /// <summary>Gets or sets the on create temporary parameter action.</summary>
        /// <remarks>This is exposed to allow setting schema of column.</remarks>
        Action<IParameter> OnCreateTemporaryParameter { get; set; }

        /// <summary>Gets or sets the on create real parameter action.</summary>
        /// <remarks>This is exposed to allow modification of parameter.</remarks>
        Action<IParameter, IDbDataParameter> OnCreateParameter { get; set; }
    }
}