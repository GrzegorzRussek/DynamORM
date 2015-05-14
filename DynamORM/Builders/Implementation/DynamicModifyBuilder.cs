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

using System.Data;
using DynamORM.Builders.Extensions;

namespace DynamORM.Builders.Implementation
{
    /// <summary>Base query builder for insert/update/delete statements.</summary>
    internal abstract class DynamicModifyBuilder : DynamicQueryBuilder
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="DynamicModifyBuilder"/> class.
        /// </summary>
        /// <param name="db">The database.</param>
        public DynamicModifyBuilder(DynamicDatabase db)
            : base(db)
        {
            VirtualMode = false;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="DynamicModifyBuilder" /> class.
        /// </summary>
        /// <param name="db">The database.</param>
        /// <param name="tableName">Name of the table.</param>
        public DynamicModifyBuilder(DynamicDatabase db, string tableName)
            : this(db)
        {
            VirtualMode = false;
            this.Table(tableName);
        }

        /// <summary>Execute this builder.</summary>
        /// <returns>Result of an execution..</returns>
        public virtual int Execute()
        {
            using (IDbConnection con = Database.Open())
            using (IDbCommand cmd = con.CreateCommand())
            {
                return cmd
                    .SetCommand(this)
                    .ExecuteNonQuery();
            }
        }
    }
}