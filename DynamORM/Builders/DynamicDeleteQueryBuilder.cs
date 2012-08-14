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
using System.Text;

namespace DynamORM.Builders
{
    /// <summary>Delete query builder.</summary>
    public class DynamicDeleteQueryBuilder : DynamicQueryBuilder<DynamicDeleteQueryBuilder>
    {
        /// <summary>Initializes a new instance of the <see cref="DynamicDeleteQueryBuilder"/> class.</summary>
        /// <param name="table">Parent dynamic table.</param>
        public DynamicDeleteQueryBuilder(DynamicTable table)
            : base(table)
        {
        }

        /// <summary>Fill command with query.</summary>
        /// <param name="command">Command to fill.</param>
        /// <returns>Filled instance of <see cref="IDbCommand"/>.</returns>
        public override IDbCommand FillCommand(IDbCommand command)
        {
            StringBuilder sb = new StringBuilder();

            sb.Append("DELETE FROM ");
            DynamicTable.Database.DecorateName(sb, TableName);

            FillWhere(command, sb);

            return command.SetCommand(sb.ToString());
        }

        /// <summary>Execute this builder.</summary>
        /// <returns>Number of affected rows.</returns>
        public override dynamic Execute()
        {
            return DynamicTable.Execute(this);
        }
    }
}