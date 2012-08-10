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

using System.Diagnostics.CodeAnalysis;
using DynamORM.Mapper;

namespace DynamORM.Tests.Helpers
{
    /// <summary>Users table representation.</summary>
    [SuppressMessage("Microsoft.StyleCop.CSharp.NamingRules", "SA1300:ElementMustBeginWithUpperCaseLetter", Justification = "Bare bone table mapping.")]
    public class users
    {
        /// <summary>Gets or sets id column value.</summary>
        [Column("id", true)]
        public long id { get; set; }

        /// <summary>Gets or sets code column value.</summary>
        public string code { get; set; }

        /// <summary>Gets or sets login column value.</summary>
        public string login { get; set; }

        /// <summary>Gets or sets first columnvalue.</summary>
        public string first { get; set; }

        /// <summary>Gets or sets last column value.</summary>
        public string last { get; set; }

        /// <summary>Gets or sets password column value.</summary>
        public string password { get; set; }

        /// <summary>Gets or sets email column value.</summary>
        public string email { get; set; }

        /// <summary>Gets or sets quote column value.</summary>
        public string quote { get; set; }

        /// <summary>Gets or sets value of aggregate fields.</summary>
        [Ignore]
        public object aggregatefield { get; set; }
    }
}