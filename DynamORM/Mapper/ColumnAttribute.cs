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
using System.Data;

namespace DynamORM.Mapper
{
    /// <summary>Allows to add table name to class.</summary>
    [AttributeUsage(AttributeTargets.Property)]
    public class ColumnAttribute : Attribute
    {
        /// <summary>Gets or sets name.</summary>
        public string Name { get; set; }

        /// <summary>Gets or sets column type.</summary>
        /// <remarks>Used when overriding schema.</remarks>
        public DbType? Type { get; set; }

        /// <summary>Gets or sets a value indicating whether column is a key.</summary>
        public bool IsKey { get; set; }

        /// <summary>Gets or sets a value indicating whether column should have unique value.</summary>
        /// <remarks>Used when overriding schema.</remarks>
        public bool? IsUnique { get; set; }

        /// <summary>Gets or sets column size.</summary>
        /// <remarks>Used when overriding schema.</remarks>
        public int? Size { get; set; }

        /// <summary>Gets or sets column precision.</summary>
        /// <remarks>Used when overriding schema.</remarks>
        public byte? Precision { get; set; }

        /// <summary>Gets or sets column scale.</summary>
        /// <remarks>Used when overriding schema.</remarks>
        public byte? Scale { get; set; }

        #region Constructors

        /// <summary>Initializes a new instance of the <see cref="ColumnAttribute" /> class.</summary>
        public ColumnAttribute() { }

        /// <summary>Initializes a new instance of the <see cref="ColumnAttribute" /> class.</summary>
        /// <param name="name">Name of column.</param>
        public ColumnAttribute(string name)
        {
            Name = name;
        }

        /// <summary>Initializes a new instance of the <see cref="ColumnAttribute" /> class.</summary>
        /// <param name="name">Name of column.</param>
        /// <param name="isKey">Set column as a key column.</param>
        public ColumnAttribute(string name, bool isKey)
            : this(name)
        {
            IsKey = isKey;
        }

        /// <summary>Initializes a new instance of the <see cref="ColumnAttribute" /> class.</summary>
        /// <param name="name">Name of column.</param>
        /// <param name="isKey">Set column as a key column.</param>
        /// <param name="type">Set column type.</param>
        public ColumnAttribute(string name, bool isKey, DbType type)
            : this(name, isKey)
        {
            Type = type;
        }

        /// <summary>Initializes a new instance of the <see cref="ColumnAttribute" /> class.</summary>
        /// <param name="name">Name of column.</param>
        /// <param name="isKey">Set column as a key column.</param>
        /// <param name="type">Set column type.</param>
        /// <param name="size">Set column value size.</param>
        public ColumnAttribute(string name, bool isKey, DbType type, int size)
            : this(name, isKey, type)
        {
            Size = size;
        }

        /// <summary>Initializes a new instance of the <see cref="ColumnAttribute" /> class.</summary>
        /// <param name="name">Name of column.</param>
        /// <param name="isKey">Set column as a key column.</param>
        /// <param name="type">Set column type.</param>
        /// <param name="precision">Set column value precision.</param>
        /// <param name="scale">Set column value scale.</param>
        public ColumnAttribute(string name, bool isKey, DbType type, byte precision, byte scale)
            : this(name, isKey, type)
        {
            Precision = precision;
            Scale = scale;
        }

        /// <summary>Initializes a new instance of the <see cref="ColumnAttribute" /> class.</summary>
        /// <param name="name">Name of column.</param>
        /// <param name="isKey">Set column as a key column.</param>
        /// <param name="type">Set column type.</param>
        /// <param name="size">Set column value size.</param>
        /// <param name="precision">Set column value precision.</param>
        /// <param name="scale">Set column value scale.</param>
        public ColumnAttribute(string name, bool isKey, DbType type, int size, byte precision, byte scale)
            : this(name, isKey, type, precision, scale)
        {
            Size = size;
        }

        /// <summary>Initializes a new instance of the <see cref="ColumnAttribute" /> class.</summary>
        /// <param name="name">Name of column.</param>
        /// <param name="isKey">Set column as a key column.</param>
        /// <param name="isUnique">Set column has unique value.</param>
        /// <param name="type">Set column type.</param>
        /// <param name="size">Set column value size.</param>
        /// <param name="precision">Set column value precision.</param>
        /// <param name="scale">Set column value scale.</param>
        public ColumnAttribute(string name, bool isKey, bool isUnique, DbType type, int size, byte precision, byte scale)
            : this(name, isKey, type, size, precision, scale)
        {
            IsUnique = isUnique;
        }

        #endregion Constructors
    }
}