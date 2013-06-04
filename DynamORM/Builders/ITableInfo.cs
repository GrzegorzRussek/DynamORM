// -----------------------------------------------------------------------
// <copyright file="ITableInfo.cs" company="Microsoft">
// TODO: Update copyright text.
// </copyright>
// -----------------------------------------------------------------------

using System.Collections.Generic;

namespace DynamORM.Builders
{
    /// <summary>Interface describing table information.</summary>
    public interface ITableInfo
    {
        /// <summary>Gets table owner name.</summary>
        string Owner { get; }

        /// <summary>Gets table name.</summary>
        string Name { get; }

        /// <summary>Gets table alias.</summary>
        string Alias { get; }

        /// <summary>Gets table schema.</summary>
        Dictionary<string, DynamicSchemaColumn> Schema { get; }
    }
}