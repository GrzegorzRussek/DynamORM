using System;
using System.Data;

namespace DynamORM.Helpers
{
    /// <summary>Extensions for data reader handling.</summary>
    public static class DataReaderExtensions
    {
        /// <summary>Gets the data table from data reader.</summary>
        /// <param name="r">The data reader.</param>
        /// <param name="name">The name to give the table. If tableName is null or an empty string, a default name is given when added to the System.Data.DataTableCollection.</param>
        /// <param name="nameSpace">The namespace for the XML representation of the data stored in the DataTable.</param>
        /// <returns></returns>
        public static DataTable GetDataTableFromDataReader(this IDataReader r, string name = null, string nameSpace = null)
        {
            DataTable schemaTable = r.GetSchemaTable();
            DataTable resultTable = new DataTable(name, nameSpace);

            foreach (DataRow col in schemaTable.Rows)
            {
                dynamic c = col.RowToDynamicUpper();

                DataColumn dataColumn = new DataColumn();
                dataColumn.ColumnName = c.COLUMNNAME;
                dataColumn.DataType = (Type)c.DATATYPE;
                dataColumn.ReadOnly = true;
                dataColumn.Unique = c.ISUNIQUE;

                resultTable.Columns.Add(dataColumn);
            }

            while (r.Read())
            {
                DataRow row = resultTable.NewRow();
                for (int i = 0; i < resultTable.Columns.Count - 1; i++)
                    row[i] = r[i];

                resultTable.Rows.Add(row);
            }

            return resultTable;
        }
    }
}
