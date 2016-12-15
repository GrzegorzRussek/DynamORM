using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Dynamic;
using System.IO;
using DynamORM.Helpers;
using DynamORM.Mapper;

namespace DynamORM
{
    /// <summary>Cache data reader in memory.</summary>
    public class DynamicCachedReader : DynamicObject, IDataReader
    {
        #region Constructor and Data

        private DataTable _schema;
        private int _fields;
        private int _rows;
        private int _position;
        private int _cachePos;

        private IList<string> _names;
        private IDictionary<string, int> _ordinals;
        private IList<Type> _types;
        private IList<object> _cache;

        private DynamicCachedReader()
        {
        }

        /// <summary>Initializes a new instance of the <see cref="DynamicCachedReader" /> class.</summary>
        /// <param name="reader">Reader to cache.</param>
        /// <param name="offset">The offset row.</param>
        /// <param name="limit">The limit to number of tows. -1 is no limit.</param>
        /// <param name="progress">The progress delegate.</param>
        public DynamicCachedReader(IDataReader reader, int offset = 0, int limit = -1, Func<DynamicCachedReader, int, bool> progress = null)
        {
            InitDataReader(reader, offset, limit, progress);
        }

        #endregion Constructor and Data

        #region Helpers

        /// <summary>Create data reader from enumerable.</summary>
        /// <typeparam name="T">Type of enumerated objects.</typeparam>
        /// <param name="objects">List of objects.</param>
        /// <returns>Instance of <see cref="DynamicCachedReader"/> containing objects data.</returns>
        public static DynamicCachedReader FromEnumerable<T>(IEnumerable<T> objects)
        {
            var mapper = DynamicMapperCache.GetMapper<T>();

            if (mapper == null)
                throw new InvalidCastException(string.Format("Object type '{0}' can't be mapped.", typeof(T).FullName));

            var r = new DynamicCachedReader();
            r.Init(mapper.ColumnsMap.Count + 1);
            r.CreateSchemaTable(mapper);
            r.FillFromEnumerable(objects, mapper);

            r.IsClosed = false;
            r._position = -1;
            r._cachePos = -1;

            return r;
        }

        /// <summary>Create data reader from enumerable.</summary>
        /// <param name="elementType">Type of enumerated objects.</param>
        /// <param name="objects">List of objects.</param>
        /// <returns>Instance of <see cref="DynamicCachedReader"/> containing objects data.</returns>
        public static DynamicCachedReader FromEnumerable(Type elementType, IEnumerable objects)
        {
            var mapper = DynamicMapperCache.GetMapper(elementType);

            if (mapper == null)
                throw new InvalidCastException(string.Format("Object type '{0}' can't be mapped.", elementType.FullName));

            var r = new DynamicCachedReader();
            r.Init(mapper.ColumnsMap.Count + 1);
            r.CreateSchemaTable(mapper);
            r.FillFromEnumerable(elementType, objects, mapper);

            r.IsClosed = false;
            r._position = -1;
            r._cachePos = -1;

            return r;
        }

        private void InitDataReader(IDataReader reader, int offset = 0, int limit = -1, Func<DynamicCachedReader, int, bool> progress = null)
        {
            _schema = reader.GetSchemaTable();
            RecordsAffected = reader.RecordsAffected;

            Init(reader.FieldCount);

            int i = 0;

            for (i = 0; i < _fields; i++)
            {
                _names.Add(reader.GetName(i));
                _types.Add(reader.GetFieldType(i));

                if (!_ordinals.ContainsKey(reader.GetName(i).ToUpper()))
                    _ordinals.Add(reader.GetName(i).ToUpper(), i);
            }

            int current = 0;
            while (reader.Read())
            {
                if (current < offset)
                {
                    current++;
                    continue;
                }

                for (i = 0; i < _fields; i++)
                    _cache.Add(reader[i]);

                _rows++;
                current++;

                if (limit >= 0 && _rows >= limit)
                    break;

                if (progress != null && !progress(this, _rows))
                    break;
            }

            IsClosed = false;
            _position = -1;
            _cachePos = -1;

            if (progress != null)
                progress(this, _rows);

            reader.Close();
        }

        private void FillFromEnumerable<T>(IEnumerable<T> objects, DynamicTypeMap mapper)
        {
            foreach (var elem in objects)
            {
                foreach (var col in mapper.ColumnsMap)
                {
                    object val = null;

                    if (col.Value.Get != null)
                        val = col.Value.Get(elem);

                    _cache.Add(val);
                }

                _cache.Add(elem);

                _rows++;
            }
        }

        private void FillFromEnumerable(Type elementType, IEnumerable objects, DynamicTypeMap mapper)
        {
            foreach (var elem in objects)
            {
                foreach (var col in mapper.ColumnsMap)
                {
                    object val = null;

                    if (col.Value.Get != null)
                        val = col.Value.Get(elem);

                    _cache.Add(val);
                }

                _cache.Add(elem);

                _rows++;
            }
        }

        private void CreateSchemaTable(DynamicTypeMap mapper)
        {
            _schema = new DataTable("DYNAMIC");
            _schema.Columns.Add(new DataColumn("ColumnName", typeof(string)));
            _schema.Columns.Add(new DataColumn("ColumnOrdinal", typeof(int)));
            _schema.Columns.Add(new DataColumn("ColumnSize", typeof(int)));
            _schema.Columns.Add(new DataColumn("NumericPrecision", typeof(short)));
            _schema.Columns.Add(new DataColumn("NumericScale", typeof(short)));
            _schema.Columns.Add(new DataColumn("DataType", typeof(Type)));
            _schema.Columns.Add(new DataColumn("ProviderType", typeof(int)));
            _schema.Columns.Add(new DataColumn("NativeType", typeof(int)));
            _schema.Columns.Add(new DataColumn("AllowDBNull", typeof(bool)));
            _schema.Columns.Add(new DataColumn("IsUnique", typeof(bool)));
            _schema.Columns.Add(new DataColumn("IsKey", typeof(bool)));
            _schema.Columns.Add(new DataColumn("IsAutoIncrement", typeof(bool)));

            int ordinal = 0;
            DataRow dr = null;

            foreach (var column in mapper.ColumnsMap)
            {
                dr = _schema.NewRow();

                dr[0] = column.Value.Column.NullOr(x => x.Name ?? column.Value.Name, column.Value.Name);
                dr[1] = ordinal;
                dr[2] = column.Value.Column.NullOr(x => x.Size ?? int.MaxValue, int.MaxValue);
                dr[3] = column.Value.Column.NullOr(x => x.Precision ?? 0, 0);
                dr[4] = column.Value.Column.NullOr(x => x.Scale ?? 0, 0);
                dr[5] = column.Value.Column.NullOr(x => x.Type.HasValue ? x.Type.Value.ToType() : column.Value.Type, column.Value.Type);
                dr[6] = column.Value.Column.NullOr(x => x.Type ?? column.Value.Type.ToDbType(), column.Value.Type.ToDbType());
                dr[7] = column.Value.Column.NullOr(x => x.Type ?? column.Value.Type.ToDbType(), column.Value.Type.ToDbType());
                dr[8] = column.Value.Column.NullOr(x => x.IsKey, false) ? true : column.Value.Column.NullOr(x => x.AllowNull, true);
                dr[9] = column.Value.Column.NullOr(x => x.IsUnique, false);
                dr[10] = column.Value.Column.NullOr(x => x.IsKey, false);
                dr[11] = false;

                _schema.Rows.Add(dr);

                _names.Add(dr[0].ToString());
                _ordinals.Add(dr[0].ToString().ToUpper(), ordinal++);
                _types.Add((Type)dr[5]);

                dr.AcceptChanges();
            }

            dr = _schema.NewRow();

            dr[0] = "#O";
            dr[1] = ordinal;
            dr[2] = int.MaxValue;
            dr[3] = 0;
            dr[4] = 0;
            dr[5] = mapper.Type;
            dr[6] = DbType.Object;
            dr[7] = DbType.Object;
            dr[8] = true;
            dr[9] = false;
            dr[10] = false;
            dr[11] = false;

            _schema.Rows.Add(dr);

            _names.Add("#O");
            _ordinals.Add("#O".ToUpper(), ordinal++);
            _types.Add(mapper.Type);

            dr.AcceptChanges();
        }

        private void Init(int fieldCount)
        {
            _rows = 0;
            _fields = fieldCount;
            _names = new List<string>(_fields);
            _ordinals = new Dictionary<string, int>(_fields);
            _types = new List<Type>(_fields);
            _cache = new List<object>(_fields * 100);
        }

        /// <summary>Sets the current position in reader.</summary>
        /// <param name="pos">The position.</param>
        public void SetPosition(int pos)
        {
            if (pos >= -1 && pos < _rows)
            {
                _position = pos;
                _cachePos = _position * _fields;
            }
            else
                throw new IndexOutOfRangeException();
        }

        #endregion Helpers

        #region IDataReader Members

        /// <summary>Closes the System.Data.IDataReader Object.</summary>
        public void Close()
        {
            IsClosed = true;
            _position = _rows;
            _cachePos = -1;
        }

        /// <summary>Gets a value indicating the depth of nesting for the current row.</summary>
        /// <remarks>This implementation use this field to indicate row count.</remarks>
        public int Depth
        {
            get { return _rows; }
        }

        /// <summary>Returns a System.Data.DataTable that describes the column metadata of the
        /// System.Data.IDataReader.</summary><returns>A System.Data.DataTable that describes
        /// the column metadata.</returns><exception cref="System.InvalidOperationException">
        /// The System.Data.IDataReader is closed.</exception>
        public DataTable GetSchemaTable()
        {
            return _schema;
        }

        /// <summary>Gets a value indicating whether the data reader is closed.</summary>
        public bool IsClosed { get; private set; }

        /// <summary>Advances the data reader to the next result, when reading the results of batch SQL statements.</summary>
        /// <returns>Returns true if there are more rows; otherwise, false.</returns>
        public bool NextResult()
        {
            _cachePos = (++_position) * _fields;

            return _position < _rows;
        }

        /// <summary>Advances the System.Data.IDataReader to the next record.</summary>
        /// <returns>Returns true if there are more rows; otherwise, false.</returns>
        public bool Read()
        {
            _cachePos = (++_position) * _fields;

            return _position < _rows;
        }

        /// <summary>Gets the number of rows changed, inserted, or deleted by execution of the SQL statement.</summary>
        /// <returns>The number of rows changed, inserted, or deleted; 0 if no rows were affected or the statement
        /// failed; and -1 for SELECT statements.</returns>
        public int RecordsAffected { get; private set; }

        #endregion IDataReader Members

        #region IDisposable Members

        /// <summary>Performs application-defined tasks associated with
        /// freeing, releasing, or resetting unmanaged resources.</summary>
        public void Dispose()
        {
            _names.Clear();
            _types.Clear();
            _cache.Clear();
            _schema.Dispose();
        }

        #endregion IDisposable Members

        #region IDataRecord Members

        /// <summary>Gets the number of columns in the current row.</summary>
        /// <remarks>When not positioned in a valid record set, 0; otherwise, the number of columns in the current record. The default is -1.</remarks>
        public int FieldCount { get { return _fields; } }

        /// <summary>Return the value of the specified field.</summary>
        /// <param name="i">The index of the field to find.</param>
        /// <returns>Field value upon return.</returns>
        public bool GetBoolean(int i)
        {
            return (bool)_cache[_cachePos + i];
        }

        /// <summary>Return the value of the specified field.</summary>
        /// <param name="i">The index of the field to find.</param>
        /// <returns>Field value upon return.</returns>
        public byte GetByte(int i)
        {
            return (byte)_cache[_cachePos + i];
        }

        /// <summary>Reads a stream of bytes from the specified column offset into the buffer
        /// as an array, starting at the given buffer offset.</summary>
        /// <param name="i">The index of the field to find.</param>
        /// <param name="fieldOffset">The index within the field from which to start the read operation.</param>
        /// <param name="buffer">The buffer into which to read the stream of bytes.</param>
        /// <param name="bufferoffset">The index for buffer to start the read operation.</param>
        /// <param name="length">The number of bytes to read.</param>
        /// <returns>The actual number of bytes read.</returns>
        public long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length)
        {
            using (MemoryStream ms = new MemoryStream((byte[])_cache[_cachePos + i]))
                return ms.Read(buffer, bufferoffset, length);
        }

        /// <summary>Return the value of the specified field.</summary>
        /// <param name="i">The index of the field to find.</param>
        /// <returns>Field value upon return.</returns>
        public char GetChar(int i)
        {
            return (char)_cache[_cachePos + i];
        }

        /// <summary>Reads a stream of characters from the specified column offset into the buffer
        /// as an array, starting at the given buffer offset.</summary>
        /// <param name="i">The zero-based column ordinal.</param>
        /// <param name="fieldoffset">The index within the row from which to start the read operation.</param>
        /// <param name="buffer">The buffer into which to read the stream of bytes.</param>
        /// <param name="bufferoffset">The index for buffer to start the read operation.</param>
        /// <param name="length">The number of bytes to read.</param>
        /// <returns>The actual number of characters read.</returns>
        public long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length)
        {
            using (MemoryStream ms = new MemoryStream((byte[])_cache[_cachePos + i]))
            {
                byte[] buff = new byte[buffer.Length];
                long ret = ms.Read(buff, bufferoffset, length);

                for (int n = bufferoffset; n < ret; n++)
                    buffer[n] = (char)buff[n];

                return ret;
            }
        }

        /// <summary>Returns an System.Data.IDataReader for the specified column ordinal.</summary>
        /// <param name="i">The index of the field to find.</param>
        /// <returns>An System.Data.IDataReader.</returns>
        public IDataReader GetData(int i)
        {
            return null;
        }

        /// <summary>Gets the data type information for the specified field.</summary>
        /// <param name="i">The index of the field to find.</param>
        /// <returns>The data type information for the specified field.</returns>
        public string GetDataTypeName(int i)
        {
            return _types[i].Name;
        }

        /// <summary>Return the value of the specified field.</summary>
        /// <param name="i">The index of the field to find.</param>
        /// <returns>Field value upon return.</returns>
        public DateTime GetDateTime(int i)
        {
            return (DateTime)_cache[_cachePos + i];
        }

        /// <summary>Return the value of the specified field.</summary>
        /// <param name="i">The index of the field to find.</param>
        /// <returns>Field value upon return.</returns>
        public decimal GetDecimal(int i)
        {
            return (decimal)_cache[_cachePos + i];
        }

        /// <summary>Return the value of the specified field.</summary>
        /// <param name="i">The index of the field to find.</param>
        /// <returns>Field value upon return.</returns>
        public double GetDouble(int i)
        {
            return (double)_cache[_cachePos + i];
        }

        /// <summary>Gets the System.Type information corresponding to the type of System.Object
        /// that would be returned from System.Data.IDataRecord.GetValue(System.Int32).</summary>
        /// <param name="i">The index of the field to find.</param>
        /// <returns>The System.Type information corresponding to the type of System.Object that
        /// would be returned from System.Data.IDataRecord.GetValue(System.Int32).</returns>
        public Type GetFieldType(int i)
        {
            return _types[i];
        }

        /// <summary>Return the value of the specified field.</summary>
        /// <param name="i">The index of the field to find.</param>
        /// <returns>Field value upon return.</returns>
        public float GetFloat(int i)
        {
            return (float)_cache[_cachePos + i];
        }

        /// <summary>Return the value of the specified field.</summary>
        /// <param name="i">The index of the field to find.</param>
        /// <returns>Field value upon return.</returns>
        public Guid GetGuid(int i)
        {
            return (Guid)_cache[_cachePos + i];
        }

        /// <summary>Return the value of the specified field.</summary>
        /// <param name="i">The index of the field to find.</param>
        /// <returns>Field value upon return.</returns>
        public short GetInt16(int i)
        {
            return (short)_cache[_cachePos + i];
        }

        /// <summary>Return the value of the specified field.</summary>
        /// <param name="i">The index of the field to find.</param>
        /// <returns>Field value upon return.</returns>
        public int GetInt32(int i)
        {
            return (int)_cache[_cachePos + i];
        }

        /// <summary>Return the value of the specified field.</summary>
        /// <param name="i">The index of the field to find.</param>
        /// <returns>Field value upon return.</returns>
        public long GetInt64(int i)
        {
            return (long)_cache[_cachePos + i];
        }

        /// <summary>Gets the name for the field to find.</summary>
        /// <param name="i">The index of the field to find.</param>
        /// <returns>The name of the field or the empty string (""), if there is no value to return.</returns>
        public string GetName(int i)
        {
            return _names[i];
        }

        /// <summary>Return the index of the named field.</summary>
        /// <param name="name">The name of the field to find.</param>
        /// <returns>The index of the named field.</returns>
        public int GetOrdinal(string name)
        {
            if (_ordinals.ContainsKey(name.ToUpper()))
                return _ordinals[name.ToUpper()];

            return -1;
        }

        /// <summary>Return the value of the specified field.</summary>
        /// <param name="i">The index of the field to find.</param>
        /// <returns>Field value upon return.</returns>
        public string GetString(int i)
        {
            return (string)_cache[_cachePos + i];
        }

        /// <summary>Return the value of the specified field.</summary>
        /// <param name="i">The index of the field to find.</param>
        /// <returns>Field value upon return.</returns>
        public object GetValue(int i)
        {
            return _cache[_cachePos + i];
        }

        /// <summary>Gets all the attribute fields in the collection for the current record.</summary>
        /// <param name="values">An array of System.Object to copy the attribute fields into.</param>
        /// <returns>The number of instances of System.Object in the array.</returns>
        public int GetValues(object[] values)
        {
            for (int i = 0; i < _fields; i++)
                values[i] = _cache[_cachePos + i];

            return _fields;
        }

        /// <summary>Return whether the specified field is set to null.</summary>
        /// <param name="i">The index of the field to find.</param>
        /// <returns>Returns true if the specified field is set to null; otherwise, false.</returns>
        public bool IsDBNull(int i)
        {
            return _cache[_cachePos + i] == null || _cache[_cachePos + i] == DBNull.Value;
        }

        /// <summary>Gets or sets specified value in current record.</summary>
        /// <param name="name">Name of column.</param>
        /// <returns>Value of specified column.</returns>
        public object this[string name]
        {
            get
            {
                if (_ordinals.ContainsKey(name.ToUpper()))
                    return _cache[_cachePos + _ordinals[name.ToUpper()]];

                throw new IndexOutOfRangeException(String.Format("Field '{0}' not found.", name));
            }
        }

        /// <summary>Gets or sets specified value in current record.</summary>
        /// <param name="i">The index of the field to find.</param>
        /// <returns>Value of specified column.</returns>
        public object this[int i]
        {
            get { return _cache[_cachePos + i]; }
        }

        #endregion IDataRecord Members
    }
}