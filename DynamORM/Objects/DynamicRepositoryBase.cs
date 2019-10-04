using System;
using System.Collections.Generic;
using System.Linq;
using DynamORM.Builders;
using DynamORM.Mapper;

namespace DynamORM.Objects
{
    /// <summary>Base repository class for specified object type.</summary>
    /// <typeparam name="T">Type of stored object.</typeparam>
    public class DynamicRepositoryBase<T> : IDisposable where T : DynamicEntityBase
    {
        private DynamicDatabase _database;

        /// <summary>Initializes a new instance of the <see cref="DynamicRepositoryBase{T}"/> class.</summary>
        /// <param name="database">The database.</param>
        public DynamicRepositoryBase(DynamicDatabase database)
        {
            _database = database;
        }

        /// <summary>Get all rows from database.</summary>
        /// <returns>Objects enumerator.</returns>
        public virtual IEnumerable<T> GetAll()
        {
            return EnumerateQuery(_database.From<T>());
        }

        /// <summary>Get rows from database by custom query.</summary>
        /// <param name="query">The query.</param>
        /// <remarks>Query must be based on object type.</remarks>
        /// <returns>Objects enumerator.</returns>
        public virtual IEnumerable<T> GetByQuery(IDynamicSelectQueryBuilder query)
        {
            return EnumerateQuery(query);
        }

        private IEnumerable<T> EnumerateQuery(IDynamicSelectQueryBuilder query, bool forceType = true)
        {
            if (forceType)
            {
                var mapper = DynamicMapperCache.GetMapper(typeof(T));

                var tn = mapper.Table == null || string.IsNullOrEmpty(mapper.Table.Name) ?
                    mapper.Type.Name : mapper.Table.Name;

                if (!query.Tables.Any(t => t.Name == tn))
                    throw new InvalidOperationException(string.Format("Query is not related to '{0}' class.", typeof(T).FullName));
            }

            foreach (var o in query.Execute<T>())
            {
                o.SetDynamicEntityState(DynamicEntityState.Existing);
                yield return o;
            }
        }

        /// <summary>Saves single object to database.</summary>
        /// <param name="element">The element.</param>
        /// <returns>Returns <c>true</c> if operation was successful.</returns>
        public virtual bool Save(T element)
        {
            return element.Save(_database);
        }

        /// <summary>Saves collection of objects to database.</summary>
        /// <param name="element">The element.</param>
        /// <returns>Returns <c>true</c> if operation was successful.</returns>
        public virtual bool Save(IEnumerable<T> element)
        {
            return element.All(x => x.Save(_database));
        }

        /// <summary>Insert single object to database.</summary>
        /// <param name="element">The element.</param>
        /// <returns>Returns <c>true</c> if operation was successful.</returns>
        public virtual bool Insert(T element)
        {
            return element.Insert(_database);
        }

        /// <summary>Insert collection of objects to database.</summary>
        /// <param name="element">The element.</param>
        /// <returns>Returns <c>true</c> if operation was successful.</returns>
        public virtual bool Insert(IEnumerable<T> element)
        {
            return element.All(x => x.Insert(_database));
        }

        /// <summary>Update single object to database.</summary>
        /// <param name="element">The element.</param>
        /// <returns>Returns <c>true</c> if operation was successful.</returns>
        public virtual bool Update(T element)
        {
            return element.Update(_database);
        }

        /// <summary>Update collection of objects to database.</summary>
        /// <param name="element">The element.</param>
        /// <returns>Returns <c>true</c> if operation was successful.</returns>
        public virtual bool Update(IEnumerable<T> element)
        {
            return element.All(x => x.Update(_database));
        }

        /// <summary>Delete single object to database.</summary>
        /// <param name="element">The element.</param>
        /// <returns>Returns <c>true</c> if operation was successful.</returns>
        public virtual bool Delete(T element)
        {
            return element.Delete(_database);
        }

        /// <summary>Delete collection of objects to database.</summary>
        /// <param name="element">The element.</param>
        /// <returns>Returns <c>true</c> if operation was successful.</returns>
        public virtual bool Delete(IEnumerable<T> element)
        {
            return element.All(x => x.Delete(_database));
        }

        /// <summary>Releases unmanaged and - optionally - managed resources.</summary>
        public virtual void Dispose()
        {
            _database = null;
        }
    }
}