using System;
using System.Collections.Generic;
using System.Linq;
using DynamORM.Builders;
using DynamORM.Mapper;
using DynamORM.Validation;

namespace DynamORM.Objects
{
    /// <summary>Base class for strong typed objects.</summary>
    public class DynamicEntityBase
    {
        private Dictionary<string, object> _changedFields = new Dictionary<string, object>();
        private DynamicEntityState _dynamicEntityState = DynamicEntityState.Unknown;

        /// <summary>Occurs when object property is changing.</summary>
        public event EventHandler<DynamicPropertyChangingEventArgs> PropertyChanging;

        /// <summary>Gets the state of the dynamic entity.</summary>
        /// <returns>Current state of entity.</returns>
        public virtual DynamicEntityState GetDynamicEntityState() { return _dynamicEntityState; }

        /// <summary>Sets the state of the dynamic entity.</summary>
        /// <remarks>Using this method will reset modified fields list.</remarks>
        /// <param name="state">The state.</param>
        public virtual void SetDynamicEntityState(DynamicEntityState state)
        {
            _dynamicEntityState = state;

            if (_changedFields != null)
                _changedFields.Clear();
        }

        /// <summary>Called when object property is changing.</summary>
        /// <param name="propertyName">Name of the property.</param>
        /// <param name="oldValue">The old property value.</param>
        /// <param name="newValue">The new property value.</param>
        protected virtual void OnPropertyChanging(string propertyName, object oldValue, object newValue)
        {
            OnPropertyChanging(new DynamicPropertyChangingEventArgs(propertyName, oldValue, newValue));
        }

        /// <summary>Raises the <see cref="E:PropertyChanging"/> event.</summary>
        /// <param name="e">The <see cref="DynamicPropertyChangingEventArgs"/> instance containing the event data.</param>
        protected virtual void OnPropertyChanging(DynamicPropertyChangingEventArgs e)
        {
            _changedFields[e.PropertyName] = e.NewValue;
            if (PropertyChanging != null)
                PropertyChanging(this, e);
        }

        /// <summary>Validates this object instance.</summary>
        /// <returns>Returns list of <see cref="ValidationResult"/> containing results of validation.</returns>
        public virtual IList<ValidationResult> Validate()
        {
            return DynamicMapperCache.GetMapper(this.GetType()).ValidateObject(this);
        }

        /// <summary>Saves this object to database.</summary>
        /// <param name="database">The database.</param>
        /// <returns>Returns <c>true</c> if operation was successful.</returns>
        /// <exception cref="InvalidOperationException">Can be thrown when object is in invalid state.</exception>
        public virtual bool Save(DynamicDatabase database)
        {
            switch (GetDynamicEntityState())
            {
                default:
                case DynamicEntityState.Unknown:
                    throw new InvalidOperationException("Unknown object state. Unable to decide whish action should be performed.");

                case DynamicEntityState.New:
                    return Insert(database);

                case DynamicEntityState.Existing:
                    if (IsModified())
                        return Update(database);

                    return true;

                case DynamicEntityState.ToBeDeleted:
                    return Delete(database);

                case DynamicEntityState.Deleted:
                    throw new InvalidOperationException("Unable to do any database action on deleted object.");
            }
        }

        /// <summary>Determines whether this instance is in existing state and fields was modified since this state was set modified.</summary>
        /// <returns>Returns <c>true</c> if this instance is modified; otherwise, <c>false</c>.</returns>
        public virtual bool IsModified()
        {
            if (GetDynamicEntityState() != DynamicEntityState.Existing)
                return false;

            return _changedFields != null && _changedFields.Any();
        }

        #region Insert/Update/Delete

        /// <summary>Inserts this object to database.</summary>
        /// <param name="db">The database.</param>
        /// <returns>Returns <c>true</c> if operation was successful.</returns>
        public virtual bool Insert(DynamicDatabase db)
        {
            if (db.Insert(this.GetType())
                       .Values(x => this)
                       .Execute() > 0)
            {
                _changedFields.Clear();
                SetDynamicEntityState(DynamicEntityState.Existing);
                return true;
            }

            return false;
        }

        /// <summary>Updates this object in database.</summary>
        /// <param name="db">The database.</param>
        /// <returns>Returns <c>true</c> if operation was successful.</returns>
        public virtual bool Update(DynamicDatabase db)
        {
            var t = GetType();
            var mapper = DynamicMapperCache.GetMapper(t);
            var query = db.Update(t);

            MakeQueryWhere(mapper, query);

            if (_changedFields.Any())
            {
                bool any = false;

                foreach (var cf in _changedFields)
                {
                    var cn = mapper.PropertyMap[cf.Key];
                    var pm = mapper.ColumnsMap[cn.ToLower()];
                    if (pm.Ignore)
                        continue;

                    if (pm.Column != null)
                    {
                        if (pm.Column.IsKey)
                            continue;

                        if (!pm.Column.AllowNull && cf.Value == null)
                            continue;
                    }

                    query.Values(cn, cf.Value);
                    any = true;
                }

                if (!any)
                    query.Set(x => this);
            }
            else
                query.Set(x => this);

            if (query.Execute() == 0)
                return false;

            SetDynamicEntityState(DynamicEntityState.Existing);
            _changedFields.Clear();

            return true;
        }

        /// <summary>Deletes this object from database.</summary>
        /// <param name="db">The database.</param>
        /// <returns>Returns <c>true</c> if operation was successful.</returns>
        public virtual bool Delete(DynamicDatabase db)
        {
            var t = this.GetType();
            var mapper = DynamicMapperCache.GetMapper(t);

            var query = db.Delete(t);

            MakeQueryWhere(mapper, query);

            if (query.Execute() == 0)
                return false;

            SetDynamicEntityState(DynamicEntityState.Deleted);

            return true;
        }

        #endregion Insert/Update/Delete

        #region Select

        /// <summary>Refresh non key data from database.</summary>
        /// <param name="db">The database.</param>
        /// <remarks>All properties that are primary key values must be filled.</remarks>
        /// <returns>Returns <c>true</c> if operation was successful.</returns>
        public virtual bool Refresh(DynamicDatabase db)
        {
            var t = this.GetType();
            var mapper = DynamicMapperCache.GetMapper(t);
            var query = db.From(t);
            MakeQueryWhere(mapper, query);
            var o = (query.Execute() as IEnumerable<dynamic>).FirstOrDefault();

            if (o == null)
                return false;

            mapper.Map(o, this);

            SetDynamicEntityState(DynamicEntityState.Existing);
            _changedFields.Clear();

            return true;
        }

        #endregion Select

        #region Query Helpers

        private void MakeQueryWhere(DynamicTypeMap mapper, IDynamicUpdateQueryBuilder query)
        {
            bool keyNotDefined = true;

            foreach (var cm in mapper.ColumnsMap)
            {
                if (cm.Value.Column != null && cm.Value.Column.IsKey)
                {
                    query.Where(cm.Key, DynamicColumn.CompareOperator.Eq, cm.Value.Get(this));
                    keyNotDefined = false;
                }
            }

            if (keyNotDefined)
                throw new InvalidOperationException(String.Format("Class '{0}' have no key columns defined",
                    this.GetType().FullName));
        }

        private void MakeQueryWhere(DynamicTypeMap mapper, IDynamicDeleteQueryBuilder query)
        {
            bool keyNotDefined = true;

            foreach (var cm in mapper.ColumnsMap)
            {
                if (cm.Value.Column != null && cm.Value.Column.IsKey)
                {
                    query.Where(cm.Key, DynamicColumn.CompareOperator.Eq, cm.Value.Get(this));
                    keyNotDefined = false;
                }
            }

            if (keyNotDefined)
                throw new InvalidOperationException(String.Format("Class '{0}' have no key columns defined",
                    this.GetType().FullName));
        }

        private void MakeQueryWhere(DynamicTypeMap mapper, IDynamicSelectQueryBuilder query)
        {
            bool keyNotDefined = true;

            foreach (var cm in mapper.ColumnsMap)
            {
                if (cm.Value.Column != null && cm.Value.Column.IsKey)
                {
                    var v = cm.Value.Get(this);

                    if (v == null)
                        throw new InvalidOperationException(String.Format("Class '{0}' have key columns {1} not filled with data.",
                            this.GetType().FullName, cm.Value.Name));

                    query.Where(cm.Key, DynamicColumn.CompareOperator.Eq, cm.Value.Get(this));
                    keyNotDefined = false;
                }
            }

            if (keyNotDefined)
                throw new InvalidOperationException(String.Format("Class '{0}' have no key columns defined",
                    this.GetType().FullName));
        }

        #endregion Query Helpers
    }
}