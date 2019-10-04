namespace DynamORM.Objects
{
    /// <summary>Possible states of dynamic database objects.</summary>
    public enum DynamicEntityState
    {
        /// <summary>Default state. This state will only permit to refresh data from database.</summary>
        /// <remarks>In this state repository will be unable to tell if object with this state should be added 
        /// or updated in database, but you can still manually perform update or insert on such object.</remarks>
        Unknown,

        /// <summary>This state should be set to new objects in database.</summary>
        New,

        /// <summary>This state is ser when data is refreshed from database or object was loaded from repository.</summary>
        Existing,

        /// <summary>You can set this state to an object if you want repository to perform delete from database.</summary>
        ToBeDeleted,

        /// <summary>This state is set for objects that were deleted from database.</summary>
        Deleted,
    }
}