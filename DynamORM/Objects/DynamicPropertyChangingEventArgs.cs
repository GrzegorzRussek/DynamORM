using System;

namespace DynamORM.Objects
{
    /// <summary>Class containing changed property data.</summary>
    /// <seealso cref="System.EventArgs" />
    public class DynamicPropertyChangingEventArgs : EventArgs
    {
        /// <summary>Gets the name of the property.</summary>
        /// <value>The name of the property.</value>
        public string PropertyName { get; private set; }

        /// <summary>Gets the old property value.</summary>
        /// <value>The old value.</value>
        public object OldValue { get; private set; }

        /// <summary>Gets the new property value.</summary>
        /// <value>The new value.</value>
        public object NewValue { get; private set; }

        /// <summary>Initializes a new instance of the <see cref="DynamicPropertyChangingEventArgs"/> class.</summary>
        /// <param name="propertyName">Name of the property.</param>
        /// <param name="oldValue">The old property value.</param>
        /// <param name="newValue">The new property value.</param>
        public DynamicPropertyChangingEventArgs(string propertyName, object oldValue, object newValue)
        {
            PropertyName = propertyName;
            OldValue = oldValue;
            NewValue = newValue;
        }
    }
}