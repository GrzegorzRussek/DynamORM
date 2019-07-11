/*
 * DynamORM - Dynamic Object-Relational Mapping library.
 * Copyright (c) 2012-2015, Grzegorz Russek (grzegorz.russek@gmail.com)
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
using System.Collections;
using System.Collections.Generic;
using System.Dynamic;

namespace DynamORM
{
    /// <summary>Dynamic expando is a simple and temporary class to resolve memory leaks inside ExpandoObject.</summary>
    public class DynamicExpando : DynamicObject, IDictionary<string, object>, ICollection<KeyValuePair<string, object>>, IEnumerable<KeyValuePair<string, object>>, IEnumerable
    {
        /// <summary>Class containing information about last accessed property of dynamic object.</summary>
        public class PropertyAccess
        {
            /// <summary>Enum describing type of access to object.</summary>
            public enum TypeOfAccess
            {
                /// <summary>Get member.</summary>
                Get,

                /// <summary>Set member.</summary>
                Set,
            }

            /// <summary>Gets the type of operation.</summary>
            public TypeOfAccess Operation { get; internal set; }

            /// <summary>Gets the name of property.</summary>
            public string Name { get; internal set; }

            /// <summary>Gets the type from binder.</summary>
            public Type RequestedType { get; internal set; }

            /// <summary>Gets the type of value stored in object.</summary>
            public Type Type { get; internal set; }

            /// <summary>Gets the value stored in object.</summary>
            public object Value { get; internal set; }

            /// <summary>Gets the last access time.</summary>
            public long Ticks { get; internal set; }
        }

        private Dictionary<string, object> _data = new Dictionary<string, object>();

        private PropertyAccess _lastProp = new PropertyAccess();

        /// <summary>Initializes a new instance of the <see cref="DynamicExpando"/> class.</summary>
        public DynamicExpando()
        {
        }

        /// <summary>Gets the last accesses property.</summary>
        /// <returns>Description of last accessed property.</returns>
        public PropertyAccess GetLastAccessesProperty()
        {
            return _lastProp;
        }

        /// <summary>Tries to get member value.</summary>
        /// <returns>Returns <c>true</c>, if get member was tried, <c>false</c> otherwise.</returns>
        /// <param name="binder">The context binder.</param>
        /// <param name="result">The invocation result.</param>
        public override bool TryGetMember(GetMemberBinder binder, out object result)
        {
            result = _data.TryGetValue(binder.Name);

            _lastProp.Operation = PropertyAccess.TypeOfAccess.Get;
            _lastProp.RequestedType = binder.ReturnType;
            _lastProp.Name = binder.Name;
            _lastProp.Value = result;
            _lastProp.Type = result == null ? typeof(void) : result.GetType();
            _lastProp.Ticks = DateTime.Now.Ticks;

            return true;
        }

        /// <summary>Tries to set member.</summary>
        /// <returns>Returns <c>true</c>, if set member was tried, <c>false</c> otherwise.</returns>
        /// <param name="binder">The context binder.</param>
        /// <param name="value">Value which will be set.</param>
        public override bool TrySetMember(SetMemberBinder binder, object value)
        {
            _data[binder.Name] = value;

            _lastProp.Operation = PropertyAccess.TypeOfAccess.Set;
            _lastProp.RequestedType = binder.ReturnType;
            _lastProp.Name = binder.Name;
            _lastProp.Value = value;
            _lastProp.Type = value == null ? typeof(void) : value.GetType();
            _lastProp.Ticks = DateTime.Now.Ticks;

            return true;
        }

        #region IDictionary implementation

        bool IDictionary<string, object>.ContainsKey(string key)
        {
            return _data.ContainsKey(key);
        }

        void IDictionary<string, object>.Add(string key, object value)
        {
            _data.Add(key, value);
        }

        bool IDictionary<string, object>.Remove(string key)
        {
            return _data.Remove(key);
        }

        bool IDictionary<string, object>.TryGetValue(string key, out object value)
        {
            return _data.TryGetValue(key, out value);
        }

        object IDictionary<string, object>.this[string index] { get { return _data[index]; } set { _data[index] = value; } }

        ICollection<string> IDictionary<string, object>.Keys { get { return _data.Keys; } }

        ICollection<object> IDictionary<string, object>.Values { get { return _data.Values; } }

        #endregion IDictionary implementation

        #region ICollection implementation

        void ICollection<KeyValuePair<string, object>>.Add(KeyValuePair<string, object> item)
        {
            ((ICollection<KeyValuePair<string, object>>)_data).Add(item);
        }

        void ICollection<KeyValuePair<string, object>>.Clear()
        {
            _data.Clear();
        }

        bool ICollection<KeyValuePair<string, object>>.Contains(KeyValuePair<string, object> item)
        {
            return ((ICollection<KeyValuePair<string, object>>)_data).Contains(item);
        }

        void ICollection<KeyValuePair<string, object>>.CopyTo(KeyValuePair<string, object>[] array, int arrayIndex)
        {
            ((ICollection<KeyValuePair<string, object>>)_data).CopyTo(array, arrayIndex);
        }

        bool ICollection<KeyValuePair<string, object>>.Remove(KeyValuePair<string, object> item)
        {
            return ((ICollection<KeyValuePair<string, object>>)_data).Remove(item);
        }

        int ICollection<KeyValuePair<string, object>>.Count { get { return _data.Count; } }

        bool ICollection<KeyValuePair<string, object>>.IsReadOnly { get { return ((ICollection<KeyValuePair<string, object>>)_data).IsReadOnly; } }

        #endregion ICollection implementation

        #region IEnumerable implementation

        IEnumerator<KeyValuePair<string, object>> IEnumerable<KeyValuePair<string, object>>.GetEnumerator()
        {
            return _data.GetEnumerator();
        }

        #endregion IEnumerable implementation

        #region IEnumerable implementation

        IEnumerator IEnumerable.GetEnumerator()
        {
            return ((IEnumerable)_data).GetEnumerator();
        }

        #endregion IEnumerable implementation
    }
}