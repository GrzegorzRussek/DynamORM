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
using System.Collections;
using System.Collections.Generic;
using System.Dynamic;
using System.Collections.Concurrent;

namespace DynamORM
{
    /// <summary>Dynamic expando is a simple and temporary class to resolve memory leaks inside ExpandoObject.</summary>
    public class DynamicExpando : DynamicObject, IDictionary<string, object>, ICollection<KeyValuePair<string, object>>, IEnumerable<KeyValuePair<string, object>>, IEnumerable
    {
        private Dictionary<string, object> _data = new Dictionary<string, object>();

        /// <summary>Initializes a new instance of the <see cref="DynamicExpando"/> class.</summary>
        public DynamicExpando()
        {
        }

        /// <summary>Tries to get member value.</summary>
        /// <returns>Returns <c>true</c>, if get member was tryed, <c>false</c> otherwise.</returns>
        /// <param name="binder">The context binder.</param>
        /// <param name="result">The invokation result.</param>
        public override bool TryGetMember(GetMemberBinder binder, out object result)
        {
            result = _data.TryGetValue(binder.Name);

            return true;
        }

        /// <summary>Tries to set member.</summary>
        /// <returns>Returns <c>true</c>, if set member was tryed, <c>false</c> otherwise.</returns>
        /// <param name="binder">The context binder.</param>
        /// <param name="value">Value which will be set.</param>
        public override bool TrySetMember(SetMemberBinder binder, object value)
        {
            _data[binder.Name] = value;

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

        object IDictionary<string, object>.this [string index] { get { return _data[index]; } set { _data[index] = value; } }

        ICollection<string> IDictionary<string, object>.Keys { get { return _data.Keys; } }

        ICollection<object> IDictionary<string, object>.Values { get { return _data.Values; } }

        #endregion

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

        int ICollection<KeyValuePair<string, object>>.Count{ get { return _data.Count; } }

        bool ICollection<KeyValuePair<string, object>>.IsReadOnly { get { return ((ICollection<KeyValuePair<string, object>>)_data).IsReadOnly; } }

        #endregion

        #region IEnumerable implementation

        IEnumerator<KeyValuePair<string, object>> IEnumerable<KeyValuePair<string, object>>.GetEnumerator()
        {
            return _data.GetEnumerator();
        }

        #endregion

        #region IEnumerable implementation

        IEnumerator IEnumerable.GetEnumerator()
        {
            return ((IEnumerable)_data).GetEnumerator();
        }

        #endregion
    }
}

