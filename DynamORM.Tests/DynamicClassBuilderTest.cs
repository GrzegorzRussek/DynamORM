using System;
using System.Collections;

namespace DynamORM.Tests
{
    public class DynamicProduct : IDictionary
    {
        private IDictionary _dict = null;

        public DynamicProduct(IDictionary dict)
        {
            _dict = dict;
        }

        // Properties from dict
        public int ID
        {
            get { return (int)(_dict["ID"] ?? 0); }
            set
            {
                if (!IsReadOnly)
                    _dict["ID"] = value;
            }
        }

        public string Name
        {
            get { return (string)(_dict["Name"] ?? 0); }
            set
            {
                if (!IsReadOnly)
                    _dict["Name"] = value;
            }
        }

        public DateTime Delivery
        {
            get { return (DateTime)(_dict["Delivery"] ?? 0); }
            set
            {
                if (!IsReadOnly)
                    _dict["Delivery"] = value;
            }
        }

        public object Data
        {
            get { return (object)(_dict["Data"] ?? 0); }
            set
            {
                if (!IsReadOnly)
                    _dict["Data"] = value;
            }
        }

        // IDictionary implementation
        public void Add(object key, object value)
        {
            _dict.Add(key, value);
        }

        public void Clear()
        {
            _dict.Clear();
        }

        public bool Contains(object key)
        {
            return _dict.Contains(key);
        }

        public IDictionaryEnumerator GetEnumerator()
        {
            return GetEnumerator();
        }

        public void Remove(object key)
        {
            _dict.Remove(key);
        }

        public void CopyTo(System.Array array, int index)
        {
            _dict.CopyTo(array, index);
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return _dict.GetEnumerator();
        }

        public bool IsFixedSize { get { return _dict.IsFixedSize; } }

        public bool IsReadOnly { get { return _dict.IsReadOnly; } }

        public ICollection Keys { get { return _dict.Keys; } }

        public ICollection Values { get { return _dict.Values; } }

        public object this[object key] { get { return _dict[key]; } set { _dict[key] = value; } }

        public int Count { get { return _dict.Count; } }

        public bool IsSynchronized { get { return _dict.IsSynchronized; } }

        public object SyncRoot { get { return _dict.SyncRoot; } }
    }
}