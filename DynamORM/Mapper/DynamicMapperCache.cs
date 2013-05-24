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
using System.Collections.Generic;

namespace DynamORM.Mapper
{
    /// <summary>Class with mapper cache.</summary>
    public static class DynamicMapperCache
    {
        private static readonly object SyncLock = new object();
        private static Dictionary<Type, DynamicTypeMap> _cache = new Dictionary<Type, DynamicTypeMap>();

        /// <summary>Get type mapper.</summary>
        /// <typeparam name="T">Type of mapper.</typeparam>
        /// <returns>Type mapper.</returns>
        public static DynamicTypeMap GetMapper<T>()
        {
            return GetMapper(typeof(T));
        }

        /// <summary>Get type mapper.</summary>
        /// <param name="type">Type of mapper.</param>
        /// <returns>Type mapper.</returns>
        public static DynamicTypeMap GetMapper(Type type)
        {
            if (type == null)
                return null;
            /*if (type.IsAnonymous())
                return null;*/

            DynamicTypeMap mapper = null;

            lock (SyncLock)
            {
                if (!_cache.TryGetValue(type, out mapper))
                {
                    mapper = new DynamicTypeMap(type);

                    if (mapper != null)
                        _cache.Add(type, mapper);
                }
            }

            return mapper;
        }
    }
}