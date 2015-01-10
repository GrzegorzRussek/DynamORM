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
using System.Data;
using System.Runtime.Serialization;

namespace DynamORM
{
    /// <summary>Dynamic query exception.</summary>
    [Serializable]
    public class DynamicQueryException : Exception, ISerializable
    {
        /// <summary>Initializes a new instance of the
        /// <see cref="DynamicQueryException"/> class.</summary>
        /// <param name="command">The command which failed.</param>
        public DynamicQueryException(IDbCommand command = null)
            : base(string.Format("Error executing command.{0}{1}", Environment.NewLine, command != null ? command.DumpToString() : string.Empty))
        {
        }

        /// <summary>Initializes a new instance of the
        /// <see cref="DynamicQueryException"/> class.</summary>
        /// <param name="message">The message.</param>
        /// <param name="command">The command which failed.</param>
        public DynamicQueryException(string message, IDbCommand command = null)
            : base(string.Format("{0}{1}{2}", message, Environment.NewLine, command != null ? command.DumpToString() : string.Empty))
        {
        }

        /// <summary>Initializes a new instance of the
        /// <see cref="DynamicQueryException"/> class.</summary>
        /// <param name="innerException">The inner exception.</param>
        /// <param name="command">The command which failed.</param>
        public DynamicQueryException(Exception innerException, IDbCommand command = null)
            : base(string.Format("Error executing command.{0}{1}", Environment.NewLine, command != null ? command.DumpToString() : string.Empty), innerException)
        {
        }

        /// <summary>Initializes a new instance of the
        /// <see cref="DynamicQueryException"/> class.</summary>
        /// <param name="message">The message.</param>
        /// <param name="innerException">The inner exception.</param>
        /// <param name="command">The command which failed.</param>
        public DynamicQueryException(string message, Exception innerException, IDbCommand command = null)
            : base(string.Format("{0}{1}{2}", message, Environment.NewLine, command != null ? command.DumpToString() : string.Empty), innerException)
        {
        }

        /// <summary>Initializes a new instance of the
        /// <see cref="DynamicQueryException"/> class.</summary>
        /// <param name="info">The <see cref="T:System.Runtime.Serialization.SerializationInfo" />
        /// that holds the serialized object data about the exception being thrown.</param>
        /// <param name="context">The <see cref="T:System.Runtime.Serialization.StreamingContext" />
        /// that contains contextual information about the source or destination.</param>
        public DynamicQueryException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
        }

        /// <summary>When overridden in a derived class, sets the
        /// <see cref="T:System.Runtime.Serialization.SerializationInfo" />
        /// with information about the exception.</summary>
        /// <param name="info">The <see cref="T:System.Runtime.Serialization.SerializationInfo" />
        /// that holds the serialized object data about the exception being thrown.</param>
        /// <param name="context">The <see cref="T:System.Runtime.Serialization.StreamingContext" />
        /// that contains contextual information about the source or destination.</param>
        /// <PermissionSet>
        ///   <IPermission class="System.Security.Permissions.FileIOPermission, mscorlib, Version=2.0.3600.0, Culture=neutral, PublicKeyToken=b77a5c561934e089" version="1" Read="*AllFiles*" PathDiscovery="*AllFiles*" />
        ///   <IPermission class="System.Security.Permissions.SecurityPermission, mscorlib, Version=2.0.3600.0, Culture=neutral, PublicKeyToken=b77a5c561934e089" version="1" Flags="SerializationFormatter" />
        /// </PermissionSet>
        public override void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            base.GetObjectData(info, context);
        }
    }
}