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

namespace DynamORM
{
    /// <summary>Represents database connection options.</summary>
    [Flags]
    [System.Reflection.ObfuscationAttribute(Feature = "renaming", ApplyToMembers = true)]
    public enum DynamicDatabaseOptions
    {
        /// <summary>No specific options.</summary>
        None = 0x00000000,

        /// <summary>Only single persistent database connection.</summary>
        SingleConnection = 0x00000001,

        /// <summary>Only one transaction.</summary>
        SingleTransaction = 0x00000002,

        /// <summary>Database supports top syntax (SELECT TOP x ... FROM ...).</summary>
        SupportTop = 0x00000080,

        /// <summary>Database supports limit offset syntax (SELECT ... FROM ... LIMIT x OFFSET y).</summary>
        SupportLimitOffset = 0x00000040,

        /// <summary>Database supports limit offset syntax (SELECT FIRST x SKIP y ... FROM ...).</summary>
        SupportFirstSkip = 0x00000020,

        /// <summary>Database support standard schema.</summary>
        SupportSchema = 0x00000010,

        /// <summary>Database support stored procedures (EXEC procedure ...).</summary>
        SupportStoredProcedures = 0x00000100,

        /// <summary>Database support with no lock syntax.</summary>
        SupportNoLock = 0x00001000,

        /// <summary>Debug option allowing to enable command dumps by default.</summary>
        DumpCommands = 0x01000000,
    }
}