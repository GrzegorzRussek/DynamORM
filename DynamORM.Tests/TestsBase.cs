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
using System.Data;
using System.IO;
using SQLiteFactory =
#if MONO

 Mono.Data.Sqlite.SqliteFactory;

#else

 System.Data.SQLite.SQLiteFactory;

#endif

namespace DynamORM.Tests
{
    /// <summary>Basic test utilities.</summary>
    public class TestsBase
    {
        private string _dbpath = Path.GetTempFileName();

        /// <summary>Gets or sets <see cref="DynamicDatabase"/> instance.</summary>
        public DynamicDatabase Database { get; set; }

        #region ADO.NET initialization

        /// <summary>Prepare database with some fixed data for tests using plain old ADO.NET.</summary>
        public void CreateTestDatabase()
        {
            Console.Out.Write("Creating database at '{0}'...", _dbpath);

            using (IDbConnection conn = SQLiteFactory.Instance.CreateConnection())
            {
                conn.ConnectionString = string.Format("Data Source={0};", _dbpath);
                conn.Open();

                using (IDbTransaction trans = conn.BeginTransaction())
                {
                    using (IDbCommand cmd = conn.CreateCommand()
                        .SetCommand(Properties.Resources.UsersTable)
                        .SetTransaction(trans))
                        cmd.ExecuteNonQuery();

                    trans.Commit();
                }
            }

            Console.Out.WriteLine(" Done.");
        }

        /// <summary>Delete test database file.</summary>
        public void DestroyTestDatabase()
        {
            File.Delete(_dbpath);
        }

        #endregion ADO.NET initialization

        #region DynamORM Initialization

        /// <summary>Create <see cref="DynamicDatabase"/> with default options for SQLite.</summary>
        public void CreateDynamicDatabase()
        {
            CreateDynamicDatabase(
                DynamicDatabaseOptions.SingleConnection |
                DynamicDatabaseOptions.SingleTransaction |
                DynamicDatabaseOptions.SupportLimitOffset |
                DynamicDatabaseOptions.SupportSchema);
        }

        /// <summary>Create <see cref="DynamicDatabase"/> with specified options.</summary>
        /// <param name="options">Database options.</param>
        public void CreateDynamicDatabase(DynamicDatabaseOptions options)
        {
            Database = new DynamicDatabase(SQLiteFactory.Instance,
                string.Format("Data Source={0};", _dbpath), options)
                {
                    DumpCommands = true
                };
        }

        /// <summary>Dispose <see cref="DynamicDatabase"/> (and rollback if transaction exist).</summary>
        public void DestroyDynamicDatabase()
        {
            if (Database != null)
                Database.Dispose();
        }

        #endregion DynamORM Initialization
    }
}