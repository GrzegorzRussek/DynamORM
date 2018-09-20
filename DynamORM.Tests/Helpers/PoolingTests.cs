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

using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace DynamORM.Tests.Helpers
{
    /// <summary>Pooling tests.</summary>
    [TestClass]
    public class PoolingTests : TestsBase
    {
        /// <summary>Setup test parameters.</summary>
        [TestInitialize]
        public virtual void SetUp()
        {
            CreateTestDatabase();
        }

        /// <summary>Tear down test objects.</summary>
        [TestCleanup]
        public virtual void TearDown()
        {
            DestroyDynamicDatabase();
            DestroyTestDatabase();
        }

        /// <summary>Test single mode command disposing.</summary>
        [TestMethod]
        public void TestSingleModeCommand()
        {
            CreateDynamicDatabase();

            var cmd = Database.Open().CreateCommand();

            cmd.SetCommand("SELECT COUNT(0) FROM sqlite_master;");

            Database.Dispose();
            Database = null;

            Assert.ThrowsException<DynamicQueryException>(() => cmd.ExecuteScalar());
        }

        /// <summary>Test single mode transaction disposing.</summary>
        [TestMethod]
        public void TestSingleModeTransaction()
        {
            try
            {
                CreateDynamicDatabase();

                using (var conn = Database.Open())
                using (var trans = conn.BeginTransaction())
                using (var cmd = conn.CreateCommand())
                {
                    Assert.AreEqual(1, cmd.SetCommand("INSERT INTO \"users\" (\"code\") VALUES ('999');").ExecuteNonQuery());

                    Database.Dispose();
                    Database = null;

                    trans.Commit();
                }

                // Verify (rollback)
                CreateDynamicDatabase();
                Assert.AreEqual(0, Database.Table("users").Count(columns: "id", code: "999"));
            }
            finally
            {
                // Remove for next tests
                Database.Dispose();
                Database = null;
            }
        }
    }
}