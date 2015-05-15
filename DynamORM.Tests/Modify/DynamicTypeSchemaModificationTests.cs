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

using System.Collections.Generic;
using DynamORM.Tests.Helpers;
using NUnit.Framework;

namespace DynamORM.Tests.Modify
{
    /// <summary>Test standard dynamic access ORM. With out schema information from database.</summary>
    [TestFixture]
    public class DynamicTypeSchemaModificationTests : DynamicModificationTests
    {
        /// <summary>Create table using specified method.</summary>
        /// <returns>Dynamic table.</returns>
        public override dynamic GetTestTable()
        {
            return Database.Table<users>();
        }

        /// <summary>
        /// Tests the bulk insert.
        /// </summary>
        [Test]
        public void TestBulkInsert()
        {
            Assert.AreEqual(2, Database.Insert<users>(new List<users>
            {
                new users
                {
                    id = 1001,
                    login = "a",
                },
                new users
                {
                    id = 1002,
                    login = "b",
                }
            }));

            Assert.AreEqual(2, Database.Delete<users>().Where(u => u.users.id.In(1001, 1002)).Execute());
        }
    }
}