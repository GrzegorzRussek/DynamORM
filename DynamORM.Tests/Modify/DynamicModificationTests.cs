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
using DynamORM.Tests.Helpers;
using NUnit.Framework;

namespace DynamORM.Tests.Modify
{
    /// <summary>Test standard dynamic access ORM.</summary>
    [TestFixture]
    public class DynamicModificationTests : TestsBase
    {
        /// <summary>Setup test parameters.</summary>
        [TestFixtureSetUp]
        public virtual void SetUp()
        {
            CreateTestDatabase();
            CreateDynamicDatabase();
        }

        /// <summary>Tear down test objects.</summary>
        [TestFixtureTearDown]
        public virtual void TearDown()
        {
            DestroyDynamicDatabase();
            DestroyTestDatabase();
        }

        /// <summary>Create table using specified method.</summary>
        /// <returns>Dynamic table.</returns>
        public virtual dynamic GetTestTable()
        {
            return Database.Table("users");
        }

        #region Insert

        /// <summary>Test row insertion by dynamic arguments.</summary>
        [Test]
        public void TestInsertByArguments()
        {
            Assert.AreEqual(1, GetTestTable().Insert(code: "201", first: null, last: "Gagarin", email: "juri.gagarin@megacorp.com", quote: "bla, bla, bla"));

            // Verify
            var o = GetTestTable().Single(code: "201");
            Assert.Less(200, o.id);
            Assert.AreEqual("201", o.code.ToString());
            Assert.AreEqual(null, o.first);
            Assert.AreEqual("Gagarin", o.last);
            Assert.AreEqual("juri.gagarin@megacorp.com", o.email);
            Assert.AreEqual("bla, bla, bla", o.quote);
            Assert.AreEqual(null, o.password);
        }

        /// <summary>Test row insertion by dynamic object.</summary>
        [Test]
        public void TestInsertByDynamicObjects()
        {
            Assert.AreEqual(1, GetTestTable().Insert(values: new { code = "202", first = DBNull.Value, last = "Gagarin", email = "juri.gagarin@megacorp.com", quote = "bla, bla, bla" }));

            // Verify
            var o = GetTestTable().Single(code: "202");
            Assert.Less(200, o.id);
            Assert.AreEqual("202", o.code.ToString());
            Assert.AreEqual(null, o.first);
            Assert.AreEqual("Gagarin", o.last);
            Assert.AreEqual("juri.gagarin@megacorp.com", o.email);
            Assert.AreEqual("bla, bla, bla", o.quote);
            Assert.AreEqual(null, o.password);
        }

        /// <summary>Test row insertion by mapped object.</summary>
        [Test]
        public void TestInsertByMappedObject()
        {
            var u = GetTestTable();

            Assert.AreEqual(1, u.Insert(values: new Users
            {
                Id = u.Max(columns: "id") + 1,
                Code = "203",
                First = null,
                Last = "Gagarin",
                Email = "juri.gagarin@megacorp.com",
                Quote = "bla, bla, bla"
            }));

            // Verify
            var o = u.Single(code: "203");
            Assert.Less(200, o.id);
            Assert.AreEqual("203", o.code.ToString());
            Assert.AreEqual(null, o.first);
            Assert.AreEqual("Gagarin", o.last);
            Assert.AreEqual("juri.gagarin@megacorp.com", o.email);
            Assert.AreEqual("bla, bla, bla", o.quote);
            Assert.AreEqual(null, o.password);
        }

        /// <summary>Test row insertion by basic object.</summary>
        [Test]
        public void TestInsertByBasicObject()
        {
            var u = GetTestTable();

            Assert.AreEqual(1, u.Insert(values: new users
            {
                id = u.Max(columns: "id") + 1,
                code = "204",
                first = null,
                last = "Gagarin",
                email = "juri.gagarin@megacorp.com",
                quote = "bla, bla, bla"
            }));

            // Verify
            var o = u.Single(code: "204");
            Assert.Less(200, o.id);
            Assert.AreEqual("204", o.code.ToString());
            Assert.AreEqual(null, o.first);
            Assert.AreEqual("Gagarin", o.last);
            Assert.AreEqual("juri.gagarin@megacorp.com", o.email);
            Assert.AreEqual("bla, bla, bla", o.quote);
            Assert.AreEqual(null, o.password);
        }

        #endregion Insert

        #region Update

        /// <summary>Test row updating by dynamic arguments.</summary>
        [Test]
        public void TestUpdateByArguments()
        {
            Assert.AreEqual(1, GetTestTable().Update(id: 1, code: "201", first: null, last: "Gagarin", email: "juri.gagarin@megacorp.com", quote: "bla, bla, bla"));

            // Verify
            var o = GetTestTable().Single(code: "201");
            Assert.AreEqual(1, o.id);
            Assert.AreEqual("201", o.code.ToString());
            Assert.AreEqual(null, o.first);
            Assert.AreEqual("Gagarin", o.last);
            Assert.AreEqual("juri.gagarin@megacorp.com", o.email);
            Assert.AreEqual("bla, bla, bla", o.quote);
            Assert.AreEqual(null, o.password);
        }

        /// <summary>Test row updating by dynamic objects.</summary>
        [Test]
        public void TestUpdateByDynamicObject()
        {
            Assert.AreEqual(1, GetTestTable().Update(update: new { id = 2, code = "202", first = DBNull.Value, last = "Gagarin", email = "juri.gagarin@megacorp.com", quote = "bla, bla, bla" }));

            // Verify
            var o = GetTestTable().Single(code: "202");
            Assert.AreEqual(2, o.id);
            Assert.AreEqual("202", o.code.ToString());
            Assert.AreEqual(null, o.first);
            Assert.AreEqual("Gagarin", o.last);
            Assert.AreEqual("juri.gagarin@megacorp.com", o.email);
            Assert.AreEqual("bla, bla, bla", o.quote);
            Assert.AreEqual(null, o.password);
        }

        /// <summary>Test row updating by mapped object.</summary>
        [Test]
        public void TestUpdateByMappedObject()
        {
            var u = GetTestTable();

            Assert.AreEqual(1, u.Update(update: new Users
            {
                Id = 3,
                Code = "203",
                First = null,
                Last = "Gagarin",
                Email = "juri.gagarin@megacorp.com",
                Quote = "bla, bla, bla"
            }));

            // Verify
            var o = u.Single(code: "203");
            Assert.AreEqual(3, o.id);
            Assert.AreEqual("203", o.code.ToString());
            Assert.AreEqual(null, o.first);
            Assert.AreEqual("Gagarin", o.last);
            Assert.AreEqual("juri.gagarin@megacorp.com", o.email);
            Assert.AreEqual("bla, bla, bla", o.quote);
            Assert.AreEqual(null, o.password);
        }

        /// <summary>Test row updating by basic object.</summary>
        [Test]
        public void TestUpdateByBasicObject()
        {
            var u = GetTestTable();

            Assert.AreEqual(1, u.Update(update: new users
            {
                id = 4,
                code = "204",
                first = null,
                last = "Gagarin",
                email = "juri.gagarin@megacorp.com",
                quote = "bla, bla, bla"
            }));

            // Verify
            var o = u.Single(code: "204");
            Assert.AreEqual(4, o.id);
            Assert.AreEqual("204", o.code.ToString());
            Assert.AreEqual(null, o.first);
            Assert.AreEqual("Gagarin", o.last);
            Assert.AreEqual("juri.gagarin@megacorp.com", o.email);
            Assert.AreEqual("bla, bla, bla", o.quote);
            Assert.AreEqual(null, o.password);
        }

        /// <summary>Test row updating by dynamic objects.</summary>
        [Test]
        public void TestUpdateByDynamicObjects()
        {
            Assert.AreEqual(1, GetTestTable().Update(values: new { code = "205", first = DBNull.Value, last = "Gagarin", email = "juri.gagarin@megacorp.com", quote = "bla, bla, bla" }, where: new { id = 5 }));

            // Verify
            var o = GetTestTable().Single(code: "205");
            Assert.AreEqual(5, o.id);
            Assert.AreEqual("205", o.code.ToString());
            Assert.AreEqual(null, o.first);
            Assert.AreEqual("Gagarin", o.last);
            Assert.AreEqual("juri.gagarin@megacorp.com", o.email);
            Assert.AreEqual("bla, bla, bla", o.quote);
            Assert.AreEqual(null, o.password);
        }

        /// <summary>Test row updating by mapped objects.</summary>
        [Test]
        public void TestUpdateByMappedObjects()
        {
            var u = GetTestTable();

            Assert.AreEqual(1, u.Update(values: new Users
            {
                Id = 6,
                Code = "206",
                First = null,
                Last = "Gagarin",
                Email = "juri.gagarin@megacorp.com",
                Quote = "bla, bla, bla"
            }, id: 6));

            // Verify
            var o = u.Single(code: "206");
            Assert.AreEqual(6, o.id);
            Assert.AreEqual("206", o.code.ToString());
            Assert.AreEqual(null, o.first);
            Assert.AreEqual("Gagarin", o.last);
            Assert.AreEqual("juri.gagarin@megacorp.com", o.email);
            Assert.AreEqual("bla, bla, bla", o.quote);
            Assert.AreEqual(null, o.password);
        }

        /// <summary>Test row updating by basic objects.</summary>
        [Test]
        public void TestUpdateByBasicObjects()
        {
            var u = GetTestTable();

            Assert.AreEqual(1, u.Update(values: new users
            {
                id = 7,
                code = "207",
                first = null,
                last = "Gagarin",
                email = "juri.gagarin@megacorp.com",
                quote = "bla, bla, bla"
            }, id: 7));

            // Verify
            var o = u.Single(code: "207");
            Assert.AreEqual(7, o.id);
            Assert.AreEqual("207", o.code.ToString());
            Assert.AreEqual(null, o.first);
            Assert.AreEqual("Gagarin", o.last);
            Assert.AreEqual("juri.gagarin@megacorp.com", o.email);
            Assert.AreEqual("bla, bla, bla", o.quote);
            Assert.AreEqual(null, o.password);
        }

        #endregion Update

        #region Delete

        /// <summary>Test row deleting by dynamic arguments.</summary>
        [Test]
        public void TestDeleteByArguments()
        {
            Assert.AreEqual(1, GetTestTable().Delete(code: "10"));

            // Verify
            Assert.AreEqual(0, GetTestTable().Count(code: "10"));
        }

        /// <summary>Test row deleting by dynamic objects (all except ID should be ignored).</summary>
        [Test]
        public void TestDeleteyDynamicObject()
        {
            Assert.AreEqual(1, GetTestTable().Delete(delete: new { id = 11, code = 11, first = "Juri", last = "Gagarin", email = "juri.gagarin@megacorp.com", quote = "bla, bla, bla" }));

            // Verify
            Assert.AreEqual(0, GetTestTable().Count(id: 11));
        }

        /// <summary>Test row deleting by mapped object.</summary>
        [Test]
        public void TestDeleteByMappedObject()
        {
            var u = GetTestTable();

            Assert.AreEqual(1, u.Delete(delete: new Users
            {
                Id = 12,
                Code = "12",
                First = "Juri",
                Last = "Gagarin",
                Email = "juri.gagarin@megacorp.com",
                Quote = "bla, bla, bla"
            }));

            // Verify
            Assert.AreEqual(0, GetTestTable().Count(id: 12));
        }

        /// <summary>Test row deleting by basic object.</summary>
        [Test]
        public void TestDeleteByBasicObject()
        {
            var u = GetTestTable();

            Assert.AreEqual(1, u.Delete(delete: new users
            {
                id = 13,
                code = "13",
                first = "Juri",
                last = "Gagarin",
                email = "juri.gagarin@megacorp.com",
                quote = "bla, bla, bla"
            }));

            // Verify
            Assert.AreEqual(0, GetTestTable().Count(id: 13));
        }

        /// <summary>Test row deleting by dynamic objects (all except ID should be ignored).</summary>
        [Test]
        public void TestDeleteyDynamicObjectWhere()
        {
            Assert.AreEqual(1, GetTestTable().Delete(where: new { id = 14, code = "14" }));

            // Verify
            Assert.AreEqual(0, GetTestTable().Count(id: 14));
        }

        #endregion Delete
    }
}