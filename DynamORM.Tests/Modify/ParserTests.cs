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

using System.Linq;
using DynamORM.Builders;
using DynamORM.Builders.Implementation;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using DynamORM.Tests.Helpers;
using System.Collections.Generic;

namespace DynamORM.Tests.Modify
{
    /// <summary>New parser tests.</summary>
    [TestClass]
    public class ParserTests : TestsBase
    {
        /// <summary>Setup test parameters.</summary>
        [TestInitialize]
        public virtual void SetUp()
        {
            CreateTestDatabase();
            CreateDynamicDatabase(
                DynamicDatabaseOptions.SingleConnection |
                DynamicDatabaseOptions.SingleTransaction |
                DynamicDatabaseOptions.SupportLimitOffset);
        }

        /// <summary>Tear down test objects.</summary>
        [TestCleanup]
        public virtual void TearDown()
        {
            DestroyDynamicDatabase();
            DestroyTestDatabase();
        }

        #region Insert

        /// <summary>
        /// Tests the basic insert.
        /// </summary>
        [TestMethod]
        public void TestInsertBasic()
        {
            IDynamicInsertQueryBuilder cmd = new DynamicInsertQueryBuilder(Database, "Users");

            cmd.Values(x => x.Users.Code = "001", x => x.Users.Name = "Admin", x => x.Users.IsAdmin = 1);

            Assert.AreEqual(string.Format(@"INSERT INTO ""Users"" (""Code"", ""Name"", ""IsAdmin"") VALUES ({0})",
                string.Join(", ", cmd.Parameters.Keys.Select(p => string.Format("[${0}]", p)))), cmd.CommandText());
        }

        /// <summary>
        /// Tests the insert with sub query.
        /// </summary>
        [TestMethod]
        public void TestInsertSubQuery()
        {
            IDynamicInsertQueryBuilder cmd = new DynamicInsertQueryBuilder(Database, "Users");

            cmd.Values(x => x.Code = "001", x => x.Name = "Admin", x => x.IsAdmin = x(cmd
                .SubQuery(a => a.AccessRights.As(a.a))
                .Select(a => a.IsAdmin)
                .Where(a => a.User_Id == "001")));

            Assert.AreEqual(string.Format(@"INSERT INTO ""Users"" (""Code"", ""Name"", ""IsAdmin"") VALUES ({0}, (SELECT a.""IsAdmin"" FROM ""AccessRights"" AS a WHERE (a.""User_Id"" = [${1}])))",
                string.Join(", ", cmd.Parameters.Keys.Take(2).Select(p => string.Format("[${0}]", p))), cmd.Parameters.Keys.Last()), cmd.CommandText());
        }

        /// <summary>
        /// Tests the basic insert using object.
        /// </summary>
        [TestMethod]
        public void TestInsertBasicObject()
        {
            IDynamicInsertQueryBuilder cmd = new DynamicInsertQueryBuilder(Database, "Users");

            cmd.Values(x => new { Code = "001", Name = "Admin", IsAdmin = 1 });

            Assert.AreEqual(string.Format(@"INSERT INTO ""Users"" (""Code"", ""Name"", ""IsAdmin"") VALUES ({0})",
                string.Join(", ", cmd.Parameters.Keys.Select(p => string.Format("[${0}]", p)))), cmd.CommandText());
        }

        /// <summary>
        /// Tests the insert using object with sub query.
        /// </summary>
        [TestMethod]
        public void TestInsertSubQueryObject()
        {
            IDynamicInsertQueryBuilder cmd = new DynamicInsertQueryBuilder(Database, "Users");

            cmd.Values(x => new
            {
                Code = "001",
                Name = "Admin",
                IsAdmin = x(cmd
                    .SubQuery(a => a.AccessRights.As(a.a))
                    .Select(a => a.IsAdmin)
                    .Where(a => a.User_Id == "001"))
            });

            Assert.AreEqual(string.Format(@"INSERT INTO ""Users"" (""Code"", ""Name"", ""IsAdmin"") VALUES ({0}, (SELECT a.""IsAdmin"" FROM ""AccessRights"" AS a WHERE (a.""User_Id"" = [${1}])))",
                string.Join(", ", cmd.Parameters.Keys.Take(2).Select(p => string.Format("[${0}]", p))), cmd.Parameters.Keys.Last()), cmd.CommandText());
        }

        #endregion Insert

        #region Update

        /// <summary>
        /// Tests the basic update.
        /// </summary>
        [TestMethod]
        public void TestUpdateBasic()
        {
            IDynamicUpdateQueryBuilder cmd = new DynamicUpdateQueryBuilder(Database, "Users");

            cmd.Set(x => x.Users.Code = "001", x => x.Users.Name = "Admin", x => x.Users.IsAdmin = 1)
                .Where(x => x.Users.Id_User == 1);

            Assert.AreEqual(string.Format(@"UPDATE ""Users"" SET ""Code"" = [${0}], ""Name"" = [${1}], ""IsAdmin"" = [${2}] WHERE (""Users"".""Id_User"" = [${3}])",
                cmd.Parameters.Keys.ToArray()[0], cmd.Parameters.Keys.ToArray()[1], cmd.Parameters.Keys.ToArray()[2], cmd.Parameters.Keys.ToArray()[3]), cmd.CommandText());
        }

        /// <summary>
        /// Tests the insert with sub query.
        /// </summary>
        [TestMethod]
        public void TestUpdateSubQuery()
        {
            IDynamicUpdateQueryBuilder cmd = new DynamicUpdateQueryBuilder(Database, "Users");

            cmd.Set(x => x.Users.Code = "001", x => x.Users.Name = "Admin", x => x.Users.IsAdmin = x(cmd
                    .SubQuery(a => a.AccessRights.As(a.a))
                    .Select(a => a.IsAdmin)
                    .Where(a => a.User_Id == a.Users.Id_User)))
                .Where(x => x.Users.Id_User == 1);

            Assert.AreEqual(string.Format(@"UPDATE ""Users"" SET ""Code"" = [${0}], ""Name"" = [${1}], ""IsAdmin"" = (SELECT a.""IsAdmin"" FROM ""AccessRights"" AS a WHERE (a.""User_Id"" = ""Users"".""Id_User"")) WHERE (""Users"".""Id_User"" = [${2}])",
                cmd.Parameters.Keys.ToArray()[0], cmd.Parameters.Keys.ToArray()[1], cmd.Parameters.Keys.ToArray()[2]), cmd.CommandText());
        }

        /// <summary>
        /// Tests the basic insert using object.
        /// </summary>
        [TestMethod]
        public void TestUpdateBasicObject()
        {
            IDynamicUpdateQueryBuilder cmd = new DynamicUpdateQueryBuilder(Database, "Users");

            cmd.Set(x => new { Code = "001", Name = "Admin", IsAdmin = 1 })
                .Where(x => new { Id_User = 1 });

            Assert.AreEqual(string.Format(@"UPDATE ""Users"" SET ""Code"" = [${0}], ""Name"" = [${1}], ""IsAdmin"" = [${2}] WHERE (""Id_User"" = [${3}])",
                cmd.Parameters.Keys.ToArray()[0], cmd.Parameters.Keys.ToArray()[1], cmd.Parameters.Keys.ToArray()[2], cmd.Parameters.Keys.ToArray()[3]), cmd.CommandText());
        }

        /// <summary>
        /// Tests the basic insert using object.
        /// </summary>
        [TestMethod]
        public void TestUpdateSubQueryObject()
        {
            IDynamicUpdateQueryBuilder cmd = new DynamicUpdateQueryBuilder(Database, "Users");

            cmd.Set(x => new
            {
                Code = "001",
                Name = "Admin",
                IsAdmin = x(cmd
                    .SubQuery(a => a.AccessRights.As(a.a))
                    .Select(a => a.IsAdmin)
                    .Where(a => a.User_Id == a.Users.Id_User))
            }).Where(x => new { Id_User = 1 });

            Assert.AreEqual(string.Format(@"UPDATE ""Users"" SET ""Code"" = [${0}], ""Name"" = [${1}], ""IsAdmin"" = (SELECT a.""IsAdmin"" FROM ""AccessRights"" AS a WHERE (a.""User_Id"" = ""Users"".""Id_User"")) WHERE (""Id_User"" = [${2}])",
                cmd.Parameters.Keys.ToArray()[0], cmd.Parameters.Keys.ToArray()[1], cmd.Parameters.Keys.ToArray()[2]), cmd.CommandText());
        }

        #endregion Update
    }
}