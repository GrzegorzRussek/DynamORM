﻿/*
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

namespace DynamORM.Tests.Select
{
    /// <summary>
    /// New parser tests.
    /// </summary>
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
                DynamicDatabaseOptions.SupportLimitOffset |
                DynamicDatabaseOptions.SupportNoLock);
        }

        /// <summary>Tear down test objects.</summary>
        [TestCleanup]
        public virtual void TearDown()
        {
            try
            {
                DestroyDynamicDatabase();
                DestroyTestDatabase();
            }
            catch { }
        }

        /// <summary>
        /// Tests from method.
        /// </summary>
        [TestMethod]
        public void TestFromGet()
        {
            IDynamicSelectQueryBuilder cmd = new DynamicSelectQueryBuilder(Database);

            cmd.From(u => u.dbo.Users);
            Assert.AreEqual("SELECT * FROM \"dbo\".\"Users\"", cmd.CommandText());
        }

        /// <summary>
        /// Tests from method with multi tables.
        /// </summary>
        [TestMethod]
        public void TestFromGetMultiKulti()
        {
            IDynamicSelectQueryBuilder cmd = new DynamicSelectQueryBuilder(Database);

            cmd.From(u => u.dbo.Users, c => c.Clients);
            Assert.AreEqual("SELECT * FROM \"dbo\".\"Users\", \"Clients\"", cmd.CommandText());
        }

        /// <summary>
        /// Tests from method with as expression in text.
        /// </summary>
        [TestMethod]
        public void TestFromGetAs1()
        {
            IDynamicSelectQueryBuilder cmd = new DynamicSelectQueryBuilder(Database);

            cmd.From(u => u.dbo.Users.As("c"));
            Assert.AreEqual("SELECT * FROM \"dbo\".\"Users\" AS c", cmd.CommandText());
        }

        /// <summary>
        /// Tests from method with as expression in text.
        /// </summary>
        [TestMethod]
        public void TestFromGetAsNoLock1()
        {
            IDynamicSelectQueryBuilder cmd = new DynamicSelectQueryBuilder(Database);

            cmd.From(u => u.dbo.Users.As("c").NoLock());
            Assert.AreEqual("SELECT * FROM \"dbo\".\"Users\" AS c WITH(NOLOCK)", cmd.CommandText());
        }

        /// <summary>
        /// Tests from method with as expression using lambda.
        /// </summary>
        [TestMethod]
        public void TestFromGetAs2()
        {
            IDynamicSelectQueryBuilder cmd = new DynamicSelectQueryBuilder(Database);

            cmd.From(u => u.dbo.Users.As(u.c));
            Assert.AreEqual("SELECT * FROM \"dbo\".\"Users\" AS c", cmd.CommandText());
        }

        /// <summary>
        /// Tests from method using text.
        /// </summary>
        [TestMethod]
        public void TestFromText()
        {
            IDynamicSelectQueryBuilder cmd = new DynamicSelectQueryBuilder(Database);

            cmd.From(u => "dbo.Users");
            Assert.AreEqual("SELECT * FROM \"dbo\".\"Users\"", cmd.CommandText());
        }

        /// <summary>
        /// Tests from method using text with decorators.
        /// </summary>
        [TestMethod]
        public void TestFromDecoratedText()
        {
            IDynamicSelectQueryBuilder cmd = new DynamicSelectQueryBuilder(Database);

            cmd.From(u => "\"dbo\".\"Users\"");
            Assert.AreEqual("SELECT * FROM \"dbo\".\"Users\"", cmd.CommandText());
        }

        /// <summary>
        /// Tests from method using text with as.
        /// </summary>
        [TestMethod]
        public void TestFromTextAs1()
        {
            IDynamicSelectQueryBuilder cmd = new DynamicSelectQueryBuilder(Database);

            cmd.From(u => "dbo.Users AS c");
            Assert.AreEqual("SELECT * FROM \"dbo\".\"Users\" AS c", cmd.CommandText());
        }

        /// <summary>
        /// Tests from method using invoke with as.
        /// </summary>
        [TestMethod]
        public void TestFromTextAs2()
        {
            IDynamicSelectQueryBuilder cmd = new DynamicSelectQueryBuilder(Database);

            cmd.From(u => u(u.dbo.Users).As(u.u));

            Assert.AreEqual("SELECT * FROM \"dbo\".\"Users\" AS u", cmd.CommandText());
        }

        /// <summary>
        /// Tests from method using invoke with as.
        /// </summary>
        [TestMethod]
        public void TestFromTextAs3()
        {
            IDynamicSelectQueryBuilder cmd = new DynamicSelectQueryBuilder(Database);

            cmd.From(u => u("\"dbo\".\"Users\"").As(u.u));

            Assert.AreEqual("SELECT * FROM \"dbo\".\"Users\" AS u", cmd.CommandText());
        }

        /// <summary>
        /// Tests from method using invoke with sub query.
        /// </summary>
        [TestMethod]
        public void TestFromSubQuery1()
        {
            IDynamicSelectQueryBuilder cmd = new DynamicSelectQueryBuilder(Database);

            cmd.From(u => u(new DynamicSelectQueryBuilder(Database).From(x => x.dbo.Users)).As("u"));

            Assert.AreEqual("SELECT * FROM (SELECT * FROM \"dbo\".\"Users\") AS u", cmd.CommandText());
        }

        /// <summary>
        /// Tests from method using invoke with sub query.
        /// </summary>
        [TestMethod]
        public void TestFromSubQuery2()
        {
            IDynamicSelectQueryBuilder cmd = new DynamicSelectQueryBuilder(Database);

            cmd.From(u => u(cmd.SubQuery(x => x.dbo.Users)).As("u"));

            Assert.AreEqual("SELECT * FROM (SELECT * FROM \"dbo\".\"Users\") AS u", cmd.CommandText());
        }

        /// <summary>
        /// Tests from method using invoke with sub query.
        /// </summary>
        [TestMethod]
        public void TestFromSubQuery3()
        {
            IDynamicSelectQueryBuilder cmd = new DynamicSelectQueryBuilder(Database);

            cmd.SubQuery((b, s) => b.From(y => y(s.From(x => x.dbo.Users)).As("u")));

            Assert.AreEqual("SELECT * FROM (SELECT * FROM \"dbo\".\"Users\") AS u", cmd.CommandText());
        }

        /// <summary>
        /// Tests from method using invoke with sub query.
        /// </summary>
        [TestMethod]
        public void TestFromSubQuery4()
        {
            IDynamicSelectQueryBuilder cmd = new DynamicSelectQueryBuilder(Database);

            cmd.From(u => u(new DynamicSelectQueryBuilder(Database).From(x => x.dbo.Users.NoLock())).As("u"));

            Assert.AreEqual("SELECT * FROM (SELECT * FROM \"dbo\".\"Users\" WITH(NOLOCK)) AS u", cmd.CommandText());
        }

        /// <summary>
        /// Tests where method with alias.
        /// </summary>
        [TestMethod]
        public void TestWhereAlias()
        {
            IDynamicSelectQueryBuilder cmd = new DynamicSelectQueryBuilder(Database);

            cmd.From(u => u.dbo.Users.As(u.c))
                .Where(u => u.c.UserName == "admin");

            Assert.AreEqual(string.Format("SELECT * FROM \"dbo\".\"Users\" AS c WHERE (c.\"UserName\" = [${0}])", cmd.Parameters.Keys.First()), cmd.CommandText());
        }

        /// <summary>
        /// Tests where method with alias.
        /// </summary>
        [TestMethod]
        public void TestHavingAlias()
        {
            IDynamicSelectQueryBuilder cmd = new DynamicSelectQueryBuilder(Database);

            cmd.From(u => u.dbo.Users.As(u.c))
                .Having(u => u.Sum(u.c.ClientsCount) > 10);

            Assert.AreEqual(string.Format("SELECT * FROM \"dbo\".\"Users\" AS c HAVING (Sum(c.\"ClientsCount\") > [${0}])", cmd.Parameters.Keys.First()), cmd.CommandText());
        }

        /// <summary>
        /// Tests complex where method with alias.
        /// </summary>
        [TestMethod]
        public void TestWhereAliasComplex()
        {
            IDynamicSelectQueryBuilder cmd = new DynamicSelectQueryBuilder(Database);

            cmd.From(u => u.dbo.Users.As(u.c))
                .Where(u => u.c.UserName == "admin" || u.c.UserName == "root")
                .Where(u => u.c.IsActive = true);

            Assert.AreEqual(string.Format("SELECT * FROM \"dbo\".\"Users\" AS c WHERE ((c.\"UserName\" = [${0}]) OR (c.\"UserName\" = [${1}])) AND c.\"IsActive\" = ([${2}])",
                cmd.Parameters.Keys.ToArray()[0], cmd.Parameters.Keys.ToArray()[1], cmd.Parameters.Keys.ToArray()[2]), cmd.CommandText());
        }

        /// <summary>
        /// Tests where method with alias using in.
        /// </summary>
        [TestMethod]
        public void TestWhereAliasIn()
        {
            IDynamicSelectQueryBuilder cmd = new DynamicSelectQueryBuilder(Database);

            int[] ids = new int[] { 0, 1, 2, 3, 4, 5 };

            cmd.From(u => u.dbo.Users.As(u.c))
                .Where(u => u.c.UserName == "admin" || u.c.UserName == "root")
                .Where(u => u.c.IsActive == true)
                .Where(u => u.c.Id_User.In(ids));

            Assert.AreEqual(string.Format("SELECT * FROM \"dbo\".\"Users\" AS c WHERE ((c.\"UserName\" = [${0}]) OR (c.\"UserName\" = [${1}])) AND (c.\"IsActive\" = [${2}]) AND c.\"Id_User\" IN({3})",
                cmd.Parameters.Keys.ToArray()[0], cmd.Parameters.Keys.ToArray()[1], cmd.Parameters.Keys.ToArray()[2],
                string.Join(", ", cmd.Parameters.Keys.Skip(3).Select(p => string.Format("[${0}]", p)))), cmd.CommandText());
        }

        /// <summary>
        /// Tests where method with alias using between.
        /// </summary>
        [TestMethod]
        public void TestWhereAliasBetween1()
        {
            IDynamicSelectQueryBuilder cmd = new DynamicSelectQueryBuilder(Database);

            int[] ids = new int[] { 0, 5 };

            cmd.From(u => u.dbo.Users.As(u.c))
                .Where(u => u.c.UserName == "admin" || u.c.UserName == "root")
                .Where(u => u.c.IsActive == true)
                .Where(u => u.c.Id_User.Between(ids));

            Assert.AreEqual(string.Format("SELECT * FROM \"dbo\".\"Users\" AS c WHERE ((c.\"UserName\" = [${0}]) OR (c.\"UserName\" = [${1}])) AND (c.\"IsActive\" = [${2}]) AND c.\"Id_User\" BETWEEN [${3}] AND [${4}]",
                cmd.Parameters.Keys.ToArray()[0], cmd.Parameters.Keys.ToArray()[1], cmd.Parameters.Keys.ToArray()[2],
                cmd.Parameters.Keys.ToArray()[3], cmd.Parameters.Keys.ToArray()[4]), cmd.CommandText());
        }

        /// <summary>
        /// Tests where method with alias using between.
        /// </summary>
        [TestMethod]
        public void TestWhereAliasBetween2()
        {
            IDynamicSelectQueryBuilder cmd = new DynamicSelectQueryBuilder(Database);

            int[] ids = new int[] { 0, 5 };

            cmd.From(u => u.dbo.Users.As(u.c))
                .Where(u => u.c.UserName == "admin" || u.c.UserName == "root")
                .Where(u => u.c.IsActive == true)
                .Where(u => u.c.Id_User.Between(ids[0], ids[1]));

            Assert.AreEqual(string.Format("SELECT * FROM \"dbo\".\"Users\" AS c WHERE ((c.\"UserName\" = [${0}]) OR (c.\"UserName\" = [${1}])) AND (c.\"IsActive\" = [${2}]) AND c.\"Id_User\" BETWEEN [${3}] AND [${4}]",
                cmd.Parameters.Keys.ToArray()[0], cmd.Parameters.Keys.ToArray()[1], cmd.Parameters.Keys.ToArray()[2],
                cmd.Parameters.Keys.ToArray()[3], cmd.Parameters.Keys.ToArray()[4]), cmd.CommandText());
        }

        /// <summary>
        /// Tests where method without alias.
        /// </summary>
        [TestMethod]
        public void TestWhereNoAlias()
        {
            IDynamicSelectQueryBuilder cmd = new DynamicSelectQueryBuilder(Database);

            cmd.From(u => u.dbo.Users)
                .Where(u => u.UserName == "admin");

            Assert.AreEqual(string.Format("SELECT * FROM \"dbo\".\"Users\" WHERE (\"UserName\" = [${0}])", cmd.Parameters.Keys.First()), cmd.CommandText());
        }

        /// <summary>
        /// Tests where method with full column name.
        /// </summary>
        [TestMethod]
        public void TestWhereNoAliasTableName()
        {
            IDynamicSelectQueryBuilder cmd = new DynamicSelectQueryBuilder(Database);

            cmd.From(u => u.dbo.Users)
                .Where(u => u.dbo.Users.UserName == "admin");

            Assert.AreEqual(string.Format("SELECT * FROM \"dbo\".\"Users\" WHERE (\"dbo\".\"Users\".\"UserName\" = [${0}])", cmd.Parameters.Keys.First()), cmd.CommandText());
        }

        /// <summary>
        /// Tests simple join method.
        /// </summary>
        [TestMethod]
        public void TestJoinClassic()
        {
            IDynamicSelectQueryBuilder cmd = new DynamicSelectQueryBuilder(Database);

            cmd.From(u => u.dbo.Users.As(u.usr))
                .Join(u => u.dbo.UserClients.AS(u.uc).On(u.usr.Id_User == u.uc.User_Id));

            Assert.AreEqual(string.Format("SELECT * FROM \"dbo\".\"Users\" AS usr JOIN \"dbo\".\"UserClients\" AS uc ON (usr.\"Id_User\" = uc.\"User_Id\")"), cmd.CommandText());
        }

        /// <summary>
        /// Tests inner join method.
        /// </summary>
        [TestMethod]
        public void TestInnerJoin()
        {
            IDynamicSelectQueryBuilder cmd = new DynamicSelectQueryBuilder(Database);

            cmd.From(u => u.dbo.Users.As(u.usr))
                .Join(u => u.Inner().dbo.UserClients.AS(u.uc).On(u.usr.Id_User == u.uc.User_Id));

            Assert.AreEqual(string.Format("SELECT * FROM \"dbo\".\"Users\" AS usr INNER JOIN \"dbo\".\"UserClients\" AS uc ON (usr.\"Id_User\" = uc.\"User_Id\")"), cmd.CommandText());
        }

        /// <summary>
        /// Tests inner join method with aliases mix.
        /// </summary>
        [TestMethod]
        public void TestInnerJoin2()
        {
            IDynamicSelectQueryBuilder cmd = new DynamicSelectQueryBuilder(Database);

            cmd.From(u => u.dbo.Users.As(u.usr))
                .Join(usr => usr.Inner().dbo.UserClients.AS(usr.uc).On(usr.Id_User == usr.uc.User_Id && usr.uc.Users != null))
                .Select(usr => usr.All(), uc => uc.Users);

            Assert.AreEqual(string.Format("SELECT usr.*, uc.\"Users\" FROM \"dbo\".\"Users\" AS usr INNER JOIN \"dbo\".\"UserClients\" AS uc ON ((usr.\"Id_User\" = uc.\"User_Id\") AND (uc.\"Users\" IS NOT NULL))"), cmd.CommandText());
        }

        /// <summary>
        /// Tests from method using invoke with sub query.
        /// </summary>
        [TestMethod]
        public void TestInnerJoin3()
        {
            IDynamicSelectQueryBuilder cmd = new DynamicSelectQueryBuilder(Database);

            cmd.From(u => u.dbo.Users.As(u.usr))
                .SubQuery((b, s) => b.Join(usr => usr(s.From(x => x.dbo.UserClients)).Inner().As(usr.uc).On(usr.Id_User == usr.uc.User_Id && usr.uc.Users != null)))
                .Select(usr => usr.All(), uc => uc.Users);

            Assert.AreEqual(string.Format("SELECT usr.*, uc.\"Users\" FROM \"dbo\".\"Users\" AS usr INNER JOIN (SELECT * FROM \"dbo\".\"UserClients\") AS uc ON ((usr.\"Id_User\" = uc.\"User_Id\") AND (uc.\"Users\" IS NOT NULL))"), cmd.CommandText());
        }

        /// <summary>
        /// Tests from method using invoke with sub query.
        /// </summary>
        [TestMethod]
        public void TestInnerJoin4()
        {
            IDynamicSelectQueryBuilder cmd = new DynamicSelectQueryBuilder(Database);

            cmd.From(u => u.dbo.Users.As(u.usr))
                .SubQuery((b, s) => b.Join(usr => usr(s.From(x => x.dbo.UserClients).Where(x => x.Deleted == 0)).Inner().As(usr.uc).On(usr.Id_User == usr.uc.User_Id && usr.uc.Users != null)))
                .Select(usr => usr.All(), uc => uc.Users);

            Assert.AreEqual(string.Format("SELECT usr.*, uc.\"Users\" FROM \"dbo\".\"Users\" AS usr INNER JOIN (SELECT * FROM \"dbo\".\"UserClients\" WHERE (\"Deleted\" = [${0}])) AS uc ON ((usr.\"Id_User\" = uc.\"User_Id\") AND (uc.\"Users\" IS NOT NULL))", cmd.Parameters.Keys.First()), cmd.CommandText());
        }

        /// <summary>
        /// Tests from method using invoke with sub query an no lock.
        /// </summary>
        [TestMethod]
        public void TestInnerJoin5()
        {
            IDynamicSelectQueryBuilder cmd = new DynamicSelectQueryBuilder(Database);

            cmd.From(u => u.dbo.Users.As(u.usr).NoLock())
                .SubQuery((b, s) => b.Join(usr => usr(s.From(x => x.dbo.UserClients.NoLock()).Where(x => x.Deleted == 0)).Inner().As(usr.uc).On(usr.Id_User == usr.uc.User_Id && usr.uc.Users != null)))
                .Select(usr => usr.All(), uc => uc.Users);

            Assert.AreEqual(string.Format("SELECT usr.*, uc.\"Users\" FROM \"dbo\".\"Users\" AS usr WITH(NOLOCK) INNER JOIN (SELECT * FROM \"dbo\".\"UserClients\" WITH(NOLOCK) WHERE (\"Deleted\" = [${0}])) AS uc ON ((usr.\"Id_User\" = uc.\"User_Id\") AND (uc.\"Users\" IS NOT NULL))", cmd.Parameters.Keys.First()), cmd.CommandText());
        }

        /// <summary>
        /// Tests inner join method with no lock.
        /// </summary>
        [TestMethod]
        public void TestInnerJoin6()
        {
            IDynamicSelectQueryBuilder cmd = new DynamicSelectQueryBuilder(Database);

            cmd.From(u => u.dbo.Users.As(u.usr).NoLock())
                .Join(u => u.Inner().dbo.UserClients.AS(u.uc).NoLock().On(u.usr.Id_User == u.uc.User_Id));

            Assert.AreEqual(string.Format("SELECT * FROM \"dbo\".\"Users\" AS usr WITH(NOLOCK) INNER JOIN \"dbo\".\"UserClients\" AS uc WITH(NOLOCK) ON (usr.\"Id_User\" = uc.\"User_Id\")"), cmd.CommandText());
        }

        /// <summary>
        /// Tests left outer join method.
        /// </summary>
        [TestMethod]
        public void TestLeftOuterJoin()
        {
            IDynamicSelectQueryBuilder cmd = new DynamicSelectQueryBuilder(Database);

            cmd.From(u => u.dbo.Users.As(u.usr))
                .Join(u => u.LeftOuter().dbo.UserClients.AS(u.uc).On(u.usr.Id_User == u.uc.User_Id));

            Assert.AreEqual(string.Format("SELECT * FROM \"dbo\".\"Users\" AS usr LEFT OUTER JOIN \"dbo\".\"UserClients\" AS uc ON (usr.\"Id_User\" = uc.\"User_Id\")"), cmd.CommandText());
        }

        /// <summary>
        /// Tests left join method.
        /// </summary>
        [TestMethod]
        public void TestLeftJoin()
        {
            IDynamicSelectQueryBuilder cmd = new DynamicSelectQueryBuilder(Database);

            cmd.From(u => u.dbo.Users.As(u.usr))
                .Join(u => u.Left().dbo.UserClients.AS(u.uc).On(u.usr.Id_User == u.uc.User_Id));

            Assert.AreEqual(string.Format("SELECT * FROM \"dbo\".\"Users\" AS usr LEFT JOIN \"dbo\".\"UserClients\" AS uc ON (usr.\"Id_User\" = uc.\"User_Id\")"), cmd.CommandText());
        }

        /// <summary>
        /// Tests right outer join method.
        /// </summary>
        [TestMethod]
        public void TestRightOuterJoin()
        {
            IDynamicSelectQueryBuilder cmd = new DynamicSelectQueryBuilder(Database);

            cmd.From(u => u.dbo.Users.As(u.usr))
                .Join(u => u.RightOuter().dbo.UserClients.AS(u.uc).On(u.usr.Id_User == u.uc.User_Id));

            Assert.AreEqual(string.Format("SELECT * FROM \"dbo\".\"Users\" AS usr RIGHT OUTER JOIN \"dbo\".\"UserClients\" AS uc ON (usr.\"Id_User\" = uc.\"User_Id\")"), cmd.CommandText());
        }

        /// <summary>
        /// Tests right join method.
        /// </summary>
        [TestMethod]
        public void TestRightJoin()
        {
            IDynamicSelectQueryBuilder cmd = new DynamicSelectQueryBuilder(Database);

            cmd.From(u => u.dbo.Users.As(u.usr))
                .Join(u => u.Right().dbo.UserClients.AS(u.uc).On(u.usr.Id_User == u.uc.User_Id));

            Assert.AreEqual(string.Format("SELECT * FROM \"dbo\".\"Users\" AS usr RIGHT JOIN \"dbo\".\"UserClients\" AS uc ON (usr.\"Id_User\" = uc.\"User_Id\")"), cmd.CommandText());
        }

        /// <summary>
        /// Tests complex join with parameters.
        /// </summary>
        [TestMethod]
        public void TestJoinClassicWithParamAndWhere()
        {
            IDynamicSelectQueryBuilder cmd = new DynamicSelectQueryBuilder(Database);

            cmd.From(u => u.dbo.Users.As(u.usr))
                .Join(u => u.dbo.UserClients.AS(u.uc).On(u.usr.Id_User == u.uc.User_Id && u.uc.Deleted == 0))
                .Where(u => u.usr.Active == true);

            Assert.AreEqual(string.Format("SELECT * FROM \"dbo\".\"Users\" AS usr JOIN \"dbo\".\"UserClients\" AS uc ON ((usr.\"Id_User\" = uc.\"User_Id\") AND (uc.\"Deleted\" = [${0}])) WHERE (usr.\"Active\" = [${1}])",
                cmd.Parameters.Keys.First(), cmd.Parameters.Keys.Last()), cmd.CommandText());
        }

        /// <summary>
        /// Tests select all.
        /// </summary>
        [TestMethod]
        public void TestSelectAll1()
        {
            IDynamicSelectQueryBuilder cmd = new DynamicSelectQueryBuilder(Database);

            cmd.From(u => u.dbo.Users.As(u.c))
                .Select(u => u.c.All());

            Assert.AreEqual("SELECT c.* FROM \"dbo\".\"Users\" AS c", cmd.CommandText());
        }

        /// <summary>
        /// Tests select all.
        /// </summary>
        [TestMethod]
        public void TestSelectAll2()
        {
            IDynamicSelectQueryBuilder cmd = new DynamicSelectQueryBuilder(Database);

            cmd.From(u => u.dbo.Users)
                .Select(u => u.dbo.Users.All());

            Assert.AreEqual("SELECT \"dbo\".\"Users\".* FROM \"dbo\".\"Users\"", cmd.CommandText());
        }

        /// <summary>
        /// Tests select field.
        /// </summary>
        [TestMethod]
        public void TestSelectField1()
        {
            IDynamicSelectQueryBuilder cmd = new DynamicSelectQueryBuilder(Database);

            cmd.From(u => u.dbo.Users.As(u.c))
                .Select(u => u.c.UserName);

            Assert.AreEqual("SELECT c.\"UserName\" FROM \"dbo\".\"Users\" AS c", cmd.CommandText());
        }

        /// <summary>
        /// Tests select field.
        /// </summary>
        [TestMethod]
        public void TestSelectField2()
        {
            IDynamicSelectQueryBuilder cmd = new DynamicSelectQueryBuilder(Database);

            cmd.From(u => u.dbo.Users)
                .Select(u => u.dbo.Users.UserName);

            Assert.AreEqual("SELECT \"dbo\".\"Users\".\"UserName\" FROM \"dbo\".\"Users\"", cmd.CommandText());
        }

        /// <summary>
        /// Tests select field with alias.
        /// </summary>
        [TestMethod]
        public void TestSelectFieldAlias1()
        {
            IDynamicSelectQueryBuilder cmd = new DynamicSelectQueryBuilder(Database);

            cmd.From(u => u.dbo.Users.As(u.c))
                .Select(u => u.c.UserName.As(u.Name));

            Assert.AreEqual("SELECT c.\"UserName\" AS \"Name\" FROM \"dbo\".\"Users\" AS c", cmd.CommandText());
        }

        /// <summary>
        /// Tests select field with alias.
        /// </summary>
        [TestMethod]
        public void TestSelectFieldAlias2()
        {
            IDynamicSelectQueryBuilder cmd = new DynamicSelectQueryBuilder(Database);

            cmd.From(u => u.dbo.Users)
                .Select(u => u.dbo.Users.UserName.As(u.Name));

            Assert.AreEqual("SELECT \"dbo\".\"Users\".\"UserName\" AS \"Name\" FROM \"dbo\".\"Users\"", cmd.CommandText());
        }

        /// <summary>
        /// Tests select field with alias.
        /// </summary>
        [TestMethod]
        public void TestSelectFieldAlias3()
        {
            IDynamicSelectQueryBuilder cmd = new DynamicSelectQueryBuilder(Database);

            cmd.From(u => u.dbo.Users.As(u.u))
                .Select(u => u.UserName.As(u.Name));

            Assert.AreEqual("SELECT u.\"UserName\" AS \"Name\" FROM \"dbo\".\"Users\" AS u", cmd.CommandText());
        }

        /// <summary>
        /// Tests select field with alias.
        /// </summary>
        [TestMethod]
        public void TestSelectFieldAlias4()
        {
            IDynamicSelectQueryBuilder cmd = new DynamicSelectQueryBuilder(Database);

            cmd.From(u => u.dbo.Users.As(u.u))
                .Select(u => u.UserName.As(u.u.Name));

            Assert.AreEqual("SELECT u.\"UserName\" AS \"Name\" FROM \"dbo\".\"Users\" AS u", cmd.CommandText());
        }

        /// <summary>
        /// Tests select aggregate field with alias (Sum).
        /// </summary>
        [TestMethod]
        public void TestSelectAggregateField1()
        {
            IDynamicSelectQueryBuilder cmd = new DynamicSelectQueryBuilder(Database);

            cmd.From(u => u.dbo.Users.As(u.c))
                .Select(u => u.Sum(u.c.UserName).As(u.Name));

            Assert.AreEqual("SELECT Sum(c.\"UserName\") AS \"Name\" FROM \"dbo\".\"Users\" AS c", cmd.CommandText());
        }

        /// <summary>
        /// Tests select aggregate field with alias (Coalesce).
        /// </summary>
        [TestMethod]
        public void TestSelectAggregateField2()
        {
            IDynamicSelectQueryBuilder cmd = new DynamicSelectQueryBuilder(Database);

            cmd.From(u => u.dbo.Users.As(u.c))
                .Select(u => u.Coalesce(u.c.UserName, u.c.FirstName + " " + u.c.LastName).As(u.Name));

            Assert.AreEqual(string.Format("SELECT Coalesce(c.\"UserName\", ((c.\"FirstName\" + [${0}]) + c.\"LastName\")) AS \"Name\" FROM \"dbo\".\"Users\" AS c",
                cmd.Parameters.Keys.First()), cmd.CommandText());
        }

        /// <summary>
        /// Tests select aggregate field with alias (Sum).
        /// </summary>
        [TestMethod]
        public void TestSelectAggregateField3()
        {
            IDynamicSelectQueryBuilder cmd = new DynamicSelectQueryBuilder(Database);

            cmd.From(u => u.dbo.Users)
                .Select(u => u.Sum(u.dbo.Users.UserName));

            Assert.AreEqual("SELECT Sum(\"dbo\".\"Users\".\"UserName\") FROM \"dbo\".\"Users\"", cmd.CommandText());
        }

        /// <summary>
        /// Tests select aggregate field with alias (Sum).
        /// </summary>
        [TestMethod]
        public void TestSelectAggregateField4()
        {
            IDynamicSelectQueryBuilder cmd = new DynamicSelectQueryBuilder(Database);

            cmd.From(u => u.dbo.Users.As(u.c))
                .Select(u => u.Sum(u("\"UserName\"")).As(u.Name));

            Assert.AreEqual("SELECT Sum(\"UserName\") AS \"Name\" FROM \"dbo\".\"Users\" AS c", cmd.CommandText());
        }

        /// <summary>
        /// Tests select aggregate field with alias (Sum).
        /// </summary>
        [TestMethod]
        public void TestSelectAggregateField5()
        {
            IDynamicSelectQueryBuilder cmd = new DynamicSelectQueryBuilder(Database);

            cmd.From(u => u.dbo.Users.As(u.c))
                .Select(u => u(u.Sum(u("\"UserName\"")), " + 1").As(u.Name));

            Assert.AreEqual("SELECT Sum(\"UserName\") + 1 AS \"Name\" FROM \"dbo\".\"Users\" AS c", cmd.CommandText());
        }

        /// <summary>
        /// Tests select from anonymous type.
        /// </summary>
        [TestMethod]
        public void TestSelectAnon()
        {
            IDynamicSelectQueryBuilder cmd = new DynamicSelectQueryBuilder(Database);

            cmd.From(u => u.dbo.Users.As(u.c))
                .Select(u => new
                {
                    Id_User = u.c.Id_User,
                    Name = u.c.UserName,
                });

            Assert.AreEqual("SELECT c.\"Id_User\" AS \"Id_User\", c.\"UserName\" AS \"Name\" FROM \"dbo\".\"Users\" AS c", cmd.CommandText());
        }

        /// <summary>
        /// Tests select escaped case.
        /// </summary>
        [TestMethod]
        public void TestSelectCaseEscaped1()
        {
            IDynamicSelectQueryBuilder cmd = new DynamicSelectQueryBuilder(Database);

            cmd.From(u => u.dbo.Users.As(u.c))
                .Select(u => u("CASE ", u.c.IsActive, " WHEN ", 1, " THEN ", 0, " ELSE ", 1, " END").As(u.Deleted));

            Assert.AreEqual(string.Format("SELECT CASE c.\"IsActive\" WHEN [${0}] THEN [${1}] ELSE [${2}] END AS \"Deleted\" FROM \"dbo\".\"Users\" AS c",
                cmd.Parameters.Keys.ToArray()[0], cmd.Parameters.Keys.ToArray()[1], cmd.Parameters.Keys.ToArray()[2]), cmd.CommandText());
        }

        /// <summary>
        /// Tests select escaped case.
        /// </summary>
        [TestMethod]
        public void TestSelectCaseEscaped2()
        {
            IDynamicSelectQueryBuilder cmd = new DynamicSelectQueryBuilder(Database);

            cmd.From(u => u.dbo.Users.As(u.c))
                .Select(u => u("CASE WHEN ", u.c.IsActive == 1, " THEN ", 0, " ELSE ", 1, " END").As(u.Deleted));

            Assert.AreEqual(string.Format("SELECT CASE WHEN (c.\"IsActive\" = [${0}]) THEN [${1}] ELSE [${2}] END AS \"Deleted\" FROM \"dbo\".\"Users\" AS c",
                cmd.Parameters.Keys.ToArray()[0], cmd.Parameters.Keys.ToArray()[1], cmd.Parameters.Keys.ToArray()[2]), cmd.CommandText());
        }

        /// <summary>
        /// Tests select escaped case.
        /// </summary>
        [TestMethod]
        public void TestCoalesceEscaped()
        {
            IDynamicSelectQueryBuilder cmd = new DynamicSelectQueryBuilder(Database);

            cmd.From(u => u.dbo.Users.As(u.c))
                .Select(u => u("COALESCE(", Database.DecorateName("ServerHash"), ", ", new byte[16] { 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 }, ")").As(u.Hash));

            Assert.AreEqual(string.Format("SELECT COALESCE(\"ServerHash\", [${0}]) AS \"Hash\" FROM \"dbo\".\"Users\" AS c",
                cmd.Parameters.Keys.ToArray()[0]), cmd.CommandText());
        }

        /// <summary>
        /// Tests select escaped case.
        /// </summary>
        [TestMethod]
        public void TestCoalesce()
        {
            IDynamicSelectQueryBuilder cmd = new DynamicSelectQueryBuilder(Database);

            cmd.From(u => u.dbo.Users.As(u.c))
                .Select(u => u.Coalesce(u.c.ServerHash, new byte[16] { 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 }).As(u.Hash));

            Assert.AreEqual(string.Format("SELECT Coalesce(c.\"ServerHash\", [${0}]) AS \"Hash\" FROM \"dbo\".\"Users\" AS c",
                cmd.Parameters.Keys.ToArray()[0]), cmd.CommandText());
        }

        /// <summary>
        /// Tests select escaped case.
        /// </summary>
        [TestMethod]
        public void TestCoalesceCalculatedArgs()
        {
            IDynamicSelectQueryBuilder cmd = new DynamicSelectQueryBuilder(Database);

            cmd.From(u => u.dbo.Users.As(u.c))
                .Select(u => u.Coalesce(u.c.Test1 + "_", u.c.Test2 + "_", u.c.Test3 + "_").As(u.Hash));

            Assert.AreEqual(string.Format("SELECT Coalesce((c.\"Test1\" + [${0}]), (c.\"Test2\" + [${1}]), (c.\"Test3\" + [${2}])) AS \"Hash\" FROM \"dbo\".\"Users\" AS c",
                cmd.Parameters.Keys.ToArray()[0], cmd.Parameters.Keys.ToArray()[1], cmd.Parameters.Keys.ToArray()[2]), cmd.CommandText());

            //var c = Database.Open().CreateCommand();
            //cmd.FillCommand(c);
            //c.Dispose();
        }

        /// <summary>
        /// Tests select escaped case.
        /// </summary>
        [TestMethod]
        public void TestCoalesceInWhere()
        {
            IDynamicSelectQueryBuilder cmd = new DynamicSelectQueryBuilder(Database);

            cmd.From(u => u.dbo.Users.As(u.c))
                .Select(u => u.ServerHash.As(u.Hash))
                .Where(u => u.Coalesce(u.c.ServerHash, new byte[16] { 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 }) == new byte[16] { 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 });

            Assert.AreEqual(string.Format("SELECT \"ServerHash\" AS \"Hash\" FROM \"dbo\".\"Users\" AS c WHERE (Coalesce(c.\"ServerHash\", [${0}]) = [${1}])",
                cmd.Parameters.Keys.ToArray()[0], cmd.Parameters.Keys.ToArray()[1]), cmd.CommandText());
        }

        /// <summary>
        /// Tests select escaped case with sub query.
        /// </summary>
        [TestMethod]
        public void TestSelectCaseEscapedAndSub()
        {
            IDynamicSelectQueryBuilder cmd = new DynamicSelectQueryBuilder(Database);

            cmd.From(u => u.dbo.Users.As(u.c))
                .Select(u => u("CASE WHEN ", u.c.IsActive == 1, " AND ", u.c.IsAdmin == u(cmd.SubQuery()
                    .From(x => x.dbo.AccessRights.As(x.a))
                    .Where(x => x.a.User_Id == x.c.Id_User)
                    .Select(x => x.a.IsAdmin)), " THEN ", 0, " ELSE ", 1, " END").As(u.Deleted));

            Assert.AreEqual(string.Format("SELECT CASE WHEN (c.\"IsActive\" = [${0}]) AND (c.\"IsAdmin\" = (SELECT a.\"IsAdmin\" FROM \"dbo\".\"AccessRights\" AS a WHERE (a.\"User_Id\" = c.\"Id_User\"))) THEN [${1}] ELSE [${2}] END AS \"Deleted\" FROM \"dbo\".\"Users\" AS c",
                cmd.Parameters.Keys.ToArray()[0], cmd.Parameters.Keys.ToArray()[1], cmd.Parameters.Keys.ToArray()[2]), cmd.CommandText());
        }

        /// <summary>
        /// Tests group by.
        /// </summary>
        [TestMethod]
        public void TestGroupBy()
        {
            IDynamicSelectQueryBuilder cmd = new DynamicSelectQueryBuilder(Database);

            cmd.From(u => u.dbo.Users.As(u.c))
                .GroupBy(u => u.c.UserName);

            Assert.AreEqual("SELECT * FROM \"dbo\".\"Users\" AS c GROUP BY c.\"UserName\"", cmd.CommandText());
        }

        /// <summary>
        /// Tests order by.
        /// </summary>
        [TestMethod]
        public void TestOrderBy()
        {
            IDynamicSelectQueryBuilder cmd = new DynamicSelectQueryBuilder(Database);

            cmd.From(u => u.dbo.Users.As(u.c))
                .OrderBy(u => u.c.UserName);

            Assert.AreEqual("SELECT * FROM \"dbo\".\"Users\" AS c ORDER BY c.\"UserName\" ASC", cmd.CommandText());
        }

        /// <summary>
        /// Tests order by using string with number.
        /// </summary>
        [TestMethod]
        public void TestOrderByNumberedColumnStr()
        {
            IDynamicSelectQueryBuilder cmd = new DynamicSelectQueryBuilder(Database);

            cmd.From(u => u.dbo.Users.As(u.c))
                .OrderBy(u => "1 DESC");

            Assert.AreEqual("SELECT * FROM \"dbo\".\"Users\" AS c ORDER BY 1 DESC", cmd.CommandText());
        }

        /// <summary>
        /// Tests order by using member with number.
        /// </summary>
        [TestMethod]
        public void TestOrderByNumberedColFn()
        {
            IDynamicSelectQueryBuilder cmd = new DynamicSelectQueryBuilder(Database);

            cmd.From(u => u.dbo.Users.As(u.c))
                .OrderBy(u => u.Desc(1));

            Assert.AreEqual("SELECT * FROM \"dbo\".\"Users\" AS c ORDER BY 1 DESC", cmd.CommandText());
        }

        /// <summary>
        /// Tests order by using member with field.
        /// </summary>
        [TestMethod]
        public void TestOrderByAlt()
        {
            IDynamicSelectQueryBuilder cmd = new DynamicSelectQueryBuilder(Database);

            cmd.From(u => u.dbo.Users.As(u.c))
                .OrderBy(u => u.Desc(u.c.UserName));

            Assert.AreEqual("SELECT * FROM \"dbo\".\"Users\" AS c ORDER BY c.\"UserName\" DESC", cmd.CommandText());
        }

        /// <summary>
        /// Tests sub query select.
        /// </summary>
        [TestMethod]
        public void TestSubQuerySelect()
        {
            IDynamicSelectQueryBuilder cmd = new DynamicSelectQueryBuilder(Database);

            cmd.From(u => u.dbo.Users.As(u.c))
                .Select(u => u(cmd.SubQuery()
                    .From(x => x.dbo.AccessRights.As(x.a))
                    .Where(x => x.a.User_Id == x.c.Id_User)
                    .Select(x => x.a.IsAdmin)).As(u.IsAdmin));

            Assert.AreEqual("SELECT (SELECT a.\"IsAdmin\" FROM \"dbo\".\"AccessRights\" AS a WHERE (a.\"User_Id\" = c.\"Id_User\")) AS \"IsAdmin\" FROM \"dbo\".\"Users\" AS c", cmd.CommandText());
        }

        /// <summary>
        /// Tests sub query where.
        /// </summary>
        [TestMethod]
        public void TestSubQueryWhere()
        {
            IDynamicSelectQueryBuilder cmd = new DynamicSelectQueryBuilder(Database);

            cmd.From(u => u.dbo.Users.As(u.c))
                .Where(u => u.c.IsAdmin == u(cmd.SubQuery()
                    .From(x => x.dbo.AccessRights.As(x.a))
                    .Where(x => x.a.User_Id == x.c.Id_User)
                    .Select(x => x.a.IsAdmin)));

            Assert.AreEqual("SELECT * FROM \"dbo\".\"Users\" AS c WHERE (c.\"IsAdmin\" = (SELECT a.\"IsAdmin\" FROM \"dbo\".\"AccessRights\" AS a WHERE (a.\"User_Id\" = c.\"Id_User\")))", cmd.CommandText());
        }

        /// <summary>
        /// Tests sub query in.
        /// </summary>
        [TestMethod]
        public void TestSubQueryWhereIn()
        {
            IDynamicSelectQueryBuilder cmd = new DynamicSelectQueryBuilder(Database);

            cmd.From(u => u.dbo.Users.As(u.c))
                .Where(u => u.c.Id_User.In(u(cmd.SubQuery()
                    .From(x => x.dbo.AccessRights.As(x.a))
                    .Where(x => x.a.IsAdmin == 1)
                    .Select(x => x.a.User_Id))));

            Assert.AreEqual(string.Format("SELECT * FROM \"dbo\".\"Users\" AS c WHERE c.\"Id_User\" IN((SELECT a.\"User_Id\" FROM \"dbo\".\"AccessRights\" AS a WHERE (a.\"IsAdmin\" = [${0}])))", cmd.Parameters.Keys.First()), cmd.CommandText());
        }

        /// <summary>
        /// Tests sub query join.
        /// </summary>
        [TestMethod]
        public void TestSubQueryJoin()
        {
            IDynamicSelectQueryBuilder cmd = new DynamicSelectQueryBuilder(Database);

            cmd.From(u => u.dbo.Users.As(u.c))
                .Join(u => u.Inner()(cmd.SubQuery()
                    .From(x => x.dbo.AccessRights.As(x.a))
                    .Select(x => x.a.IsAdmin, x => x.a.User_Id)).As(u.ar).On(u.ar.User_Id == u.c.Id_User));

            Assert.AreEqual("SELECT * FROM \"dbo\".\"Users\" AS c INNER JOIN (SELECT a.\"IsAdmin\", a.\"User_Id\" FROM \"dbo\".\"AccessRights\" AS a) AS ar ON (ar.\"User_Id\" = c.\"Id_User\")", cmd.CommandText());
        }
    }
}