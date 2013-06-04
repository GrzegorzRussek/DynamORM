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

using System.Linq;
using DynamORM.Builders;
using DynamORM.Builders.Implementation;
using NUnit.Framework;

namespace DynamORM.Tests.Select
{
    /// <summary>
    /// New parser tests.
    /// </summary>
    public class ParserTests : TestsBase
    {
        /// <summary>Setup test parameters.</summary>
        [TestFixtureSetUp]
        public virtual void SetUp()
        {
            CreateTestDatabase();
            CreateDynamicDatabase(
                DynamicDatabaseOptions.SingleConnection |
                DynamicDatabaseOptions.SingleTransaction |
                DynamicDatabaseOptions.SupportLimitOffset);
        }

        /// <summary>Tear down test objects.</summary>
        [TestFixtureTearDown]
        public virtual void TearDown()
        {
            DestroyDynamicDatabase();
            DestroyTestDatabase();
        }

        /// <summary>
        /// Tests from method.
        /// </summary>
        [Test]
        public void TestFromGet()
        {
            IDynamicSelectQueryBuilder cmd = new DynamicSelectQueryBuilder(Database);

            cmd.From(u => u.dbo.Users);
            Assert.AreEqual("SELECT * FROM \"dbo\".\"Users\"", cmd.CommandText());
        }

        /// <summary>
        /// Tests from method with multi tables.
        /// </summary>
        [Test]
        public void TestFromGetMultiKulti()
        {
            IDynamicSelectQueryBuilder cmd = new DynamicSelectQueryBuilder(Database);

            cmd.From(u => u.dbo.Users, c => c.Clients);
            Assert.AreEqual("SELECT * FROM \"dbo\".\"Users\", \"Clients\"", cmd.CommandText());
        }

        /// <summary>
        /// Tests from method with as expression in text.
        /// </summary>
        [Test]
        public void TestFromGetAs1()
        {
            IDynamicSelectQueryBuilder cmd = new DynamicSelectQueryBuilder(Database);

            cmd.From(u => u.dbo.Users.As("c"));
            Assert.AreEqual("SELECT * FROM \"dbo\".\"Users\" AS c", cmd.CommandText());
        }

        /// <summary>
        /// Tests from method with as expression using lambda.
        /// </summary>
        [Test]
        public void TestFromGetAs2()
        {
            IDynamicSelectQueryBuilder cmd = new DynamicSelectQueryBuilder(Database);

            cmd.From(u => u.dbo.Users.As(u.c));
            Assert.AreEqual("SELECT * FROM \"dbo\".\"Users\" AS c", cmd.CommandText());
        }

        /// <summary>
        /// Tests from method using text.
        /// </summary>
        [Test]
        public void TestFromText()
        {
            IDynamicSelectQueryBuilder cmd = new DynamicSelectQueryBuilder(Database);

            cmd.From(u => "dbo.Users");
            Assert.AreEqual("SELECT * FROM \"dbo\".\"Users\"", cmd.CommandText());
        }

        /// <summary>
        /// Tests from method using text with decorators.
        /// </summary>
        [Test]
        public void TestFromDecoratedText()
        {
            IDynamicSelectQueryBuilder cmd = new DynamicSelectQueryBuilder(Database);

            cmd.From(u => "\"dbo\".\"Users\"");
            Assert.AreEqual("SELECT * FROM \"dbo\".\"Users\"", cmd.CommandText());
        }

        /// <summary>
        /// Tests from method using text with as.
        /// </summary>
        [Test]
        public void TestFromTextAs1()
        {
            IDynamicSelectQueryBuilder cmd = new DynamicSelectQueryBuilder(Database);

            cmd.From(u => "dbo.Users AS c");
            Assert.AreEqual("SELECT * FROM \"dbo\".\"Users\" AS c", cmd.CommandText());
        }

        /// <summary>
        /// Tests from method using invoke with as.
        /// </summary>
        [Test]
        public void TestFromTextAs2()
        {
            IDynamicSelectQueryBuilder cmd = new DynamicSelectQueryBuilder(Database);

            cmd.From(u => u(u.dbo.Users).As(u.u));

            Assert.AreEqual("SELECT * FROM \"dbo\".\"Users\" AS u", cmd.CommandText());
        }

        /// <summary>
        /// Tests from method using invoke with sub query.
        /// </summary>
        [Test]
        public void TestFromSubQuery1()
        {
            IDynamicSelectQueryBuilder cmd = new DynamicSelectQueryBuilder(Database);

            cmd.From(u => u(new DynamicSelectQueryBuilder(Database).From(x => x.dbo.Users)).As("u"));

            Assert.AreEqual("SELECT * FROM (SELECT * FROM \"dbo\".\"Users\") AS u", cmd.CommandText());
        }

        /// <summary>
        /// Tests from method using invoke with sub query.
        /// </summary>
        [Test]
        public void TestFromSubQuery2()
        {
            IDynamicSelectQueryBuilder cmd = new DynamicSelectQueryBuilder(Database);

            cmd.From(u => u(cmd.SubQuery(x => x.dbo.Users)).As("u"));

            Assert.AreEqual("SELECT * FROM (SELECT * FROM \"dbo\".\"Users\") AS u", cmd.CommandText());
        }

        /// <summary>
        /// Tests where method with alias.
        /// </summary>
        [Test]
        public void TestWhereAlias()
        {
            IDynamicSelectQueryBuilder cmd = new DynamicSelectQueryBuilder(Database);

            cmd.From(u => u.dbo.Users.As(u.c))
                .Where(u => u.c.UserName == "admin");

            Assert.AreEqual(string.Format("SELECT * FROM \"dbo\".\"Users\" AS c WHERE (c.\"UserName\" = [${0}])", cmd.Parameters.Keys.First()), cmd.CommandText());
        }

        /// <summary>
        /// Tests complex where method with alias.
        /// </summary>
        [Test]
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
        [Test]
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
        [Test]
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
        [Test]
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
        [Test]
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
        [Test]
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
        [Test]
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
        [Test]
        public void TestInnerJoin()
        {
            IDynamicSelectQueryBuilder cmd = new DynamicSelectQueryBuilder(Database);

            cmd.From(u => u.dbo.Users.As(u.usr))
                .Join(u => u.Inner().dbo.UserClients.AS(u.uc).On(u.usr.Id_User == u.uc.User_Id));

            Assert.AreEqual(string.Format("SELECT * FROM \"dbo\".\"Users\" AS usr INNER JOIN \"dbo\".\"UserClients\" AS uc ON (usr.\"Id_User\" = uc.\"User_Id\")"), cmd.CommandText());
        }

        /// <summary>
        /// Tests left outer join method.
        /// </summary>
        [Test]
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
        [Test]
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
        [Test]
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
        [Test]
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
        [Test]
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
        [Test]
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
        [Test]
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
        [Test]
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
        [Test]
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
        [Test]
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
        [Test]
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
        [Test]
        public void TestSelectFieldAlias3()
        {
            IDynamicSelectQueryBuilder cmd = new DynamicSelectQueryBuilder(Database);

            cmd.From(u => u.dbo.Users.As(u.u))
                .Select(u => u.UserName.As(u.Name));

            Assert.AreEqual("SELECT u.\"UserName\" AS \"Name\" FROM \"dbo\".\"Users\" AS u", cmd.CommandText());
        }

        /// <summary>
        /// Tests select aggregate field with alias (Sum).
        /// </summary>
        [Test]
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
        [Test]
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
        [Test]
        public void TestSelectAggregateField3()
        {
            IDynamicSelectQueryBuilder cmd = new DynamicSelectQueryBuilder(Database);

            cmd.From(u => u.dbo.Users)
                .Select(u => u.Sum(u.dbo.Users.UserName));

            Assert.AreEqual("SELECT Sum(\"dbo\".\"Users\".\"UserName\") FROM \"dbo\".\"Users\"", cmd.CommandText());
        }

        /// <summary>
        /// Tests select from anonymous type.
        /// </summary>
        [Test]
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
        [Test]
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
        [Test]
        public void TestSelectCaseEscaped2()
        {
            IDynamicSelectQueryBuilder cmd = new DynamicSelectQueryBuilder(Database);

            cmd.From(u => u.dbo.Users.As(u.c))
                .Select(u => u("CASE WHEN ", u.c.IsActive == 1, " THEN ", 0, " ELSE ", 1, " END").As(u.Deleted));

            Assert.AreEqual(string.Format("SELECT CASE WHEN (c.\"IsActive\" = [${0}]) THEN [${1}] ELSE [${2}] END AS \"Deleted\" FROM \"dbo\".\"Users\" AS c",
                cmd.Parameters.Keys.ToArray()[0], cmd.Parameters.Keys.ToArray()[1], cmd.Parameters.Keys.ToArray()[2]), cmd.CommandText());
        }

        /// <summary>
        /// Tests select escaped case with sub query.
        /// </summary>
        [Test]
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
        [Test]
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
        [Test]
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
        [Test]
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
        [Test]
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
        [Test]
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
        [Test]
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
        [Test]
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
        [Test]
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
        [Test]
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