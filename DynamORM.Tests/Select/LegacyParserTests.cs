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
    /// <summary>Tests of legacy parser methods.</summary>
    [TestFixture]
    public class LegacyParserTests : TestsBase
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
        /// Tests the where expression equal.
        /// </summary>
        [Test]
        public void TestWhereEq()
        {
            IDynamicSelectQueryBuilder cmd = new DynamicSelectQueryBuilder(Database);

            cmd.From(x => x.dbo.Users.As(x.u))
                .Where(new DynamicColumn("u.Deleted").Eq(0));

            Assert.AreEqual(string.Format("SELECT * FROM \"dbo\".\"Users\" AS u WHERE (u.\"Deleted\" = [${0}])", cmd.Parameters.Keys.First()), cmd.CommandText());
        }

        /// <summary>
        /// Tests the where expression equal with brackets.
        /// </summary>
        [Test]
        public void TestWhereBracketsEq()
        {
            IDynamicSelectQueryBuilder cmd = new DynamicSelectQueryBuilder(Database);

            cmd.From(x => x.dbo.Users.As(x.u))
                .Where(new DynamicColumn("u.Deleted").Eq(0).SetBeginBlock())
                .Where(new DynamicColumn("u.IsActive").Eq(1).SetEndBlock());

            Assert.AreEqual(string.Format("SELECT * FROM \"dbo\".\"Users\" AS u WHERE ((u.\"Deleted\" = [${0}]) AND (u.\"IsActive\" = [${1}]))", cmd.Parameters.Keys.First(), cmd.Parameters.Keys.Last()), cmd.CommandText());
        }

        /// <summary>
        /// Tests the where expression equal with brackets and or condition.
        /// </summary>
        [Test]
        public void TestWhereBracketsOrEq()
        {
            IDynamicSelectQueryBuilder cmd = new DynamicSelectQueryBuilder(Database);

            cmd.From(x => x.dbo.Users.As(x.u))
                .Where(new DynamicColumn("u.Deleted").Eq(0).SetBeginBlock())
                .Where(new DynamicColumn("u.IsActive").Eq(1).SetOr().SetEndBlock());

            Assert.AreEqual(string.Format("SELECT * FROM \"dbo\".\"Users\" AS u WHERE ((u.\"Deleted\" = [${0}]) OR (u.\"IsActive\" = [${1}]))", cmd.Parameters.Keys.First(), cmd.Parameters.Keys.Last()), cmd.CommandText());
        }

        /// <summary>
        /// Tests the where expression equal with brackets.
        /// </summary>
        [Test]
        public void TestWhereBracketsOrEq2()
        {
            IDynamicSelectQueryBuilder cmd = new DynamicSelectQueryBuilder(Database);

            cmd.From(x => x.dbo.Users.As(x.u))
                .Where(new DynamicColumn("u.Id_User").Greater(1))
                .Where(new DynamicColumn("u.Deleted").Eq(0).SetBeginBlock())
                .Where(new DynamicColumn("u.IsActive").Eq(1).SetOr().SetEndBlock());

            Assert.AreEqual(string.Format("SELECT * FROM \"dbo\".\"Users\" AS u WHERE (u.\"Id_User\" > [${0}]) AND ((u.\"Deleted\" = [${1}]) OR (u.\"IsActive\" = [${2}]))",
                cmd.Parameters.Keys.ToArray()[0], cmd.Parameters.Keys.ToArray()[1], cmd.Parameters.Keys.ToArray()[2]), cmd.CommandText());
        }

        /// <summary>
        /// Tests the where expression equal with brackets.
        /// </summary>
        [Test]
        public void TestWhereBracketsOrEqForgotToEnd()
        {
            IDynamicSelectQueryBuilder cmd = new DynamicSelectQueryBuilder(Database);

            cmd.From(x => x.dbo.Users.As(x.u))
                .Where(new DynamicColumn("u.Id_User").Greater(1))
                .Where(new DynamicColumn("u.Deleted").Eq(0).SetBeginBlock())
                .Where(new DynamicColumn("u.IsActive").Eq(1).SetOr());

            using (var con = Database.Open())
            using (var c = con.CreateCommand())
                Assert.AreEqual(string.Format("SELECT * FROM \"dbo\".\"Users\" AS u WHERE (u.\"Id_User\" > @0) AND ((u.\"Deleted\" = @1) OR (u.\"IsActive\" = @2))"),
                    c.SetCommand(cmd).CommandText);
        }

        /// <summary>
        /// Tests the where expression not equal.
        /// </summary>
        [Test]
        public void TestWhereNotEq()
        {
            IDynamicSelectQueryBuilder cmd = new DynamicSelectQueryBuilder(Database);

            cmd.From(x => x.dbo.Users.As(x.u))
                .Where(new DynamicColumn("u.Deleted").Not(0));

            Assert.AreEqual(string.Format("SELECT * FROM \"dbo\".\"Users\" AS u WHERE (u.\"Deleted\" <> [${0}])", cmd.Parameters.Keys.First()), cmd.CommandText());
        }

        /// <summary>
        /// Tests the where expression greater.
        /// </summary>
        [Test]
        public void TestWhereGreater()
        {
            IDynamicSelectQueryBuilder cmd = new DynamicSelectQueryBuilder(Database);

            cmd.From(x => x.dbo.Users.As(x.u))
                .Where(new DynamicColumn("u.Deleted").Greater(0));

            Assert.AreEqual(string.Format("SELECT * FROM \"dbo\".\"Users\" AS u WHERE (u.\"Deleted\" > [${0}])", cmd.Parameters.Keys.First()), cmd.CommandText());
        }

        /// <summary>
        /// Tests the where expression greater or equal.
        /// </summary>
        [Test]
        public void TestWhereGreaterOrEqual()
        {
            IDynamicSelectQueryBuilder cmd = new DynamicSelectQueryBuilder(Database);

            cmd.From(x => x.dbo.Users.As(x.u))
                .Where(new DynamicColumn("u.Deleted").GreaterOrEqual(0));

            Assert.AreEqual(string.Format("SELECT * FROM \"dbo\".\"Users\" AS u WHERE (u.\"Deleted\" >= [${0}])", cmd.Parameters.Keys.First()), cmd.CommandText());
        }

        /// <summary>
        /// Tests the where expression less.
        /// </summary>
        [Test]
        public void TestWhereLess()
        {
            IDynamicSelectQueryBuilder cmd = new DynamicSelectQueryBuilder(Database);

            cmd.From(x => x.dbo.Users.As(x.u))
                .Where(new DynamicColumn("u.Deleted").Less(1));

            Assert.AreEqual(string.Format("SELECT * FROM \"dbo\".\"Users\" AS u WHERE (u.\"Deleted\" < [${0}])", cmd.Parameters.Keys.First()), cmd.CommandText());
        }

        /// <summary>
        /// Tests the where expression less or equal.
        /// </summary>
        [Test]
        public void TestWhereLessOrEqual()
        {
            IDynamicSelectQueryBuilder cmd = new DynamicSelectQueryBuilder(Database);

            cmd.From(x => x.dbo.Users.As(x.u))
                .Where(new DynamicColumn("u.Deleted").LessOrEqual(1));

            Assert.AreEqual(string.Format("SELECT * FROM \"dbo\".\"Users\" AS u WHERE (u.\"Deleted\" <= [${0}])", cmd.Parameters.Keys.First()), cmd.CommandText());
        }

        /// <summary>
        /// Tests the where expression like.
        /// </summary>
        [Test]
        public void TestWhereLike()
        {
            IDynamicSelectQueryBuilder cmd = new DynamicSelectQueryBuilder(Database);

            cmd.From(x => x.dbo.Users.As(x.u))
                .Where(new DynamicColumn("u.Deleted").Like("%1"));

            Assert.AreEqual(string.Format("SELECT * FROM \"dbo\".\"Users\" AS u WHERE u.\"Deleted\" LIKE [${0}]", cmd.Parameters.Keys.First()), cmd.CommandText());
        }

        /// <summary>
        /// Tests the where expression not like.
        /// </summary>
        [Test]
        public void TestWhereNotLike()
        {
            IDynamicSelectQueryBuilder cmd = new DynamicSelectQueryBuilder(Database);

            cmd.From(x => x.dbo.Users.As(x.u))
                .Where(new DynamicColumn("u.Deleted").NotLike("%1"));

            Assert.AreEqual(string.Format("SELECT * FROM \"dbo\".\"Users\" AS u WHERE u.\"Deleted\" NOT LIKE [${0}]", cmd.Parameters.Keys.First()), cmd.CommandText());
        }

        /// <summary>
        /// Tests the where expression between.
        /// </summary>
        [Test]
        public void TestWhereBetween()
        {
            IDynamicSelectQueryBuilder cmd = new DynamicSelectQueryBuilder(Database);

            cmd.From(x => x.dbo.Users.As(x.u))
                .Where(new DynamicColumn("u.Deleted").Between(0, 1));

            Assert.AreEqual(string.Format("SELECT * FROM \"dbo\".\"Users\" AS u WHERE u.\"Deleted\" BETWEEN [${0}] AND [${1}]", cmd.Parameters.Keys.First(), cmd.Parameters.Keys.Last()), cmd.CommandText());
        }

        /// <summary>
        /// Tests the where expression in.
        /// </summary>
        [Test]
        public void TestWhereIn()
        {
            IDynamicSelectQueryBuilder cmd = new DynamicSelectQueryBuilder(Database);

            cmd.From(x => x.dbo.Users.As(x.u))
                .Where(new DynamicColumn("u.Deleted").In(0, 1));

            Assert.AreEqual(string.Format("SELECT * FROM \"dbo\".\"Users\" AS u WHERE u.\"Deleted\" IN([${0}], [${1}])", cmd.Parameters.Keys.First(), cmd.Parameters.Keys.Last()), cmd.CommandText());
        }

        /// <summary>
        /// Tests the where expression using anonymous types.
        /// </summary>
        [Test]
        public void TestWhereAnon()
        {
            IDynamicSelectQueryBuilder cmd = new DynamicSelectQueryBuilder(Database);

            cmd.From(x => x.dbo.Users.As(x.u))
                .Where(new { Deleted = 0, IsActive = 1, _table = "u" });

            Assert.AreEqual(string.Format("SELECT * FROM \"dbo\".\"Users\" AS u WHERE (u.\"Deleted\" = [${0}]) AND (u.\"IsActive\" = [${1}])", cmd.Parameters.Keys.First(), cmd.Parameters.Keys.Last()), cmd.CommandText());
        }

        /// <summary>
        /// Tests the order by column.
        /// </summary>
        [Test]
        public void TestOrderByCol()
        {
            IDynamicSelectQueryBuilder cmd = new DynamicSelectQueryBuilder(Database);

            cmd.From(x => x.dbo.Users.As(x.u))
                .OrderBy(new DynamicColumn("u.Name").Desc());

            Assert.AreEqual(string.Format("SELECT * FROM \"dbo\".\"Users\" AS u ORDER BY u.\"Name\" DESC"), cmd.CommandText());
        }

        /// <summary>
        /// Tests the order by column number.
        /// </summary>
        [Test]
        public void TestOrderByNum()
        {
            IDynamicSelectQueryBuilder cmd = new DynamicSelectQueryBuilder(Database);

            cmd.From(x => x.dbo.Users.As(x.u))
                .OrderBy(new DynamicColumn("u.Name").SetAlias("1").Desc());

            Assert.AreEqual(string.Format("SELECT * FROM \"dbo\".\"Users\" AS u ORDER BY 1 DESC"), cmd.CommandText());
        }

        /// <summary>
        /// Tests the group by column.
        /// </summary>
        [Test]
        public void TestGroupByCol()
        {
            IDynamicSelectQueryBuilder cmd = new DynamicSelectQueryBuilder(Database);

            cmd.From(x => x.dbo.Users.As(x.u))
                .GroupBy(new DynamicColumn("u.Name"));

            Assert.AreEqual(string.Format("SELECT * FROM \"dbo\".\"Users\" AS u GROUP BY u.\"Name\""), cmd.CommandText());
        }
    }
}