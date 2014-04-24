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
using System.Collections.Generic;
using System.Linq;
using DynamORM.Builders;
using NUnit.Framework;

namespace DynamORM.Tests.Select
{
    /// <summary>Test standard dynamic access ORM.</summary>
    [TestFixture]
    public class DynamicAccessTests : TestsBase
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

        /// <summary>Create table using specified method.</summary>
        /// <returns>Dynamic table.</returns>
        public virtual IDynamicSelectQueryBuilder GetTestBuilder()
        {
            return Database.Table("users").Query() as IDynamicSelectQueryBuilder;
        }

        #region Select

        /// <summary>Test unknown op.</summary>
        [Test]
        public void TestUnknownOperation()
        {
            Assert.Throws<InvalidOperationException>(() => GetTestTable().MakeMeASandwitch(with: "cheese"));
        }

        /// <summary>Test dynamic <c>Count</c> method.</summary>
        [Test]
        public void TestCount()
        {
            Assert.AreEqual(200, GetTestTable().Count(columns: "id"));
        }

        /// <summary>Test dynamic <c>Count</c> method.</summary>
        [Test]
        public void TestCount2()
        {
            Assert.AreEqual(200, GetTestBuilder().Select(x => x.Count(x.id)).Scalar());
        }

        /// <summary>Test count with in statement.</summary>
        [Test]
        public void TestSelectInEnumerableCount()
        {
            Assert.AreEqual(4, GetTestTable().Count(last: new DynamicColumn
            {
                Operator = DynamicColumn.CompareOperator.In,
                Value = new object[] { "Hendricks", "Goodwin", "Freeman" }.Take(3)
            }));
        }

        /// <summary>Test count with in statement.</summary>
        [Test]
        public void TestSelectInEnumerableCount2()
        {
            Assert.AreEqual(4, GetTestBuilder()
                .Where(x => x.last.In(new object[] { "Hendricks", "Goodwin", "Freeman" }.Take(3)))
                .Select(x => x.Count())
                .Scalar());
        }

        /// <summary>Test count with in statement.</summary>
        [Test]
        public void TestSelectInArrayCount()
        {
            Assert.AreEqual(4, GetTestTable().Count(last: new DynamicColumn
            {
                Operator = DynamicColumn.CompareOperator.In,
                Value = new object[] { "Hendricks", "Goodwin", "Freeman" }
            }));
        }

        /// <summary>Test count with in statement.</summary>
        [Test]
        public void TestSelectInArrayCount2()
        {
            Assert.AreEqual(4, GetTestBuilder()
                .Where(x => x.last.In(new object[] { "Hendricks", "Goodwin", "Freeman" }))
                .Select(x => x.Count())
                .Scalar());
        }

        /// <summary>Test dynamic <c>First</c> method.</summary>
        [Test]
        public void TestFirst()
        {
            Assert.AreEqual(1, GetTestTable().First(columns: "id").id);
        }

        /// <summary>Test dynamic <c>First</c> method.</summary>
        [Test]
        public void TestFirst2()
        {
            Assert.AreEqual(1, GetTestBuilder()
                .Select(x => x.id)
                .Execute()
                .First().id);
        }

        /// <summary>Test dynamic <c>Last</c> method.</summary>
        [Test]
        public void TestLast()
        {
            Assert.AreEqual(200, GetTestTable().Last(columns: "id").id);
        }

        /// <summary>Test dynamic <c>Last</c> method.</summary>
        [Test]
        public void TestLast2()
        {
            Assert.AreEqual(200, GetTestBuilder()
                .Select(x => x.id)
                .Execute()
                .Last().id);
        }

        /// <summary>Test dynamic <c>Count</c> method.</summary>
        [Test]
        public void TestCountSpecificRecord()
        {
            Assert.AreEqual(1, GetTestTable().Count(first: "Ori"));
        }

        /// <summary>Test dynamic <c>Count</c> method.</summary>
        [Test]
        public void TestCountSpecificRecord2()
        {
            Assert.AreEqual(1, GetTestBuilder()
                .Where(x => x.first == "Ori")
                .Select(x => x.Count())
                .Scalar());
        }

        /// <summary>Test dynamic <c>Min</c> method.</summary>
        [Test]
        public void TestMin()
        {
            Assert.AreEqual(1, GetTestTable().Min(columns: "id"));
        }

        /// <summary>Test dynamic <c>Min</c> method.</summary>
        [Test]
        public void TestMin2()
        {
            Assert.AreEqual(1, GetTestBuilder()
                .Select(x => x.Min(x.id))
                .Scalar());
        }

        /// <summary>Test dynamic <c>Min</c> method.</summary>
        [Test]
        public void TestMax()
        {
            Assert.AreEqual(200, GetTestTable().Max(columns: "id"));
        }

        /// <summary>Test dynamic <c>Min</c> method.</summary>
        [Test]
        public void TestMax2()
        {
            Assert.AreEqual(200, GetTestBuilder()
                .Select(x => x.Max(x.id))
                .Scalar());
        }

        /// <summary>Test dynamic <c>Min</c> method.</summary>
        [Test]
        public void TesttAvg()
        {
            Assert.AreEqual(100.5, GetTestTable().Avg(columns: "id"));
        }

        /// <summary>Test dynamic <c>Min</c> method.</summary>
        [Test]
        public void TesttAvg2()
        {
            Assert.AreEqual(100.5, GetTestBuilder()
                .Select(x => x.Avg(x.id))
                .Scalar());
        }

        /// <summary>Test dynamic <c>Sum</c> method.</summary>
        [Test]
        public void TestSum()
        {
            Assert.AreEqual(20100, GetTestTable().Sum(columns: "id"));
        }

        /// <summary>Test dynamic <c>Sum</c> method.</summary>
        [Test]
        public void TestSum2()
        {
            Assert.AreEqual(20100, GetTestBuilder()
                .Select(x => x.Sum(x.id))
                .Scalar());
        }

        /// <summary>Test dynamic <c>Scalar</c> method for invalid operation exception.</summary>
        [Test]
        public void TestScalarException()
        {
            Assert.Throws<InvalidOperationException>(() => GetTestTable().Scalar(id: 19));
        }

        /// <summary>Test dynamic <c>Scalar</c> method.</summary>
        [Test]
        public void TestScalar()
        {
            Assert.AreEqual("Ori", GetTestTable().Scalar(columns: "first", id: 19));
        }

        /// <summary>Test dynamic <c>Scalar</c> method.</summary>
        [Test]
        public void TestScalar2()
        {
            Assert.AreEqual("Ori", GetTestBuilder()
                .Where(x => x.id == 19)
                .Select(x => x.first)
                .Scalar());
        }

        /// <summary>Test dynamic <c>Scalar</c> method with SQLite specific aggregate.</summary>
        [Test]
        public void TestScalarGroupConcat()
        {
            // This test should produce something like this:
            // select group_concat("first") AS first from "users" where "id" < 20;
            Assert.AreEqual("Clarke,Marny,Dai,Forrest,Blossom,George,Ivory,Inez,Sigourney,Fulton,Logan,Anne,Alexandra,Adena,Lionel,Aimee,Selma,Lara,Ori",
                GetTestTable().Scalar(columns: "first:first:group_concat", id: new DynamicColumn { Operator = DynamicColumn.CompareOperator.Lt, Value = 20 }));
        }

        /// <summary>Test dynamic <c>Scalar</c> method with SQLite specific aggregate.</summary>
        [Test]
        public void TestScalarGroupConcat2()
        {
            // This test should produce something like this:
            // select group_concat("first") AS first from "users" where "id" < 20;
            Assert.AreEqual("Clarke,Marny,Dai,Forrest,Blossom,George,Ivory,Inez,Sigourney,Fulton,Logan,Anne,Alexandra,Adena,Lionel,Aimee,Selma,Lara,Ori",
                GetTestBuilder()
                    .Where(x => x.id < 20)
                    .Select(x => x.group_concat(x.first).As(x.first))
                    .Scalar());
        }

        /// <summary>Test dynamic <c>Scalar</c> method with SQLite specific aggregate not using aggregate field.</summary>
        [Test]
        public void TestScalarGroupConcatNoAggregateField()
        {
            // This test should produce something like this:
            // select group_concat(first) AS first from "users" where "id" < 20;
            Assert.AreEqual("Clarke,Marny,Dai,Forrest,Blossom,George,Ivory,Inez,Sigourney,Fulton,Logan,Anne,Alexandra,Adena,Lionel,Aimee,Selma,Lara,Ori",
                GetTestTable().Scalar(columns: "group_concat(first):first", id: new DynamicColumn { Operator = DynamicColumn.CompareOperator.Lt, Value = 20 }));
        }

        /// <summary>Test dynamic <c>Scalar</c> method with SQLite specific aggregate not using aggregate field.</summary>
        [Test]
        public void TestScalarGroupConcatNoAggregateField2()
        {
            // This test should produce something like this:
            // select group_concat(first) AS first from "users" where "id" < 20;
            Assert.AreEqual("Clarke,Marny,Dai,Forrest,Blossom,George,Ivory,Inez,Sigourney,Fulton,Logan,Anne,Alexandra,Adena,Lionel,Aimee,Selma,Lara,Ori",
                GetTestBuilder()
                    .Where(x => x.id < 20)
                    .SelectColumn("group_concat(first):first")
                    .Scalar());
        }

        /// <summary>Test something fancy... like: <code>select "first", count("first") occurs from "users" group by "first" order by 2 desc;</code>.</summary>
        [Test]
        public void TestFancyAggregateQuery()
        {
            var v = (GetTestTable().Query(columns: "first,first:occurs:count", group: "first", order: ":desc:2") as IEnumerable<dynamic>).ToList();

            Assert.IsNotNull(v);
            Assert.AreEqual(187, v.Count());
            Assert.AreEqual(4, v.First().occurs);
            Assert.AreEqual("Logan", v.First().first);
            Assert.AreEqual(2, v.Take(10).Last().occurs);
            Assert.AreEqual(1, v.Take(11).Last().occurs);
            Assert.AreEqual(1, v.Last().occurs);
        }

        /// <summary>Test something fancy... like: <code>select "first", count("first") occurs from "users" group by "first" order by 2 desc;</code>.</summary>
        [Test]
        public void TestFancyAggregateQuery2()
        {
            var v = GetTestBuilder()
                .Select(x => x.first, x => x.Count(x.first).As(x.occurs))
                .GroupBy(x => x.first)
                .OrderBy(x => x.Desc(2))
                .Execute()
                .ToList();

            Assert.IsNotNull(v);
            Assert.AreEqual(187, v.Count());
            Assert.AreEqual(4, v.First().occurs);
            Assert.AreEqual("Logan", v.First().first);
            Assert.AreEqual(2, v.Take(10).Last().occurs);
            Assert.AreEqual(1, v.Take(11).Last().occurs);
            Assert.AreEqual(1, v.Last().occurs);
        }

        /// <summary>This time also something fancy... aggregate in aggregate <code>select AVG(LENGTH("login")) len from "users";</code>.</summary>
        [Test]
        public void TestAggregateInAggregate()
        {
            Assert.AreEqual(12.77, GetTestTable().Scalar(columns: @"length(""login""):len:avg"));
        }

        /// <summary>This time also something fancy... aggregate in aggregate <code>select AVG(LENGTH("login")) len from "users";</code>.</summary>
        [Test]
        public void TestAggregateInAggregate2()
        {
            Assert.AreEqual(12.77, GetTestBuilder()
                .Select(x => x.Avg(x.Length(x.login)).As(x.len))
                .Scalar());
        }

        /// <summary>This time also something fancy... aggregate in aggregate <code>select AVG(LENGTH("email")) len from "users";</code>.</summary>
        [Test]
        public void TestAggregateInAggregateMark2()
        {
            Assert.AreEqual(27.7, GetTestTable().Avg(columns: @"length(""email""):len"));
        }

        /// <summary>This time also something fancy... aggregate in aggregate <code>select AVG(LENGTH("email")) len from "users";</code>.</summary>
        [Test]
        public void TestAggregateInAggregateMark3()
        {
            Assert.AreEqual(27.7, GetTestBuilder()
                .Select(x => "AVG(LENGTH(email)) AS LEN")
                .Scalar());
        }

        /// <summary>Test emails longer than 27 chars. <code>select count(*) from "users" where length("email") > 27;</code>.</summary>
        public void TestFunctionInWhere()
        {
            Assert.AreEqual(97,
                GetTestTable().Count(condition1:
                    new DynamicColumn()
                    {
                        ColumnName = "email",
                        Aggregate = "length",
                        Operator = DynamicColumn.CompareOperator.Gt,
                        Value = 27
                    }));
        }

        /// <summary>Test emails longer than 27 chars. <code>select count(*) from "users" where length("email") > 27;</code>.</summary>
        public void TestFunctionInWhere2()
        {
            Assert.AreEqual(97, GetTestBuilder()
                .Where(x => x.Length(x.email) > 27)
                .Select(x => x.Count())
                .Scalar());
        }

        /// <summary>Test dynamic <c>Single</c> multi.</summary>
        [Test]
        public void TestSingleObject()
        {
            var exp = new { id = 19, first = "Ori", last = "Ellis" };
            var o = GetTestTable().Single(columns: "id,first,last", id: 19);

            Assert.AreEqual(exp.id, o.id);
            Assert.AreEqual(exp.first, o.first);
            Assert.AreEqual(exp.last, o.last);
        }

        /// <summary>Test dynamic <c>Single</c> multi.</summary>
        [Test]
        public void TestSingleObject2()
        {
            var exp = new { id = 19, first = "Ori", last = "Ellis" };
            var o = GetTestBuilder()
                .Where(x => x.id == 19)
                .Select(x => new { id = x.id, first = x.first, last = x.last })
                .Execute()
                .First();

            Assert.AreEqual(exp.id, o.id);
            Assert.AreEqual(exp.first, o.first);
            Assert.AreEqual(exp.last, o.last);
        }

        /// <summary>Test dynamic duplicate column name occurrence.</summary>
        [Test]
        public void TestDuplicateColumnNameException()
        {
            Assert.Throws<ArgumentException>(() => GetTestBuilder()
                .Where(x => x.id == 19)
                .Select(x => new
                {
                    id = x.id,
                    first = x.first,
                    last = x.last,
                })
                .Select(x => x.last.As(x.first)) // Make last be first
                .Execute()
                .First());
        }

        #endregion Select

        #region Where

        /// <summary>Test dynamic where expression equal.</summary>
        [Test]
        public void TestWhereEq()
        {
            Assert.AreEqual("hoyt.tran", GetTestTable().Single(where: new DynamicColumn("id").Eq(100)).login);
        }

        /// <summary>Test dynamic where expression equal.</summary>
        [Test]
        public void TestWhereEq2()
        {
            Assert.AreEqual("hoyt.tran", GetTestBuilder()
                .Where(x => x.id == 100).Execute().First().login);
        }

        /// <summary>Test dynamic where expression not equal.</summary>
        [Test]
        public void TestWhereNot()
        {
            Assert.AreEqual(199, GetTestTable().Count(where: new DynamicColumn("id").Not(100)));
        }

        /// <summary>Test dynamic where expression not equal.</summary>
        [Test]
        public void TestWhereNot2()
        {
            Assert.AreEqual(199, GetTestBuilder()
                .Where(x => x.id != 100)
                .Select(x => x.Count())
                .Scalar());
        }

        /// <summary>Test dynamic where expression like.</summary>
        [Test]
        public void TestWhereLike()
        {
            Assert.AreEqual(100, GetTestTable().Single(where: new DynamicColumn("login").Like("Hoyt.%")).id);
        }

        /// <summary>Test dynamic where expression like.</summary>
        [Test]
        public void TestWhereLike2()
        {
            Assert.AreEqual(100, GetTestBuilder()
                .Where(x => x.login.Like("Hoyt.%")).Execute().First().id);
        }

        /// <summary>Test dynamic where expression not like.</summary>
        [Test]
        public void TestWhereNotLike()
        {
            Assert.AreEqual(199, GetTestTable().Count(where: new DynamicColumn("login").NotLike("Hoyt.%")));
        }

        /// <summary>Test dynamic where expression not like.</summary>
        [Test]
        public void TestWhereNotLike2()
        {
            Assert.AreEqual(199, GetTestBuilder()
                .Where(x => x.login.NotLike("Hoyt.%"))
                .Select(x => x.Count())
                .Scalar());
        }

        /// <summary>Test dynamic where expression not like.</summary>
        [Test]
        public void TestWhereNotLike3()
        {
            Assert.AreEqual(199, GetTestBuilder()
                .Where(x => !x.login.Like("Hoyt.%"))
                .Select(x => x.Count())
                .Scalar());
        }

        /// <summary>Test dynamic where expression greater.</summary>
        [Test]
        public void TestWhereGt()
        {
            Assert.AreEqual(100, GetTestTable().Count(where: new DynamicColumn("id").Greater(100)));
        }

        /// <summary>Test dynamic where expression greater.</summary>
        [Test]
        public void TestWhereGt2()
        {
            Assert.AreEqual(100, GetTestBuilder()
                .Where(x => x.id > 100)
                .Select(x => x.Count())
                .Scalar());
        }

        /// <summary>Test dynamic where expression greater or equal.</summary>
        [Test]
        public void TestWhereGte()
        {
            Assert.AreEqual(101, GetTestTable().Count(where: new DynamicColumn("id").GreaterOrEqual(100)));
        }

        /// <summary>Test dynamic where expression greater or equal.</summary>
        [Test]
        public void TestWhereGte2()
        {
            Assert.AreEqual(101, GetTestBuilder()
               .Where(x => x.id >= 100)
               .Select(x => x.Count())
               .Scalar());
        }

        /// <summary>Test dynamic where expression less.</summary>
        [Test]
        public void TestWhereLt()
        {
            Assert.AreEqual(99, GetTestTable().Count(where: new DynamicColumn("id").Less(100)));
        }

        /// <summary>Test dynamic where expression less.</summary>
        [Test]
        public void TestWhereLt2()
        {
            Assert.AreEqual(99, GetTestBuilder()
                .Where(x => x.id < 100)
                .Select(x => x.Count())
                .Scalar());
        }

        /// <summary>Test dynamic where expression less or equal.</summary>
        [Test]
        public void TestWhereLte()
        {
            Assert.AreEqual(100, GetTestTable().Count(where: new DynamicColumn("id").LessOrEqual(100)));
        }

        /// <summary>Test dynamic where expression less or equal.</summary>
        [Test]
        public void TestWhereLte2()
        {
            Assert.AreEqual(100, GetTestBuilder()
                .Where(x => x.id <= 100)
                .Select(x => x.Count())
                .Scalar());
        }

        /// <summary>Test dynamic where expression between.</summary>
        [Test]
        public void TestWhereBetween()
        {
            Assert.AreEqual(26, GetTestTable().Count(where: new DynamicColumn("id").Between(75, 100)));
        }

        /// <summary>Test dynamic where expression between.</summary>
        [Test]
        public void TestWhereBetween2()
        {
            Assert.AreEqual(26, GetTestBuilder()
                .Where(x => x.id.Between(75, 100))
                .Select(x => x.Count())
                .Scalar());
        }

        /// <summary>Test dynamic where expression in parameters.</summary>
        [Test]
        public void TestWhereIn1()
        {
            Assert.AreEqual(3, GetTestTable().Count(where: new DynamicColumn("id").In(75, 99, 100)));
        }

        /// <summary>Test dynamic where expression in array.</summary>
        [Test]
        public void TestWhereIn2()
        {
            Assert.AreEqual(3, GetTestTable().Count(where: new DynamicColumn("id").In(new[] { 75, 99, 100 })));
        }

        /// <summary>Test dynamic where expression in parameters.</summary>
        [Test]
        public void TestWhereIn3()
        {
            Assert.AreEqual(3, GetTestBuilder()
                .Where(x => x.id.In(75, 99, 100))
                .Select(x => x.Count())
                .Scalar());
        }

        /// <summary>Test dynamic where expression in parameters.</summary>
        [Test]
        public void TestWhereIn4()
        {
            Assert.AreEqual(3, GetTestBuilder()
                .Where(x => x.id.In(new[] { 75, 99, 100 }))
                .Select(x => x.Count())
                .Scalar());
        }

        #endregion Where
    }
}