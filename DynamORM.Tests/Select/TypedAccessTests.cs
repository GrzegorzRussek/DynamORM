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
using System.Collections.Generic;
using System.Linq;
using DynamORM.Builders;
using DynamORM.Tests.Helpers;
using NUnit.Framework;

namespace DynamORM.Tests.Select
{
    /// <summary>Test typed ORM.</summary>
    /// <typeparam name="T">Type to test.</typeparam>
    [TestFixture(typeof(users))]
    public class TypedAccessTests<T> : TestsBase where T : class
    {
        /// <summary>Setup test parameters.</summary>
        [TestFixtureSetUp]
        public virtual void SetUp()
        {
            CreateTestDatabase();
            CreateDynamicDatabase();

            // Cache table (profiler freaks out)
            GetTestTable();
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
            return Database.Table();
        }

        /// <summary>Create table using specified method.</summary>
        /// <returns>Dynamic table.</returns>
        public virtual IDynamicSelectQueryBuilder GetTestBuilder()
        {
            return Database.Table().Query() as IDynamicSelectQueryBuilder;
        }

        #region Select typed

        /// <summary>Test load all rows into mapped list alternate way.</summary>
        [Test]
        public virtual void TestTypedGetAll()
        {
            var list = (GetTestTable().Query(type: typeof(T)) as IEnumerable<object>).Cast<T>().ToList();

            Assert.AreEqual(200, list.Count);
        }

        /// <summary>Test load all rows into mapped list alternate way.</summary>
        [Test]
        public virtual void TestTypedGetAll2()
        {
            var list = GetTestBuilder()
                .From(x => x(typeof(T)).As(x.t))
                .Execute()
                .MapEnumerable<T>()
                .ToList();

            Assert.AreEqual(200, list.Count);
        }

        /// <summary>Test load all rows into mapped list alternate way.</summary>
        [Test]
        public virtual void TestTypedGetAll3()
        {
            var list = GetTestBuilder()
                .From(x => x(typeof(T)).As(x.t))
                .Execute<T>()
                .ToList();

            Assert.AreEqual(200, list.Count);
        }

        /// <summary>Test unknown op.</summary>
        [Test]
        public virtual void TestTypedUnknownOperation()
        {
            Assert.Throws<InvalidOperationException>(() => GetTestTable().MakeMeASandwitch(type: typeof(T), with: "cheese"));
        }

        /// <summary>Test typed <c>Count</c> method.</summary>
        [Test]
        public virtual void TestTypedCount()
        {
            Assert.AreEqual(200, GetTestTable().Count(type: typeof(T), columns: "id"));
        }

        /// <summary>Test typed <c>Count</c> method.</summary>
        [Test]
        public virtual void TestTypedCount2()
        {
            Assert.AreEqual(200, GetTestBuilder()
                .From(x => x(typeof(T)).As(x.t))
                .Select(x => x.Count(x.t.id))
                .Scalar());
        }

        /// <summary>Test count with in statement.</summary>
        [Test]
        public virtual void TestTypedSelectInEnumerableCount()
        {
            Assert.AreEqual(4, GetTestTable().Count(type: typeof(T), last: new DynamicColumn
            {
                Operator = DynamicColumn.CompareOperator.In,
                Value = new object[] { "Hendricks", "Goodwin", "Freeman" }.Take(3)
            }));
        }

        /// <summary>Test count with in statement.</summary>
        [Test]
        public virtual void TestTypedSelectInEnumerableCount2()
        {
            Assert.AreEqual(4, GetTestBuilder()
                .From(x => x(typeof(T)).As(x.t))
                .Where(x => x.last.In(new object[] { "Hendricks", "Goodwin", "Freeman" }.Take(3)))
                .Select(x => x.Count())
                .Scalar());
        }

        /// <summary>Test count with in statement.</summary>
        [Test]
        public virtual void TestTypedSelectInArrayCount()
        {
            Assert.AreEqual(4, GetTestTable().Count(type: typeof(T), last: new DynamicColumn
            {
                Operator = DynamicColumn.CompareOperator.In,
                Value = new object[] { "Hendricks", "Goodwin", "Freeman" }
            }));
        }

        /// <summary>Test count with in statement.</summary>
        [Test]
        public virtual void TestTypedSelectInArrayCount2()
        {
            Assert.AreEqual(4, GetTestBuilder()
                .From(x => x(typeof(T)).As(x.t))
                .Where(x => x.last.In(new object[] { "Hendricks", "Goodwin", "Freeman" }))
                .Select(x => x.Count())
                .Scalar());
        }

        /// <summary>Test typed <c>First</c> method.</summary>
        [Test]
        public virtual void TestTypedFirst()
        {
            Assert.AreEqual(1, GetTestTable().First(type: typeof(T), columns: "id").id);
        }

        /// <summary>Test typed <c>First</c> method.</summary>
        [Test]
        public virtual void TestTypedFirst2()
        {
            Assert.AreEqual(1, GetTestBuilder()
                .From(x => x(typeof(T)).As(x.t))
                .Select(x => x.t.id)
                .Execute()
                .First().id);
        }

        /// <summary>Test typed <c>Last</c> method.</summary>
        [Test]
        public virtual void TestTypedLast()
        {
            Assert.AreEqual(200, GetTestTable().Last(type: typeof(T), columns: "id").id);
        }

        /// <summary>Test typed <c>Last</c> method.</summary>
        [Test]
        public virtual void TestTypedLast2()
        {
            Assert.AreEqual(200, GetTestBuilder()
                .From(x => x(typeof(T)).As(x.t))
                .Select(x => x.t.id)
                .Execute()
                .Last().id);
        }

        /// <summary>Test typed <c>Count</c> method.</summary>
        [Test]
        public virtual void TestTypedCountSpecificRecord()
        {
            Assert.AreEqual(1, GetTestTable().Count(type: typeof(T), first: "Ori"));
        }

        /// <summary>Test typed <c>Min</c> method.</summary>
        [Test]
        public virtual void TestTypedMin()
        {
            Assert.AreEqual(1, GetTestTable().Min(type: typeof(T), columns: "id"));
        }

        /// <summary>Test typed <c>Min</c> method.</summary>
        [Test]
        public virtual void TestTypedMin2()
        {
            Assert.AreEqual(1, GetTestBuilder()
                .From(x => x(typeof(T)).As(x.t))
                .Select(x => x.Min(x.t.id))
                .Scalar());
        }

        /// <summary>Test typed <c>Min</c> method.</summary>
        [Test]
        public virtual void TestTypedMax()
        {
            Assert.AreEqual(200, GetTestTable().Max(type: typeof(T), columns: "id"));
        }

        /// <summary>Test typed <c>Min</c> method.</summary>
        [Test]
        public virtual void TestTypedMax2()
        {
            Assert.AreEqual(200, GetTestBuilder()
                .From(x => x(typeof(T)).As(x.t))
                .Select(x => x.Max(x.t.id))
                .Scalar());
        }

        /// <summary>Test typed <c>Min</c> method.</summary>
        [Test]
        public virtual void TestTypedtAvg()
        {
            Assert.AreEqual(100.5, GetTestTable().Avg(type: typeof(T), columns: "id"));
        }

        /// <summary>Test typed <c>Min</c> method.</summary>
        [Test]
        public virtual void TestTypedtAvg2()
        {
            Assert.AreEqual(100.5, GetTestBuilder()
                .From(x => x(typeof(T)).As(x.t))
                .Select(x => x.Avg(x.t.id))
                .Scalar());
        }

        /// <summary>Test typed <c>Sum</c> method.</summary>
        [Test]
        public virtual void TestTypedSum()
        {
            Assert.AreEqual(20100, GetTestTable().Sum(type: typeof(T), columns: "id"));
        }

        /// <summary>Test typed <c>Sum</c> method.</summary>
        [Test]
        public virtual void TestTypedSum2()
        {
            Assert.AreEqual(20100, GetTestBuilder()
                .From(x => x(typeof(T)).As(x.t))
                .Select(x => x.Sum(x.t.id))
                .Scalar());
        }

        /// <summary>Test typed <c>Scalar</c> method for invalid operation exception.</summary>
        [Test]
        public virtual void TestTypedScalarException()
        {
            Assert.Throws<InvalidOperationException>(() => GetTestTable().Scalar(type: typeof(T), id: 19));
        }

        /// <summary>Test typed <c>Scalar</c> method.</summary>
        [Test]
        public virtual void TestTypedScalar()
        {
            Assert.AreEqual("Ori", GetTestTable().Scalar(type: typeof(T), columns: "first", id: 19));
        }

        /// <summary>Test typed <c>Scalar</c> method.</summary>
        [Test]
        public void TestTypedScalar2()
        {
            Assert.AreEqual("Ori", GetTestBuilder()
                .From(x => x(typeof(T)).As(x.t))
                .Where(x => x.t.id == 19)
                .Select(x => x.t.first)
                .Scalar());
        }

        /// <summary>Test typed <c>Scalar</c> method with SQLite specific aggregate.</summary>
        [Test]
        public virtual void TestTypedScalarGroupConcat()
        {
            // This test should produce something like this:
            // select group_concat("first") AS first from "users" where "id" < 20;
            Assert.AreEqual("Clarke,Marny,Dai,Forrest,Blossom,George,Ivory,Inez,Sigourney,Fulton,Logan,Anne,Alexandra,Adena,Lionel,Aimee,Selma,Lara,Ori",
                GetTestTable().Scalar(type: typeof(T), columns: "first:first:group_concat", id: new DynamicColumn { Operator = DynamicColumn.CompareOperator.Lt, Value = 20 }));
        }

        /// <summary>Test typed <c>Scalar</c> method with SQLite specific aggregate.</summary>
        [Test]
        public virtual void TestTypedScalarGroupConcat2()
        {
            // This test should produce something like this:
            // select group_concat("first") AS first from "users" where "id" < 20;
            Assert.AreEqual("Clarke,Marny,Dai,Forrest,Blossom,George,Ivory,Inez,Sigourney,Fulton,Logan,Anne,Alexandra,Adena,Lionel,Aimee,Selma,Lara,Ori",
                GetTestBuilder()
                    .From(x => x(typeof(T)).As(x.t))
                    .Where(x => x.t.id < 20)
                    .Select(x => x.group_concat(x.t.first).As(x.first))
                    .Scalar());
        }

        /// <summary>Test typed <c>Scalar</c> method with SQLite specific aggregate not using aggregate field.</summary>
        [Test]
        public virtual void TestTypedScalarGroupConcatNoAggregateField()
        {
            // This test should produce something like this:
            // select group_concat(first) AS first from "users" where "id" < 20;
            Assert.AreEqual("Clarke,Marny,Dai,Forrest,Blossom,George,Ivory,Inez,Sigourney,Fulton,Logan,Anne,Alexandra,Adena,Lionel,Aimee,Selma,Lara,Ori",
                GetTestTable().Scalar(type: typeof(T), columns: "group_concat(first):first", id: new DynamicColumn { Operator = DynamicColumn.CompareOperator.Lt, Value = 20 }));
        }

        /// <summary>Test typed <c>Scalar</c> method with SQLite specific aggregate not using aggregate field.</summary>
        [Test]
        public virtual void TestTypedScalarGroupConcatNoAggregateField2()
        {
            // This test should produce something like this:
            // select group_concat("first") AS first from "users" where "id" < 20;
            Assert.AreEqual("Clarke,Marny,Dai,Forrest,Blossom,George,Ivory,Inez,Sigourney,Fulton,Logan,Anne,Alexandra,Adena,Lionel,Aimee,Selma,Lara,Ori",
                GetTestBuilder()
                    .From(x => x(typeof(T)).As(x.t))
                    .Where(x => x.t.id < 20)
                    .SelectColumn("group_concat(first):first")
                    .Scalar());
        }

        /// <summary>Test something fancy... like: <code>select "first", count("first") aggregatefield from "users" group by "first" order by 2 desc;</code>.</summary>
        [Test]
        public virtual void TestTypedFancyAggregateQuery()
        {
            var v = (GetTestTable().Query(type: typeof(T), columns: "first,first:aggregatefield:count", group: "first", order: ":desc:2") as IEnumerable<dynamic>).ToList();

            Assert.IsNotNull(v);
            Assert.AreEqual(187, v.Count());
            Assert.AreEqual(4, v.First().aggregatefield);
            Assert.AreEqual("Logan", v.First().first);
            Assert.AreEqual(2, v.Take(10).Last().aggregatefield);
            Assert.AreEqual(1, v.Take(11).Last().aggregatefield);
            Assert.AreEqual(1, v.Last().aggregatefield);
        }

        /// <summary>Test something fancy... like: <code>select "first", count("first") aggregatefield from "users" group by "first" order by 2 desc;</code>.</summary>
        [Test]
        public virtual void TestTypedFancyAggregateQuery2()
        {
            var v = GetTestBuilder()
                .From(x => x(typeof(T)).As(x.t))
                .Select(x => x.t.first, x => x.Count(x.t.first).As(x.aggregatefield))
                .GroupBy(x => x.t.first)
                .OrderBy(x => x.Desc(2))
                .Execute()
                .ToList();

            Assert.IsNotNull(v);
            Assert.AreEqual(187, v.Count());
            Assert.AreEqual(4, v.First().aggregatefield);
            Assert.AreEqual("Logan", v.First().first);
            Assert.AreEqual(2, v.Take(10).Last().aggregatefield);
            Assert.AreEqual(1, v.Take(11).Last().aggregatefield);
            Assert.AreEqual(1, v.Last().aggregatefield);
        }

        /// <summary>This time also something fancy... aggregate in aggregate <code>select AVG(LENGTH("login")) len from "users";</code>.</summary>
        [Test]
        public virtual void TestTypedAggregateInAggregate()
        {
            Assert.AreEqual(12.77, GetTestTable().Scalar(type: typeof(T), columns: @"length(""login""):len:avg"));
        }

        /// <summary>This time also something fancy... aggregate in aggregate <code>select AVG(LENGTH("login")) len from "users";</code>.</summary>
        [Test]
        public virtual void TestTypedAggregateInAggregate2()
        {
            Assert.AreEqual(12.77, GetTestBuilder()
                .From(x => x(typeof(T)).As(x.t))
                .Select(x => x.Avg(x.Length(x.t.login)).As(x.len))
                .Scalar());
        }

        /// <summary>This time also something fancy... aggregate in aggregate <code>select AVG(LENGTH("email")) len from "users";</code>.</summary>
        [Test]
        public virtual void TestTypedAggregateInAggregateMark2()
        {
            Assert.AreEqual(27.7, GetTestTable().Avg(type: typeof(T), columns: @"length(""email""):len"));
        }

        /// <summary>This time also something fancy... aggregate in aggregate <code>select AVG(LENGTH("email")) len from "users";</code>.</summary>
        [Test]
        public virtual void TestTypedAggregateInAggregateMark3()
        {
            Assert.AreEqual(27.7, GetTestBuilder()
                .From(x => x(typeof(T)).As(x.t))
                .Select(x => "AVG(LENGTH(t.email)) AS LEN")
                .Scalar());
        }

        /// <summary>Test emails longer than 27 chars. <code>select count(*) from "users" where length("email") > 27;</code>.</summary>
        public virtual void TestTypedFunctionInWhere()
        {
            Assert.AreEqual(97,
                GetTestTable().Count(type: typeof(T), condition1:
                    new DynamicColumn()
                    {
                        ColumnName = "email",
                        Aggregate = "length",
                        Operator = DynamicColumn.CompareOperator.Gt,
                        Value = 27
                    }));
        }

        /// <summary>Test emails longer than 27 chars. <code>select count(*) from "users" where length("email") > 27;</code>.</summary>
        public virtual void TestTypedFunctionInWhere2()
        {
            Assert.AreEqual(97, GetTestBuilder()
                .From(x => x(typeof(T)).As(x.t))
                .Where(x => x.Length(x.t.email) > 27)
                .Select(x => x.Count(x.t.All()))
                .Scalar());
        }

        /// <summary>Test typed <c>Single</c> multi.</summary>
        [Test]
        public virtual void TestTypedSingleObject()
        {
            var exp = new { id = 19, first = "Ori", last = "Ellis" };
            var o = GetTestTable().Single(type: typeof(T), columns: "id,first,last", id: 19);

            Assert.AreEqual(exp.id, o.id);
            Assert.AreEqual(exp.first, o.first);
            Assert.AreEqual(exp.last, o.last);
        }

        /// <summary>Test typed <c>Single</c> multi.</summary>
        [Test]
        public virtual void TestTypedSingleObject2()
        {
            var exp = new { id = 19, first = "Ori", last = "Ellis" };
            var o = GetTestBuilder()
                .From(x => x(typeof(T)).As(x.t))
                .Where(x => x.t.id == 19)
                .Select(x => new { id = x.t.id, first = x.t.first, last = x.t.last })
                .Execute()
                .First();

            Assert.AreEqual(exp.id, o.id);
            Assert.AreEqual(exp.first, o.first);
            Assert.AreEqual(exp.last, o.last);
        }

        #endregion Select typed

        #region Where typed

        /// <summary>Test typed where expression equal.</summary>
        [Test]
        public virtual void TestTypedWhereEq()
        {
            Assert.AreEqual("hoyt.tran", GetTestTable().Single(type: typeof(T), where: new DynamicColumn("id").Eq(100)).login);
        }

        /// <summary>Test typed where expression equal.</summary>
        [Test]
        public virtual void TestTypedWhereEq2()
        {
            Assert.AreEqual("hoyt.tran", GetTestBuilder()
                .From(x => x(typeof(T)).As(x.t))
                .Where(x => x.t.id == 100).Execute().First().login);
        }

        /// <summary>Test typed where expression not equal.</summary>
        [Test]
        public virtual void TestTypedWhereNot()
        {
            Assert.AreEqual(199, GetTestTable().Count(type: typeof(T), where: new DynamicColumn("id").Not(100)));
        }

        /// <summary>Test typed where expression not equal.</summary>
        [Test]
        public virtual void TestTypedWhereNot2()
        {
            Assert.AreEqual(199, GetTestBuilder()
                .From(x => x(typeof(T)).As(x.t))
                .Where(x => x.t.id != 100)
                .Select(x => x.Count())
                .Scalar());
        }

        /// <summary>Test typed where expression like.</summary>
        [Test]
        public virtual void TestTypedWhereLike()
        {
            Assert.AreEqual(100, GetTestTable().Single(type: typeof(T), where: new DynamicColumn("login").Like("Hoyt.%")).id);
        }

        /// <summary>Test typed where expression like.</summary>
        [Test]
        public virtual void TestTypedWhereLike2()
        {
            Assert.AreEqual(100, GetTestBuilder()
                .From(x => x(typeof(T)).As(x.t))
                .Where(x => x.t.login.Like("Hoyt.%")).Execute().First().id);
        }

        /// <summary>Test typed where expression not like.</summary>
        [Test]
        public virtual void TestTypedWhereNotLike()
        {
            Assert.AreEqual(199, GetTestTable().Count(type: typeof(T), where: new DynamicColumn("login").NotLike("Hoyt.%")));
        }

        /// <summary>Test typed where expression not like.</summary>
        [Test]
        public virtual void TestTypedWhereNotLike2()
        {
            Assert.AreEqual(199, GetTestBuilder()
                .From(x => x(typeof(T)).As(x.t))
                .Where(x => x.t.login.NotLike("Hoyt.%"))
                .Select(x => x.Count())
                .Scalar());
        }

        /// <summary>Test typed where expression not like.</summary>
        [Test]
        public virtual void TestTypedWhereNotLike3()
        {
            Assert.AreEqual(199, GetTestBuilder()
                .From(x => x(typeof(T)).As(x.t))
                .Where(x => !x.t.login.Like("Hoyt.%"))
                .Select(x => x.Count())
                .Scalar());
        }

        /// <summary>Test typed where expression greater.</summary>
        [Test]
        public virtual void TestTypedWhereGt()
        {
            Assert.AreEqual(100, GetTestTable().Count(type: typeof(T), where: new DynamicColumn("id").Greater(100)));
        }

        /// <summary>Test typed where expression greater.</summary>
        [Test]
        public virtual void TestTypedWhereGt2()
        {
            Assert.AreEqual(100, GetTestBuilder()
                .From(x => x(typeof(T)).As(x.t))
                .Where(x => x.t.id > 100)
                .Select(x => x.Count())
                .Scalar());
        }

        /// <summary>Test typed where expression greater or equal.</summary>
        [Test]
        public virtual void TestTypedWhereGte()
        {
            Assert.AreEqual(101, GetTestTable().Count(type: typeof(T), where: new DynamicColumn("id").GreaterOrEqual(100)));
        }

        /// <summary>Test typed where expression greater or equal.</summary>
        [Test]
        public virtual void TestTypedWhereGte2()
        {
            Assert.AreEqual(101, GetTestBuilder()
                .From(x => x(typeof(T)).As(x.t))
                .Where(x => x.t.id >= 100)
                .Select(x => x.Count())
                .Scalar());
        }

        /// <summary>Test typed where expression less.</summary>
        [Test]
        public virtual void TestTypedWhereLt()
        {
            Assert.AreEqual(99, GetTestTable().Count(type: typeof(T), where: new DynamicColumn("id").Less(100)));
        }

        /// <summary>Test typed where expression less.</summary>
        [Test]
        public virtual void TestTypedWhereLt2()
        {
            Assert.AreEqual(99, GetTestBuilder()
                .From(x => x(typeof(T)).As(x.t))
                .Where(x => x.t.id < 100)
                .Select(x => x.Count())
                .Scalar());
        }

        /// <summary>Test typed where expression less or equal.</summary>
        [Test]
        public virtual void TestTypedWhereLte()
        {
            Assert.AreEqual(100, GetTestTable().Count(type: typeof(T), where: new DynamicColumn("id").LessOrEqual(100)));
        }

        /// <summary>Test typed where expression less or equal.</summary>
        [Test]
        public virtual void TestTypedWhereLte2()
        {
            Assert.AreEqual(100, GetTestBuilder()
                .From(x => x(typeof(T)).As(x.t))
                .Where(x => x.t.id <= 100)
                .Select(x => x.Count())
                .Scalar());
        }

        /// <summary>Test typed where expression between.</summary>
        [Test]
        public virtual void TestTypedWhereBetween()
        {
            Assert.AreEqual(26, GetTestTable().Count(type: typeof(T), where: new DynamicColumn("id").Between(75, 100)));
        }

        /// <summary>Test typed where expression between.</summary>
        [Test]
        public virtual void TestTypedWhereBetween2()
        {
            Assert.AreEqual(26, GetTestBuilder()
                .From(x => x(typeof(T)).As(x.t))
                .Where(x => x.t.id.Between(75, 100))
                .Select(x => x.Count())
                .Scalar());
        }

        /// <summary>Test typed where expression in parameters.</summary>
        [Test]
        public virtual void TestTypedWhereIn1()
        {
            Assert.AreEqual(3, GetTestTable().Count(type: typeof(T), where: new DynamicColumn("id").In(75, 99, 100)));
        }

        /// <summary>Test typed where expression in array.</summary>
        [Test]
        public virtual void TestTypedWhereIn2()
        {
            Assert.AreEqual(3, GetTestTable().Count(type: typeof(T), where: new DynamicColumn("id").In(new[] { 75, 99, 100 })));
        }

        /// <summary>Test typed where expression in parameters.</summary>
        [Test]
        public virtual void TestTypedWhereIn3()
        {
            Assert.AreEqual(3, GetTestBuilder()
                .From(x => x(typeof(T)).As(x.t))
                .Where(x => x.t.id.In(75, 99, 100))
                .Select(x => x.Count())
                .Scalar());
        }

        /// <summary>Test typed where expression in array.</summary>
        [Test]
        public virtual void TestTypedWhereIn4()
        {
            Assert.AreEqual(3, GetTestBuilder()
                .From(x => x(typeof(T)).As(x.t))
                .Where(x => x.t.id.In(new[] { 75, 99, 100 }))
                .Select(x => x.Count())
                .Scalar());
        }

        #endregion Where typed

        #region Select generic

        /// <summary>Test load all rows into mapped list alternate way.</summary>
        [Test]
        public virtual void TestGenericGetAll()
        {
            var list = (GetTestTable().Query<T>() as IEnumerable<object>).Cast<T>().ToList();

            Assert.AreEqual(200, list.Count);
        }

        /// <summary>Test unknown op.</summary>
        [Test]
        public virtual void TestGenericUnknownOperation()
        {
            Assert.Throws<InvalidOperationException>(() => GetTestTable().MakeMeASandwitch<T>(with: "cheese"));
        }

        /// <summary>Test generic <c>Count</c> method.</summary>
        [Test]
        public virtual void TestGenericCount()
        {
            Assert.AreEqual(200, GetTestTable().Count<T>(columns: "id"));
        }

        /// <summary>Test count with in statement.</summary>
        [Test]
        public virtual void TestGenericSelectInEnumerableCount()
        {
            Assert.AreEqual(4, GetTestTable().Count<T>(last: new DynamicColumn
            {
                Operator = DynamicColumn.CompareOperator.In,
                Value = new object[] { "Hendricks", "Goodwin", "Freeman" }.Take(3)
            }));
        }

        /// <summary>Test count with in statement.</summary>
        [Test]
        public virtual void TestGenericSelectInArrayCount()
        {
            Assert.AreEqual(4, GetTestTable().Count<T>(last: new DynamicColumn
            {
                Operator = DynamicColumn.CompareOperator.In,
                Value = new object[] { "Hendricks", "Goodwin", "Freeman" }
            }));
        }

        /// <summary>Test generic <c>First</c> method.</summary>
        [Test]
        public virtual void TestGenericFirst()
        {
            Assert.AreEqual(1, GetTestTable().First<T>(columns: "id").id);
        }

        /// <summary>Test generic <c>Last</c> method.</summary>
        [Test]
        public virtual void TestGenericLast()
        {
            Assert.AreEqual(200, GetTestTable().Last<T>(columns: "id").id);
        }

        /// <summary>Test generic <c>Count</c> method.</summary>
        [Test]
        public virtual void TestGenericCountSpecificRecord()
        {
            Assert.AreEqual(1, GetTestTable().Count<T>(first: "Ori"));
        }

        /// <summary>Test generic <c>Min</c> method.</summary>
        [Test]
        public virtual void TestGenericMin()
        {
            Assert.AreEqual(1, GetTestTable().Min<T>(columns: "id"));
        }

        /// <summary>Test generic <c>Min</c> method.</summary>
        [Test]
        public virtual void TestGenericMax()
        {
            Assert.AreEqual(200, GetTestTable().Max<T>(columns: "id"));
        }

        /// <summary>Test generic <c>Min</c> method.</summary>
        [Test]
        public virtual void TestGenerictAvg()
        {
            Assert.AreEqual(100.5, GetTestTable().Avg<T>(columns: "id"));
        }

        /// <summary>Test generic <c>Sum</c> method.</summary>
        [Test]
        public virtual void TestGenericSum()
        {
            Assert.AreEqual(20100, GetTestTable().Sum<T>(columns: "id"));
        }

        /// <summary>Test generic <c>Scalar</c> method for invalid operation exception.</summary>
        [Test]
        public virtual void TestGenericScalarException()
        {
            Assert.Throws<InvalidOperationException>(() => GetTestTable().Scalar<T>(id: 19));
        }

        /// <summary>Test generic <c>Scalar</c> method.</summary>
        [Test]
        public virtual void TestGenericScalar()
        {
            Assert.AreEqual("Ori", GetTestTable().Scalar<T>(columns: "first", id: 19));
        }

        /// <summary>Test generic <c>Scalar</c> method with SQLite specific aggregate.</summary>
        [Test]
        public virtual void TestGenericScalarGroupConcat()
        {
            // This test should produce something like this:
            // select group_concat("first") AS first from "users" where "id" < 20;
            Assert.AreEqual("Clarke,Marny,Dai,Forrest,Blossom,George,Ivory,Inez,Sigourney,Fulton,Logan,Anne,Alexandra,Adena,Lionel,Aimee,Selma,Lara,Ori",
                GetTestTable().Scalar<T>(columns: "first:first:group_concat", id: new DynamicColumn { Operator = DynamicColumn.CompareOperator.Lt, Value = 20 }));
        }

        /// <summary>Test generic <c>Scalar</c> method with SQLite specific aggregate not using aggregate field.</summary>
        [Test]
        public virtual void TestGenericScalarGroupConcatNoAggregateField()
        {
            // This test should produce something like this:
            // select group_concat(first) AS first from "users" where "id" < 20;
            Assert.AreEqual("Clarke,Marny,Dai,Forrest,Blossom,George,Ivory,Inez,Sigourney,Fulton,Logan,Anne,Alexandra,Adena,Lionel,Aimee,Selma,Lara,Ori",
                GetTestTable().Scalar<T>(columns: "group_concat(first):first", id: new DynamicColumn { Operator = DynamicColumn.CompareOperator.Lt, Value = 20 }));
        }

        /// <summary>Test something fancy... like: <code>select "first", count("first") aggregatefield from "users" group by "first" order by 2 desc;</code>.</summary>
        [Test]
        public virtual void TestGenericFancyAggregateQuery()
        {
            var v = (GetTestTable().Query<T>(columns: "first,first:aggregatefield:count", group: "first", order: ":desc:2") as IEnumerable<dynamic>).ToList();

            Assert.IsNotNull(v);
            Assert.AreEqual(187, v.Count());
            Assert.AreEqual(4, v.First().aggregatefield);
            Assert.AreEqual("Logan", v.First().first);
            Assert.AreEqual(2, v.Take(10).Last().aggregatefield);
            Assert.AreEqual(1, v.Take(11).Last().aggregatefield);
            Assert.AreEqual(1, v.Last().aggregatefield);
        }

        /// <summary>This time also something fancy... aggregate in aggregate <code>select AVG(LENGTH("login")) len from "users";</code>.</summary>
        [Test]
        public virtual void TestGenericAggregateInAggregate()
        {
            Assert.AreEqual(12.77, GetTestTable().Scalar<T>(columns: @"length(""login""):len:avg"));
        }

        /// <summary>This time also something fancy... aggregate in aggregate <code>select AVG(LENGTH("email")) len from "users";</code>.</summary>
        [Test]
        public virtual void TestGenericAggregateInAggregateMark2()
        {
            Assert.AreEqual(27.7, GetTestTable().Avg<T>(columns: @"length(""email""):len"));
        }

        /// <summary>Test emails longer than 27 chars. <code>select count(*) from "users" where length("email") > 27;</code>.</summary>
        public virtual void TestGenericFunctionInWhere()
        {
            Assert.AreEqual(97,
                GetTestTable().Count<T>(condition1:
                    new DynamicColumn()
                    {
                        ColumnName = "email",
                        Aggregate = "length",
                        Operator = DynamicColumn.CompareOperator.Gt,
                        Value = 27
                    }));
        }

        /// <summary>Test generic <c>Single</c> multi.</summary>
        [Test]
        public virtual void TestGenericSingleObject()
        {
            var exp = new { id = 19, first = "Ori", last = "Ellis" };
            var o = GetTestTable().Single<T>(columns: "id,first,last", id: 19);

            Assert.AreEqual(exp.id, o.id);
            Assert.AreEqual(exp.first, o.first);
            Assert.AreEqual(exp.last, o.last);
        }

        #endregion Select generic

        #region Where generic

        /// <summary>Test generic where expression equal.</summary>
        [Test]
        public virtual void TestGenericWhereEq()
        {
            Assert.AreEqual("hoyt.tran", GetTestTable().Single<T>(where: new DynamicColumn("id").Eq(100)).login);
        }

        /// <summary>Test generic where expression not equal.</summary>
        [Test]
        public virtual void TestGenericWhereNot()
        {
            Assert.AreEqual(199, GetTestTable().Count<T>(where: new DynamicColumn("id").Not(100)));
        }

        /// <summary>Test generic where expression like.</summary>
        [Test]
        public virtual void TestGenericWhereLike()
        {
            Assert.AreEqual(100, GetTestTable().Single<T>(where: new DynamicColumn("login").Like("Hoyt.%")).id);
        }

        /// <summary>Test generic where expression not like.</summary>
        [Test]
        public virtual void TestGenericWhereNotLike()
        {
            Assert.AreEqual(199, GetTestTable().Count<T>(where: new DynamicColumn("login").NotLike("Hoyt.%")));
        }

        /// <summary>Test generic where expression greater.</summary>
        [Test]
        public virtual void TestGenericWhereGt()
        {
            Assert.AreEqual(100, GetTestTable().Count<T>(where: new DynamicColumn("id").Greater(100)));
        }

        /// <summary>Test generic where expression greater or equal.</summary>
        [Test]
        public virtual void TestGenericWhereGte()
        {
            Assert.AreEqual(101, GetTestTable().Count<T>(where: new DynamicColumn("id").GreaterOrEqual(100)));
        }

        /// <summary>Test generic where expression less.</summary>
        [Test]
        public virtual void TestGenericWhereLt()
        {
            Assert.AreEqual(99, GetTestTable().Count<T>(where: new DynamicColumn("id").Less(100)));
        }

        /// <summary>Test generic where expression less or equal.</summary>
        [Test]
        public virtual void TestGenericWhereLte()
        {
            Assert.AreEqual(100, GetTestTable().Count<T>(where: new DynamicColumn("id").LessOrEqual(100)));
        }

        /// <summary>Test generic where expression between.</summary>
        [Test]
        public virtual void TestGenericWhereBetween()
        {
            Assert.AreEqual(26, GetTestTable().Count<T>(where: new DynamicColumn("id").Between(75, 100)));
        }

        /// <summary>Test generic where expression in parameters.</summary>
        [Test]
        public virtual void TestGenericWhereIn1()
        {
            Assert.AreEqual(3, GetTestTable().Count<T>(where: new DynamicColumn("id").In(75, 99, 100)));
        }

        /// <summary>Test generic where expression in array.</summary>
        [Test]
        public virtual void TestGenericWhereIn2()
        {
            Assert.AreEqual(3, GetTestTable().Count<T>(where: new DynamicColumn("id").In(new[] { 75, 99, 100 })));
        }

        #endregion Where generic
    }
}