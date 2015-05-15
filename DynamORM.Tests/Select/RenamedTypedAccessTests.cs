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
using System.Linq;
using DynamORM.Tests.Helpers;
using NUnit.Framework;

namespace DynamORM.Tests.Select
{
    /// <summary>Test typed ORM.</summary>
    [TestFixture]
    public class RenamedTypedAccessTests : TypedAccessTests<Users>
    {
        /// <summary>Test something fancy... like: <code>select "first", count("first") aggregatefield from "users" group by "first" order by 2 desc;</code>.</summary>
        [Test]
        public override void TestTypedFancyAggregateQuery()
        {
            var v = (GetTestTable().Query(type: typeof(Users), columns: "first,first:AggregateField:count", group: "first", order: ":desc:2") as IEnumerable<dynamic>).ToList();

            Assert.IsNotNull(v);
            Assert.AreEqual(187, v.Count());
            Assert.AreEqual(4, v.First().AggregateField);
            Assert.AreEqual("Logan", v.First().First);
            Assert.AreEqual(2, v.Take(10).Last().AggregateField);
            Assert.AreEqual(1, v.Take(11).Last().AggregateField);
            Assert.AreEqual(1, v.Last().AggregateField);
        }

        /// <summary>Test something fancy... like: <code>select "first", count("first") aggregatefield from "users" group by "first" order by 2 desc;</code>.</summary>
        [Test]
        public override void TestGenericFancyAggregateQuery()
        {
            var v = (GetTestTable().Query<Users>(columns: "first,first:AggregateField:count", group: "first", order: ":desc:2") as IEnumerable<dynamic>).ToList();

            Assert.IsNotNull(v);
            Assert.AreEqual(187, v.Count());
            Assert.AreEqual(4, v.First().AggregateField);
            Assert.AreEqual("Logan", v.First().First);
            Assert.AreEqual(2, v.Take(10).Last().AggregateField);
            Assert.AreEqual(1, v.Take(11).Last().AggregateField);
            Assert.AreEqual(1, v.Last().AggregateField);
        }

        /// <summary>Test typed <c>First</c> method.</summary>
        [Test]
        public override void TestTypedFirst()
        {
            Assert.AreEqual(1, GetTestTable().First(type: typeof(Users), columns: "id").Id);
        }

        /// <summary>Test typed <c>Last</c> method.</summary>
        [Test]
        public override void TestTypedLast()
        {
            Assert.AreEqual(200, GetTestTable().Last(type: typeof(Users), columns: "id").Id);
        }

        /// <summary>Test typed <c>Single</c> multi.</summary>
        [Test]
        public override void TestTypedSingleObject()
        {
            var exp = new { id = 19, first = "Ori", last = "Ellis" };
            var o = GetTestTable().Single(type: typeof(Users), columns: "id,first,last", id: 19);

            Assert.AreEqual(exp.id, o.Id);
            Assert.AreEqual(exp.first, o.First);
            Assert.AreEqual(exp.last, o.Last);
        }

        /// <summary>Test typed where expression equal.</summary>
        [Test]
        public override void TestTypedWhereEq()
        {
            Assert.AreEqual("hoyt.tran", GetTestTable().Single(type: typeof(Users), where: new DynamicColumn("id").Eq(100)).Login);
        }

        /// <summary>Test typed where expression like.</summary>
        [Test]
        public override void TestTypedWhereLike()
        {
            Assert.AreEqual(100, GetTestTable().Single(type: typeof(Users), where: new DynamicColumn("login").Like("Hoyt.%")).Id);
        }

        /// <summary>Test generic <c>First</c> method.</summary>
        [Test]
        public override void TestGenericFirst()
        {
            Assert.AreEqual(1, GetTestTable().First<Users>(columns: "id").Id);
        }

        /// <summary>Test generic <c>Last</c> method.</summary>
        [Test]
        public override void TestGenericLast()
        {
            Assert.AreEqual(200, GetTestTable().Last<Users>(columns: "id").Id);
        }

        /// <summary>Test generic <c>Single</c> multi.</summary>
        [Test]
        public override void TestGenericSingleObject()
        {
            var exp = new { id = 19, first = "Ori", last = "Ellis" };
            var o = GetTestTable().Single<Users>(columns: "id,first,last", id: 19);

            Assert.AreEqual(exp.id, o.Id);
            Assert.AreEqual(exp.first, o.First);
            Assert.AreEqual(exp.last, o.Last);
        }

        /// <summary>Test generic where expression equal.</summary>
        [Test]
        public override void TestGenericWhereEq()
        {
            Assert.AreEqual("hoyt.tran", GetTestTable().Single<Users>(where: new DynamicColumn("id").Eq(100)).Login);
        }

        /// <summary>Test generic where expression like.</summary>
        [Test]
        public override void TestGenericWhereLike()
        {
            Assert.AreEqual(100, GetTestTable().Single<Users>(where: new DynamicColumn("login").Like("Hoyt.%")).Id);
        }
    }
}