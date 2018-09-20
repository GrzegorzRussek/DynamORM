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
using DynamORM.Helpers.Dynamics;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace DynamORM.Tests.Helpers.Dynamic
{
    /// <summary><see cref="DynamicParser"/> tests.</summary>
    [TestClass]
    public class DynamicParserTests
    {
        /// <summary>
        /// Tests the get member.
        /// </summary>
        [TestMethod]
        public void TestGetMember()
        {
            Func<dynamic, object> f = x => x.SomePropery;

            var val = DynamicParser.Parse(f).Result as DynamicParser.Node.GetMember;

            Assert.IsNotNull(val);
            Assert.AreEqual("SomePropery", val.Name);
        }

        /// <summary>
        /// Tests the set member.
        /// </summary>
        [TestMethod]
        public void TestSetMember()
        {
            Func<dynamic, object> f = x => x.SomePropery = "value";

            var val = DynamicParser.Parse(f).Result as DynamicParser.Node.SetMember;

            Assert.IsNotNull(val);
            Assert.AreEqual("SomePropery", val.Name);
            Assert.AreEqual("value", val.Value);
        }

        /// <summary>
        /// Tests the index of the get.
        /// </summary>
        [TestMethod]
        public void TestGetIndex()
        {
            Func<dynamic, object> f = x => x.SomePropery[0];

            var val = DynamicParser.Parse(f).Result as DynamicParser.Node.GetIndex;

            Assert.IsNotNull(val);
        }

        /// <summary>
        /// Tests the index of the set.
        /// </summary>
        [TestMethod]
        public void TestSetIndex()
        {
            Func<dynamic, object> f = x => x.SomePropery[0] = "value";

            var val = DynamicParser.Parse(f).Result as DynamicParser.Node.SetIndex;

            Assert.IsNotNull(val);
            Assert.AreEqual("value", val.Value);
        }

        /// <summary>
        /// Tests something.
        /// </summary>
        [TestMethod]
        public void TestSomething()
        {
            Func<dynamic, object> f = x => x.SomePropery == "value" || x.OtherProperty == -1;

            var p = DynamicParser.Parse(f);
            var val = p.Result as DynamicParser.Node.Binary;

            Assert.IsNotNull(val);

            var left = val.Host as DynamicParser.Node.Binary;
            var right = val.Right as DynamicParser.Node.Binary;

            Assert.IsNotNull(left);
            Assert.IsNotNull(right);

            Assert.IsInstanceOfType(left.Host, typeof(DynamicParser.Node.GetMember));
            Assert.IsInstanceOfType(right.Host, typeof(DynamicParser.Node.GetMember));

            Assert.AreEqual("value", left.Right);
            Assert.AreEqual(-1, right.Right);
        }
    }
}