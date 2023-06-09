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

using System.Collections.Generic;
using System.Diagnostics;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace DynamORM.Tests.Helpers
{
    /// <summary>Class responsible for users operations testing.</summary>
    [TestClass]
    public class AttachToDebugger
    {
        /// <summary>Test anonymous type compatibility.</summary>
        [TestMethod]
        public void TestAnonType()
        {
            var a = new { x = 1, y = 2 };
            var b = new { x = 3, y = 4 };

            Assert.AreEqual(a.GetType(), b.GetType());
        }

        /// <summary>Test anonymous type value.</summary>
        [TestMethod]
        public void TestAnonTypeValue()
        {
            var a = new { x = 1, y = "bla bla" };
            var b = new { x = 1, y = "bla bla" };

            Assert.AreEqual(a, b);
            Assert.IsTrue(a.Equals(b));

            Dictionary<object, int> dict = new Dictionary<object, int>() { { a, 999 } };

            Assert.IsTrue(dict.ContainsKey(b));
        }
    }
}