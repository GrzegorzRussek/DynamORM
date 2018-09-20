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

using DynamORM.Mapper;
using DynamORM.Validation;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace DynamORM.Tests.Helpers.Validation
{
    [TestClass]
    public class ObjectValidationTest
    {
        public class TestObject
        {
            [Required(1f, 10f)]
            public int TestInt { get; set; }

            [Required(7, false, false)]
            public string CanBeNull { get; set; }

            [Required(2, true)]
            [Required(7, 18, ElementRequirement = true)]
            public decimal[] ArrayTest { get; set; }
        }

        [TestMethod]
        public void ValidateCorrectObject()
        {
            var result = DynamicMapperCache.GetMapper<TestObject>().ValidateObject(
                new TestObject
                {
                    TestInt = 2,
                    ArrayTest = new decimal[] { 7, 18 },
                });

            Assert.IsNotNull(result);
            Assert.AreEqual(0, result.Count);
        }

        [TestMethod]
        public void ValidateIncorrectObject()
        {
            var result = DynamicMapperCache.GetMapper<TestObject>().ValidateObject(
                new TestObject
                {
                    TestInt = 0,
                    CanBeNull = string.Empty,
                    ArrayTest = new decimal[] { 0, 0 },
                });

            Assert.IsNotNull(result);
            Assert.AreEqual(4, result.Count);
        }
    }
}