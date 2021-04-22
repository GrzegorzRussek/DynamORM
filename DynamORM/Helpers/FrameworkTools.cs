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
using System.Dynamic;
using System.Linq.Expressions;
using System.Reflection;

namespace DynamORM.Helpers
{
    /// <summary>Framework detection and specific implementations.</summary>
    public static class FrameworkTools
    {
        #region Mono or .NET Framework detection

        /// <summary>This is pretty simple trick.</summary>
        private static bool _isMono = Type.GetType("Mono.Runtime") != null;

        /// <summary>Gets a value indicating whether application is running under mono runtime.</summary>
        public static bool IsMono { get { return _isMono; } }

        #endregion Mono or .NET Framework detection

        static FrameworkTools()
        {
            _frameworkTypeArgumentsGetter = CreateTypeArgumentsGetter();
        }

        #region GetGenericTypeArguments

        private static Func<InvokeMemberBinder, IList<Type>> _frameworkTypeArgumentsGetter = null;

        private static Func<InvokeMemberBinder, IList<Type>> CreateTypeArgumentsGetter()
        {
            // HACK: Creating binders assuming types are correct... this may fail.
            if (IsMono)
            {
                Type binderType = typeof(Microsoft.CSharp.RuntimeBinder.RuntimeBinderException).Assembly.GetType("Microsoft.CSharp.RuntimeBinder.CSharpInvokeMemberBinder");

                if (binderType != null)
                {
                    ParameterExpression param = Expression.Parameter(typeof(InvokeMemberBinder), "o");

                    try
                    {
                        return Expression.Lambda<Func<InvokeMemberBinder, IList<Type>>>(
                            Expression.TypeAs(
                                Expression.Field(
                                    Expression.TypeAs(param, binderType), "typeArguments"),
                                typeof(IList<Type>)), param).Compile();
                    }
                    catch
                    {
                    }

                    PropertyInfo prop = binderType.GetProperty("TypeArguments");

                    if (!prop.CanRead)
                        return null;

                    return Expression.Lambda<Func<InvokeMemberBinder, IList<Type>>>(
                        Expression.TypeAs(
                            Expression.Property(
                                Expression.TypeAs(param, binderType), prop.Name),
                            typeof(IList<Type>)), param).Compile();
                }
            }
            else
            {
                Type inter = typeof(Microsoft.CSharp.RuntimeBinder.RuntimeBinderException).Assembly.GetType("Microsoft.CSharp.RuntimeBinder.ICSharpInvokeOrInvokeMemberBinder");

                if (inter != null)
                {
                    PropertyInfo prop = inter.GetProperty("TypeArguments");

                    if (!prop.CanRead)
                        return null;

                    ParameterExpression objParm = Expression.Parameter(typeof(InvokeMemberBinder), "o");

                    return Expression.Lambda<Func<InvokeMemberBinder, IList<Type>>>(
                        Expression.TypeAs(
                            Expression.Property(
                                Expression.TypeAs(objParm, inter), prop.Name),
                            typeof(IList<Type>)), objParm).Compile();
                }
            }

            return null;
        }

        /// <summary>Extension method allowing to easily extract generic type
        /// arguments from <see cref="InvokeMemberBinder"/> assuming that it
        /// inherits from
        /// <c>Microsoft.CSharp.RuntimeBinder.ICSharpInvokeOrInvokeMemberBinder</c>
        /// in .NET Framework or
        /// <c>Microsoft.CSharp.RuntimeBinder.CSharpInvokeMemberBinder</c>
        /// under Mono.</summary>
        /// <param name="binder">Binder from which get type arguments.</param>
        /// <remarks>This is generally a bad solution, but there is no other
        /// currently so we have to go with it.</remarks>
        /// <returns>List of types passed as generic parameters.</returns>
        public static IList<Type> GetGenericTypeArguments(this InvokeMemberBinder binder)
        {
            // First try to use delegate if exist
            if (_frameworkTypeArgumentsGetter != null)
                return _frameworkTypeArgumentsGetter(binder);

            if (_isMono)
            {
                // HACK: Using Reflection
                // In mono this is trivial.

                // First we get field info.
                FieldInfo field = binder.GetType().GetField("typeArguments", BindingFlags.Instance |
                    BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static);

                // If this was a success get and return it's value
                if (field != null)
                    return field.GetValue(binder) as IList<Type>;
                else
                {
                    PropertyInfo prop = binder.GetType().GetProperty("TypeArguments");

                    // If we have a property, return it's value
                    if (prop != null)
                        return prop.GetValue(binder, null) as IList<Type>;
                }
            }
            else
            {
                // HACK: Using Reflection
                // In this case, we need more aerobic :D

                // First, get the interface
                Type inter = binder.GetType().GetInterface("Microsoft.CSharp.RuntimeBinder.ICSharpInvokeOrInvokeMemberBinder");

                if (inter != null)
                {
                    // Now get property.
                    PropertyInfo prop = inter.GetProperty("TypeArguments");

                    // If we have a property, return it's value
                    if (prop != null)
                        return prop.GetValue(binder, null) as IList<Type>;
                }
            }

            // Sadly return null if failed.
            return null;
        }

        #endregion GetGenericTypeArguments
    }
}