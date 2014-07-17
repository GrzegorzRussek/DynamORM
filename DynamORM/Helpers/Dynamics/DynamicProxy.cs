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
using System.Dynamic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using DynamORM.Mapper;

namespace DynamORM.Helpers.Dynamics
{
    /// <summary>Class that allows to use interfaces as dynamic objects.</summary>
    /// <typeparam name="T">Type of class to proxy.</typeparam>
    /// <remarks>This is temporary solution. Which allows to use builders as a dynamic type.</remarks>
    public class DynamicProxy<T> : DynamicObject
    {
        private T _proxy;
        private Type _type;
        private Dictionary<string, DynamicPropertyInvoker> _properties;
        private Dictionary<MethodInfo, Delegate> _methods;

        /// <summary>
        /// Initializes a new instance of the <see cref="DynamicProxy{T}" /> class.
        /// </summary>
        /// <param name="proxiedObject">The object to which proxy should be created.</param>
        /// <exception cref="System.ArgumentNullException">The object to which proxy should be created is null.</exception>
        public DynamicProxy(T proxiedObject)
        {
            if (proxiedObject == null)
                throw new ArgumentNullException("proxiedObject");

            _proxy = proxiedObject;
            _type = typeof(T);

            var members = GetAllMembers(_type);

            _properties = members
                .Where(x => x is PropertyInfo)
                .ToDictionary(
                    k => k.Name,
                    v => new DynamicPropertyInvoker((PropertyInfo)v, null));

            _methods = members
                .Where(x => x is MethodInfo)
                .Cast<MethodInfo>()
                .Where(m => !((m.Name.StartsWith("set_") && m.ReturnType == typeof(void)) || m.Name.StartsWith("get_")))
                .Where(m => !m.IsStatic && !m.IsGenericMethod)
                .ToDictionary(
                    k => k,
                    v =>
                    {
                        try
                        {
                            return Delegate.CreateDelegate(Expression.GetDelegateType(v.GetParameters().Select(t => t.ParameterType).Concat(new[] { v.ReflectedType }).ToArray()), _proxy, v.Name);
                        }
                        catch (ArgumentException)
                        {
                            return null;
                        }
                    });
        }

        /// <summary>Provides implementation for type conversion operations.
        /// Classes derived from the <see cref="T:System.Dynamic.DynamicObject" />
        /// class can override this method to specify dynamic behavior for
        /// operations that convert an object from one type to another.</summary>
        /// <param name="binder">Provides information about the conversion operation.
        /// The binder.Type property provides the type to which the object must be
        /// converted. For example, for the statement (String)sampleObject in C#
        /// (CType(sampleObject, Type) in Visual Basic), where sampleObject is an
        /// instance of the class derived from the <see cref="T:System.Dynamic.DynamicObject" />
        /// class, binder.Type returns the <see cref="T:System.String" /> type.
        /// The binder.Explicit property provides information about the kind of
        /// conversion that occurs. It returns true for explicit conversion and
        /// false for implicit conversion.</param>
        /// <param name="result">The result of the type conversion operation.</param>
        /// <returns>Returns <c>true</c> if the operation is successful; otherwise, <c>false</c>.
        /// If this method returns false, the run-time binder of the language determines the
        /// behavior. (In most cases, a language-specific run-time exception is thrown).</returns>
        public override bool TryConvert(ConvertBinder binder, out object result)
        {
            if (binder.Type == typeof(T))
            {
                result = _proxy;
                return true;
            }

            if (_proxy != null &&
                binder.Type.IsAssignableFrom(_proxy.GetType()))
            {
                result = _proxy;
                return true;
            }

            return base.TryConvert(binder, out result);
        }

        /// <summary>Provides the implementation for operations that get member
        /// values. Classes derived from the <see cref="T:System.Dynamic.DynamicObject" />
        /// class can override this method to specify dynamic behavior for
        /// operations such as getting a value for a property.</summary>
        /// <param name="binder">Provides information about the object that
        /// called the dynamic operation. The binder.Name property provides
        /// the name of the member on which the dynamic operation is performed.
        /// For example, for the Console.WriteLine(sampleObject.SampleProperty)
        /// statement, where sampleObject is an instance of the class derived
        /// from the <see cref="T:System.Dynamic.DynamicObject" /> class,
        /// binder.Name returns "SampleProperty". The binder.IgnoreCase property
        /// specifies whether the member name is case-sensitive.</param>
        /// <param name="result">The result of the get operation. For example,
        /// if the method is called for a property, you can assign the property
        /// value to <paramref name="result" />.</param>
        /// <returns>Returns <c>true</c> if the operation is successful; otherwise,
        /// <c>false</c>. If this method returns false, the run-time binder of the
        /// language determines the behavior. (In most cases, a run-time exception
        /// is thrown).</returns>
        public override bool TryGetMember(GetMemberBinder binder, out object result)
        {
            try
            {
                var prop = _properties.TryGetValue(binder.Name);

                result = prop.NullOr(p => p.Get.NullOr(g => g(_proxy), null), null);

                return prop != null && prop.Get != null;
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException(string.Format("Cannot get member {0}", binder.Name), ex);
            }
        }

        /// <summary>Provides the implementation for operations that set member
        /// values. Classes derived from the <see cref="T:System.Dynamic.DynamicObject" />
        /// class can override this method to specify dynamic behavior for operations
        /// such as setting a value for a property.</summary>
        /// <param name="binder">Provides information about the object that called
        /// the dynamic operation. The binder.Name property provides the name of
        /// the member to which the value is being assigned. For example, for the
        /// statement sampleObject.SampleProperty = "Test", where sampleObject is
        /// an instance of the class derived from the <see cref="T:System.Dynamic.DynamicObject" />
        /// class, binder.Name returns "SampleProperty". The binder.IgnoreCase
        /// property specifies whether the member name is case-sensitive.</param>
        /// <param name="value">The value to set to the member. For example, for
        /// sampleObject.SampleProperty = "Test", where sampleObject is an instance
        /// of the class derived from the <see cref="T:System.Dynamic.DynamicObject" />
        /// class, the <paramref name="value" /> is "Test".</param>
        /// <returns>Returns <c>true</c> if the operation is successful; otherwise,
        /// <c>false</c>. If this method returns false, the run-time binder of the
        /// language determines the behavior. (In most cases, a language-specific
        /// run-time exception is thrown).</returns>
        public override bool TrySetMember(SetMemberBinder binder, object value)
        {
            try
            {
                var prop = _properties.TryGetValue(binder.Name);

                if (prop != null && prop.Set != null)
                {
                    prop.Set(_proxy, value);
                    return true;
                }

                return false;
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException(string.Format("Cannot set member {0} to '{1}'", binder.Name, value), ex);
            }
        }

        /// <summary>Provides the implementation for operations that invoke a member.
        /// Classes derived from the <see cref="T:System.Dynamic.DynamicObject" />
        /// class can override this method to specify dynamic behavior for
        /// operations such as calling a method.</summary>
        /// <param name="binder">Provides information about the dynamic operation.
        /// The binder.Name property provides the name of the member on which the
        /// dynamic operation is performed. For example, for the statement
        /// sampleObject.SampleMethod(100), where sampleObject is an instance of
        /// the class derived from the <see cref="T:System.Dynamic.DynamicObject" />
        /// class, binder.Name returns "SampleMethod". The binder.IgnoreCase property
        /// specifies whether the member name is case-sensitive.</param>
        /// <param name="args">The arguments that are passed to the object member
        /// during the invoke operation. For example, for the statement
        /// sampleObject.SampleMethod(100), where sampleObject is derived from the
        /// <see cref="T:System.Dynamic.DynamicObject" /> class,
        /// First element of <paramref name="args" /> is equal to 100.</param>
        /// <param name="result">The result of the member invocation.</param>
        /// <returns>Returns <c>true</c> if the operation is successful; otherwise,
        /// <c>false</c>. If this method returns false, the run-time binder of the
        /// language determines the behavior. (In most cases, a language-specific
        /// run-time exception is thrown).</returns>
        public override bool TryInvokeMember(InvokeMemberBinder binder, object[] args, out object result)
        {
            return TryInvokeMethod(binder.Name, out result, args) || base.TryInvokeMember(binder, args, out result);
        }

        private bool TryInvokeMethod(string name, out object result, object[] args)
        {
            result = null;

            MethodInfo mi = _methods.Keys
                .Where(m => m.Name == name)
                .FirstOrDefault(m =>
                    CompareTypes(m.GetParameters().ToArray(),
                    args.Select(a => a.GetType()).ToArray()));

            Delegate d = _methods.TryGetValue(mi);

            if (d != null)
            {
                result = d.DynamicInvoke(CompleteArguments(mi.GetParameters().ToArray(), args));

                if (d.Method.ReturnType == _type && result is T)
                    result = new DynamicProxy<T>((T)result);

                return true;
            }
            else if (mi != null)
            {
                result = mi.Invoke(_proxy, CompleteArguments(mi.GetParameters().ToArray(), args));

                if (mi.ReturnType == _type && result is T)
                    result = new DynamicProxy<T>((T)result);

                return true;
            }

            return false;
        }

        private bool CompareTypes(ParameterInfo[] parameters, Type[] types)
        {
            if (parameters.Length < types.Length || parameters.Count(p => !p.IsOptional) > types.Length)
                return false;

            for (int i = 0; i < types.Length; i++)
                if (types[i] != parameters[i].ParameterType && !parameters[i].ParameterType.IsAssignableFrom(types[i]))
                    return false;

            return true;
        }

        private object[] CompleteArguments(ParameterInfo[] parameters, object[] arguments)
        {
            return arguments.Concat(parameters.Skip(arguments.Length).Select(p => p.DefaultValue)).ToArray();
        }

        private IEnumerable<MemberInfo> GetAllMembers(Type type)
        {
            if (type.IsInterface)
            {
                var members = new List<MemberInfo>();

                var considered = new List<Type>();
                var queue = new Queue<Type>();

                considered.Add(type);
                queue.Enqueue(type);

                while (queue.Count > 0)
                {
                    var subType = queue.Dequeue();
                    foreach (var subInterface in subType.GetInterfaces())
                    {
                        if (considered.Contains(subInterface)) continue;

                        considered.Add(subInterface);
                        queue.Enqueue(subInterface);
                    }

                    var typeProperties = subType.GetMembers(
                        BindingFlags.FlattenHierarchy
                        | BindingFlags.Public
                        | BindingFlags.Instance);

                    var newPropertyInfos = typeProperties
                        .Where(x => !members.Contains(x));

                    members.InsertRange(0, newPropertyInfos);
                }

                return members;
            }

            return type.GetMembers(BindingFlags.FlattenHierarchy | BindingFlags.Public | BindingFlags.Instance);
        }
    }
}