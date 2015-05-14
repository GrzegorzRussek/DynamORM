/*
 * DynamORM - Dynamic Object-Relational Mapping library.
 * Copyright (c) 2012, Grzegorz Russek (grzegorz.russek@gmail.com)
 * All rights reserved.
 *
 * This code file is based on Kerosene ORM solution for parsing dynamic
 * lambda expressions by Moisés Barba Cebeira
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
using System.Runtime.CompilerServices;
using System.Runtime.Serialization;
using System.Text;

namespace DynamORM.Helpers.Dynamics
{
    /// <summary>
    /// Class able to parse dynamic lambda expressions. Allows to create dynamic logic.
    /// </summary>
    public class DynamicParser : IExtendedDisposable
    {
        #region Node

        /// <summary>
        /// Generic bindable operation where some of its operands is a dynamic argument, or a dynamic member or
        /// a method of that argument.
        /// </summary>
        [Serializable]
        public class Node : IDynamicMetaObjectProvider, IFinalizerDisposable, ISerializable
        {
            private DynamicParser _parser = null;

            #region MetaNode

            /// <summary>
            /// Represents the dynamic binding and a binding logic of
            /// an object participating in the dynamic binding.
            /// </summary>
            internal class MetaNode : DynamicMetaObject
            {
                /// <summary>
                /// Initializes a new instance of the <see cref="MetaNode"/> class.
                /// </summary>
                /// <param name="parameter">The parameter.</param>
                /// <param name="rest">The restrictions.</param>
                /// <param name="value">The value.</param>
                public MetaNode(Expression parameter, BindingRestrictions rest, object value)
                    : base(parameter, rest, value)
                {
                }

                // Func was cool but caused memory leaks
                private DynamicMetaObject GetBinder(Node node)
                {
                    Node o = (Node)this.Value;
                    node.Parser = o.Parser;
                    o.Parser.Last = node;

                    ParameterExpression p = Expression.Variable(typeof(Node), "ret");
                    BlockExpression exp = Expression.Block(new ParameterExpression[] { p }, Expression.Assign(p, Expression.Constant(node)));

                    return new MetaNode(exp, this.Restrictions, node);
                }

                /// <summary>
                /// Performs the binding of the dynamic get member operation.
                /// </summary>
                /// <param name="binder">An instance of the <see cref="T:System.Dynamic.GetMemberBinder" /> that represents the details of the dynamic operation.</param>
                /// <returns>
                /// The new <see cref="T:System.Dynamic.DynamicMetaObject" /> representing the result of the binding.
                /// </returns>
                public override DynamicMetaObject BindGetMember(GetMemberBinder binder)
                {
                    return GetBinder(new GetMember((Node)this.Value, binder.Name));
                }

                /// <summary>
                /// Performs the binding of the dynamic set member operation.
                /// </summary>
                /// <param name="binder">An instance of the <see cref="T:System.Dynamic.SetMemberBinder" /> that represents the details of the dynamic operation.</param>
                /// <param name="value">The <see cref="T:System.Dynamic.DynamicMetaObject" /> representing the value for the set member operation.</param>
                /// <returns>
                /// The new <see cref="T:System.Dynamic.DynamicMetaObject" /> representing the result of the binding.
                /// </returns>
                public override DynamicMetaObject BindSetMember(SetMemberBinder binder, DynamicMetaObject value)
                {
                    return GetBinder(new SetMember((Node)this.Value, binder.Name, value.Value));
                }

                /// <summary>
                /// Performs the binding of the dynamic get index operation.
                /// </summary>
                /// <param name="binder">An instance of the <see cref="T:System.Dynamic.GetIndexBinder" /> that represents the details of the dynamic operation.</param>
                /// <param name="indexes">An array of <see cref="T:System.Dynamic.DynamicMetaObject" /> instances - indexes for the get index operation.</param>
                /// <returns>
                /// The new <see cref="T:System.Dynamic.DynamicMetaObject" /> representing the result of the binding.
                /// </returns>
                public override DynamicMetaObject BindGetIndex(GetIndexBinder binder, DynamicMetaObject[] indexes)
                {
                    return GetBinder(new GetIndex((Node)this.Value, MetaList2List(indexes)));
                }

                /// <summary>
                /// Performs the binding of the dynamic set index operation.
                /// </summary>
                /// <param name="binder">An instance of the <see cref="T:System.Dynamic.SetIndexBinder" /> that represents the details of the dynamic operation.</param>
                /// <param name="indexes">An array of <see cref="T:System.Dynamic.DynamicMetaObject" /> instances - indexes for the set index operation.</param>
                /// <param name="value">The <see cref="T:System.Dynamic.DynamicMetaObject" /> representing the value for the set index operation.</param>
                /// <returns>
                /// The new <see cref="T:System.Dynamic.DynamicMetaObject" /> representing the result of the binding.
                /// </returns>
                public override DynamicMetaObject BindSetIndex(SetIndexBinder binder, DynamicMetaObject[] indexes, DynamicMetaObject value)
                {
                    return GetBinder(new SetIndex((Node)this.Value, MetaList2List(indexes), value.Value));
                }

                /// <summary>
                /// Performs the binding of the dynamic invoke operation.
                /// </summary>
                /// <param name="binder">An instance of the <see cref="T:System.Dynamic.InvokeBinder" /> that represents the details of the dynamic operation.</param>
                /// <param name="args">An array of <see cref="T:System.Dynamic.DynamicMetaObject" /> instances - arguments to the invoke operation.</param>
                /// <returns>
                /// The new <see cref="T:System.Dynamic.DynamicMetaObject" /> representing the result of the binding.
                /// </returns>
                public override DynamicMetaObject BindInvoke(InvokeBinder binder, DynamicMetaObject[] args)
                {
                    return GetBinder(new Invoke((Node)this.Value, MetaList2List(args)));
                }

                /// <summary>
                /// Performs the binding of the dynamic invoke member operation.
                /// </summary>
                /// <param name="binder">An instance of the <see cref="T:System.Dynamic.InvokeMemberBinder" /> that represents the details of the dynamic operation.</param>
                /// <param name="args">An array of <see cref="T:System.Dynamic.DynamicMetaObject" /> instances - arguments to the invoke member operation.</param>
                /// <returns>
                /// The new <see cref="T:System.Dynamic.DynamicMetaObject" /> representing the result of the binding.
                /// </returns>
                public override DynamicMetaObject BindInvokeMember(InvokeMemberBinder binder, DynamicMetaObject[] args)
                {
                    return GetBinder(new Method((Node)this.Value, binder.Name, MetaList2List(args)));
                }

                /// <summary>
                /// Performs the binding of the dynamic binary operation.
                /// </summary>
                /// <param name="binder">An instance of the <see cref="T:System.Dynamic.BinaryOperationBinder" /> that represents the details of the dynamic operation.</param>
                /// <param name="arg">An instance of the <see cref="T:System.Dynamic.DynamicMetaObject" /> representing the right hand side of the binary operation.</param>
                /// <returns>
                /// The new <see cref="T:System.Dynamic.DynamicMetaObject" /> representing the result of the binding.
                /// </returns>
                public override DynamicMetaObject BindBinaryOperation(BinaryOperationBinder binder, DynamicMetaObject arg)
                {
                    return GetBinder(new Binary((Node)this.Value, binder.Operation, arg.Value));
                }

                /// <summary>
                /// Performs the binding of the dynamic unary operation.
                /// </summary>
                /// <param name="binder">An instance of the <see cref="T:System.Dynamic.UnaryOperationBinder" /> that represents the details of the dynamic operation.</param>
                /// <returns>
                /// The new <see cref="T:System.Dynamic.DynamicMetaObject" /> representing the result of the binding.
                /// </returns>
                public override DynamicMetaObject BindUnaryOperation(UnaryOperationBinder binder)
                {
                    Node o = (Node)this.Value;
                    Unary node = new Unary(o, binder.Operation) { Parser = o.Parser };
                    o.Parser.Last = node;

                    // If operation is 'IsTrue' or 'IsFalse', we will return false to keep the engine working...
                    object ret = node;
                    if (binder.Operation == ExpressionType.IsTrue) ret = (object)false;
                    if (binder.Operation == ExpressionType.IsFalse) ret = (object)false;

                    ParameterExpression p = Expression.Variable(ret.GetType(), "ret"); // the type is now obtained from "ret"
                    BlockExpression exp = Expression.Block(
                        new ParameterExpression[] { p },
                        Expression.Assign(p, Expression.Constant(ret))); // the expression is now obtained from "ret"

                    return new MetaNode(exp, this.Restrictions, node);
                }

                /// <summary>
                /// Performs the binding of the dynamic conversion operation.
                /// </summary>
                /// <param name="binder">An instance of the <see cref="T:System.Dynamic.ConvertBinder" /> that represents the details of the dynamic operation.</param>
                /// <returns>
                /// The new <see cref="T:System.Dynamic.DynamicMetaObject" /> representing the result of the binding.
                /// </returns>
                public override DynamicMetaObject BindConvert(ConvertBinder binder)
                {
                    Node o = (Node)this.Value;
                    Convert node = new Convert(o, binder.ReturnType) { Parser = o.Parser };
                    o.Parser.Last = node;

                    // Reducing the object to return if this is an assignment node...
                    object ret = o;
                    bool done = false;

                    while (!done)
                    {
                        if (ret is SetMember)
                            ret = ((SetMember)o).Value;
                        else if (ret is SetIndex)
                            ret = ((SetIndex)o).Value;
                        else
                            done = true;
                    }

                    // Creating an instance...
                    if (binder.ReturnType == typeof(string)) ret = ret.ToString();
                    else
                    {
                        try
                        {
                            if (binder.ReturnType.IsNullableType())
                                ret = null; // to avoid cast exceptions
                            else
                                ret = Activator.CreateInstance(binder.ReturnType, true); // true to allow non-public ctor as well
                        }
                        catch
                        {
                            // as the last resort scenario
                            ret = new object();
                        }
                    }

                    ParameterExpression p = Expression.Variable(binder.ReturnType, "ret");
                    BlockExpression exp = Expression.Block(
                        new ParameterExpression[] { p },
                        Expression.Assign(p, Expression.Constant(ret, binder.ReturnType))); // specifying binder.ReturnType

                    return new MetaNode(exp, this.Restrictions, node);
                }

                private static object[] MetaList2List(DynamicMetaObject[] metaObjects)
                {
                    if (metaObjects == null) return null;

                    object[] list = new object[metaObjects.Length];
                    for (int i = 0; i < metaObjects.Length; i++)
                        list[i] = metaObjects[i].Value;

                    return list;
                }
            }

            #endregion MetaNode

            #region Argument

            /// <summary>
            /// Describe a dynamic argument used in a dynamic lambda expression.
            /// </summary>
            [Serializable]
            public class Argument : Node, ISerializable
            {
                /// <summary>
                /// Initializes a new instance of the <see cref="Argument"/> class.
                /// </summary>
                /// <param name="name">The name.</param>
                public Argument(string name)
                    : base(name)
                {
                }

                /// <summary>
                /// Initializes a new instance of the <see cref="Argument"/> class.
                /// </summary>
                /// <param name="info">The info.</param>
                /// <param name="context">The context.</param>
                protected Argument(SerializationInfo info, StreamingContext context)
                    : base(info, context)
                {
                }

                /// <summary>Returns a <see cref="System.String" /> that represents this instance.</summary>
                /// <returns>A <see cref="System.String" /> that represents this instance.</returns>
                public override string ToString()
                {
                    if (IsDisposed)
                        return "{DynamicParser::Node::Argument::Disposed}";
                    return Name;
                }
            }

            #endregion Argument

            #region GetMember

            /// <summary>
            /// Describe a 'get member' operation, as in 'x => x.Member'.
            /// </summary>
            [Serializable]
            public class GetMember : Node, ISerializable
            {
                /// <summary>
                /// Initializes a new instance of the <see cref="GetMember"/> class.
                /// </summary>
                /// <param name="host">The host.</param>
                /// <param name="name">The name.</param>
                public GetMember(Node host, string name)
                    : base(host, name)
                {
                }

                /// <summary>
                /// Initializes a new instance of the <see cref="GetMember"/> class.
                /// </summary>
                /// <param name="info">The info.</param>
                /// <param name="context">The context.</param>
                protected GetMember(SerializationInfo info, StreamingContext context)
                    : base(info, context)
                {
                }

                /// <summary>Returns a <see cref="System.String" /> that represents this instance.</summary>
                /// <returns>A <see cref="System.String" /> that represents this instance.</returns>
                public override string ToString()
                {
                    if (IsDisposed)
                        return "{DynamicParser::Node::GetMember::Disposed}";
                    return string.Format("{0}.{1}", Host.Sketch(), Name.Sketch());
                }
            }

            #endregion GetMember

            #region SetMember

            /// <summary>
            /// Describe a 'set member' operation, as in 'x => x.Member = y'.
            /// </summary>
            [Serializable]
            public class SetMember : Node, ISerializable
            {
                /// <summary>
                /// Gets the value that has been (virtually) assigned to this member. It might be null if the null value has been
                /// assigned to this instance, or if this instance is disposed.
                /// </summary>
                public object Value { get; private set; }

                /// <summary>
                /// Initializes a new instance of the <see cref="SetMember"/> class.
                /// </summary>
                /// <param name="host">The host.</param>
                /// <param name="name">The name.</param>
                /// <param name="value">The value.</param>
                public SetMember(Node host, string name, object value)
                    : base(host, name)
                {
                    Value = value;
                }

                /// <summary>
                /// Initializes a new instance of the <see cref="SetMember"/> class.
                /// </summary>
                /// <param name="info">The info.</param>
                /// <param name="context">The context.</param>
                protected SetMember(SerializationInfo info, StreamingContext context)
                    : base(info, context)
                {
                    string type = info.GetString("MemberType");
                    Value = type == "NULL" ? null : info.GetValue("MemberValue", Type.GetType(type));
                }

                /// <summary>
                /// Gets the object data.
                /// </summary>
                /// <param name="info">The info.</param>
                /// <param name="context">The context.</param>
                public override void GetObjectData(SerializationInfo info, StreamingContext context)
                {
                    info.AddValue("MemberType", Value == null ? "NULL" : Value.GetType().AssemblyQualifiedName);
                    if (Value != null)
                        info.AddValue("MemberValue", Value);

                    base.GetObjectData(info, context);
                }

                /// <summary>Returns a <see cref="System.String" /> that represents this instance.</summary>
                /// <returns>A <see cref="System.String" /> that represents this instance.</returns>
                public override string ToString()
                {
                    if (IsDisposed)
                        return "{DynamicParser::Node::SetMember::Disposed}";
                    return string.Format("({0}.{1} = {2})", Host.Sketch(), Name.Sketch(), Value.Sketch());
                }
            }

            #endregion SetMember

            #region GetIndex

            /// <summary>
            /// Describe a 'get indexed' operation, as in 'x => x.Member[...]'.
            /// </summary>
            [Serializable]
            public class GetIndex : Node, ISerializable
            {
                /// <summary>Gets the indexes.</summary>
                public object[] Indexes { get; internal set; }

                /// <summary>
                /// Initializes a new instance of the <see cref="GetIndex"/> class.
                /// </summary>
                /// <param name="host">The host.</param>
                /// <param name="indexes">The indexes.</param>
                /// <exception cref="System.ArgumentNullException">Indexes array cannot be null.</exception>
                /// <exception cref="System.ArgumentException">Indexes array cannot be empty.</exception>
                public GetIndex(Node host, object[] indexes)
                    : base(host)
                {
                    if (indexes == null)
                        throw new ArgumentNullException("indexes", "Indexes array cannot be null.");
                    if (indexes.Length == 0)
                        throw new ArgumentException("Indexes array cannot be empty.");

                    Indexes = indexes;
                }

                /// <summary>
                /// Initializes a new instance of the <see cref="GetIndex"/> class.
                /// </summary>
                /// <param name="info">The info.</param>
                /// <param name="context">The context.</param>
                protected GetIndex(SerializationInfo info, StreamingContext context)
                    : base(info, context)
                {
                    int count = (int)info.GetValue("IndexCount", typeof(int));

                    if (count != 0)
                    {
                        Indexes = new object[count]; for (int i = 0; i < count; i++)
                        {
                            string typeName = info.GetString("IndexType" + i);
                            object obj = typeName == "NULL" ? null : info.GetValue("IndexValue" + i, Type.GetType(typeName));
                            Indexes[i] = obj;
                        }
                    }
                }

                /// <summary>
                /// Gets the object data.
                /// </summary>
                /// <param name="info">The info.</param>
                /// <param name="context">The context.</param>
                public override void GetObjectData(SerializationInfo info, StreamingContext context)
                {
                    int count = Indexes == null ? 0 : Indexes.Length; info.AddValue("IndexCount", count);
                    for (int i = 0; i < count; i++)
                    {
                        info.AddValue("IndexType" + i, Indexes[i] == null ? "NULL" : Indexes[i].GetType().AssemblyQualifiedName);
                        if (Indexes[i] != null) info.AddValue("IndexValue" + i, Indexes[i]);
                    }

                    base.GetObjectData(info, context);
                }

                /// <summary>Returns a <see cref="System.String" /> that represents this instance.</summary>
                /// <returns>A <see cref="System.String" /> that represents this instance.</returns>
                public override string ToString()
                {
                    if (IsDisposed)
                        return "{DynamicParser::Node::GetIndex::Disposed}";

                    return string.Format("{0}{1}", Host.Sketch(), Indexes == null ? "[empty]" : Indexes.Sketch());
                }
            }

            #endregion GetIndex

            #region SetIndex

            /// <summary>
            /// Describe a 'set indexed' operation, as in 'x => x.Member[...] = Value'.
            /// </summary>
            [Serializable]
            public class SetIndex : GetIndex, ISerializable
            {
                /// <summary>
                /// Gets the value that has been (virtually) assigned to this member. It might be null if the null value has been
                /// assigned to this instance, or if this instance is disposed.
                /// </summary>
                public object Value { get; private set; }

                /// <summary>
                /// Initializes a new instance of the <see cref="SetIndex"/> class.
                /// </summary>
                /// <param name="host">The host.</param>
                /// <param name="indexes">The indexes.</param>
                /// <param name="value">The value.</param>
                public SetIndex(Node host, object[] indexes, object value)
                    : base(host, indexes)
                {
                    Value = value;
                }

                /// <summary>
                /// Initializes a new instance of the <see cref="SetIndex"/> class.
                /// </summary>
                /// <param name="info">The info.</param>
                /// <param name="context">The context.</param>
                protected SetIndex(SerializationInfo info, StreamingContext context)
                    : base(info, context)
                {
                    string type = info.GetString("MemberType");
                    Value = type == "NULL" ? null : info.GetValue("MemberValue", Type.GetType(type));
                }

                /// <summary>
                /// Gets the object data.
                /// </summary>
                /// <param name="info">The info.</param>
                /// <param name="context">The context.</param>
                public override void GetObjectData(SerializationInfo info, StreamingContext context)
                {
                    info.AddValue("MemberType", Value == null ? "NULL" : Value.GetType().AssemblyQualifiedName);
                    if (Value != null) info.AddValue("MemberValue", Value);

                    base.GetObjectData(info, context);
                }

                /// <summary>Returns a <see cref="System.String" /> that represents this instance.</summary>
                /// <returns>A <see cref="System.String" /> that represents this instance.</returns>
                public override string ToString()
                {
                    if (IsDisposed)
                        return "{DynamicParser::Node::SetIndex::Disposed}";

                    return string.Format("({0}{1} = {2})", Host.Sketch(), Indexes == null ? "[empty]" : Indexes.Sketch(), Value.Sketch());
                }
            }

            #endregion SetIndex

            #region Invoke

            /// <summary>
            /// Describe a method invocation operation, as in 'x => x.Method(...)".
            /// </summary>
            [Serializable]
            public class Invoke : Node, ISerializable
            {
                /// <summary>Gets the arguments.</summary>
                public object[] Arguments { get; internal set; }

                /// <summary>
                /// Initializes a new instance of the <see cref="Invoke"/> class.
                /// </summary>
                /// <param name="host">The host.</param>
                /// <param name="arguments">The arguments.</param>
                public Invoke(Node host, object[] arguments)
                    : base(host)
                {
                    Arguments = arguments == null || arguments.Length == 0 ? null : arguments;
                }

                /// <summary>
                /// Initializes a new instance of the <see cref="Invoke"/> class.
                /// </summary>
                /// <param name="info">The info.</param>
                /// <param name="context">The context.</param>
                protected Invoke(SerializationInfo info, StreamingContext context)
                    : base(info, context)
                {
                    int count = (int)info.GetValue("ArgumentCount", typeof(int));

                    if (count != 0)
                    {
                        Arguments = new object[count]; for (int i = 0; i < count; i++)
                        {
                            string typeName = info.GetString("ArgumentType" + i);
                            object obj = typeName == "NULL" ? null : info.GetValue("ArgumentValue" + i, Type.GetType(typeName));
                            Arguments[i] = obj;
                        }
                    }
                }

                /// <summary>
                /// Gets the object data.
                /// </summary>
                /// <param name="info">The info.</param>
                /// <param name="context">The context.</param>
                public override void GetObjectData(SerializationInfo info, StreamingContext context)
                {
                    int count = Arguments == null ? 0 : Arguments.Length; info.AddValue("ArgumentCount", count);
                    for (int i = 0; i < count; i++)
                    {
                        info.AddValue("ArgumentType" + i, Arguments[i] == null ? "NULL" : Arguments[i].GetType().AssemblyQualifiedName);
                        if (Arguments[i] != null) info.AddValue("ArgumentValue" + i, Arguments[i]);
                    }

                    base.GetObjectData(info, context);
                }

                /// <summary>Returns a <see cref="System.String" /> that represents this instance.</summary>
                /// <returns>A <see cref="System.String" /> that represents this instance.</returns>
                public override string ToString()
                {
                    if (IsDisposed)
                        return "{DynamicParser::Node::Invoke::Disposed}";

                    return string.Format("{0}{1}", Host.Sketch(), Arguments == null ? "()" : Arguments.Sketch(brackets: "()".ToCharArray()));
                }
            }

            #endregion Invoke

            #region Method

            /// <summary>
            /// Describe a method invocation operation, as in 'x => x.Method(...)".
            /// </summary>
            [Serializable]
            public class Method : Node, ISerializable
            {
                /// <summary>Gets the arguments.</summary>
                public object[] Arguments { get; internal set; }

                /// <summary>
                /// Initializes a new instance of the <see cref="Method"/> class.
                /// </summary>
                /// <param name="host">The host.</param>
                /// <param name="name">The name.</param>
                /// <param name="arguments">The arguments.</param>
                public Method(Node host, string name, object[] arguments)
                    : base(host, name)
                {
                    Arguments = arguments == null || arguments.Length == 0 ? null : arguments;
                }

                /// <summary>
                /// Initializes a new instance of the <see cref="Method"/> class.
                /// </summary>
                /// <param name="info">The info.</param>
                /// <param name="context">The context.</param>
                protected Method(SerializationInfo info, StreamingContext context)
                    : base(info, context)
                {
                    int count = (int)info.GetValue("ArgumentCount", typeof(int));

                    if (count != 0)
                    {
                        Arguments = new object[count]; for (int i = 0; i < count; i++)
                        {
                            string typeName = info.GetString("ArgumentType" + i);
                            object obj = typeName == "NULL" ? null : info.GetValue("ArgumentValue" + i, Type.GetType(typeName));
                            Arguments[i] = obj;
                        }
                    }
                }

                /// <summary>
                /// Gets the object data.
                /// </summary>
                /// <param name="info">The info.</param>
                /// <param name="context">The context.</param>
                public override void GetObjectData(SerializationInfo info, StreamingContext context)
                {
                    int count = Arguments == null ? 0 : Arguments.Length; info.AddValue("ArgumentCount", count);
                    for (int i = 0; i < count; i++)
                    {
                        info.AddValue("ArgumentType" + i, Arguments[i] == null ? "NULL" : Arguments[i].GetType().AssemblyQualifiedName);
                        if (Arguments[i] != null) info.AddValue("ArgumentValue" + i, Arguments[i]);
                    }

                    base.GetObjectData(info, context);
                }

                /// <summary>Returns a <see cref="System.String" /> that represents this instance.</summary>
                /// <returns>A <see cref="System.String" /> that represents this instance.</returns>
                public override string ToString()
                {
                    if (IsDisposed)
                        return "{DynamicParser::Node::Method::Disposed}";

                    return string.Format("{0}.{1}{2}", Host.Sketch(), Name.Sketch(), Arguments == null ? "()" : Arguments.Sketch(brackets: "()".ToCharArray()));
                }
            }

            #endregion Method

            #region Binary

            /// <summary>
            /// Represents a binary operation between a dynamic element and an arbitrary object, including null ones, as in
            /// 'x =&gt; (x &amp;&amp; null)'. The left operand must be an instance of <see cref="Node"/>, whereas the right one
            /// can be any object, including null values.
            /// </summary>
            [Serializable]
            public class Binary : Node, ISerializable
            {
                /// <summary>Gets the operation.</summary>
                public ExpressionType Operation { get; private set; }

                /// <summary>Gets host of the <see cref="Node"/>.</summary>
                public Node Left { get { return Host; } }

                /// <summary>Gets the right side value.</summary>
                public object Right { get; private set; }

                /// <summary>
                /// Initializes a new instance of the <see cref="Binary"/> class.
                /// </summary>
                /// <param name="left">The left.</param>
                /// <param name="operation">The operation.</param>
                /// <param name="right">The right.</param>
                public Binary(Node left, ExpressionType operation, object right)
                    : base(left)
                {
                    Operation = operation;
                    Right = right;
                }

                /// <summary>
                /// Initializes a new instance of the <see cref="Binary"/> class.
                /// </summary>
                /// <param name="info">The info.</param>
                /// <param name="context">The context.</param>
                protected Binary(SerializationInfo info, StreamingContext context)
                    : base(info, context)
                {
                    Operation = (ExpressionType)info.GetValue("Operation", typeof(ExpressionType));

                    string type = info.GetString("RightType");
                    Right = type == "NULL" ? null : (Node)info.GetValue("RightItem", Type.GetType(type));
                }

                /// <summary>
                /// Gets the object data.
                /// </summary>
                /// <param name="info">The info.</param>
                /// <param name="context">The context.</param>
                public override void GetObjectData(SerializationInfo info, StreamingContext context)
                {
                    info.AddValue("Operation", Operation);

                    info.AddValue("RightType", Right == null ? "NULL" : Right.GetType().AssemblyQualifiedName);
                    if (Right != null)
                        info.AddValue("RightItem", Right);

                    base.GetObjectData(info, context);
                }

                /// <summary>Returns a <see cref="System.String" /> that represents this instance.</summary>
                /// <returns>A <see cref="System.String" /> that represents this instance.</returns>
                public override string ToString()
                {
                    if (IsDisposed)
                        return "{DynamicParser::Node::Binary::Disposed}";

                    return string.Format("({0} {1} {2})", Host.Sketch(), Operation, Right.Sketch());
                }

                /// <summary>Performs application-defined tasks associated with
                /// freeing, releasing, or resetting unmanaged resources.</summary>
                /// <param name="disposing">If set to <c>true</c> dispose object.</param>
                public override void Dispose(bool disposing)
                {
                    base.Dispose(disposing);

                    if (disposing && Right != null && Right is Node)
                    {
                        Node n = (Node)Right;

                        if (!n.IsDisposed)
                            n.Dispose(disposing);

                        Right = null;
                    }
                }
            }

            #endregion Binary

            #region Unary

            /// <summary>
            /// Represents an unary operation, as in 'x => !x'. The target must be a <see cref="Node"/> instance. There
            /// is no distinction between pre- and post- version of the same operation.
            /// </summary>
            [Serializable]
            public class Unary : Node, ISerializable
            {
                /// <summary>Gets the operation.</summary>
                public ExpressionType Operation { get; private set; }

                /// <summary>Gets host of the <see cref="Node"/>.</summary>
                public Node Target { get { return Host; } }

                /// <summary>
                /// Initializes a new instance of the <see cref="Unary"/> class.
                /// </summary>
                /// <param name="target">The target.</param>
                /// <param name="operation">The operation.</param>
                public Unary(Node target, ExpressionType operation)
                    : base(target)
                {
                    Operation = operation;
                }

                /// <summary>
                /// Initializes a new instance of the <see cref="Unary"/> class.
                /// </summary>
                /// <param name="info">The info.</param>
                /// <param name="context">The context.</param>
                protected Unary(SerializationInfo info, StreamingContext context)
                    : base(info, context)
                {
                    Operation = (ExpressionType)info.GetValue("Operation", typeof(ExpressionType));
                }

                /// <summary>
                /// Gets the object data.
                /// </summary>
                /// <param name="info">The info.</param>
                /// <param name="context">The context.</param>
                public override void GetObjectData(SerializationInfo info, StreamingContext context)
                {
                    info.AddValue("Operation", Operation);

                    base.GetObjectData(info, context);
                }

                /// <summary>Returns a <see cref="System.String" /> that represents this instance.</summary>
                /// <returns>A <see cref="System.String" /> that represents this instance.</returns>
                public override string ToString()
                {
                    if (IsDisposed)
                        return "{DynamicParser::Node::Binary::Disposed}";

                    return string.Format("({0} {1})", Operation, Host.Sketch());
                }
            }

            #endregion Unary

            #region Convert

            /// <summary>
            /// Represents a conversion operation, as in 'x => (string)x'.
            /// </summary>
            [Serializable]
            public class Convert : Node, ISerializable
            {
                /// <summary>Gets the new type to which value will be converted.</summary>
                public Type NewType { get; private set; }

                /// <summary>Gets host of the <see cref="Node"/>.</summary>
                public Node Target { get { return Host; } }

                /// <summary>
                /// Initializes a new instance of the <see cref="Convert"/> class.
                /// </summary>
                /// <param name="target">The target.</param>
                /// <param name="newType">The new type.</param>
                public Convert(Node target, Type newType)
                    : base(target)
                {
                    NewType = newType;
                }

                /// <summary>
                /// Initializes a new instance of the <see cref="Convert"/> class.
                /// </summary>
                /// <param name="info">The info.</param>
                /// <param name="context">The context.</param>
                protected Convert(SerializationInfo info, StreamingContext context)
                    : base(info, context)
                {
                    NewType = (Type)info.GetValue("NewType", typeof(Type));
                }

                /// <summary>
                /// Gets the object data.
                /// </summary>
                /// <param name="info">The info.</param>
                /// <param name="context">The context.</param>
                public override void GetObjectData(SerializationInfo info, StreamingContext context)
                {
                    info.AddValue("NewType", NewType);

                    base.GetObjectData(info, context);
                }
            }

            #endregion Convert

            /// <summary>
            /// Gets the name of the member. It might be null if this instance is disposed.
            /// </summary>
            public string Name { get; internal set; }

            /// <summary>Gets host of the <see cref="Node"/>.</summary>
            public Node Host { get; internal set; }

            /// <summary>Gets reference to the parser.</summary>
            public DynamicParser Parser
            {
                get { return _parser; }
                internal set
                {
                    _parser = value;
                    if (_parser != null)
                        _parser._allNodes.Add(this);
                }
            }

            /// <summary>
            /// Initializes a new instance of the <see cref="Node"/> class.
            /// </summary>
            internal Node()
            {
                IsDisposed = false;
            }

            /// <summary>
            /// Initializes a new instance of the <see cref="Node"/> class.
            /// </summary>
            /// <param name="host">The host.</param>
            internal Node(Node host)
                : this()
            {
                if (host == null)
                    throw new ArgumentNullException("host", "Host cannot be null.");

                Host = host;
            }

            /// <summary>
            /// Initializes a new instance of the <see cref="Node"/> class.
            /// </summary>
            /// <param name="name">The name.</param>
            internal Node(string name)
                : this()
            {
                Name = name.Validated("Name");
            }

            /// <summary>
            /// Initializes a new instance of the <see cref="Node"/> class.
            /// </summary>
            /// <param name="host">The host.</param>
            /// <param name="name">The name.</param>
            /// <exception cref="System.ArgumentNullException">Host cannot be null.</exception>
            internal Node(Node host, string name)
                : this(host)
            {
                Name = name.Validated("Name");
            }

            /// <summary>
            /// Initializes a new instance of the <see cref="Node"/> class.
            /// </summary>
            /// <param name="info">The info.</param>
            /// <param name="context">The context.</param>
            protected Node(SerializationInfo info, StreamingContext context)
            {
                Name = info.GetString("MemberName");

                string type = info.GetString("HostType");
                Host = type == "NULL" ? null : (Node)info.GetValue("HostItem", Type.GetType(type));
            }

            /// <summary>Returns a <see cref="System.String" /> that represents this instance.</summary>
            /// <returns>A <see cref="System.String" /> that represents this instance.</returns>
            public override string ToString()
            {
                if (IsDisposed)
                    return "{DynamicParser::Node::Disposed}";

                return "{DynamicParser::Node::Empty}";
            }

            #region Implementation of IDynamicMetaObjectProvider

            /// <summary>Returns the <see cref="T:System.Dynamic.DynamicMetaObject" /> responsible
            /// for binding operations performed on this object.</summary>
            /// <param name="parameter">The expression tree representation of the runtime value.</param>
            /// <returns>The <see cref="T:System.Dynamic.DynamicMetaObject" /> to bind this object.</returns>
            /// <exception cref="System.ObjectDisposedException">Thrown if this instance is disposed.</exception>
            public DynamicMetaObject GetMetaObject(System.Linq.Expressions.Expression parameter)
            {
                if (IsDisposed)
                    throw new ObjectDisposedException("DynamicParser.Node");

                return new MetaNode(
                    parameter,
                    BindingRestrictions.GetInstanceRestriction(parameter, this),
                    this);
            }

            #endregion Implementation of IDynamicMetaObjectProvider

            #region Implementation of IFinalizerDisposable

            /// <summary>Finalizes an instance of the <see cref="Node"/> class.</summary>
            ~Node()
            {
                Dispose(false);
            }

            /// <summary>Gets a value indicating whether this instance is disposed.</summary>
            public bool IsDisposed { get; private set; }

            /// <summary>Performs application-defined tasks associated with
            /// freeing, releasing, or resetting unmanaged resources.</summary>
            public virtual void Dispose()
            {
                Dispose(true);
                GC.SuppressFinalize(this);
            }

            /// <summary>Performs application-defined tasks associated with
            /// freeing, releasing, or resetting unmanaged resources.</summary>
            /// <param name="disposing">If set to <c>true</c> dispose object.</param>
            public virtual void Dispose(bool disposing)
            {
                if (disposing)
                {
                    IsDisposed = true;

                    if (Host != null && !Host.IsDisposed)
                        Host.Dispose();

                    Host = null;

                    Parser = null;
                }
            }

            #endregion Implementation of IFinalizerDisposable

            #region Implementation of ISerializable

            /// <summary>
            /// Populates a <see cref="T:System.Runtime.Serialization.SerializationInfo" /> with the data needed to serialize the target object.
            /// </summary>
            /// <param name="info">The <see cref="T:System.Runtime.Serialization.SerializationInfo" /> to populate with data.</param>
            /// <param name="context">The destination (see <see cref="T:System.Runtime.Serialization.StreamingContext" />) for this serialization.</param>
            public virtual void GetObjectData(SerializationInfo info, StreamingContext context)
            {
                if (!string.IsNullOrEmpty(Name))
                    info.AddValue("MemberName", Name);

                info.AddValue("HostType", Host == null ? "NULL" : Host.GetType().AssemblyQualifiedName);
                if (Host != null)
                    info.AddValue("HostItem", Host);
            }

            #endregion Implementation of ISerializable
        }

        #endregion Node

        #region Data

        private List<Node.Argument> _arguments = new List<Node.Argument>();
        private List<Node> _allNodes = new List<Node>();
        private object _uncertainResult;

        #endregion Data

        #region Properties

        /// <summary>Gets the last node (root of the tree).</summary>
        public Node Last { get; internal set; }

        /// <summary>
        /// Gets an enumeration containing the dynamic arguments used in the dynamic lambda expression parsed.
        /// </summary>
        public IEnumerable<Node.Argument> Arguments
        {
            get
            {
                List<Node.Argument> list = new List<Node.Argument>();
                if (!IsDisposed && _arguments != null)
                    list.AddRange(_arguments);

                foreach (Node.Argument arg in list)
                    yield return arg;

                list.Clear();
                list = null;
            }
        }

        /// <summary>
        /// Gets the number of dynamic arguments used in the dynamic lambda expression parsed.
        /// </summary>
        public int Count
        {
            get { return _arguments == null ? 0 : _arguments.Count; }
        }

        /// <summary>
        /// Gets the result of the parsing of the dynamic lambda expression. This result can be either an arbitrary object,
        /// including null, if the expression resolves to it, or an instance of the <see cref="Node"/> class that
        /// contains the last logic expression evaluated when parsing the dynamic lambda expression.
        /// </summary>
        public object Result
        {
            get { return _uncertainResult ?? Last; }
        }

        #endregion Properties

        private DynamicParser(Delegate f)
        {
            // I know this can be almost a one liner
            // but it causes memory leaks when so.
            ParameterInfo[] pars = f.Method.GetParameters();
            foreach (ParameterInfo p in pars)
            {
                int attrs = p.GetCustomAttributes(typeof(DynamicAttribute), true).Length;
                if (attrs != 0)
                {
                    Node.Argument par = new Node.Argument(p.Name) { Parser = this };
                    this._arguments.Add(par);
                }
                else
                    throw new ArgumentException(string.Format("Argument '{0}' must be dynamic.", p.Name));
            }

            _uncertainResult = f.DynamicInvoke(_arguments.ToArray());
        }

        /// <summary>
        /// Parses the dynamic lambda expression given in the form of a delegate, and returns a new instance of the
        /// <see cref="DynamicParser"/> class that holds the dynamic arguments used in the dynamic lambda expression, and
        /// the result of the parsing.
        /// </summary>
        /// <param name="f">The dynamic lambda expression to parse.</param>
        /// <returns>A new instance of <see cref="DynamicParser"/>.</returns>
        public static DynamicParser Parse(Delegate f)
        {
            return new DynamicParser(f);
        }

        /// <summary>Returns a <see cref="System.String" /> that represents this instance.</summary>
        /// <returns>A <see cref="System.String" /> that represents this instance.</returns>
        public override string ToString()
        {
            if (IsDisposed)
                return "{DynamicParser::Disposed}";

            StringBuilder sb = new StringBuilder();

            sb.Append("(");
            bool first = true;

            if (_arguments != null)
            {
                foreach (Node.Argument arg in _arguments)
                {
                    if (!first) sb.Append(", "); else first = false;
                    sb.Append(arg);
                }
            }

            sb.Append(")");

            sb.AppendFormat(" => {0}", Result.Sketch());

            return sb.ToString();
        }

        #region Implementation of IExtendedDisposable

        /// <summary>Gets a value indicating whether this instance is disposed.</summary>
        public bool IsDisposed { get; private set; }

        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        public void Dispose()
        {
            IsDisposed = true;

            if (_uncertainResult != null && _uncertainResult is Node)
            {
                ((Node)_uncertainResult).Dispose();
                _uncertainResult = null;
            }

            if (Last != null && !Last.IsDisposed)
            {
                Last.Dispose();
                Last = null;
            }

            if (_arguments != null)
            {
                _arguments.ForEach(x => { if (!x.IsDisposed) x.Dispose(); });

                _arguments.Clear();
                _arguments = null;
            }

            if (_allNodes != null)
            {
                _allNodes.ForEach(x => { if (!x.IsDisposed) x.Dispose(); });

                _allNodes.Clear();
                _allNodes = null;
            }
        }

        #endregion Implementation of IExtendedDisposable
    }
}