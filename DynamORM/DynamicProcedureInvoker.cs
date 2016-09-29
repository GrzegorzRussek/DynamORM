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
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Dynamic;
using System.Linq;
using DynamORM.Helpers;
using DynamORM.Mapper;

namespace DynamORM
{
    /// <summary>Dynamic procedure invoker.</summary>
    /// <remarks>Unfortunately I can use <c>out</c> and <c>ref</c> to
    /// return parameters, <see href="http://stackoverflow.com/questions/2475310/c-sharp-4-0-dynamic-doesnt-set-ref-out-arguments"/>.
    /// But see example for workaround. If there aren't any return parameters execution will return scalar value.
    /// Scalar result is not converted to provided generic type (if any). For output results there is possibility to map to provided class.
    /// </remarks><example>You still can use out, return and both way parameters by providing variable prefix:<code>
    /// dynamic res = db.Procedures.sp_Test_Scalar_In_Out(inp: Guid.NewGuid(), out_outp: Guid.Empty);
    /// Console.Out.WriteLine(res.outp);</code>
    /// Prefixes: <c>out_</c>, <c>ret_</c>, <c>both_</c>. Result will contain field without prefix.
    /// Here is an example with result class:<code>
    /// public class ProcResult { [Column("outp")] public Guid Output { get; set; } }
    /// ProcResult res4 = db.Procedures.sp_Test_Scalar_In_Out&lt;ProcResult&gt;(inp: Guid.NewGuid(), out_outp: Guid.Empty) as ProcResult;
    /// </code>As you can se, you can use mapper to do job for you.</example>
    public class DynamicProcedureInvoker : DynamicObject, IDisposable
    {
        private DynamicDatabase _db;
        private List<string> _prefixes;

        internal DynamicProcedureInvoker(DynamicDatabase db, List<string> prefixes = null)
        {
            _prefixes = prefixes;
            _db = db;
        }

        /// <summary>This is where the magic begins.</summary>
        /// <param name="binder">Binder to create owner.</param>
        /// <param name="result">Binder invoke result.</param>
        /// <returns>Returns <c>true</c> if invoke was performed.</returns>
        public override bool TryGetMember(GetMemberBinder binder, out object result)
        {
            List<string> pref = new List<string>();

            if (_prefixes != null)
                pref.AddRange(_prefixes);

            pref.Add(binder.Name);

            result = new DynamicProcedureInvoker(_db, pref);

            return true;
        }

        /// <summary>This is where the magic begins.</summary>
        /// <param name="binder">Binder to invoke.</param>
        /// <param name="args">Binder arguments.</param>
        /// <param name="result">Binder invoke result.</param>
        /// <returns>Returns <c>true</c> if invoke was performed.</returns>
        public override bool TryInvokeMember(InvokeMemberBinder binder, object[] args, out object result)
        {
            // parse the method
            CallInfo info = binder.CallInfo;

            // Get generic types
            IList<Type> types = binder.GetGenericTypeArguments();

            Dictionary<string, int> retParams = null;

            using (IDbConnection con = _db.Open())
            using (IDbCommand cmd = con.CreateCommand())
            {
                if (_prefixes == null || _prefixes.Count == 0)
                    cmd.SetCommand(CommandType.StoredProcedure, binder.Name);
                else
                    cmd.SetCommand(CommandType.StoredProcedure, string.Format("{0}.{1}", string.Join(".", _prefixes), binder.Name));

                #region Prepare arguments

                int alen = args.Length;
                if (alen > 0)
                {
                    for (int i = 0; i < alen; i++)
                    {
                        object arg = args[i];

                        if (arg is DynamicExpando)
                            cmd.AddParameters(_db, (DynamicExpando)arg);
                        else if (arg is ExpandoObject)
                            cmd.AddParameters(_db, (ExpandoObject)arg);
                        else
                        {
                            if (info.ArgumentNames.Count > i && !string.IsNullOrEmpty(info.ArgumentNames[i]))
                            {
                                bool isOut = info.ArgumentNames[i].StartsWith("out_");
                                bool isRet = info.ArgumentNames[i].StartsWith("ret_");
                                bool isBoth = info.ArgumentNames[i].StartsWith("both_");

                                string paramName = isOut || isRet ?
                                    info.ArgumentNames[i].Substring(4) :
                                    isBoth ? info.ArgumentNames[i].Substring(5) :
                                    info.ArgumentNames[i];

                                if (isOut || isBoth || isRet)
                                {
                                    if (retParams == null)
                                        retParams = new Dictionary<string, int>();
                                    retParams.Add(paramName, cmd.Parameters.Count);
                                }

                                cmd.AddParameter(
                                    _db.GetParameterName(paramName),
                                    isOut ? ParameterDirection.Output :
                                        isRet ? ParameterDirection.ReturnValue :
                                            isBoth ? ParameterDirection.InputOutput : ParameterDirection.Input,
                                    arg == null ? DbType.String : arg.GetType().ToDbType(), 0, isOut ? DBNull.Value : arg);
                            }
                            else
                                cmd.AddParameter(_db, arg);
                        }
                    }
                }

                #endregion Prepare arguments

                #region Get main result

                object mainResult = null;

                if (types.Count > 0)
                {
                    mainResult = types[0].GetDefaultValue();

                    if (types[0] == typeof(IDataReader))
                    {
                        using (IDataReader rdr = cmd.ExecuteReader())
                            mainResult = rdr.CachedReader();
                    }
                    else if (types[0].IsGenericEnumerable())
                    {
                        Type argType = types[0].GetGenericArguments().First();
                        if (argType == typeof(object))
                        {
                            IDataReader cache = null;
                            using (IDataReader rdr = cmd.ExecuteReader())
                                cache = rdr.CachedReader();

                            mainResult = cache.EnumerateReader().ToList();
                        }
                        else if (argType.IsValueType)
                        {
                            Type listType = typeof(List<>).MakeGenericType(new Type[] { argType });
                            IList listInstance = (IList)Activator.CreateInstance(listType);

                            object defVal = listType.GetDefaultValue();

                            IDataReader cache = null;
                            using (IDataReader rdr = cmd.ExecuteReader())
                                cache = rdr.CachedReader();

                            while (cache.Read())
                                listInstance.Add(cache[0] == DBNull.Value ? defVal : argType.CastObject(cache[0]));

                            mainResult = listInstance;
                        }
                        else
                        {
                            DynamicTypeMap mapper = DynamicMapperCache.GetMapper(argType);
                            if (mapper == null)
                                throw new InvalidCastException(string.Format("Don't konw what to do with this type: '{0}'.", argType.ToString()));

                            IDataReader cache = null;
                            using (IDataReader rdr = cmd.ExecuteReader())
                                cache = rdr.CachedReader();

                            mainResult = cache.EnumerateReader().MapEnumerable(argType).ToList();
                        }
                    }
                    else if (types[0].IsValueType)
                    {
                        mainResult = cmd.ExecuteScalar();
                        if (mainResult != DBNull.Value)
                            mainResult = types[0].CastObject(mainResult);
                    }
                    else
                    {
                        DynamicTypeMap mapper = DynamicMapperCache.GetMapper(types[0]);
                        if (mapper == null)
                            throw new InvalidCastException(string.Format("Don't konw what to do with this type: '{0}'.", types[0].ToString()));

                        using (IDataReader rdr = cmd.ExecuteReader())
                            if (rdr.Read())
                                mainResult = (rdr.ToDynamic() as object).Map(types[0]);
                            else
                                mainResult = null;
                    }
                }
                else
                    mainResult = cmd.ExecuteNonQuery();

                #endregion Get main result

                #region Handle out params

                if (retParams != null)
                {
                    Dictionary<string, object> res = new Dictionary<string, object>();

                    if (mainResult != null)
                    {
                        if (mainResult == DBNull.Value)
                            res.Add(binder.Name, null);
                        else
                            res.Add(binder.Name, mainResult);
                    }

                    foreach (KeyValuePair<string, int> pos in retParams)
                        res.Add(pos.Key, ((IDbDataParameter)cmd.Parameters[pos.Value]).Value);

                    if (types.Count > 1)
                    {
                        DynamicTypeMap mapper = DynamicMapperCache.GetMapper(types[1]);

                        if (mapper != null)
                            result = mapper.Create(res.ToDynamic());
                        else
                            result = res.ToDynamic();
                    }
                    else
                        result = res.ToDynamic();
                }
                else
                    result = mainResult;

                #endregion Handle out params
            }

            return true;
        }

        /// <summary>Performs application-defined tasks associated with
        /// freeing, releasing, or resetting unmanaged resources.</summary>
        public void Dispose()
        {
        }
    }
}