using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;

namespace AmalgamationTool
{
    internal class Program
    {
        private static void Main(string[] args)
        {
            List<string> usings = new List<string>();
            Dictionary<string, List<string>> classes = new Dictionary<string, List<string>>();

            // Build a file using string builder.
            StringBuilder sb = new StringBuilder();

            foreach (var f in new DirectoryInfo(Path.GetFullPath(args[0].Trim('"', '\''))).GetFiles("*.cs", SearchOption.AllDirectories))
            {
                string content = File.ReadAllText(f.FullName);

                string namespaceName = string.Empty;

                // Deal with usings
                foreach (var u in content.Split(new string[] { Environment.NewLine }, StringSplitOptions.None)
                    .Where(l => l.Trim().StartsWith("using "))
                    .Select(l => l.Trim()))
                    if (!usings.Contains(u))
                        usings.Add(u);

                // Extract namespace

                //if (args.Length > 2)
                //{
                //    var tcontent = Regex.Replace(content, @"^\s*using\s+.*\s*;$", string.Empty);
                //    tcontent = Regex.Replace(content, @"^\s*namespace\s+.*\s*", string.Empty).Trim();

                //    var ns = Regex.Match(content, @"^\s*namespace\s+(?<ns>.*)\s*");

                //    if (ns.Success)
                //    {
                //        if (!classes.ContainsKey(ns.Groups["ns"].Value))
                //            classes.Add(ns.Groups["ns"].Value, new List<string>());

                //        classes[ns.Groups["ns"].Value].Add(tcontent);
                //    }
                //}
                //else
                {
                    if (content.Trim().Length == 0)
                        continue;

                    var nstart = content.IndexOf("namespace ") + "namespace ".Length;
                    var bbrace = content.IndexOf("{", nstart);
                    var nlen = bbrace - nstart;

                    if (nstart < "namespace ".Length)
                    {
                        if (f.Name.ToLower() == "assemblyinfo.cs")
                        {
                            var hs = content.IndexOf("/*");
                            var es = content.IndexOf("*/", hs) + 2;
                            if (es > hs)
                            {
                                sb.AppendLine(content.Substring(hs, es - hs));
                                sb.AppendLine();
                            }
                        }

                        continue;
                    }

                    string ns = content.Substring(nstart, nlen).Trim();

                    // Add namespace if not exist
                    if (!classes.ContainsKey(ns))
                        classes.Add(ns, new List<string>());

                    var ebrace = content.LastIndexOf('}');

                    // Cut content as class/enum
                    classes[ns].Add(content.Substring(bbrace + 1, ebrace - bbrace - 1));
                }
            }

            usings.Sort();

            foreach (var u in usings)
                sb.AppendLine(u);

            sb.AppendLine(@"
[module: System.Diagnostics.CodeAnalysis.SuppressMessage(""StyleCop.CSharp.MaintainabilityRules"", ""SA1402:FileMayOnlyContainASingleClass"", Justification = ""This is a generated file which generates all the necessary support classes."")]
[module: System.Diagnostics.CodeAnalysis.SuppressMessage(""StyleCop.CSharp.MaintainabilityRules"", ""SA1403:FileMayOnlyContainASingleNamespace"", Justification = ""This is a generated file which generates all the necessary support classes."")]");

            FillClassesAndNamespacesIddented(classes, sb);

            File.WriteAllText(Path.GetFullPath(args[1].Trim('"', '\'')), sb.ToString());
        }

        private static void FillClassesAndNamespaces(Dictionary<string, List<string>> classes, StringBuilder sb)
        {
            foreach (var n in classes)
            {
                sb.AppendFormat("namespace {0}{1}{{", n.Key, Environment.NewLine);
                n.Value.ForEach(c => sb.Append(c));
                sb.AppendLine("}");
                sb.AppendLine(string.Empty);
            }
        }

        private static void FillClassesAndNamespacesIddented(Dictionary<string, List<string>> classes, StringBuilder sb)
        {
            var min = classes.Min(k => k.Key.Split('.').Count());

            foreach (var n in classes.Where(nc => nc.Key.Split('.').Count() == min))
            {
                sb.AppendFormat("namespace {0}{1}{{", n.Key, Environment.NewLine);
                n.Value.ForEach(c => sb.Append(c));

                SubNamespaces(classes, n.Key, sb, min);

                sb.AppendLine("}");
                sb.AppendLine(string.Empty);
            }
        }

        private static void SubNamespaces(Dictionary<string, List<string>> classes, string p, StringBuilder sb, int ident)
        {
            sb.AppendLine(string.Empty);

            foreach (var n in classes.Where(nc => nc.Key.Split('.').Count() == ident + 1 && nc.Key.StartsWith(p)))
            {
                for (int i = 0; i < ident; i++) sb.Append("    ");
                sb.AppendFormat("namespace {0}{1}", n.Key.Substring(p.Length + 1), Environment.NewLine);

                for (int i = 0; i < ident; i++) sb.Append("    ");
                sb.Append("{");
                n.Value.ForEach(c =>
                {
                    foreach (var l in c.Split(new string[] { Environment.NewLine }, StringSplitOptions.None))
                    {
                        for (int i = 0; i < ident; i++) sb.Append("    ");
                        sb.AppendLine(l);
                    }
                });

                SubNamespaces(classes, n.Key, sb, ident + 1);

                for (int i = 0; i < ident; i++) sb.Append("    ");
                sb.AppendLine("}");
                sb.AppendLine(string.Empty);
            }
        }
    }
}