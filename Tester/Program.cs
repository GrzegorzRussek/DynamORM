using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Text;

namespace Tester
{
    internal class Program
    {
        private static DynamORM.DynamicDatabase GetORM()
        {
            return new DynamORM.DynamicDatabase(System.Data.SqlClient.SqlClientFactory.Instance,
                //"packet size=4096;User Id=sa;Password=Sa123;data source=192.168.1.9,1434;initial catalog=MAH_Melle-GAGARIN;",
                "packet size=4096;User Id=sa;Password=sa123;data source=192.168.1.9,1433;initial catalog=MOM_NEXT_Florentyna_WMS_PROD;",
                DynamORM.DynamicDatabaseOptions.SingleConnection | DynamORM.DynamicDatabaseOptions.SingleTransaction | DynamORM.DynamicDatabaseOptions.SupportStoredProcedures |
                DynamORM.DynamicDatabaseOptions.SupportSchema | DynamORM.DynamicDatabaseOptions.SupportTop);

            //return new DynamORM.DynamicDatabase(System.Data.SQLite.SQLiteFactory.Instance,
            //    "Data Source=test.db3;",
            //    DynamORM.DynamicDatabaseOptions.SingleConnection | DynamORM.DynamicDatabaseOptions.SingleTransaction |
            //    DynamORM.DynamicDatabaseOptions.SupportSchema | DynamORM.DynamicDatabaseOptions.SupportLimitOffset);
        }

        private static void Main(string[] args)
        {
            //StringBuilder code = new StringBuilder();
            //using (var db = GetORM())
            //{
            //    List<dynamic> procs = db.From(x => x.INFORMATION_SCHEMA.ROUTINES).OrderBy(x => x.SPECIFIC_NAME).Where(x => x.ROUTINE_TYPE == "PROCEDURE").Execute().ToList();

            //    foreach (dynamic d in procs)
            //        PrintWhatYouKnow(d.SPECIFIC_NAME, code);
            //}

            //Console.ReadLine();
            //Console.WriteLine(code.ToString());
            //File.WriteAllText("code.cs", code.ToString());
            //Console.ReadLine();

            Dictionary<DateTime, DateTime> times = new Dictionary<DateTime, DateTime>()
            {
                /*{ new DateTime(2015,03,07,13,00,00), new DateTime(2015,03,07,23,30,00) },
                { new DateTime(2015,03,08,14,00,00), new DateTime(2015,03,08,19,00,00) },
                { new DateTime(2015,03,14,13,00,00), new DateTime(2015,03,15,01,30,00) },
                { new DateTime(2015,03,15,14,00,00), new DateTime(2015,03,16,00,00,00) },
                { new DateTime(2015,03,28,13,00,00), new DateTime(2015,03,28,22,30,00) },
                { new DateTime(2015,03,29,14,00,00), new DateTime(2015,03,29,21,00,00) },
                { new DateTime(2015,04,02,20,30,00), new DateTime(2015,04,03,00,30,00) },
                { new DateTime(2015,04,04,12,00,00), new DateTime(2015,04,04,23,30,00) },
                { new DateTime(2015,04,05,15,00,00), new DateTime(2015,04,05,21,00,00) },
                { new DateTime(2015,04,06,18,00,00), new DateTime(2015,04,06,23,30,00) },
                { new DateTime(2015,04,07,20,00,00), new DateTime(2015,04,08,00,00,00) },
                { new DateTime(2015,04,08,20,00,00), new DateTime(2015,04,08,23,30,00) },
                { new DateTime(2015,04,09,22,30,00), new DateTime(2015,04,10,00,00,00) },
                { new DateTime(2015,04,10,21,00,00), new DateTime(2015,04,11,00,00,00) },
                { new DateTime(2015,04,11,13,00,00), new DateTime(2015,04,11,18,00,00) },
                { new DateTime(2015,04,11,20,30,00), new DateTime(2015,04,11,23,30,00) },
                { new DateTime(2015,04,12,10,00,00), new DateTime(2015,04,12,13,00,00) },
                { new DateTime(2015,04,12,16,00,00), new DateTime(2015,04,13,00,00,00) },
                { new DateTime(2015,04,13,21,30,00), new DateTime(2015,04,14,00,00,00) },
                { new DateTime(2015,04,14,21,00,00), new DateTime(2015,04,15,00,30,00) },
                { new DateTime(2015,04,17,20,30,00), new DateTime(2015,04,17,23,30,00) },
                { new DateTime(2015,04,18,14,00,00), new DateTime(2015,04,18,18,30,00) },
                { new DateTime(2015,04,18,20,30,00), new DateTime(2015,04,19,02,00,00) },
                { new DateTime(2015,04,19,13,30,00), new DateTime(2015,04,19,17,00,00) },
                { new DateTime(2015,04,19,19,00,00), new DateTime(2015,04,20,00,00,00) },
                { new DateTime(2015,04,20,21,00,00), new DateTime(2015,04,21,00,00,00) },
                { new DateTime(2015,04,21,21,30,00), new DateTime(2015,04,22,00,00,00) },
                { new DateTime(2015,04,22,21,00,00), new DateTime(2015,04,22,23,30,00) },
                { new DateTime(2015,04,23,20,30,00), new DateTime(2015,04,24,00,00,00) },
                { new DateTime(2015,04,25,09,30,00), new DateTime(2015,04,25,15,00,00) },
                { new DateTime(2015,04,25,15,30,00), new DateTime(2015,04,25,19,30,00) },
                { new DateTime(2015,04,25,21,00,00), new DateTime(2015,04,26,02,00,00) },
                { new DateTime(2015,04,26,10,00,00), new DateTime(2015,04,26,12,00,00) },
                { new DateTime(2015,04,26,18,00,00), new DateTime(2015,04,26,19,30,00) },
                { new DateTime(2015,04,26,20,00,00), new DateTime(2015,04,27,00,00,00) },
                { new DateTime(2015,04,27,20,30,00), new DateTime(2015,04,28,01,00,00) },
                { new DateTime(2015,04,28,20,00,00), new DateTime(2015,04,29,01,00,00) },
                { new DateTime(2015,04,29,20,30,00), new DateTime(2015,04,30,01,00,00) },
                { new DateTime(2015,04,30,21,00,00), new DateTime(2015,05,01,02,30,00) },
                { new DateTime(2015,05,01,10,00,00), new DateTime(2015,05,01,14,30,00) },
                { new DateTime(2015,05,02,11,00,00), new DateTime(2015,05,02,17,30,00) },
                { new DateTime(2015,05,03,10,00,00), new DateTime(2015,05,03,22,00,00) },
                { new DateTime(2015,05,04,20,30,00), new DateTime(2015,05,05,01,00,00) },
                { new DateTime(2015,05,05,17,30,00), new DateTime(2015,05,05,19,00,00) },
                { new DateTime(2015,05,05,20,30,00), new DateTime(2015,05,06,01,00,00) },
                { new DateTime(2015,05,06,17,30,00), new DateTime(2015,05,06,19,00,00) },
                { new DateTime(2015,05,06,20,00,00), new DateTime(2015,05,07,01,00,00) },
                { new DateTime(2015,05,07,20,30,00), new DateTime(2015,05,08,01,00,00) },
                { new DateTime(2015,05,08,17,30,00), new DateTime(2015,05,08,19,00,00) },
                { new DateTime(2015,05,08,20,30,00), new DateTime(2015,05,09,02,00,00) },
                { new DateTime(2015,05,09,11,00,00), new DateTime(2015,05,09,16,30,00) },
                { new DateTime(2015,05,10,10,00,00), new DateTime(2015,05,10,15,30,00) },
                { new DateTime(2015,05,10,16,30,00), new DateTime(2015,05,10,23,30,00) },
                { new DateTime(2015,05,11,21,00,00), new DateTime(2015,05,12,01,00,00) },
                { new DateTime(2015,05,12,21,30,00), new DateTime(2015,05,13,01,30,00) },
                { new DateTime(2015,05,13,20,30,00), new DateTime(2015,05,14,00,30,00) },
                { new DateTime(2015,05,14,21,00,00), new DateTime(2015,05,15,02,00,00) },
                { new DateTime(2015,05,15,22,00,00), new DateTime(2015,05,16,02,00,00) },
                { new DateTime(2015,05,16,16,00,00), new DateTime(2015,05,17,01,00,00) },
                { new DateTime(2015,05,17,19,00,00), new DateTime(2015,05,18,00,30,00) },
                { new DateTime(2015,05,18,20,30,00), new DateTime(2015,05,19,01,00,00) },
                { new DateTime(2015,05,19,13,00,00), new DateTime(2015,05,19,15,00,00) },
                { new DateTime(2015,05,19,20,00,00), new DateTime(2015,05,20,00,30,00) },
                { new DateTime(2015,05,20,20,30,00), new DateTime(2015,05,21,00,00,00) },
                { new DateTime(2015,05,21,21,00,00), new DateTime(2015,05,21,23,00,00) },
                { new DateTime(2015,05,22,20,30,00), new DateTime(2015,05,23,02,30,00) },
                { new DateTime(2015,05,23,09,30,00), new DateTime(2015,05,23,17,30,00) },
                { new DateTime(2015,05,23,18,30,00), new DateTime(2015,05,23,22,30,00) },
                { new DateTime(2015,05,24,13,00,00), new DateTime(2015,05,25,00,00,00) },
                { new DateTime(2015,06,02,20,00,00), new DateTime(2015,06,02,23,00,00) },
                { new DateTime(2015,06,05,17,30,00), new DateTime(2015,06,05,19,30,00) },*/
                { new DateTime(2015,05,26,21,00,00), new DateTime(2015,05,26,23,30,00) },
                { new DateTime(2015,05,27,21,00,00), new DateTime(2015,05,28,01,00,00) },
                { new DateTime(2015,05,28,20,00,00), new DateTime(2015,05,29,00,00,00) },
                { new DateTime(2015,05,30,10,30,00), new DateTime(2015,05,30,14,30,00) },
                { new DateTime(2015,05,30,16,00,00), new DateTime(2015,05,30,21,00,00) },
                { new DateTime(2015,05,30,22,00,00), new DateTime(2015,05,31,02,30,00) },
                { new DateTime(2015,05,31,13,30,00), new DateTime(2015,05,31,15,30,00) },
                { new DateTime(2015,05,31,16,30,00), new DateTime(2015,05,31,23,30,00) },
                { new DateTime(2015,06,01,21,00,00), new DateTime(2015,06,01,22,00,00) },
                { new DateTime(2015,06,04,18,00,00), new DateTime(2015,06,04,22,00,00) },
            };

            StringBuilder inserts = new StringBuilder();

            int counter = 0;

            foreach (var item in times)
                AddInserts(inserts, item.Key, item.Value, ref counter);

            File.WriteAllText("times.sql", inserts.ToString());
        }

        private static void AddInserts(StringBuilder sb, DateTime from, DateTime to, ref int counter)
        {
            string format = @"
INSERT INTO ticket_change(ticket, ""time"", author, field, oldvalue, newvalue) VALUES (26114, {0}, 'grzegorz.russek', 'comment', '{1}', 'Google calendar import.');
INSERT INTO ticket_change(ticket, ""time"", author, field, oldvalue, newvalue) VALUES (26114, {0}, 'grzegorz.russek', 'hours', '0', '{2:0.000000}');";

            if (from.Date != to.Date)
            {
                if (to.Hour == 0 && to.Minute == 0)
                    sb.AppendFormat(format, GetTracEpoch(to.AddSeconds(-1)), ++counter, (to - from).TotalHours);
                else
                {
                    sb.AppendFormat(format, GetTracEpoch(to.Date.AddSeconds(-1)), ++counter, (to.Date - from).TotalHours);
                    sb.AppendFormat(format, GetTracEpoch(to), ++counter, (to - to.Date).TotalHours);
                }
            }
            else
                sb.AppendFormat(format, GetTracEpoch(to), ++counter, (to - from).TotalHours);
        }

        private static long GetTracEpoch(DateTime date)
        {
            return Convert.ToInt64((date.ToUniversalTime() - new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc)).TotalSeconds) * 1000000;
        }

        private static void PrintWhatYouKnow(string name, StringBuilder code)
        {
            string yourConnStr = "packet size=4096;User Id=sa;Password=sa123;data source=192.168.1.9,1433;initial catalog=MOM_NEXT_Florentyna_WMS_PROD;";
            using (SqlConnection sqlConn = new SqlConnection(yourConnStr))
            using (SqlCommand sqlCmd = new SqlCommand(name, sqlConn))
            {
                StringBuilder body = new StringBuilder();

                sqlConn.Open();
                sqlCmd.CommandType = CommandType.StoredProcedure;
                try
                {
                    SqlCommandBuilder.DeriveParameters(sqlCmd);
                    Console.WriteLine("PROCEDURE: {0}", name);
                    Console.WriteLine(sqlCmd.Parameters.Count.ToString());

                    code.AppendFormat("public static void {0}(this DynamicDatabase db", name);
                    body.AppendFormat("    db.Procedures.{0}(x$x$", name);

                    foreach (SqlParameter p in sqlCmd.Parameters)
                    {
                        Console.WriteLine(p.ParameterName.ToString() + "\t"
                                + p.Direction.ToString() + "\t" + p.DbType.ToString());

                        if (p.Direction != ParameterDirection.ReturnValue)
                        {
                            code.Append(", "); body.Append(", ");

                            switch (p.Direction)
                            {
                                case ParameterDirection.InputOutput:
                                    code.Append("ref "); body.Append("both_");
                                    break;

                                case ParameterDirection.Output:
                                    code.Append("out "); body.Append("out_");
                                    break;

                                default:
                                    break;
                            }

                            code.AppendFormat("{0} {1}", p.DbType, p.ParameterName.Trim('@'));
                            body.AppendFormat("{0}: {0}", p.ParameterName.Trim('@'));
                        }
                    }

                    code.AppendFormat("){0}{{{0}{1});{0}}}{0}", Environment.NewLine, body.ToString().Replace("(x$x$, ", "("));
                }
                catch (Exception ex)
                {
                    Console.WriteLine("PROCEDURE: {0}, has errors: {1}", name, ex.Message);
                }
            }
        }
    }
}