using System;
using System.Linq;
using DynamORM;
using DynamORM.Mapper;
using System.Collections.Generic;
using System.Dynamic;

namespace Tester
{
    internal class Program
    {
        [Table(Name = "mom_Sessions")]
        internal class Session
        {
            [Column(IsKey = true)]
            public virtual Guid ms_id { get; set; }

            [Column]
            public virtual Guid ms_mus_id { get; set; }

            [Column]
            public virtual long ms_last_activity { get; set; }

            [Column]
            public virtual int ms_type { get; set; }

            [Column]
            public virtual string ms_desc { get; set; }

            [Ignore]
            public virtual string ms_did { get; set; }
        }

        private static DynamORM.DynamicDatabase GetORM()
        {
            //return new DynamORM.DynamicDatabase(System.Data.SqlClient.SqlClientFactory.Instance,
            //    "packet size=4096;User Id=sa;Password=sa123;data source=192.168.1.9,1433;initial catalog=MOM_SIERPC_WMS_TEST;",
            //    DynamORM.DynamicDatabaseOptions.SingleConnection | DynamORM.DynamicDatabaseOptions.SingleTransaction |
            //    DynamORM.DynamicDatabaseOptions.SupportSchema | DynamORM.DynamicDatabaseOptions.SupportTop);
            return new DynamORM.DynamicDatabase(System.Data.SQLite.SQLiteFactory.Instance,
                "Data Source=test.db3;",
                DynamORM.DynamicDatabaseOptions.SingleConnection | DynamORM.DynamicDatabaseOptions.SingleTransaction |
                DynamORM.DynamicDatabaseOptions.SupportSchema | DynamORM.DynamicDatabaseOptions.SupportLimitOffset);
        }

        private static void Main(string[] args)
        {
            using (var db = GetORM())
            using (var con = db.Open())
            using (var cmd = con.CreateCommand())
                new List<string>()
                {
                    @"CREATE TABLE IF NOT EXISTS Test(id int NOT NULL PRIMARY KEY, val text);",
                    @"DELETE FROM Test;",
                    @"INSERT INTO Test VALUES(1, 'Test');",
                }.ForEach(x =>
                    cmd.SetCommand(x)
                        .ExecuteNonQuery());

            Console.Out.WriteLine("Press ENTER to launch bombardment... or q and ENTER to quit.");
            while (Console.In.ReadLine() != "q")
            {
                Console.Out.WriteLine("Bombardment...");

                long membefore = GC.GetTotalMemory(true);

                BombsAway();

                GC.WaitForPendingFinalizers();
                GC.Collect();

                Console.Out.WriteLine("Mem Before: {0}, Mem After: {1}. Done.", membefore, GC.GetTotalMemory(true));
            }
        }

        private static void BombsAway()
        {
            string test_str = string.Empty;

            /*for (int y = 0; y < 10; y++)
            {
                string val = null;
                for (int i = 0; i < 1000; i++)
                {
                    dynamic o = new ExpandoObject();
                    o.Test = "123";
                    o.Test2 = 123;
                    o.Test3 = true;
                    o.Test4 = (object)null;

                    val = o.Test;
                    val = o.Test2.ToString();
                }

                GC.WaitForPendingFinalizers();
                GC.Collect();

                Console.Out.WriteLine("Expr: [ExpandoObject only], Mem: {0}", GC.GetTotalMemory(true));
            }

            for (int y = 0; y < 10; y++)
            {
                string val = null;

                for (int i = 0; i < 1000; i++)
                {
                    dynamic o = new ExpandoObject();
                    var d = o as IDictionary<string, object>;
                    d.Add("Test", "123");
                    d.Add("Test2", 123);
                    d.Add("Test3", true);
                    d.Add("Test4", null);

                    val = o.Test;
                    val = o.Test2.ToString();
                }

                GC.WaitForPendingFinalizers();
                GC.Collect();

                Console.Out.WriteLine("Expr: [ExpandoObject as Dict], Mem: {0}", GC.GetTotalMemory(true));
            }*/

            /*for (int y = 0; y < 10; y++)
            {
                Func<dynamic, object> f = x => x.To == 0;
                for (int i = 0; i < 1000; i++)
                    using (var p = DynamORM.Helpers.Dynamics.DynamicParser.Parse(f))
                        test_str = p.ToString();

                GC.WaitForPendingFinalizers();
                GC.Collect();

                Console.Out.WriteLine("Expr: {0}, Mem: {1}", test_str, GC.GetTotalMemory(true));
            }*/

            for (int y = 0; y < 10; y++)
            {
                using (var dbt = GetORM())
                using (dbt.Table("Test"))
                    ;

                GC.WaitForPendingFinalizers();
                GC.Collect();

                Console.Out.WriteLine("Expr: [Create and destroy ORM], Mem: {0}", GC.GetTotalMemory(true));
            }
                
            //using (var db = GetORM())
            {
                for (int y = 0; y < 5; y++)
                {
                    using (var db = GetORM())
                        for (int i = 0; i < 1000; i++)
                            test_str = ((int)db.Table("Test").Scalar("SELECT id FROM Test;")).ToString();

                    GC.WaitForPendingFinalizers();
                    GC.Collect();

                    Console.Out.WriteLine("Expr: [db.Table(\"Test\").Scalar(\"SELECT id FROM Test;\")] = {0}, Mem: {1}", test_str, GC.GetTotalMemory(true));
                }

                for (int y = 0; y < 5; y++)
                {
                    using (var db = GetORM())
                        for (int i = 0; i < 1000; i++)
                            test_str = ((int)db.Table("Test").Scalar(columns: "id")).ToString();

                    GC.WaitForPendingFinalizers();
                    GC.Collect();

                    Console.Out.WriteLine("Expr: [db.Table(\"Test\").Scalar(columns: \"id\")] = {0}, Mem: {1}", test_str, GC.GetTotalMemory(true));
                }

                for (int y = 0; y < 5; y++)
                {
                    using (var db = GetORM())
                        for (int i = 0; i < 1000; i++)
                            test_str = db.From(x => x.Test.As(x.t)).Where(t => t.id == 1).ToList().First().val;

                    GC.WaitForPendingFinalizers();
                    GC.Collect();

                    Console.Out.WriteLine("Expr: [db.From(x => x.Test.As(x.t)).Where(t => t.id == 1).ToList().First().val] = {0}, Mem: {1}", test_str, GC.GetTotalMemory(true));
                }

                for (int y = 0; y < 5; y++)
                {
                    using (var db = GetORM())
                        for (int i = 0; i < 1000; i++)
                            test_str = ((int)db.From(x => x.Test.As(x.t)).Where(t => t.id == 1).Select(t => t.id).Scalar()).ToString();

                    GC.WaitForPendingFinalizers();
                    GC.Collect();

                    Console.Out.WriteLine("Expr: [db.From(x => x.Test.As(x.t)).Where(t => t.id == 1).Select(t => t.id).Scalar()] = {0}, Mem: {1}", test_str, GC.GetTotalMemory(true));
                }
            }

            /*for (int i = 0; i < 1000; i++)
                using (var db = GetORM())
                {
                    //var session = db.From(x => x.mom_Sessions.As(x.s))
                    //    .Where(s => s.ms_id == Guid.Empty && s.ms_mus_id == Guid.Empty)
                    //    .Execute<Session>()
                    //    .FirstOrDefault();
                    var session = db.From(x => x.mom_Sessions.As(x.s))
                        .Where(s => s.ms_id == Guid.Empty && s.ms_mus_id == Guid.Empty)
                        .Execute()
                        .FirstOrDefault();

                    //db.Table("mom_Sessions").Delete()
                    //    .Where("ms_id", Guid.Empty)
                    //    .Where("ms_mus_id", Guid.Empty)
                    //    .Execute();

                    //var session = (db.Table().Query("SELECT * FROM mom_Sessions WHERE ms_id = @0 AND ms_mus_id = @1", Guid.Empty, Guid.Empty)
                    //    as IEnumerable<dynamic>).FirstOrDefault();
                }*/
        }
    }
}