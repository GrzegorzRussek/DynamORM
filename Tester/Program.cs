using System;
using System.Linq;
using DynamORM.Mapper;

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
            return new DynamORM.DynamicDatabase(System.Data.SqlClient.SqlClientFactory.Instance,
                "packet size=4096;User Id=sa;Password=sa123;data source=192.168.1.9,1433;initial catalog=MOM_SIERPC_WMS_TEST;",
                DynamORM.DynamicDatabaseOptions.SingleConnection | DynamORM.DynamicDatabaseOptions.SingleTransaction |
                DynamORM.DynamicDatabaseOptions.SupportSchema | DynamORM.DynamicDatabaseOptions.SupportTop);
        }

        private static void Main(string[] args)
        {
            Console.Out.WriteLine("Press ENTER to launch bombardment... or q and ENTER to quit.");
            while (Console.In.ReadLine() != "q")
            {
                Console.Out.WriteLine("Bombardment...");
                BombsAway();

                Console.Out.WriteLine("Done.");
            }
        }

        private static void BombsAway()
        {
            for (int i = 0; i < 1000; i++)
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
                }
        }
    }
}