using System;
using DynamORM.Mapper;

namespace Tester
{
    internal class Program
    {
        private static DynamORM.DynamicDatabase GetORM()
        {
            return new DynamORM.DynamicDatabase(System.Data.SqlClient.SqlClientFactory.Instance,
                "packet size=4096;User Id=sa;Password=Sa123;data source=192.168.1.9,1434;initial catalog=MAH_Melle-GAGARIN;",
                DynamORM.DynamicDatabaseOptions.SingleConnection | DynamORM.DynamicDatabaseOptions.SingleTransaction | DynamORM.DynamicDatabaseOptions.SupportStoredProcedures |
                DynamORM.DynamicDatabaseOptions.SupportSchema | DynamORM.DynamicDatabaseOptions.SupportTop);

            //return new DynamORM.DynamicDatabase(System.Data.SQLite.SQLiteFactory.Instance,
            //    "Data Source=test.db3;",
            //    DynamORM.DynamicDatabaseOptions.SingleConnection | DynamORM.DynamicDatabaseOptions.SingleTransaction |
            //    DynamORM.DynamicDatabaseOptions.SupportSchema | DynamORM.DynamicDatabaseOptions.SupportLimitOffset);
        }

        public class ProcResult
        {
            [Column("sp_Test_Scalar_In_Out")]
            public Guid Result { get; set; }

            [Column("outp")]
            public Guid Output { get; set; }
        }

        private static void Main(string[] args)
        {
            using (var db = GetORM())
            {
                Guid res1 = db.Procedures.sp_Test_Scalar<Guid>();
                object res2 = db.Procedures.sp_Test_NonScalar();
                object res3 = db.Procedures.sp_Test_Scalar_In_Out<Guid>(inp: Guid.NewGuid(), out_outp: Guid.Empty);
                ProcResult res4 = db.Procedures.sp_Test_Scalar_In_Out<Guid, ProcResult>(inp: Guid.NewGuid(), out_outp: Guid.Empty);

                Console.Out.WriteLine(res1);
                Console.Out.WriteLine(res2);
                Console.Out.WriteLine(res3);
                Console.Out.WriteLine(res4.Output);
            }
        }
    }
}