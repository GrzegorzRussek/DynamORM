using System;
using System.Collections.Generic;
using System.Linq;
using DynamORM;

namespace Tester
{
    internal class Program
    {
        private static DynamicDatabase GetORM()
        {
            return new DynamicDatabase(System.Data.SqlClient.SqlClientFactory.Instance,
                "packet size=4096;User Id=sa;Password=sa123;data source=127.0.0.1,1435;initial catalog=MOM_LANGOWSKI_WMS_TEST;",
                //"packet size=4096;User Id=sa;Password=sa123;data source=192.168.1.9,1433;initial catalog=MOM_NEXT_Florentyna_WMS_PROD;",
                //"packet size=4096;User Id=sa;Password=sa123;data source=127.0.0.1;initial catalog=DynamORM;",
                DynamicDatabaseOptions.SingleConnection | DynamicDatabaseOptions.SingleTransaction | DynamicDatabaseOptions.SupportSchema | 
                DynamicDatabaseOptions.SupportStoredProcedures | DynamicDatabaseOptions.SupportTop | DynamicDatabaseOptions.DumpCommands);

            ////return new DynamORM.DynamicDatabase(System.Data.SQLite.SQLiteFactory.Instance,
            ////    "Data Source=test.db3;",
            ////    DynamORM.DynamicDatabaseOptions.SingleConnection | DynamORM.DynamicDatabaseOptions.SingleTransaction |
            ////    DynamORM.DynamicDatabaseOptions.SupportSchema | DynamORM.DynamicDatabaseOptions.SupportLimitOffset);
            ///
        }

        private static void Main(string[] args)
        {
            //var c = new System.Data.SqlClient.SqlConnection("packet size=4096;User Id=sa;Password=sa123;data source=192.168.0.6;initial catalog=DynamORM;");

            using (var db = GetORM())
            {
                //try
                //{
                //    db.Execute("DROP TABLE Experiments ");
                //}
                //catch { }

                //db.Execute("CREATE TABLE Experiments (t1 nvarchar(50) NOT NULL DEFAULT N'', t2 varchar(50) NOT NULL DEFAULT '');");

                //var q = db.From(x => x.Experiments.As(x.e1));
                //q
                //    .Where(x => x.t2 = "Dupa")
                //    .Where(x => x.Exists(
                //        q.SubQuery()
                //            .From(y => y.Experiments.As(x.e2))
                //            .Where(y => y.e2.t1 == y.e1.t1)))
                //    .Execute().ToList();

                //db.Execute("DROP TABLE Experiments ");

                db.Procedures.usp_API_Generate_Doc_Number<string>(key: Guid.NewGuid(), mdn_id: "ZZ");

                var resL = (db.Procedures.GetProductDesc<IList<GetProductDesc_Result>>() as IEnumerable<dynamic>)
                    .Cast<GetProductDesc_Result>()
                    .ToArray();
                var res = db.Procedures.GetProductDesc_withparameters<GetProductDesc_Result>(PID: 707);
                res = db.Procedures.GetProductDesc_withDefaultparameters<GetProductDesc_Result>();

                int id = -1;
                var resD = db.Procedures.ins_NewEmp_with_outputparamaters(Ename: "Test2", out_EId: id);
            }
        }

        private class GetProductDesc_Result
        {
            public virtual int ProductID { get; set; }
            public virtual string ProductName { get; set; }
            public virtual string ProductDescription { get; set; }
        }
    }
}