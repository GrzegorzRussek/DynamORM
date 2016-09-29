using System.Linq;
using DynamORM;

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
            DynamicDatabase db = new DynamicDatabase(System.Data.SqlClient.SqlClientFactory.Instance, "packet size=4096;User Id=sa;Password=Sa123;data source=127.0.0.1,1434;initial catalog=MAH_Levant;",
                DynamicDatabaseOptions.SingleConnection | DynamicDatabaseOptions.SingleTransaction | DynamicDatabaseOptions.SupportSchema |
                DynamicDatabaseOptions.SupportStoredProcedures | DynamicDatabaseOptions.SupportTop | DynamicDatabaseOptions.DumpCommands);

            try
            {
                db.Execute("DROP TABLE Experiments ");
            }
            catch { }

            db.Execute("CREATE TABLE Experiments (t1 nvarchar(50) NOT NULL DEFAULT N'', t2 varchar(50) NOT NULL DEFAULT '');");

            var q = db.From(x => x.Experiments.As(x.e1));
            q
                .Where(x => x.t2 = "Dupą")
                .Where(x => x.Exists( 
                    q.SubQuery()
                        .From(y => y.Experiments.As(x.e2))
                        .Where(y => y.e2.t1 == y.e1.t1)))
                .Execute().ToList();

            db.Execute("DROP TABLE Experiments ");
        }
    }
}