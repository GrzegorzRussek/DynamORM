using System;
using System.Collections.Generic;
using System.Data;
using DynamORM;
using DynamORM.Mapper;

namespace Tester
{
    internal class Program
    {
        [Table(Name = "dist_Distances")]
        public class Distance
        {
            [Column(IsKey = true)]
            public Guid StartAddress_Id { get; set; }

            [Column(IsKey = true)]
            public Guid EndAddress_Id { get; set; }

            [Column(IsKey = true)]
            public int VehicleType { get; set; }

            [Column]
            public int PostcodeDistanceInSecs { get; set; }

            [Column]
            public decimal PostcodeDistanceInKms { get; set; }

            [Column]
            public int IsDepotDistance { get; set; }
        }

        private static void Main(string[] args)
        {
            string conns = string.Format("Server={0};Port={1};Userid={2};Password={3};Database={4};Protocol=3;SSL=false;Pooling=true;MinPoolSize=1;MaxPoolSize=20;Encoding=UNICODE;Timeout=15;",
                "192.168.1.6", 5432, "ted", "ted123", "TED_Altom");

            using (var db = new DynamicDatabase(Npgsql.NpgsqlFactory.Instance, conns, DynamORM.DynamicDatabaseOptions.SupportSchema | DynamORM.DynamicDatabaseOptions.SupportLimitOffset))
            {
                var s = db.GetSchema<Distance>();
                Console.Out.WriteLine(s.Count);

                DynamicTypeMap mapper = DynamORM.Mapper.DynamicMapperCache.GetMapper<Distance>();
                using (var con = db.Open())
                using (var cmd = con.CreateCommand())
                {
                    Dictionary<IDbDataParameter, DynamicPropertyInvoker> parameters = new Dictionary<IDbDataParameter, DynamicPropertyInvoker>();

                    db.Insert<Distance>(new Distance[] { });

                    //db.PrepareBatchInsert<Distance>(mapper, cmd, parameters);
                }
            }
        }
    }
}