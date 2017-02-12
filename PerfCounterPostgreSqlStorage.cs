using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Npgsql;
using Perfon.Interfaces.Common;
using Perfon.Interfaces.PerfCounterStorage;

namespace Perfon.Storage.PostgreSql
{
    /// <summary>
    /// Driver for store/restore performance counter values in PostgreSql.
    /// </summary>
    public class PerfCounterPostgreSqlStorage : IPerfomanceCountersStorage
    {
        private string DbConnectionString { get; set; }

        private ConcurrentBag<string> counterNames = new ConcurrentBag<string>();

        /// <summary>
        /// Reports about errors and exceptions occured.
        /// </summary>
        public event EventHandler<IPerfonErrorEventArgs> OnError;


        public PerfCounterPostgreSqlStorage(string dbConnectionString)
        {
            DbConnectionString = dbConnectionString;
        }

        /// <summary>
        /// Awaitable.
        /// </summary>
        /// <param name="counters"></param>
        /// <returns></returns>
        public Task StorePerfCounters(IEnumerable<IPerfCounterInputData> counters, DateTime? nowArg = null, string appId = null)
        {
            try
            {
                var now = DateTime.Now;
                if (nowArg.HasValue)
                {
                    now = nowArg.Value;
                }


                List<short> counterId = new List<short>();

                bool updateNames = false;

                foreach (var counter in counters)
                {
                    if (!counterNames.Contains(counter.Name))
                    {
                        updateNames = true;
                        break;
                    }
                    counterId.Add((short)(Tools.CalculateHash(counter.Name) % (ulong)short.MaxValue));
                }


                using (var conn = new NpgsqlConnection(DbConnectionString))
                {
                    conn.Open();

                    if (updateNames)
                    {
                        counterId.Clear();

                        using (var cmd = new NpgsqlCommand())
                        {
                            cmd.Connection = conn;

                            foreach (var counter in counters)
                            {
                                short id = -1;

                                // Retrieve counters id
                                cmd.CommandText = @"SELECT ""Id"" FROM ""CounterNames"" WHERE ""Name""='" + counter.Name + "'";
                                cmd.Parameters.Add("counterName", NpgsqlTypes.NpgsqlDbType.Varchar).Value = counter.Name;
                                using (var reader = cmd.ExecuteReader())
                                {
                                    while (reader.Read())
                                    {
                                        id = reader.GetInt16(0);
                                    }
                                }
                                cmd.Parameters.Clear();

                                if (id == -1)
                                {
                                    id = (short)(Tools.CalculateHash(counter.Name) % (ulong)short.MaxValue);
                                    cmd.CommandText = @"INSERT INTO ""CounterNames"" (""Id"",""Name"") VALUES (@id, @counterName)";
                                    cmd.Parameters.Add("id", NpgsqlTypes.NpgsqlDbType.Smallint).Value = id;
                                    cmd.Parameters.Add("counterName", NpgsqlTypes.NpgsqlDbType.Varchar).Value = counter.Name;
                                    cmd.ExecuteNonQuery();
                                    cmd.Parameters.Clear();
                                }
                                counterId.Add(id);
                                counterNames.Add(counter.Name);
                            }
                        }
                    }


                    using (var writer = conn.BeginBinaryImport(@"COPY ""PerfomanceCounterValues"" (""AppId"", ""CounterId"", ""Timestamp"", ""Value"") FROM STDIN (FORMAT BINARY)"))
                    {
                        int i = 0;
                        foreach (var counter in counters)
                        {
                            writer.StartRow();

                            writer.Write(0, NpgsqlTypes.NpgsqlDbType.Smallint);
                            writer.Write(counterId[i], NpgsqlTypes.NpgsqlDbType.Smallint);
                            writer.Write(now, NpgsqlTypes.NpgsqlDbType.Timestamp);
                            writer.Write(counter.Value, NpgsqlTypes.NpgsqlDbType.Real);

                            i++;
                        }

                        //cmd.CommandText = @"INSERT INTO ""PerfomanceCounterValues"" (""AppId"", ""CounterId"", ""Timestamp"", ""Value"") VALUES (0, @id, @timestamp, @value)";
                        //cmd.Parameters.Add("id", NpgsqlTypes.NpgsqlDbType.Smallint).Value = id;
                        //cmd.Parameters.Add("timestamp", NpgsqlTypes.NpgsqlDbType.Timestamp).Value = now;
                        //cmd.Parameters.Add("value", NpgsqlTypes.NpgsqlDbType.Real).Value = counter.Value;
                        //cmd.ExecuteNonQuery();
                        //cmd.Parameters.Clear();
                    }

                }

            }
            catch (Exception exc)
            {
                if (OnError != null)
                {
                    OnError(new object(), new PerfonErrorEventArgs(exc.ToString()));
                }
            }

            return Task.Delay(0);
        }

        public Task<IEnumerable<IPerfCounterValue>> QueryCounterValues(string counterName, DateTime? date = null, int skip = 0, string appId = null)
        {
            var list = new List<IPerfCounterValue>();

            if (!date.HasValue)
            {
                date = DateTime.Now;
            }

            date = date.Value.Date;

            try
            {
                using (var conn = new NpgsqlConnection(DbConnectionString))
                {
                    conn.Open();

                    using (var cmd = new NpgsqlCommand())
                    {
                        cmd.Connection = conn;

                        var id = (short)(Tools.CalculateHash(counterName) % (ulong)short.MaxValue);

                        cmd.CommandText = @"SELECT ""Timestamp"",""Value"" FROM ""PerfomanceCounterValues"" WHERE ""AppId""=0 AND ""CounterId""=@id AND ""Timestamp"" >= @timestamp AND CAST(""Timestamp"" AS DATE) = @timestamp";
                        cmd.Parameters.Add("id", NpgsqlTypes.NpgsqlDbType.Smallint).Value = id;
                        cmd.Parameters.Add("timestamp", NpgsqlTypes.NpgsqlDbType.Timestamp).Value = date;
                        using (var reader = cmd.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                var timeStamp = reader.GetDateTime(0);
                                var value = reader.GetFloat(1);

                                list.Add(new PerfCounterValue(timeStamp, value));
                            }
                        }
                    }

                }
            }
            catch (Exception exc)
            {
                if (OnError != null)
                {
                    OnError(new object(), new PerfonErrorEventArgs(exc.ToString()));
                }
            }

            return Task.FromResult(list as IEnumerable<IPerfCounterValue>);
        }

        public Task<IEnumerable<string>> GetCountersList()
        {
            var res = new List<string>();

            try
            {
                using (var conn = new NpgsqlConnection(DbConnectionString))
                {
                    conn.Open();
                    using (var cmd = new NpgsqlCommand())
                    {
                        cmd.Connection = conn;

                        cmd.CommandText = @"SELECT ""Name"" FROM ""CounterNames"" ";
                        using (var reader = cmd.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                res.Add(reader.GetString(0));
                            }
                        }
                    }
                }
            }
            catch (Exception exc)
            {
                if (OnError != null)
                {
                    OnError(new object(), new PerfonErrorEventArgs(exc.ToString()));
                }
            }

            return Task.FromResult(res as IEnumerable<string>);
        }



    }
}
