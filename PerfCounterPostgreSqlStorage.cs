using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
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

        /// <summary>
        /// Reports about errors and exceptions occured.
        /// </summary>
        public event EventHandler<IPerfonErrorEventArgs> OnError;


        public PerfCounterPostgreSqlStorage(string dbConnectionString)
        {
            DbConnectionString = dbConnectionString + "\\";
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
