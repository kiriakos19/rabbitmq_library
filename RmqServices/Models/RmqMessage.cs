using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RmqServices.Models
{
    internal class RmqMessage
    {
        public RmqMessage() : this(string.Empty)
        {
        }

        public RmqMessage(string route, params object[] data)
        {
            Route = route;
            Data = data;
        }

        public string Route { get; set; }
        public object?[] Data { get; set; }
    }
}
