using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RmqServices.Models
{
    internal class RmqResponse
    {
        public RmqResponse() { }
        public RmqResponse(object? data, bool hasError = false)
        {
            Data = data;
            HasError = hasError;
        }

        public object? Data { get; set; }
        public bool HasError { get; set; }
    }
}
