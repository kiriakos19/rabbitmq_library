using System;
using System.IO;

namespace RmqServices.Models
{
    [AttributeUsage(AttributeTargets.Method, Inherited = false, AllowMultiple = true)]
    public sealed class MqRouteAttribute : Attribute
    {
        public MqRouteAttribute(string route)
        {
            Route = string.Join("/", route.Split(new char[] { Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar }, StringSplitOptions.RemoveEmptyEntries));
        }

        public string Route { get; }
    }

    [AttributeUsage(AttributeTargets.Class, Inherited = false, AllowMultiple = true)]
    public sealed class MqRouteBaseAttribute : Attribute
    {
        public MqRouteBaseAttribute(string route)
        {
            RouteBase = string.Join("/", route.Split(new char[] { Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar }, StringSplitOptions.RemoveEmptyEntries));
        }

        public string RouteBase { get; }
    }
}
