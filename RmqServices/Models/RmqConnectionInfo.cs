using System.Web;

namespace RmqServices.Models
{
    public class RmqConnectionInfo
    {
        public string Host { get; set; } = string.Empty;
        public string VirtualHost { get; set; } = string.Empty;
        public string Username { get; set; } = string.Empty;
        public string Password { get; set; } = string.Empty;
        public int? Port { get; set; }

        public string ConnectionString
        {
            get
            {
                return $"amqp://{HttpUtility.UrlEncode(Username)}:{HttpUtility.UrlEncode(Password)}@{Host}:{Port}/{VirtualHost}".Trim().TrimEnd('/', ':');
            }
        }
    }
}
