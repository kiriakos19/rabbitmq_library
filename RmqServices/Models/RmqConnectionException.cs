using System.Runtime.Serialization;
using System.Security.Permissions;

namespace RmqServices.Models
{
    public enum ConnectionError
    {
        CouldNotBeEstablished,
        Closed
    }

    [Serializable]
    public class RmqConnectionException : Exception
    {
        public RmqConnectionException(ConnectionError connectionError)
        {
            ConnectionError = connectionError;
        }

        public RmqConnectionException(string? message, ConnectionError connectionError) : base(message)
        {
            ConnectionError = connectionError;
        }

        public RmqConnectionException(string? message, ConnectionError connectionError, Exception? innerException) : base(message, innerException)
        {
            ConnectionError = connectionError;
        }

        protected RmqConnectionException(SerializationInfo info, StreamingContext context) : base(info, context)
        {
            if (info == null)
            {
                throw new ArgumentNullException(nameof(info));
            }
            var value = info.GetValue(nameof(ConnectionError), typeof(ConnectionError));
            if (value == null)
            {
                throw new SerializationException("Missing ConnectionError value in Exception's SerializationInfo");
            }
            ConnectionError = (ConnectionError)value;
        }

        public ConnectionError ConnectionError { get; set; }

        public override void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            if (info == null)
            {
                throw new ArgumentNullException(nameof(info));
            }

            info.AddValue(nameof(ConnectionError), ConnectionError);

            base.GetObjectData(info, context);
        }
    }
}
