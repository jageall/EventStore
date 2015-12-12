using System;
using System.Threading;
using System.Threading.Tasks;

namespace EventStore.Transport.Http.Client
{
    internal static class CancellationTokenSourceExtensions
    {
        public static CancellationTokenSource CancelAfter(this CancellationTokenSource source,TimeSpan after)
        {
            var timer = new Timer(self =>
            {
                ((Timer)self).Dispose();
                try
                {
                    source.Cancel();
                } catch (ObjectDisposedException) { }
            });
            timer.Change((int)after.TotalMilliseconds, -1);
            return source;
        }
    }
}