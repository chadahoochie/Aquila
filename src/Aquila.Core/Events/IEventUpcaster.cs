using System;

namespace Aquila.Core.Events;

public interface IEventUpcaster
{
    Type SourceType { get; }
    Type TargetType { get; }
    object Upcast(object oldEvent);
}

public abstract class EventUpcaster<TOld, TNew> : IEventUpcaster 
    where TOld : class 
    where TNew : class
{
    public Type SourceType => typeof(TOld);
    public Type TargetType => typeof(TNew);
    public abstract TNew Upcast(TOld oldEvent);
    object IEventUpcaster.Upcast(object oldEvent) => Upcast((TOld)oldEvent);
}
