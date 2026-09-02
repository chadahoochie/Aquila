namespace Aquila.Core.Events;

/// <summary>
/// Pairs a raw event payload with the tags it should carry when appended.
/// </summary>
public readonly struct TaggedEvent
{
    private static readonly IReadOnlySet<string> EmptyTags = new HashSet<string>();

    public object Data { get; }
    public IReadOnlySet<string> Tags { get; }

    public TaggedEvent(object data, IReadOnlySet<string>? tags = null)
    {
        ArgumentNullException.ThrowIfNull(data);
        Data = data;
        Tags = tags ?? EmptyTags;
    }

    public TaggedEvent(object data, IEnumerable<string> tags)
        : this(data, tags is null ? EmptyTags : new HashSet<string>(tags))
    {
    }
}
