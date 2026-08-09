namespace Aquila.Core.Exceptions;

/// <summary>
/// Base exception for all Aquila framework errors.
/// </summary>
public class AquilaException : Exception
{
    public AquilaException(string message) : base(message) { }
    public AquilaException(string message, Exception innerException) : base(message, innerException) { }
}

/// <summary>
/// Thrown when optimistic concurrency checks fail (e.g. ETag mismatch or event stream version mismatch).
/// </summary>
public sealed class AquilaConcurrencyException : AquilaException
{
    public string DocumentId { get; }
    public string ExpectedVersion { get; }
    public string ActualVersion { get; }

    public AquilaConcurrencyException(string documentId, string expectedVersion, string actualVersion)
        : base($"Optimistic concurrency violation for document '{documentId}'. Expected version: '{expectedVersion}', Actual version: '{actualVersion}'.")
    {
        DocumentId = documentId;
        ExpectedVersion = expectedVersion;
        ActualVersion = actualVersion;
    }

    public AquilaConcurrencyException(string message, Exception innerException)
        : base(message, innerException)
    {
        DocumentId = string.Empty;
        ExpectedVersion = string.Empty;
        ActualVersion = string.Empty;
    }
}
