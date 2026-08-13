using Aquila.Core.Serialization;
using System.IO;
using System.Text;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json;

namespace Aquila.Cosmos.Storage;

public sealed class AquilaCosmosJsonSerializer : CosmosSerializer
{
    private static readonly Encoding DefaultEncoding = new UTF8Encoding(false, true);
    private readonly JsonSerializer _serializer;

    public AquilaCosmosJsonSerializer(JsonSerializerSettings? settings = null)
    {
        _serializer = JsonSerializer.Create(settings ?? PrivateConstructorContractResolver.Settings);
    }

    public override T FromStream<T>(Stream stream)
    {
        if (stream == null || stream.CanRead == false)
        {
            return default!;
        }

        using (stream)
        using (var sr = new StreamReader(stream, DefaultEncoding))
        using (var jtr = new JsonTextReader(sr))
        {
            return _serializer.Deserialize<T>(jtr)!;
        }
    }

    public override Stream ToStream<T>(T input)
    {
        var ms = new MemoryStream();
        using (var sw = new StreamWriter(ms, DefaultEncoding, 1024, true))
        using (var jw = new JsonTextWriter(sw))
        {
            jw.Formatting = _serializer.Formatting;
            _serializer.Serialize(jw, input);
            jw.Flush();
            sw.Flush();
        }

        ms.Position = 0;
        return ms;
    }
}
