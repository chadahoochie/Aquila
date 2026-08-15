using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Exporters;
using BenchmarkDotNet.Running;

namespace Aquila.Benchmarks;

public static class Program
{
    public static void Main(string[] args)
    {
        var defaultConfig = DefaultConfig.Instance;
        var config = ManualConfig.CreateEmpty()
            .AddColumnProvider(defaultConfig.GetColumnProviders().ToArray())
            .AddLogger(defaultConfig.GetLoggers().ToArray())
            .AddDiagnoser(defaultConfig.GetDiagnosers().ToArray())
            .AddAnalyser(defaultConfig.GetAnalysers().ToArray())
            .AddValidator(defaultConfig.GetValidators().ToArray())
            .AddJob(defaultConfig.GetJobs().ToArray())
            .AddHardwareCounters(defaultConfig.GetHardwareCounters().ToArray())
            .AddFilter(defaultConfig.GetFilters().ToArray())
            .AddLogicalGroupRules(defaultConfig.GetLogicalGroupRules().ToArray())
            .AddExporter(MarkdownExporter.GitHub)
            .WithSummaryStyle(defaultConfig.SummaryStyle)
            .WithOptions(defaultConfig.Options);

        if (defaultConfig.Orderer is not null)
        {
            config.WithOrderer(defaultConfig.Orderer);
        }

        if (defaultConfig.CategoryDiscoverer is not null)
        {
            config.WithCategoryDiscoverer(defaultConfig.CategoryDiscoverer);
        }

        BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args, config);
    }
}
