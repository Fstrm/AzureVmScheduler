using Azure.Identity;
using Azure.ResourceManager;
using Azure.ResourceManager.Compute;
using System.Collections.Concurrent;
using Azure.ResourceManager.Resources;

var credential = new DefaultAzureCredential();
var armClient = new ArmClient(credential);
var semaphore = new SemaphoreSlim(10);
var path = "logs.csv";
var runningStartTimes = new ConcurrentDictionary<string, DateTimeOffset>();
var lastKnownStates = new ConcurrentDictionary<string, string>();

if (!File.Exists(path))
    await File.WriteAllTextAsync(path, "Timestamp,SubscriptionId,ResourceGroup,ComputerName,PowerState\n");

Console.WriteLine("Starting...");

using var timer = new PeriodicTimer(TimeSpan.FromMinutes(5));
do
{
    try
    {
        await PollAsync();
    }
    catch (Exception ex)
    {
        Console.WriteLine($"Poll failed: {ex.Message}");
    }
}
while (await timer.WaitForNextTickAsync());

async Task PollAsync()
{
    var now = DateTimeOffset.UtcNow;
    Console.WriteLine($"{now:u} polling...");

    var tasks = new List<Task<string?[]>>();
    await foreach (var sub in armClient.GetSubscriptions())
        tasks.Add(HandleSubscriptionAsync(sub, now));

    var batches = await Task.WhenAll(tasks);
    var allRows = batches.SelectMany(b => b)
        .Where(r => !string.IsNullOrWhiteSpace(r))
        .Select(r => r!)
        .ToArray();

    if (allRows.Length > 0)
        await File.AppendAllLinesAsync(path, allRows);
}

async Task<string?[]> HandleSubscriptionAsync(SubscriptionResource sub, DateTimeOffset pollTime)
{
    var tasks = new List<Task<string?>>();

    await foreach (var vm in sub.GetVirtualMachinesAsync())
        tasks.Add(HandleVmAsync(vm, sub.Data.SubscriptionId!, pollTime));

    var csvBatch = await Task.WhenAll(tasks);
    
    return csvBatch;
}

async Task<string?> HandleVmAsync(VirtualMachineResource vm, string subId, DateTimeOffset pollTime)
{
    await semaphore.WaitAsync();

    try
    {
        var instanceView = (await vm.InstanceViewAsync()).Value;
        var powerState = instanceView.Statuses
            .FirstOrDefault(s => s.Code?.StartsWith("PowerState/") == true)
            ?.Code?.Replace("PowerState/", "") ?? "unknown";

        var autoshutdown = vm.Data.Tags.TryGetValue("Autoshutdown", out var tagValue) && tagValue == "1";
        var computerName = vm.Data.OSProfile?.ComputerName ?? vm.Data.Name;

        Console.WriteLine($"{vm.Data.Name}: {powerState}");

        if (autoshutdown)
            await CheckAndApplyPowerRules(vm, powerState, pollTime);

        return $"{pollTime:u},{subId},{vm.Id.ResourceGroupName},{computerName},{powerState}";
    }
    catch (Exception ex)
    {
        Console.WriteLine($"{vm.Data.Name} error: {ex.Message}");
        return null;
    }
    finally
    {
        semaphore.Release();
    }
}

// Runtime is approximated by tracking transitions into "running" state during the lifetime of this application
async Task CheckAndApplyPowerRules(VirtualMachineResource vm, string powerState, DateTimeOffset pollTime)
{
    var vmId = vm.Id.ToString();
    lastKnownStates.TryGetValue(vmId, out var previousState);
    
    if (powerState == "running")
    {
        if (previousState != "running")
        {
            runningStartTimes[vmId] = pollTime;
        }

        if (runningStartTimes.TryGetValue(vmId, out var startTime))
        {
            var elapsed = pollTime - startTime;
            
            if (elapsed.TotalHours >= 8)
            {
                Console.WriteLine($"{vm.Data.Name} running {elapsed.TotalHours:F1}h, powering off");
                await vm.PowerOffAsync(Azure.WaitUntil.Completed);
                runningStartTimes.TryRemove(vmId, out _);
            }
        }
    }
    else
    {
        runningStartTimes.TryRemove(vmId, out _);

        if (powerState == "stopped")
        {
            Console.WriteLine($"{vm.Data.Name} stopped but allocated, deallocating");
            await vm.DeallocateAsync(Azure.WaitUntil.Completed);
        }
    }
    
    lastKnownStates[vmId] = powerState;
}