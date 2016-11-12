<#PSScriptInfo

.VERSION 1.0.0

.GUID 8b95b4d6-fcef-488c-bf06-90462df15ac5

.AUTHOR Mike Hendrickson

#>

<# 
.SYNOPSIS
 Used to gather summary statistics of performance counter data from one or more performance monitor .blg files, on one or more computers.

.DESCRIPTION 
 Used to gather summary statistics of performance counter data from one or more performance monitor .blg files, on one or more computers.
 Summary statistics for each counter include: Average, Maximum, Minimum, Sum, and Count.
 Groups and averages related counters into per file, per computer, and all computer groups.

 The script can run either locally, or remotely, in parallel, against one or more target computers.
 Remote collections are performed using Remote PowerShell. A collection job is sent to each target machine, where it is executed locally on each machine.
 The counters discovered on each target computer are summarized, and only the summaries are returned to the computer where the script is being executed.
 
.PARAMETER Path
 The path(s) to look for and analyze performance monitor .blg files within. Paths are recursively searched for files.

.PARAMETER ComputerName
 Optional list of one or more target computers to send remote collection jobs to. If nothing is specified, the collection will be done locally.

.PARAMETER Counter
 Optional list of one or more counters to inspect within the discovered .blg files. If nothing is specified (not recommended), all counters within the
 log will be inspected. Wildcards are permitted, and recommended.

.PARAMETER MaxSamples
 Specifies the maximum number of samples of each counter in each log to import. Defaults to Unlimited.

.PARAMETER StartTime
 The date and time of oldest counter sample that should be inspected with the found .blg files. Defaults to the beginning of time.

.PARAMETER EndTime
 The date and time of newest counter sample that should be inspected with the found .blg files. Defaults to the end of time.

.PARAMETER OnlyInspectFilesInDateRange
 Whether to only look at .blg files that have a Creation Date, or Last Write Date, that falls between StartTime and EndTime. Defaults to False.

.PARAMETER DataCollectorName
 Optional name of a Performance Monitor Data Collector to look for. Used in Conjunction with RestartRunningDataCollector or StartDataCollectorIfStopped

.PARAMETER RestartRunningDataCollector
 If DataCollectorName is specified, and the Data Collector is found to be running, restarts the Data Collector before doing the log analysis.
 This causes the current log file to be closed, which allows for all recent records to be committed to the logs before inspection. Defaults to False.

.PARAMETER StartDataCollectorIfStopped
 If DataCollectorName is specified, and the Data Collector is found to be stopped, starts the Data Collector. Defaults to False.

.PARAMETER GroupedComputerName
 Specifies the ComputerName to use in the output when counters across all computers are grouped. Defaults to an empty string.

.PARAMETER GroupedFileName
 Specifies the FileName to use in the output when discovered counters are grouped by computer. Defaults to an empty string.

.EXAMPLE
 >Performs a remote log analysis in path 'C:\PerfLogs' and inspects all counters in all logs. Not specifying any counters is not recommended for large log files, as the analysis could be very CPU and memory intensive.
 PS> .\Get-PerfmonSummaryStats.ps1 -Path "C:\PerfLogs" -ComputerName "computer1","computer2" -Verbose

.EXAMPLE
 >Performs a remote log analysis on all counters in all files created by the default data collector in Exchange 2013+. Starts the Data Collector if it's stopped, and restarts it if it's running.
 PS> .\Get-PerfmonSummaryStats.ps1 -Path "C:\Program Files\Microsoft\Exchange Server\V15\Logging\Diagnostics\DailyPerformanceLogs" -DataCollectorName "ExchangeDiagnosticsDailyPerformanceLog" -RestartRunningDataCollector $true -StartDataCollectorIfStopped $true -ComputerName "computer1","computer2" -Verbose

.EXAMPLE
 >Performs a local log analysis in path 'C:\PerfLogs' and inspects the \LogicalDisk(*)\Avg. Disk Sec/Read and \LogicalDisk(*)\Avg. Disk Sec/Write counters for any computer found within a log. Exports the contents to CSV.
 PS> .\Get-PerfmonSummaryStats.ps1 -Path "C:\PerfLogs" -Counter "\\*\LogicalDisk(*)\Avg. Disk Sec/Read","\\*\LogicalDisk(*)\Avg. Disk Sec/Write" -Verbose | Export-Csv -Path C:\PerfLogs\PerfmonSummaries.csv -NoTypeInformation

.EXAMPLE
 >Performs a local log analysis in path 'C:\PerfLogs' and inspects the \LogicalDisk(*)\Avg. Disk Sec/Read counters for any logs found which were originally created on computer1. Saves the output in a variable.
 PS> $perfmonSummaries = .\Get-PerfmonSummaryStats.ps1 -Path "C:\PerfLogs" -Counter "\\computer1\LogicalDisk(*)\Avg. Disk Sec/Read" -Verbose

.EXAMPLE
 >Performs a remote log analysis in path 'C:\PerfLogs' and inspects all Process counters for all processes
 PS> .\Get-PerfmonSummaryStats.ps1 -Path "C:\PerfLogs" -Counter "\\*\Process(*)\*" -ComputerName "computer1","computer2" -Verbose

.EXAMPLE
 >Performs a remote log analysis in path 'C:\PerfLogs' and inspects all LogicalDisk counters for all disks for the duration of the previous day
 PS> .\Get-PerfmonSummaryStats.ps1 -Path "C:\PerfLogs" -Counter "\\*\LogicalDisk(*)\*" -ComputerName "computer1","computer2" -StartTime ([DateTime]::Today.AddDays(-1)) -EndTime ([DateTime]::Today) -Verbose

.INPUTS
None. You cannot pipe objects to Get-PerfmonSummaryStats.ps1.

.OUTPUTS
[System.Object[]]. Get-PerfmonSummaryStats.ps1 returns a an array of Object's containing Perfmon Summary Stats.
#>

[CmdletBinding()]
param
(
    [parameter(Mandatory = $true)] 
    [String[]]
    $Path = @(),

    [String[]]
    $ComputerName = @(),

    [String[]]
    $Counter = @(),
    
    [Int64]
    $MaxSamples = [Int64]::MaxValue,

    [DateTime]
    $StartTime = [DateTime]::MinValue,
    
    [DateTime]
    $EndTime = [DateTime]::MaxValue,

    [Boolean]
    $OnlyInspectFilesInDateRange = $false,

    [String]
    $DataCollectorName = "",

    [Boolean]
    $RestartRunningDataCollector = $false,

    [Boolean]
    $StartDataCollectorIfStopped = $false,
    
    [String]
    $GroupedComputerName = "",

    [String]
    $GroupedFileName = ""
)

#Reads performance counters in the specified LogPath, and returns measurements for each file
#matching the specified criteria.
function Get-PerfmonSummaryStatsLocal
{
    [CmdletBinding()]
    [OutputType([Object[]])]
    param
    (
        [parameter(Mandatory = $true)]
        [String[]]
        $Path,

        [String[]]
        $Counter,
    
        [Int64]
        $MaxSamples,

        [DateTime]
        $StartTime,
    
        [DateTime]
        $EndTime,

        [Boolean]
        $OnlyInspectFilesInDateRange,

        [String]
        $DataCollectorName,

        [Boolean]
        $RestartRunningDataCollector,

        [Boolean]
        $StartDataCollectorIfStopped
    )

    Write-Verbose "$([DateTime]::Now): Entered Function Get-PerfmonSummaryStatsLocal"

    [PSObject[]]$counterSummaries = @()

    if (([string]::IsNullOrEmpty($DataCollectorName) -eq $false) -and ($RestartRunningDataCollector -eq $true -or $StartDataCollectorIfStopped -eq $true))
    {
        Write-Verbose "$([DateTime]::Now): Checking for Data Collector"

        $logmanOutput = logman query $DataCollectorName

        if ($null -eq ($logmanOutput | Where-Object {$_ -like "*Data Collector Set was not found*"}))
        {
            $collectorRunning = $null -ne ($logmanOutput | Where-Object {$_.StartsWith("Status:")}) -and ($logmanOutput | Where-Object {$_.StartsWith("Status:")}).Contains("Running")

            if ($collectorRunning -eq $true)
            {
                #See if any files have been created after the NewestEntry
                $files = Get-ChildItem $Path -Recurse | Where-Object {$_.GetType().Name -like "FileInfo" -and $_.CreationTime -ge $EndTime}

                #If nothing has been created, the log is probably still running, so we should restart it
                if ($null -eq $files)
                {
                    Write-Verbose "$([DateTime]::Now): Restarting Data Collector"

                    logman stop $DataCollectorName | Out-Null
                    logman start $DataCollectorName | Out-Null
                }
            }
            else
            {
                Write-Verbose "$([DateTime]::Now): Starting Data Collector"

                logman start $DataCollectorName | Out-Null
            }
        }        
    }

    foreach ($logPath in $Path)
    {
        Write-Verbose "$([DateTime]::Now): Getting Perfmon files in path '$($logPath)'"

        #Now process the perfmon files
        $files = Get-ChildItem $logPath -Recurse -Include "*.blg" | Where-Object {$_.GetType().Name -like "FileInfo"} | Sort-Object CreationTime
        $excludeFiles = $files | Where-Object {!(($_.CreationTime -ge $StartTime -and $_.CreationTime -le $EndTime) -or ($_.LastWriteTime -ge $StartTime -and $_.LastWriteTime -le $EndTime) -or ($_.LastAccessTime -ge $StartTime -and $_.LastAccessTime -le $EndTime))}        

        if ($OnlyInspectFilesInDateRange -and $files.Count -gt 0 -and ($files.Count - $excludeFiles.Count) -eq 0)
        {
            Write-Warning "$([DateTime]::Now): Excluding all $($files.Count) files from '$($logPath)' due to being out of date range."
        }
        elseif ($files.Count -gt 0)
        {
            Write-Verbose "$([DateTime]::Now): Found $($files.Count) files."

            foreach ($file in $files)
            {
                $importParams = @{
                    Path        = $file.FullName
                    StartTime   = $StartTime
                    EndTime     = $EndTime
                    MaxSamples  = $MaxSamples
                    ErrorAction = "SilentlyContinue"
                    Verbose     = $false
                }

                if ($null -ne $Counter -and $Counter.Count -gt 0)
                {
                    $importParams.Add("Counter", $Counter)
                }

                Write-Verbose "$([DateTime]::Now): Importing and grouping counters from file $($file.FullName)"

                [Object[]]$countersGrouped = (Import-Counter @importParams).CounterSamples | Group-Object -Property Path

                if ($null -ne $countersGrouped)
                {
                    Write-Verbose "$([DateTime]::Now): Found $($countersGrouped.Count) unique counter groups"

                    foreach ($counterGroup in $countersGrouped)
                    {
                        $counterPath = $counterGroup.Name
                        [Object[]]$samples = $counterGroup.Group

                        Write-Verbose "$([DateTime]::Now): Measuring counter group"

                        $measured = $samples | Measure-Object -Property CookedValue -Sum -Maximum -Minimum

                        #Average calculation for Average counters taken from these references:
                            #https://msdn.microsoft.com/en-us/library/ms804010.aspx
                            #https://blogs.msdn.microsoft.com/ntdebugging/2013/09/30/performance-monitor-averages-the-right-way-and-the-wrong-way/

                        [Decimal]$average = 0

                        $numTicksDiff = 0
                        $frequency = 0
                        $numOpsDiff = 0

                        if ($samples[0].CounterType -like "AverageTimer*")
                        {
                            $numTicksDiff = $samples[-1].RawValue - $samples[0].RawValue
                            $frequency = $samples[-1].TimeBase
                            $numOpsDiff = $samples[-1].SecondValue - $samples[0].SecondValue

                            if ($frequency -ne 0 -and $numOpsDiff -ne 0)
                            {
                                [Decimal]$average = ($numTicksDiff / $frequency) / $numOpsDiff
                            }                        
                        }
                        elseif ($measured.Count -ne 0)
                        {
                            [Decimal]$average = $measured.Sum / $measured.Count
                        }

                        $summary = New-Object PSObject -Property `
                                                            @{
                                                                Computer        = $counterPath.Substring(2, $counterPath.IndexOf('\',2) - 2).ToLower()
                                                                CounterPath     = $counterPath.Substring($counterPath.IndexOf('\', 2)).ToLower()
                                                                OldestTimestamp = $samples[0].Timestamp
                                                                NewestTimestamp = $samples[-1].Timestamp
                                                                Count           = $measured.Count
                                                                Average         = $average
                                                                Maximum         = $measured.Maximum
                                                                Minimum         = $measured.Minimum
                                                                Sum             = $measured.Sum
                                                                NumTicksDiff    = $numTicksDiff
                                                                Frequency       = $frequency
                                                                NumOpsDiff      = $numOpsDiff
                                                                CounterType     = $samples[0].CounterType
                                                                File            = $file.FullName
                                                            }

                        $counterSummaries += $summary
                    }
                }
                else
                {
                    Write-Warning "$([DateTime]::Now): Found 0 matching counters in file $($file.FullName)"
                }
            }
        }
    }

    Write-Verbose "$([DateTime]::Now): Finished processing logs"

    if ($counterSummaries.Count -ne 0)
    {
        return ($counterSummaries | `
                    Select-Object -Property Computer, CounterPath, OldestTimestamp, NewestTimestamp, Count, Average, Maximum, Minimum, Sum, NumTicksDiff, Frequency, NumOpsDiff, CounterType, File | `
                    Sort-Object -Property Computer, CounterPath, OldestTimestamp, NewestTimestamp)
    }
    else
    {
        return $counterSummaries
    }    
}

#Sends a performance counter analysis job to one or more computers
function Get-PerfmonSummaryStatsRemote
{
    [CmdletBinding()]
    [OutputType([PSObject[]])]
    param
    (
        [parameter(Mandatory = $true)]
        [String[]]
        $Path,

        [String[]]
        $ComputerName,

        [String[]]
        $Counter,
  
        [Int64]
        $MaxSamples,

        [DateTime]
        $StartTime,
    
        [DateTime]
        $EndTime,

        [Boolean]
        $OnlyInspectFilesInDateRange,

        [String]
        $DataCollectorName,

        [Boolean]
        $RestartRunningDataCollector,

        [Boolean]
        $StartDataCollectorIfStopped
    )

    Write-Verbose "$([DateTime]::Now): Sending performance counter collection jobs to $($ComputerName.Count) computers."

    $jobs = Invoke-Command -ComputerName $ComputerName -ScriptBlock ${function:Get-PerfmonSummaryStatsLocal} -ArgumentList $Path,$Counter,$MaxSamples,$StartTime,$EndTime,$OnlyInspectFilesInDateRange,$DataCollectorName,$RestartRunningDataCollector,$StartDataCollectorIfStopped -AsJob

    Write-Verbose "$([DateTime]::Now): Waiting for jobs to complete"

    Wait-Job $jobs | Out-Null

    [PSObject[]]$allCounterSummaries = Receive-Job $jobs

    return $allCounterSummaries
}

#Merges measurements within a related measurement group
function Merge-SummaryGroup
{
    [CmdletBinding()]
    param
    (
        [Object]
        $SummaryGroup,

        [String]
        $Computer,

        [String]
        $File
    )
      
    $mergedSummary = New-Object PSObject -Property @{Computer=$Computer;CounterPath="";OldestTimestamp=$null;NewestTimestamp=$null;Count=[Decimal]0;Average=[Decimal]0;Maximum=[Decimal]0;Minimum=[Decimal]0;Sum=[Decimal]0;NumOpsDiff=[Decimal]0;CounterType="";File=$File;Group=$SummaryGroup.Name;MemberCount=[Decimal]0}

    [Object[]]$group = $SummaryGroup.Group

    if ($group.Count -gt 0)
    {
        $mergedSummary.CounterPath = $group[0].CounterPath
        $mergedSummary.Maximum = $group[0].Maximum
        $mergedSummary.Minimum = $group[0].Minimum
        $mergedSummary.OldestTimestamp = $group[0].OldestTimestamp
        $mergedSummary.NewestTimestamp = $group[0].NewestTimestamp
        $mergedSummary.CounterType = $group[0].CounterType
        $mergedSummary.MemberCount = $group.Count

        foreach ($measurement in $group)
        {
            $mergedSummary.Count += $measurement.Count
            $mergedSummary.Sum += $measurement.Sum
            $mergedSummary.NumOpsDiff += $measurement.NumOpsDiff

            if ($mergedSummary.Maximum -lt $measurement.Maximum)
            {
                $mergedSummary.Maximum = $measurement.Maximum
            }

            if ($mergedSummary.Minimum -gt $measurement.Minimum)
            {
                $mergedSummary.Minimum = $measurement.Minimum
            }

            if ($mergedSummary.OldestTimestamp -gt $measurement.OldestTimestamp)
            {
                $mergedSummary.OldestTimestamp = $measurement.NewestTimestamp
            }

            if ($mergedSummary.NewestTimestamp -lt $measurement.OldestTimestamp)
            {
                $mergedSummary.NewestTimestamp = $measurement.NewestTimestamp
            }
        }

        #If this is an average counter, we need to do a weighted average based on number of operations
        if ($group[0].CounterType -like "Average*")
        {
            if ($mergedSummary.NumOpsDiff -ne 0)
            {
                foreach ($measurement in $group)
                {
                    $mergedSummary.Average += (($measurement.NumOpsDiff / $mergedSummary.NumOpsDiff) * $measurement.Average)
                }
            }
        }
        elseif ($mergedSummary.Count -ne 0)
        {
            $mergedSummary.Average = $mergedSummary.Sum / $mergedSummary.Count
        }
    }

    return $mergedSummary
}

#Merges the given counter summaries into related groups of:
#Computer, CounterPath, and File (per file groupings)
#Computer and CounterPath (per computer groupings)
#CounterPath (all computer groupings)
function Get-SummariesMerged
{
    [CmdletBinding()]
    [OutputType([PSObject[]])]
    param
    (
        [PSObject[]]
        $CounterSummaries,

        [String]
        $GroupedComputerName,

        [String]
        $GroupedFileName
    )

    [PSObject[]]$mergedSummaries = @()

    foreach ($computerPathAndFileGroup in ($CounterSummaries | Group-Object -Property Computer, CounterPath, File))
    {
        $mergedSummaries += (Merge-SummaryGroup -SummaryGroup $computerPathAndFileGroup -Computer $computerPathAndFileGroup.Group[0].Computer -File $computerPathAndFileGroup.Group[0].File)
    }

    foreach ($computerAndPathGroup in ($CounterSummaries | Group-Object -Property Computer, CounterPath))
    {
        $mergedSummaries += (Merge-SummaryGroup -SummaryGroup $computerAndPathGroup -Computer $computerAndPathGroup.Group[0].Computer -File $GroupedFileName)
    }

    foreach ($counterPathGroup in ($CounterSummaries | Group-Object -Property CounterPath))
    {
        $mergedSummaries += (Merge-SummaryGroup -SummaryGroup $counterPathGroup -Computer $GroupedComputerName -File $GroupedFileName)
    }

    if ($mergedSummaries.Count -ne 0)
    {
        return ($mergedSummaries | `
            Select-Object -Property Computer, CounterPath, OldestTimestamp, NewestTimestamp, Count, Average, Maximum, Minimum, Sum, NumOpsDiff, CounterType, File, Group, MemberCount | `
            Sort-Object -Property Computer, CounterPath, File)
    }
    else
    {
        return $mergedSummaries
    }  
}


### SCRIPT EXECUTION BEGINS HERE ###

if ($null -eq $ComputerName -or $ComputerName.Count -eq 0) #Do a collection against the local computer
{    
    $counterSummaries = Get-PerfmonSummaryStatsLocal -Path $Path -Counter $Counter -MaxSamples $MaxSamples -StartTime $StartTime -EndTime $EndTime -OnlyInspectFilesInDateRange $OnlyInspectFilesInDateRange -DataCollectorName $DataCollectorName -RestartRunningDataCollector $RestartRunningDataCollector -StartDataCollectorIfStopped $StartDataCollectorIfStopped -Verbose
}
else #Do remote collections
{
    $counterSummaries = Get-PerfmonSummaryStatsRemote -Path $Path -ComputerName $ComputerName -Counter $Counter -MaxSamples $MaxSamples -StartTime $StartTime -EndTime $EndTime -OnlyInspectFilesInDateRange $OnlyInspectFilesInDateRange -DataCollectorName $DataCollectorName -RestartRunningDataCollector $RestartRunningDataCollector -StartDataCollectorIfStopped $StartDataCollectorIfStopped -Verbose
}

if ($counterSummaries.Count -gt 0)
{
    $mergedSummaries = Get-SummariesMerged -CounterSummaries $counterSummaries
    return $mergedSummaries
}
else
{
    return
}
