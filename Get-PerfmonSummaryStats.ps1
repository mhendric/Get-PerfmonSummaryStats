<#PSScriptInfo

.VERSION 1.0.2

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
 Optional list of one or more counters to inspect within the discovered .blg files. If nothing is specified, all counters within the
 log will be inspected. Wildcards are permitted.

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
 >Performs a remote log analysis in path 'C:\PerfLogs' and inspects all counters in all logs found.
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
[System.Collections.Generic.List[System.Object]]. Get-PerfmonSummaryStats.ps1 returns a List of Object's containing Perfmon Summary Stats.
#>

[CmdletBinding()]
[OutputType([System.Collections.Generic.List[System.Object]])]
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
    [OutputType([System.Collections.Generic.List[System.Object]])]
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

    [System.Collections.Generic.List[System.Object]]$counterSummaries = New-Object System.Collections.Generic.List[System.Object]

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
                $files = Get-ChildItem -Path $Path -Recurse | Where-Object {$_.GetType().Name -like "FileInfo" -and $_.CreationTime -ge $EndTime}
                
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

    Write-Verbose "$([DateTime]::Now): Getting Perfmon files in specified paths."

    #Now process the perfmon files
    $files = Get-ChildItem -Path $Path -Recurse -Include "*.blg" | Where-Object {$_.GetType().Name -like "FileInfo"} | Sort-Object CreationTime
    $excludeFiles = $files | Where-Object {!(($_.CreationTime -ge $StartTime -and $_.CreationTime -le $EndTime) -or ($_.LastWriteTime -ge $StartTime -and $_.LastWriteTime -le $EndTime) -or ($_.LastAccessTime -ge $StartTime -and $_.LastAccessTime -le $EndTime))}        

    if ($OnlyInspectFilesInDateRange -and $files.Count -gt 0 -and ($files.Count - $excludeFiles.Count) -eq 0)
    {
        Write-Warning "$([DateTime]::Now): Excluding all $($files.Count) discovered files due to being out of date range."
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

            Write-Verbose "$([DateTime]::Now): Importing and grouping counters from file. File Size: $($file.Length / 1024 / 1024)MB. File Name: $($file.FullName)."

            [Object[]]$countersGrouped = $null
            [Object[]]$countersGrouped = (Import-Counter @importParams).CounterSamples | Group-Object -Property Path

            if ($countersGrouped.Count -gt 0)
            {
                Write-Verbose "$([DateTime]::Now): Measuring $($countersGrouped.Count) unique counter paths"

                foreach ($counterGroup in $countersGrouped)
                {
                    $measured = $counterGroup.Group | Measure-Object -Property CookedValue -Sum -Maximum -Minimum

                    $summary = New-Object PSObject -Property `
                                                        @{
                                                            Computer        = $counterGroup.Name.Substring(2, $counterGroup.Name.IndexOf('\',2) - 2).ToLower()
                                                            CounterPath     = $counterGroup.Name.Substring($counterGroup.Name.IndexOf('\', 2)).ToLower()
                                                            Average         = [Decimal]0
                                                            Maximum         = $measured.Maximum
                                                            Minimum         = $measured.Minimum
                                                            Sum             = $measured.Sum
                                                            Count           = $measured.Count
                                                            OldestTimestamp = $counterGroup.Group[0].Timestamp
                                                            NewestTimestamp = $counterGroup.Group[-1].Timestamp
                                                            NumTicksDiff    = 0
                                                            Frequency       = 0
                                                            NumOpsDiff      = 0
                                                            CounterType     = $counterGroup.Group[0].CounterType
                                                            File            = $file.FullName
                                                            MemberCount     = 1
                                                        }

                    #Average calculation for Average counters taken from these references:
                        #https://msdn.microsoft.com/en-us/library/ms804010.aspx
                        #https://blogs.msdn.microsoft.com/ntdebugging/2013/09/30/performance-monitor-averages-the-right-way-and-the-wrong-way/

                    if ($summary.CounterType -like "AverageTimer*")
                    {
                        $summary.NumTicksDiff = $counterGroup.Group[-1].RawValue - $counterGroup.Group[0].RawValue
                        $summary.Frequency = $counterGroup.Group[-1].TimeBase
                        $summary.NumOpsDiff = $counterGroup.Group[-1].SecondValue - $counterGroup.Group[0].SecondValue

                        if ($summary.Frequency -ne 0 -and $summary.NumOpsDiff -ne 0)
                        {
                            [Decimal]$summary.Average = ($summary.NumTicksDiff / $summary.Frequency) / $summary.NumOpsDiff
                        }                        
                    }
                    elseif ($measured.Count -ne 0)
                    {
                        [Decimal]$summary.Average = $measured.Sum / $measured.Count
                    }

                    $counterSummaries.Add($summary)
                }
            }
            else
            {
                Write-Verbose "$([DateTime]::Now): Found 0 matching counters in file $($file.FullName)"
            }
        }
    }

    Write-Verbose "$([DateTime]::Now): Finished reading logs and getting initial counter summaries."

    return $counterSummaries   
}

#Sends a performance counter analysis job to one or more computers
function Get-PerfmonSummaryStatsRemote
{
    [CmdletBinding()]
    [OutputType([System.Collections.Generic.List[System.Object]])]
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

    [System.Collections.Generic.List[System.Object]]$allCounterSummaries = Receive-Job $jobs

    return $allCounterSummaries
}

#Merges measurements within a related measurement group
function Merge-SummaryGroup
{
    [CmdletBinding()]
    [OutputType([System.Object])]
    param
    (
        [String]
        $GroupName,

        [Object[]]
        $Group,

        [String]
        $Computer,

        [String]
        $File
    )        

    if ($Group.Count -gt 0)
    {
        $mergedSummary = New-Object PSObject -Property @{
                                                Computer        = $Computer
                                                CounterPath     = $Group[0].CounterPath                                                
                                                Average         = [Decimal]0
                                                Maximum         = $Group[0].Maximum
                                                Minimum         = $Group[0].Minimum
                                                Sum             = [Decimal]0
                                                Count           = 0
                                                OldestTimestamp = $Group[0].OldestTimestamp
                                                NewestTimestamp = $Group[0].NewestTimestamp
                                                Frequency       = $Group[0].Frequency
                                                NumTicksDiff    = 0
                                                NumOpsDiff      = 0
                                                CounterType     = $Group[0].CounterType
                                                File            = $File
                                                MemberCount     = $Group.Count
                                            }

        foreach ($measurement in $Group)
        {
            $mergedSummary.Count += $measurement.Count
            $mergedSummary.Sum += $measurement.Sum
            $mergedSummary.NumTicksDiff += $measurement.NumTicksDiff
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
        if ($Group[0].CounterType -like "Average*")
        {
            if ($mergedSummary.NumOpsDiff -ne 0)
            {
                foreach ($measurement in $Group)
                {
                    $mergedSummary.Average += (($measurement.NumOpsDiff / $mergedSummary.NumOpsDiff) * $measurement.Average)
                }
            }
        }
        elseif ($mergedSummary.Count -ne 0)
        {
            $mergedSummary.Average = $mergedSummary.Sum / $mergedSummary.Count
        }

        return $mergedSummary
    }

    return $null
}

#Merges the given counter summaries into related groups of:
#Computer, CounterPath, and File (per file groupings)
#Computer and CounterPath (per computer groupings)
#CounterPath (all computer groupings)
function Get-SummariesMerged
{
    [CmdletBinding()]
    [OutputType([System.Collections.Generic.List[System.Object]])]
    param
    (
        [System.Collections.Generic.List[System.Object]]
        $PerFileCounterSummaries,

        [String]
        $GroupedComputerName,

        [String]
        $GroupedFileName
    )

    Write-Verbose "$([DateTime]::Now): Merging measurements on $($PerFileCounterSummaries.Count) per file counter path summaries."

    $mergedSummaries = New-Object PSObject -Property @{
                                                FileSummaries = $PerFileCounterSummaries
                                                ComputerSummaries = (New-Object System.Collections.Generic.List[System.Object])
                                                CounterSummaries = (New-Object System.Collections.Generic.List[System.Object])
                                            }

    $groupedByCounterPath = $PerFileCounterSummaries | Group-Object -Property CounterPath

    Write-Verbose "$([DateTime]::Now): Merging measurements from $($groupedByCounterPath.Count) unique counter instances."

    #Merge by CounterPath alone
    foreach ($counterPathGroup in $groupedByCounterPath)
    {
        $mergedCounterPathSummary = $null
        $mergedCounterPathSummary = Merge-SummaryGroup -GroupName $counterPathGroup.Name -Group $counterPathGroup.Group -Computer $GroupedComputerName -File $GroupedFileName

        if ($null -ne $mergedCounterPathSummary)
        {
            $mergedSummaries.CounterSummaries.Add($mergedCounterPathSummary)

            #Merge by CounterPath and Computer
            foreach ($computerAndPathGroup in ($counterPathGroup.Group | Group-Object -Property Computer))
            {
                $mergedComputerAndPathSummary = $null
                $mergedComputerAndPathSummary = Merge-SummaryGroup -GroupName $computerAndPathGroup.Name -Group $computerAndPathGroup.Group -Computer $computerAndPathGroup.Group[0].Computer -File $GroupedFileName

                if ($null -ne $mergedComputerAndPathSummary)
                {
                    $mergedSummaries.ComputerSummaries.Add($mergedComputerAndPathSummary)
                }
            }
        }
    }

    Write-Verbose "$([DateTime]::Now): Sorting final results."

    $mergedSummaries.FileSummaries = $mergedSummaries.FileSummaries | Select-Object -Property Computer, CounterPath, File, Average, Maximum, Minimum, Sum, Count, OldestTimestamp, NewestTimestamp, Frequency, NumTicksDiff, NumOpsDiff, CounterType | Sort-Object CounterPath, Computer, File
    $mergedSummaries.ComputerSummaries = $mergedSummaries.ComputerSummaries | Select-Object -Property Computer, CounterPath, Average, Maximum, Minimum, Sum, Count, OldestTimestamp, NewestTimestamp, Frequency, NumTicksDiff, NumOpsDiff, CounterType, MemberCount | Sort-Object CounterPath, Computer
    $mergedSummaries.CounterSummaries = $mergedSummaries.CounterSummaries | Select-Object -Property CounterPath, Average, Maximum, Minimum, Sum, Count, OldestTimestamp, NewestTimestamp, Frequency, NumTicksDiff, NumOpsDiff, CounterType, MemberCount | Sort-Object CounterPath

    return $mergedSummaries | Select-Object -Property CounterSummaries, ComputerSummaries, FileSummaries
}


### SCRIPT EXECUTION BEGINS HERE ###

Write-Verbose "$([DateTime]::Now): Beginning script execution"

if ($null -eq $ComputerName -or $ComputerName.Count -eq 0) #Do a collection against the local computer
{    
    [System.Collections.Generic.List[System.Object]]$perFileCounterSummaries = Get-PerfmonSummaryStatsLocal -Path $Path -Counter $Counter -MaxSamples $MaxSamples -StartTime $StartTime -EndTime $EndTime -OnlyInspectFilesInDateRange $OnlyInspectFilesInDateRange -DataCollectorName $DataCollectorName -RestartRunningDataCollector $RestartRunningDataCollector -StartDataCollectorIfStopped $StartDataCollectorIfStopped -Verbose
}
else #Do remote collections
{
    [System.Collections.Generic.List[System.Object]]$perFileCounterSummaries = Get-PerfmonSummaryStatsRemote -Path $Path -ComputerName $ComputerName -Counter $Counter -MaxSamples $MaxSamples -StartTime $StartTime -EndTime $EndTime -OnlyInspectFilesInDateRange $OnlyInspectFilesInDateRange -DataCollectorName $DataCollectorName -RestartRunningDataCollector $RestartRunningDataCollector -StartDataCollectorIfStopped $StartDataCollectorIfStopped -Verbose
}

if ($perFileCounterSummaries.Count -gt 0)
{
    $mergedSummaries = Get-SummariesMerged -PerFileCounterSummaries $perFileCounterSummaries
}

Write-Verbose "$([DateTime]::Now): Finished script execution"

return $mergedSummaries