---
title: "Debugging by Jattach"
hide_title: true
---
We have installed jattach in Docker image of datahub-gms, datahub-mae-consumer, datahub-mce-consumer
and datahub-frontend to facilitating troubleshooting in a production environment.

# Introduction to Jattach
Jattach is a utility to send commands to a JVM process via Dynamic Attach mechanism.

Supported commands:
- load            : load agent library
- properties      : print system properties
- agentProperties : print agent properties
- datadump        : show heap and thread summary
- threaddump      : dump all stack traces (like jstack)
- dumpheap        : dump heap (like jmap)
- inspectheap     : heap histogram (like jmap -histo)
- setflag         : modify manageable VM flag
- printflag       : print VM flag
- jcmd            : execute jcmd command

# Use examples

Jattach is a All-in-one **jmap + jstack + jcmd + jinfo** functionality in a single tiny program.
The scenarios where these commands were previously used can now be replaced with jattach.

## Example 1: Dump heap
The jattach dumpheap command is typically used in the following scenarios:

- Memory leak analysis: When an application experiences memory leaks or abnormal memory growth, the jattach dumpheap
command is used to generate a heap dump file for further analysis and identification of the memory leak causes.

- Memory analysis: By generating a heap dump file, various memory analysis tools such as Eclipse Memory Analyzer and
VisualVM can be utilized to analyze objects, reference relationships, and memory usage within the heap. This helps 
identify potential performance issues and optimization opportunities.

- Troubleshooting: When encountering frequent Full GCs, memory out-of-memory errors, or other memory-related issues, 
generating a heap dump file provides additional information for troubleshooting and debugging purposes.

- Performance optimization: By analyzing the heap dump file, it becomes possible to identify which objects in the 
application consume a significant amount of memory. This information can be used for targeted performance optimizations,
such as reducing object creation or optimizing cache usage.

It's important to note that generating a heap dump file can have an impact on the performance of the application. 
Therefore, caution should be exercised when using it in a production environment, ensuring sufficient resources and
permissions are available for the operation.

The command is as follows:
```bash
jattach <pid> dumpheap /tmp/heap.bin
```

## Example 2: Dump thread
The jattach threaddump command is typically used in the following scenarios:

- Deadlock analysis: When an application experiences deadlocks or thread contention issues, the jattach threaddump 
command is used to generate a thread dump file. It provides the current thread's stack trace information, helping to
identify the code paths and lock contention causing the deadlock.

- Thread troubleshooting: When threads in an application encounter exceptions, long blocking times, excessive CPU usage,
or other issues, generating a thread dump file provides detailed information about each thread's state, stack trace, and
resource usage. This helps with troubleshooting and analysis.

- Performance analysis: By generating a thread dump file, it becomes possible to understand the thread activity, call
paths, and resource contention within the application. This aids in identifying potential performance bottlenecks and
concurrency issues.

- Monitoring and diagnostics: Regularly generating thread dump files can be used to monitor the health of an application
and provide historical data for subsequent diagnostics. This helps gain a better understanding of the application's 
behavior and performance.

It's important to note that generating a thread dump file may impact the performance of the application. Therefore,
caution should be exercised when using it in a production environment, ensuring sufficient resources and permissions
are available for the operation.

The command is as follows:
```bash
jattach <pid> threaddump -l > /tmp/jstack.out
```