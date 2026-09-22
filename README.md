# io-thread-controller

`io-thread-controller` discovers VMs through pluggable backends, refreshes
their I/O-worker state, asks the selected scaling engine for decisions, and
keeps the live inventory synchronized with backend discovery.

## Core control loop

Backends and scaling engines are independent extension points around the
controller-owned inventory:

```text
                         +-------------------------+
                         | Controller              |
                         | owns inventory + engine |
                         | refreshes and actuates  |
                         +-----------+-------------+
                                     |
                  +------------------+------------------+
                  |                                     |
                  v                                     v
+--------------------------------+     +--------------------------------+
| Backend        (e.g. QEMU)     |     | Scaling engine                 |
| discovers zero or more VMs     |     | evaluates the refreshed fleet |
| creates one Instance per VM    |     | returns one scaling plan       |
| supplies per-VM InstanceClient |     | observes applied outcomes      |
+-----------------+--------------+     +--------------------------------+
                  |
                  v
    +---------------------------+
    | Instance data record      |
    | one VM + InstanceClient   |
    | (operations for that VM)  |
    +---------------------------+
```

Scaling decisions pass through VM ownership, controller thread-count, vCPU,
and host-CPU guards before the controller changes the backend pool. One atomic
`vm-ownership.json` registry below `/run/io-thread-controller` preserves
managed and unmanaged classifications across daemon restarts.

```text
                          startup
                             |
                             v
                  load config + select engine
                             |
                             v
+----------------> discover backend VMs <------------------+
|                            |                             |
|                            v                             |
|             +------ reconcile live inventory <-----+     |
|             |                                      |     |
|             v                                      |     |
|        wait for event                              |     |
|       /              \                             |     |
| poll timer       backend/inotify event             |     |
|     |                   |                          |     |
|     v                   +--------------------------+     |
| refresh every VM concurrently                            |
|     |                                                    |
|     +-- refresh failed --> remove VM --> rediscover------+
|     |                                    on next event   |
|     v                                                    |
| engine evaluates the refreshed fleet                     |
|     |                                                    |
|     v                                                    |
+--- Hold                                                  |
|     |                                                    |
+-- Scale --> guards reject -------------------------------+
                |                                          |
                v                                          |
        set backend thread count                           |
                |                                          |
      +---------+---------+                                |
      |                   |                                |
    error              success                             |
      |                   |                                |
      v                   v                                |
 log failure       update live count                       |
      |                   |                                |
      +---------+---------+                                |
                |                                          |
                +------------------------------------------+
```

## Scale-up state machine

```text
refreshed
   |
   +-- below threshold / at controller maximum ----------> hold
   |
   +-- saturated --> propose current + 1
                         |
                         +-- backend ownership rejects -> hold
                         |
                         +-- host CPU guard -------------> hold
                         |
                         +-- target outside bounds ------> hold
                         |
                         +-- target > vCPUs -------------> hold
                         |
                         +-- backend error -------------> report failure
                         |
                         +-- backend accepts -----------> report success
```

## Controller architecture

An `Instance` is a backend-neutral data record for one VM, not a backend trait.
It stores VM identity, latest metrics, process-local override state, and an
`InstanceClient` trait object that performs operations on exactly that VM.

A `Backend` is the fleet-level adapter for a whole class of VMs. It owns
backend-wide configuration and discovers zero or more `Instance` values. The
two abstractions are separate because discovery has fleet scope, while metric
queries and thread-count changes need an independently stateful handle for each
VM. This keeps the controller independent of libvirt, QMP, control sockets, and
other transport details.

After a successful action, the engine waits
`scale_validation_sample_polls` complete samples. A scale-up is reverted unless
IOPS gained at least `scale_up_min_gain_percent`; a scale-down is reverted when
IOPS lost more than `scale_down_revert_drop_percent`. Setting either percentage
to zero disables that direction's validation.

## D-Bus

The daemon owns:

- bus name: `com.nutanix.io_thread_controller1`
- object path: `/com/nutanix/io_thread_controller1`
- interface: `com.nutanix.io_thread_controller1`

Debug builds expose `SetThreadCount(vm, threads, sticky)` to change a tracked
instance's thread count. Setting `sticky=true` suppresses automatic scaling for
that instance until a later call clears it. This debug-only override is kept in
memory and is lost when the daemon restarts.

```sh
busctl --system call \
  com.nutanix.io_thread_controller1 \
  /com/nutanix/io_thread_controller1 \
  com.nutanix.io_thread_controller1 \
  SetThreadCount sub <vm> 4 true
```

Release builds do not expose this method. The shipped D-Bus policy restricts
the debug method to root.
