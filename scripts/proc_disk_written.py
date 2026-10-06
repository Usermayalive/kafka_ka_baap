#!/usr/bin/env python3
"""Bytes written to disk by a process (macOS proc_pid_rusage, RUSAGE_INFO_V2), in MB."""
import ctypes, sys

class RUsageInfoV2(ctypes.Structure):
    _fields_ = [("ri_uuid", ctypes.c_uint8 * 16)] + [(name, ctypes.c_uint64) for name in (
        "ri_user_time", "ri_system_time", "ri_pkg_idle_wkups", "ri_interrupt_wkups", "ri_pageins",
        "ri_wired_size", "ri_resident_size", "ri_phys_footprint", "ri_proc_start_abstime",
        "ri_proc_exit_abstime", "ri_child_user_time", "ri_child_system_time", "ri_child_pkg_idle_wkups",
        "ri_child_interrupt_wkups", "ri_child_pageins", "ri_child_elapsed_abstime",
        "ri_diskio_bytesread", "ri_diskio_byteswritten")]

libproc = ctypes.CDLL("/usr/lib/libproc.dylib")
info = RUsageInfoV2()
if libproc.proc_pid_rusage(int(sys.argv[1]), 2, ctypes.byref(info)) != 0:
    sys.exit("proc_pid_rusage failed")
print(f"{info.ri_diskio_byteswritten / (1024 * 1024):.1f}")
