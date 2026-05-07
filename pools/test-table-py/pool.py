import signal
import sys
import select
import os # required for setting path to morloc dependencies
import time
import copy
import array
import struct
import socket as _socket
from collections import OrderedDict
from multiprocessing import Process, Value, RawValue
import ctypes
import functools


# Global variables for clean signal handling
daemon = None
workers = []
global_state = dict()
_shutdown_wakeup_fd = -1

# AUTO include sources start
sys.path = [os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..")), os.path.expanduser("."), os.path.expanduser("/home/dev/.local/share/morloc/opt"), os.path.expanduser("/home/dev/.local/share/morloc/src/morloc/plane")] + sys.path
import importlib
import pymorloc as morloc
default_root_py_core = importlib.import_module("default.root-py.core")
default_table_test_table_test = importlib.import_module("default.table.test.table-test")
default_table_py_table = importlib.import_module("default.table-py.table")
mlc_schema_table = ["<bool>b"]
# AUTO include sources end

# Dynamic worker spawning: monkey-patch foreign_call to track busy workers.
# Workers atomically increment busy_count before a foreign_call and decrement
# after. When busy_count reaches total_workers, a byte is written to a wake-up
# pipe to tell the main process to spawn a new worker.
_original_foreign_call = morloc.foreign_call
_busy_ref = None
_total_ref = None
_wakeup_fd = -1

def _init_worker_tracking(busy, total, wakeup_fd):
    global _busy_ref, _total_ref, _wakeup_fd
    _busy_ref = busy
    _total_ref = total
    _wakeup_fd = wakeup_fd
    morloc.foreign_call = _tracked_foreign_call

def _tracked_foreign_call(*args):
    prev = _busy_ref.value
    _busy_ref.value = prev + 1
    if prev + 1 >= _total_ref.value and _wakeup_fd >= 0:
        try:
            os.write(_wakeup_fd, b'!')
        except OSError:
            pass
    try:
        return _original_foreign_call(*args)
    finally:
        _busy_ref.value -= 1

# AUTO include manifolds start
def m2765(n39):
    try:
        n40 = default_table_py_table.morloc_sliceRows(0, 2, n39)
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m2765):\n{e!s}")
    return(n40)

def m2770(n35):
    try:
        n41 = default_table_py_table.morloc_asCol("x", n35)
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m2770):\n{e!s}")
    return(n41)

def m2783(n39):
    try:
        n42 = default_table_py_table.morloc_nrow(n39)
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m2783):\n{e!s}")
    return(n42)

def m2787(n39):
    try:
        n44 = default_table_py_table.morloc_nrow(n39)
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m2787):\n{e!s}")
    return(n44)

def m2779(n39):
    try:
        n43 = (m2783(n39) - 2)
        n45 = default_table_py_table.morloc_sliceRows(n43, m2787(n39), n39)
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m2779):\n{e!s}")
    return(n45)

def m2791(n36):
    try:
        n46 = default_table_py_table.morloc_asCol("x", n36)
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m2791):\n{e!s}")
    return(n46)

def m2807(n39):
    try:
        n47 = default_table_py_table.morloc_nrow(n39)
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m2807):\n{e!s}")
    return(n47)

def m2798(n39):
    try:
        n48 = (m2807(n39) - 1)
        n49 = default_root_py_core.morloc_range(0, n48)
        n50 = default_root_py_core.morloc_reverse(n49)
        n51 = default_table_py_table.morloc_pickRows(n50, n39)
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m2798):\n{e!s}")
    return(n51)

def m2812(n37):
    try:
        n52 = default_table_py_table.morloc_asCol("x", n37)
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m2812):\n{e!s}")
    return(n52)

def m2877():
    try:
        n55 = [0, 1, 2]
        n56 = default_table_py_table.morloc_asCol("x", n55)
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m2877):\n{e!s}")
    return(n56)

def m2880():
    try:
        n57 = [0, 1, 2]
        n58 = default_table_py_table.morloc_asCol("x", n57)
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m2880):\n{e!s}")
    return(n58)

def m2875():
    try:
        n59 = default_table_py_table.morloc_rbind(m2877(), m2880())
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m2875):\n{e!s}")
    return(n59)

def m2873():
    try:
        n60 = default_table_py_table.morloc_nrow(m2875())
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m2873):\n{e!s}")
    return(n60)

def m2891():
    try:
        n61 = [0, 1, 2]
        n62 = default_table_py_table.morloc_asCol("x", n61)
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m2891):\n{e!s}")
    return(n62)

def m2894():
    try:
        n63 = [0, 1, 2]
        n64 = default_table_py_table.morloc_asCol("x", n63)
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m2894):\n{e!s}")
    return(n64)

def m2889():
    try:
        n65 = default_table_py_table.morloc_rbind(m2891(), m2894())
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m2889):\n{e!s}")
    return(n65)

def m2907():
    try:
        n66 = [0, 1, 2]
        n67 = default_table_py_table.morloc_asCol("x", n66)
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m2907):\n{e!s}")
    return(n67)

def m2910():
    try:
        n68 = ["0", "1", "2"]
        n69 = default_table_py_table.morloc_asCol("y", n68)
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m2910):\n{e!s}")
    return(n69)

def m2905():
    try:
        n70 = default_table_py_table.morloc_cbind(m2907(), m2910())
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m2905):\n{e!s}")
    return(n70)

def m2903():
    try:
        n71 = default_table_py_table.morloc_ncol(m2905())
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m2903):\n{e!s}")
    return(n71)

def m2919():
    try:
        n72 = [0, 1, 2]
        n73 = default_table_py_table.morloc_asCol("x", n72)
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m2919):\n{e!s}")
    return(n73)

def m2922():
    try:
        n74 = ["0", "1", "2"]
        n75 = default_table_py_table.morloc_asCol("y", n74)
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m2922):\n{e!s}")
    return(n75)

def m2917():
    try:
        n76 = default_table_py_table.morloc_cbind(m2919(), m2922())
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m2917):\n{e!s}")
    return(n76)

def m3023():
    try:
        n78 = [0, 1, 2]
        n79 = default_table_py_table.morloc_asCol("x", n78)
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3023):\n{e!s}")
    return(n79)

def m2925():
    try:
        n77 = ["0", "1", "2"]
        n80 = default_table_py_table.morloc_setCol("y", n77, m3023())
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m2925):\n{e!s}")
    return(n80)

def m3063():
    try:
        n81 = [0, 1, 2]
        n82 = default_table_py_table.morloc_asCol("x", n81)
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3063):\n{e!s}")
    return(n82)

def m3059():
    try:
        n83 = default_table_py_table.morloc_renameCol("x", "a", m3063())
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3059):\n{e!s}")
    return(n83)

def m3066():
    try:
        n84 = [0, 1, 2]
        n85 = default_table_py_table.morloc_asCol("a", n84)
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3066):\n{e!s}")
    return(n85)

def m3080():
    try:
        n86 = [0, 1, 2]
        n87 = default_table_py_table.morloc_asCol("x", n86)
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3080):\n{e!s}")
    return(n87)

def m3076():
    try:
        n88 = default_table_py_table.morloc_renameCol("x", "a", m3080())
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3076):\n{e!s}")
    return(n88)

def m3073():
    try:
        n89 = default_table_py_table.morloc_getCol("a", m3076())
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3073):\n{e!s}")
    return(n89)

def m3174():
    try:
        n94 = [0, 1, 2]
        n95 = default_table_py_table.morloc_asCol("x", n94)
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3174):\n{e!s}")
    return(n95)

def m3170():
    try:
        n93 = ["0", "1", "2"]
        n96 = default_table_py_table.morloc_setCol("y", n93, m3174())
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3170):\n{e!s}")
    return(n96)

def m3144():
    try:
        n92 = [0.0, 0.5, 1.0]
        n97 = default_table_py_table.morloc_setCol("z", n92, m3170())
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3144):\n{e!s}")
    return(n97)

def m3140():
    try:
        n91 = ["x"]
        n98 = default_table_py_table.morloc_selectColsDyn(n91, m3144())
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3140):\n{e!s}")
    return(n98)

def m3145():
    try:
        n99 = [0, 1, 2]
        n100 = default_table_py_table.morloc_asCol("x", n99)
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3145):\n{e!s}")
    return(n100)

def m3207():
    try:
        n104 = [0, 1, 2]
        n105 = default_table_py_table.morloc_asCol("x", n104)
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3207):\n{e!s}")
    return(n105)

def m3203():
    try:
        n103 = ["0", "1", "2"]
        n106 = default_table_py_table.morloc_setCol("y", n103, m3207())
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3203):\n{e!s}")
    return(n106)

def m3158():
    try:
        n102 = [0.0, 0.5, 1.0]
        n107 = default_table_py_table.morloc_setCol("z", n102, m3203())
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3158):\n{e!s}")
    return(n107)

def m3153():
    try:
        n101 = ["y", "z"]
        n108 = default_table_py_table.morloc_selectColsDyn(n101, m3158())
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3153):\n{e!s}")
    return(n108)

def m3151():
    try:
        n109 = default_table_py_table.morloc_ncol(m3153())
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3151):\n{e!s}")
    return(n109)

def m3295():
    try:
        n112 = [0, 1, 2]
        n113 = default_table_py_table.morloc_asCol("x", n112)
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3295):\n{e!s}")
    return(n113)

def m3251():
    try:
        n111 = ["0", "1", "2"]
        n114 = default_table_py_table.morloc_setCol("y", n111, m3295())
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3251):\n{e!s}")
    return(n114)

def m3247():
    try:
        n110 = ["x"]
        n115 = default_table_py_table.morloc_selectCols(n110, m3251())
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3247):\n{e!s}")
    return(n115)

def m3254():
    try:
        n116 = [0, 1, 2]
        n117 = default_table_py_table.morloc_asCol("x", n116)
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3254):\n{e!s}")
    return(n117)

def m3327():
    try:
        n120 = [0, 1, 2]
        n121 = default_table_py_table.morloc_asCol("x", n120)
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3327):\n{e!s}")
    return(n121)

def m3266():
    try:
        n119 = ["0", "1", "2"]
        n122 = default_table_py_table.morloc_setCol("y", n119, m3327())
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3266):\n{e!s}")
    return(n122)

def m3262():
    try:
        n118 = ["y"]
        n123 = default_table_py_table.morloc_selectCols(n118, m3266())
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3262):\n{e!s}")
    return(n123)

def m3269():
    try:
        n124 = ["0", "1", "2"]
        n125 = default_table_py_table.morloc_asCol("y", n124)
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3269):\n{e!s}")
    return(n125)

def m3356():
    try:
        n129 = [0, 1, 2]
        n130 = default_table_py_table.morloc_asCol("x", n129)
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3356):\n{e!s}")
    return(n130)

def m3352():
    try:
        n128 = ["0", "1", "2"]
        n131 = default_table_py_table.morloc_setCol("y", n128, m3356())
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3352):\n{e!s}")
    return(n131)

def m3280():
    try:
        n127 = [0.0, 0.5, 1.0]
        n132 = default_table_py_table.morloc_setCol("z", n127, m3352())
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3280):\n{e!s}")
    return(n132)

def m3275():
    try:
        n126 = ["x", "y"]
        n133 = default_table_py_table.morloc_selectCols(n126, m3280())
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3275):\n{e!s}")
    return(n133)

def m3377():
    try:
        n135 = [0, 1, 2]
        n136 = default_table_py_table.morloc_asCol("x", n135)
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3377):\n{e!s}")
    return(n136)

def m3281():
    try:
        n134 = ["0", "1", "2"]
        n137 = default_table_py_table.morloc_setCol("y", n134, m3377())
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3281):\n{e!s}")
    return(n137)

def m3462():
    try:
        n141 = [0, 1, 2]
        n142 = default_table_py_table.morloc_asCol("x", n141)
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3462):\n{e!s}")
    return(n142)

def m3458():
    try:
        n140 = ["0", "1", "2"]
        n143 = default_table_py_table.morloc_setCol("y", n140, m3462())
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3458):\n{e!s}")
    return(n143)

def m3417():
    try:
        n139 = [0.0, 0.5, 1.0]
        n144 = default_table_py_table.morloc_setCol("z", n139, m3458())
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3417):\n{e!s}")
    return(n144)

def m3413():
    try:
        n138 = ["z"]
        n145 = default_table_py_table.morloc_dropCols(n138, m3417())
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3413):\n{e!s}")
    return(n145)

def m3483():
    try:
        n147 = [0, 1, 2]
        n148 = default_table_py_table.morloc_asCol("x", n147)
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3483):\n{e!s}")
    return(n148)

def m3418():
    try:
        n146 = ["0", "1", "2"]
        n149 = default_table_py_table.morloc_setCol("y", n146, m3483())
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3418):\n{e!s}")
    return(n149)

def m3509():
    try:
        n153 = [0, 1, 2]
        n154 = default_table_py_table.morloc_asCol("x", n153)
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3509):\n{e!s}")
    return(n154)

def m3505():
    try:
        n152 = ["0", "1", "2"]
        n155 = default_table_py_table.morloc_setCol("y", n152, m3509())
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3505):\n{e!s}")
    return(n155)

def m3431():
    try:
        n151 = [0.0, 0.5, 1.0]
        n156 = default_table_py_table.morloc_setCol("z", n151, m3505())
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3431):\n{e!s}")
    return(n156)

def m3426():
    try:
        n150 = ["x", "z"]
        n157 = default_table_py_table.morloc_dropCols(n150, m3431())
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3426):\n{e!s}")
    return(n157)

def m3432():
    try:
        n158 = ["0", "1", "2"]
        n159 = default_table_py_table.morloc_asCol("y", n158)
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3432):\n{e!s}")
    return(n159)

def m3442():
    try:
        n161 = [0, 1, 2]
        n162 = default_table_py_table.morloc_asCol("x", n161)
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3442):\n{e!s}")
    return(n162)

def m3438():
    try:
        n160 = ["absent"]
        n163 = default_table_py_table.morloc_dropCols(n160, m3442())
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3438):\n{e!s}")
    return(n163)

def m3445():
    try:
        n164 = [0, 1, 2]
        n165 = default_table_py_table.morloc_asCol("x", n164)
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3445):\n{e!s}")
    return(n165)

def m3629():
    try:
        n169 = [0, 1, 2]
        n170 = default_table_py_table.morloc_asCol("x", n169)
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3629):\n{e!s}")
    return(n170)

def m3625():
    try:
        n168 = ["0", "1", "2"]
        n171 = default_table_py_table.morloc_setCol("y", n168, m3629())
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3625):\n{e!s}")
    return(n171)

def m3580():
    try:
        n167 = [0.0, 0.5, 1.0]
        n172 = default_table_py_table.morloc_setCol("z", n167, m3625())
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3580):\n{e!s}")
    return(n172)

def m3650():
    try:
        n175 = [0, 1, 2]
        n176 = default_table_py_table.morloc_asCol("x", n175)
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3650):\n{e!s}")
    return(n176)

def m3585():
    try:
        n174 = ["0", "1", "2"]
        n177 = default_table_py_table.morloc_setCol("y", n174, m3650())
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3585):\n{e!s}")
    return(n177)

def m3597():
    try:
        n180 = [0, 1, 2]
        n181 = default_table_py_table.morloc_asCol("x", n180)
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3597):\n{e!s}")
    return(n181)

def m3593():
    try:
        n179 = ["0", "1", "2"]
        n182 = default_table_py_table.morloc_setCol("y", n179, m3597())
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3593):\n{e!s}")
    return(n182)

def m3686():
    try:
        n184 = [0, 1, 2]
        n185 = default_table_py_table.morloc_asCol("x", n184)
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3686):\n{e!s}")
    return(n185)

def m3600():
    try:
        n183 = ["0", "1", "2"]
        n186 = default_table_py_table.morloc_setCol("y", n183, m3686())
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3600):\n{e!s}")
    return(n186)

def m3713():
    try:
        n189 = [0, 1, 2]
        n190 = default_table_py_table.morloc_asCol("x", n189)
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3713):\n{e!s}")
    return(n190)

def m3612():
    try:
        n188 = ["0", "1", "2"]
        n191 = default_table_py_table.morloc_setCol("y", n188, m3713())
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3612):\n{e!s}")
    return(n191)

def m3608():
    try:
        n187 = [0.0, 0.5, 1.0]
        n192 = default_table_py_table.morloc_setCol("z", n187, m3612())
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3608):\n{e!s}")
    return(n192)

def m3733():
    try:
        n195 = [0, 1, 2]
        n196 = default_table_py_table.morloc_asCol("x", n195)
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3733):\n{e!s}")
    return(n196)

def m3729():
    try:
        n194 = ["0", "1", "2"]
        n197 = default_table_py_table.morloc_setCol("y", n194, m3733())
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3729):\n{e!s}")
    return(n197)

def m3615():
    try:
        n193 = [0.0, 0.5, 1.0]
        n198 = default_table_py_table.morloc_setCol("z", n193, m3729())
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3615):\n{e!s}")
    return(n198)

def m3776():
    try:
        n199 = [0, 1, 2]
        n200 = default_table_py_table.morloc_asCol("x", n199)
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3776):\n{e!s}")
    return(n200)

def m3773():
    try:
        n201 = default_table_py_table.morloc_getCol("x", m3776())
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3773):\n{e!s}")
    return(n201)

def m3831():
    try:
        n205 = [0, 1, 2]
        n206 = default_table_py_table.morloc_asCol("x", n205)
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3831):\n{e!s}")
    return(n206)

def m3827():
    try:
        n204 = ["0", "1", "2"]
        n207 = default_table_py_table.morloc_setCol("y", n204, m3831())
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3827):\n{e!s}")
    return(n207)

def m3788():
    try:
        n203 = [0.0, 0.5, 1.0]
        n208 = default_table_py_table.morloc_setCol("z", n203, m3827())
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3788):\n{e!s}")
    return(n208)

def m3785():
    try:
        n209 = default_table_py_table.morloc_getCol("y", m3788())
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3785):\n{e!s}")
    return(n209)

def m3859():
    try:
        n213 = [0, 1, 2]
        n214 = default_table_py_table.morloc_asCol("x", n213)
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3859):\n{e!s}")
    return(n214)

def m3855():
    try:
        n212 = ["0", "1", "2"]
        n215 = default_table_py_table.morloc_setCol("y", n212, m3859())
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3855):\n{e!s}")
    return(n215)

def m3796():
    try:
        n211 = [0.0, 0.5, 1.0]
        n216 = default_table_py_table.morloc_setCol("z", n211, m3855())
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3796):\n{e!s}")
    return(n216)

def m3793():
    try:
        n217 = default_table_py_table.morloc_getCol("z", m3796())
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3793):\n{e!s}")
    return(n217)

def m3914():
    try:
        n222 = [0, 1, 2]
        n223 = default_table_py_table.morloc_asCol("x", n222)
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3914):\n{e!s}")
    return(n223)

def m3908():
    try:
        n220 = ("x", True)
        n221 = [n220]
        n224 = default_table_py_table.morloc_sortRows(n221, m3914())
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3908):\n{e!s}")
    return(n224)

def m3917():
    try:
        n225 = [0, 1, 2]
        n226 = default_table_py_table.morloc_asCol("x", n225)
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3917):\n{e!s}")
    return(n226)

def m3931(n219):
    try:
        n229 = default_table_py_table.morloc_asCol("x", n219)
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3931):\n{e!s}")
    return(n229)

def m3925(n219):
    try:
        n227 = ("x", True)
        n228 = [n227]
        n230 = default_table_py_table.morloc_sortRows(n228, m3931(n219))
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3925):\n{e!s}")
    return(n230)

def m3935():
    try:
        n231 = [0, 1, 2]
        n232 = default_table_py_table.morloc_asCol("x", n231)
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3935):\n{e!s}")
    return(n232)

def m3949():
    try:
        n235 = [0, 1, 2]
        n236 = default_table_py_table.morloc_asCol("x", n235)
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3949):\n{e!s}")
    return(n236)

def m3943():
    try:
        n233 = ("x", False)
        n234 = [n233]
        n237 = default_table_py_table.morloc_sortRows(n234, m3949())
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3943):\n{e!s}")
    return(n237)

def m3952(n219):
    try:
        n238 = default_table_py_table.morloc_asCol("x", n219)
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3952):\n{e!s}")
    return(n238)

def m3962():
    try:
        n240 = [0, 1, 2]
        n241 = default_table_py_table.morloc_asCol("x", n240)
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3962):\n{e!s}")
    return(n241)

def m3959():
    try:
        n239 = []
        n242 = default_table_py_table.morloc_sortRows(n239, m3962())
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3959):\n{e!s}")
    return(n242)

def m3965():
    try:
        n243 = [0, 1, 2]
        n244 = default_table_py_table.morloc_asCol("x", n243)
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3965):\n{e!s}")
    return(n244)

def m4095():
    try:
        n252 = [0, 1, 2]
        n253 = default_table_py_table.morloc_asCol("x", n252)
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4095):\n{e!s}")
    return(n253)

def m4093():
    try:
        n254 = default_table_py_table.morloc_distinctRows(m4095())
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4093):\n{e!s}")
    return(n254)

def m4098():
    try:
        n255 = [0, 1, 2]
        n256 = default_table_py_table.morloc_asCol("x", n255)
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4098):\n{e!s}")
    return(n256)

def m4106(n246):
    try:
        n257 = default_table_py_table.morloc_distinctRows(n246)
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4106):\n{e!s}")
    return(n257)

def m4109(n251):
    try:
        n258 = default_table_py_table.morloc_asCol("x", n251)
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4109):\n{e!s}")
    return(n258)

def m4116(n248):
    try:
        n259 = default_table_py_table.morloc_distinctRows(n248)
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4116):\n{e!s}")
    return(n259)

def m4208():
    try:
        n267 = [0, 1, 2]
        n268 = default_table_py_table.morloc_asCol("x", n267)
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4208):\n{e!s}")
    return(n268)

def m4205(n260):
    try:
        n269 = default_table_py_table.morloc_pickRows(n260, m4208())
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4205):\n{e!s}")
    return(n269)

def m4211():
    try:
        n270 = [0, 1, 2]
        n271 = default_table_py_table.morloc_asCol("x", n270)
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4211):\n{e!s}")
    return(n271)

def m4222():
    try:
        n272 = [0, 1, 2]
        n273 = default_table_py_table.morloc_asCol("x", n272)
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4222):\n{e!s}")
    return(n273)

def m4219(n261):
    try:
        n274 = default_table_py_table.morloc_pickRows(n261, m4222())
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4219):\n{e!s}")
    return(n274)

def m4225(n264):
    try:
        n275 = default_table_py_table.morloc_asCol("x", n264)
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4225):\n{e!s}")
    return(n275)

def m4237():
    try:
        n276 = [0, 1, 2]
        n277 = default_table_py_table.morloc_asCol("x", n276)
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4237):\n{e!s}")
    return(n277)

def m4234(n262):
    try:
        n278 = default_table_py_table.morloc_pickRows(n262, m4237())
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4234):\n{e!s}")
    return(n278)

def m4240(n265):
    try:
        n279 = default_table_py_table.morloc_asCol("x", n265)
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4240):\n{e!s}")
    return(n279)

def m4250():
    try:
        n280 = [0, 1, 2]
        n281 = default_table_py_table.morloc_asCol("x", n280)
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4250):\n{e!s}")
    return(n281)

def m4247(n263):
    try:
        n282 = default_table_py_table.morloc_pickRows(n263, m4250())
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4247):\n{e!s}")
    return(n282)

def m4253(n266):
    try:
        n283 = default_table_py_table.morloc_asCol("x", n266)
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4253):\n{e!s}")
    return(n283)

def m4363():
    try:
        n288 = [0, 1, 2]
        n289 = default_table_py_table.morloc_asCol("x", n288)
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4363):\n{e!s}")
    return(n289)

def m4360(n284):
    try:
        n290 = default_table_py_table.morloc_filterRows(n284, m4363())
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4360):\n{e!s}")
    return(n290)

def m4366():
    try:
        n291 = [0, 1, 2]
        n292 = default_table_py_table.morloc_asCol("x", n291)
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4366):\n{e!s}")
    return(n292)

def m4379():
    try:
        n293 = [0, 1, 2]
        n294 = default_table_py_table.morloc_asCol("x", n293)
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4379):\n{e!s}")
    return(n294)

def m4376(n285):
    try:
        n295 = default_table_py_table.morloc_filterRows(n285, m4379())
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4376):\n{e!s}")
    return(n295)

def m4374(n285):
    try:
        n296 = default_table_py_table.morloc_nrow(m4376(n285))
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4374):\n{e!s}")
    return(n296)

def m4391():
    try:
        n297 = [0, 1, 2]
        n298 = default_table_py_table.morloc_asCol("x", n297)
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4391):\n{e!s}")
    return(n298)

def m4388(n286):
    try:
        n299 = default_table_py_table.morloc_filterRows(n286, m4391())
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4388):\n{e!s}")
    return(n299)

def m4394(n287):
    try:
        n300 = default_table_py_table.morloc_asCol("x", n287)
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4394):\n{e!s}")
    return(n300)

def m4406():
    try:
        n301 = [0, 1, 2]
        n302 = default_table_py_table.morloc_asCol("x", n301)
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4406):\n{e!s}")
    return(n302)

def m4403(n285):
    try:
        n303 = default_table_py_table.morloc_filterRows(n285, m4406())
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4403):\n{e!s}")
    return(n303)

def m4401(n285):
    try:
        n304 = default_table_py_table.morloc_names(m4403(n285))
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4401):\n{e!s}")
    return(n304)

def m4520(n313):
    try:
        n314 = default_table_py_table.morloc_sliceRows(0, 0, n313)
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4520):\n{e!s}")
    return(n314)

def m4518(n313):
    try:
        n315 = default_table_py_table.morloc_nrow(m4520(n313))
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4518):\n{e!s}")
    return(n315)

def m4533(n313):
    try:
        n316 = default_table_py_table.morloc_sliceRows(0, 0, n313)
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4533):\n{e!s}")
    return(n316)

def m4531(n313):
    try:
        n317 = default_table_py_table.morloc_names(m4533(n313))
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4531):\n{e!s}")
    return(n317)

def m4545(n313):
    try:
        n319 = default_table_py_table.morloc_sliceRows(0, 1, n313)
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4545):\n{e!s}")
    return(n319)

def m4550(n306):
    try:
        n320 = default_table_py_table.morloc_asCol("x", n306)
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4550):\n{e!s}")
    return(n320)

def m4559(n313):
    try:
        n321 = default_table_py_table.morloc_sliceRows(0, 2, n313)
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4559):\n{e!s}")
    return(n321)

def m4564(n309):
    try:
        n322 = default_table_py_table.morloc_asCol("x", n309)
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4564):\n{e!s}")
    return(n322)

def m4573(n313):
    try:
        n323 = default_table_py_table.morloc_sliceRows(0, 3, n313)
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4573):\n{e!s}")
    return(n323)

def m4578(n311):
    try:
        n324 = default_table_py_table.morloc_asCol("x", n311)
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4578):\n{e!s}")
    return(n324)

def m4589(n313):
    try:
        n325 = default_table_py_table.morloc_sliceRows(1, 0, n313)
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4589):\n{e!s}")
    return(n325)

def m4587(n313):
    try:
        n326 = default_table_py_table.morloc_nrow(m4589(n313))
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4587):\n{e!s}")
    return(n326)

def m4602(n313):
    try:
        n327 = default_table_py_table.morloc_sliceRows(1, 1, n313)
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4602):\n{e!s}")
    return(n327)

def m4600(n313):
    try:
        n328 = default_table_py_table.morloc_nrow(m4602(n313))
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4600):\n{e!s}")
    return(n328)

def m4613(n313):
    try:
        n329 = default_table_py_table.morloc_sliceRows(1, 2, n313)
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4613):\n{e!s}")
    return(n329)

def m4618(n307):
    try:
        n330 = default_table_py_table.morloc_asCol("x", n307)
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4618):\n{e!s}")
    return(n330)

def m4627(n313):
    try:
        n331 = default_table_py_table.morloc_sliceRows(1, 3, n313)
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4627):\n{e!s}")
    return(n331)

def m4632(n310):
    try:
        n332 = default_table_py_table.morloc_asCol("x", n310)
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4632):\n{e!s}")
    return(n332)

def m4641(n313):
    try:
        n333 = default_table_py_table.morloc_sliceRows(1, 4, n313)
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4641):\n{e!s}")
    return(n333)

def m4646(n310):
    try:
        n334 = default_table_py_table.morloc_asCol("x", n310)
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4646):\n{e!s}")
    return(n334)

def m4655(n313):
    try:
        n335 = default_table_py_table.morloc_sliceRows(2, 3, n313)
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4655):\n{e!s}")
    return(n335)

def m4660(n308):
    try:
        n336 = default_table_py_table.morloc_asCol("x", n308)
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4660):\n{e!s}")
    return(n336)

def m4671(n313):
    try:
        n337 = default_table_py_table.morloc_sliceRows(3, 4, n313)
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4671):\n{e!s}")
    return(n337)

def m4669(n313):
    try:
        n338 = default_table_py_table.morloc_nrow(m4671(n313))
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4669):\n{e!s}")
    return(n338)

def m4682(n313):
    try:
        n339 = default_table_py_table.morloc_sliceRows(5, 9, n313)
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4682):\n{e!s}")
    return(n339)

def m4680(n313):
    try:
        n340 = default_table_py_table.morloc_nrow(m4682(n313))
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4680):\n{e!s}")
    return(n340)

def m4795():
    try:
        n341 = [0, 1, 2]
        n342 = default_table_py_table.morloc_asCol("x", n341)
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4795):\n{e!s}")
    return(n342)

def m4793():
    try:
        n343 = default_table_py_table.morloc_names(m4795())
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4793):\n{e!s}")
    return(n343)

def m4833():
    try:
        n347 = [0, 1, 2]
        n348 = default_table_py_table.morloc_asCol("x", n347)
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4833):\n{e!s}")
    return(n348)

def m4829():
    try:
        n346 = ["0", "1", "2"]
        n349 = default_table_py_table.morloc_setCol("y", n346, m4833())
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4829):\n{e!s}")
    return(n349)

def m4805():
    try:
        n345 = [0.0, 0.5, 1.0]
        n350 = default_table_py_table.morloc_setCol("z", n345, m4829())
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4805):\n{e!s}")
    return(n350)

def m4803():
    try:
        n351 = default_table_py_table.morloc_names(m4805())
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4803):\n{e!s}")
    return(n351)

def m4867():
    try:
        n353 = [0, 1, 2]
        n354 = default_table_py_table.morloc_asCol("x", n353)
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4867):\n{e!s}")
    return(n354)

def m4871():
    try:
        n355 = [0, 1, 2]
        n356 = default_table_py_table.morloc_asCol("x", n355)
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4871):\n{e!s}")
    return(n356)

def m4879():
    try:
        n357 = ["0", "1", "2"]
        n358 = default_table_py_table.morloc_asCol("y", n357)
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4879):\n{e!s}")
    return(n358)

def m4886():
    try:
        n359 = ["0", "1", "2"]
        n360 = default_table_py_table.morloc_asCol("y", n359)
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4886):\n{e!s}")
    return(n360)

def m4896():
    try:
        n361 = [0, 1, 2]
        n362 = default_table_py_table.morloc_asCol("x", n361)
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4896):\n{e!s}")
    return(n362)

def m4894():
    try:
        n363 = default_table_py_table.morloc_nrow(m4896())
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4894):\n{e!s}")
    return(n363)

def m4964():
    try:
        n366 = [0, 1, 2]
        n367 = default_table_py_table.morloc_asCol("x", n366)
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4964):\n{e!s}")
    return(n367)

def m4960():
    try:
        n365 = ["0", "1", "2"]
        n368 = default_table_py_table.morloc_setCol("y", n365, m4964())
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4960):\n{e!s}")
    return(n368)

def m4905():
    try:
        n364 = [0.0, 0.5, 1.0]
        n369 = default_table_py_table.morloc_setCol("z", n364, m4960())
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4905):\n{e!s}")
    return(n369)

def m4903():
    try:
        n370 = default_table_py_table.morloc_ncol(m4905())
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4903):\n{e!s}")
    return(n370)

def m4859():
    try:
        n371 = (0, 0)
        n372 = default_table_test_table_test.printMsg( "asCol / nrow / ncol"
        , n371 )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4859):\n{e!s}")
    return(n372)

def m4944():
    try:
        n373 = default_table_test_table_test.testEqual( "ncol (mkXYZ3) = 3"
        , m4903()
        , 3
        , m4859() )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4944):\n{e!s}")
    return(n373)

def m4929():
    try:
        n374 = default_table_test_table_test.testEqual( "nrow (mkX 3) = 3"
        , m4894()
        , 3
        , m4944() )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4929):\n{e!s}")
    return(n374)

def m4910():
    try:
        n375 = default_table_test_table_test.testEqual( "asCol from List literal"
        , m4879()
        , m4886()
        , m4929() )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4910):\n{e!s}")
    return(n375)

def m4772():
    try:
        n376 = default_table_test_table_test.testEqual( "asCol \"x\" xs3 == mkX 3"
        , m4867()
        , m4871()
        , m4910() )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4772):\n{e!s}")
    return(n376)

def m4785():
    try:
        n377 = default_table_test_table_test.printMsg("names", m4772())
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4785):\n{e!s}")
    return(n377)

def m4813():
    try:
        n352 = ["x", "y", "z"]
        n378 = default_table_test_table_test.testEqual( "names mkXYZ3 = [\"x\", \"y\", \"z\"]"
        , m4803()
        , n352
        , m4785() )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4813):\n{e!s}")
    return(n378)

def m4477():
    try:
        n344 = ["x"]
        n379 = default_table_test_table_test.testEqual( "names (mkX 3) = [\"x\"]"
        , m4793()
        , n344
        , m4813() )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4477):\n{e!s}")
    return(n379)

def m4490():
    try:
        n380 = default_table_test_table_test.printMsg("sliceRows", m4477())
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4490):\n{e!s}")
    return(n380)

def m4766(n313):
    try:
        n381 = default_table_test_table_test.testEqual( "sliceRows 5 9 t == empty (start past end)"
        , m4680(n313)
        , 0
        , m4490() )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4766):\n{e!s}")
    return(n381)

def m4760(n313):
    try:
        n382 = default_table_test_table_test.testEqual( "sliceRows 3 4 t == empty (start at end)"
        , m4669(n313)
        , 0
        , m4766(n313) )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4760):\n{e!s}")
    return(n382)

def m4754(n313, n308):
    try:
        n383 = default_table_test_table_test.testEqual( "sliceRows 2 3 t == [2] (last row)"
        , m4655(n313)
        , m4660(n308)
        , m4760(n313) )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4754):\n{e!s}")
    return(n383)

def m4748(n313, n310, n308):
    try:
        n384 = default_table_test_table_test.testEqual( "sliceRows 1 4 t == [1, 2] (end past nrow, clamped)"
        , m4641(n313)
        , m4646(n310)
        , m4754(n313, n308) )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4748):\n{e!s}")
    return(n384)

def m4742(n313, n310, n308):
    try:
        n385 = default_table_test_table_test.testEqual( "sliceRows 1 3 t == [1, 2]"
        , m4627(n313)
        , m4632(n310)
        , m4748(n313, n310, n308) )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4742):\n{e!s}")
    return(n385)

def m4736(n313, n310, n308, n307):
    try:
        n386 = default_table_test_table_test.testEqual( "sliceRows 1 2 t == [1] (single row)"
        , m4613(n313)
        , m4618(n307)
        , m4742(n313, n310, n308) )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4736):\n{e!s}")
    return(n386)

def m4730(n313, n310, n308, n307):
    try:
        n387 = default_table_test_table_test.testEqual( "sliceRows 1 1 t == empty (end == start)"
        , m4600(n313)
        , 0
        , m4736(n313, n310, n308, n307) )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4730):\n{e!s}")
    return(n387)

def m4724(n313, n310, n308, n307):
    try:
        n388 = default_table_test_table_test.testEqual( "sliceRows 1 0 t == empty (end < start, no reverse)"
        , m4587(n313)
        , 0
        , m4730(n313, n310, n308, n307) )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4724):\n{e!s}")
    return(n388)

def m4718(n313, n311, n310, n308, n307):
    try:
        n389 = default_table_test_table_test.testEqual( "sliceRows 0 3 t == full table"
        , m4573(n313)
        , m4578(n311)
        , m4724(n313, n310, n308, n307) )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4718):\n{e!s}")
    return(n389)

def m4712(n313, n311, n310, n309, n308, n307):
    try:
        n390 = default_table_test_table_test.testEqual( "sliceRows 0 2 t == [0, 1]"
        , m4559(n313)
        , m4564(n309)
        , m4718(n313, n311, n310, n308, n307) )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4712):\n{e!s}")
    return(n390)

def m4706(n313, n311, n310, n309, n308, n307, n306):
    try:
        n391 = default_table_test_table_test.testEqual( "sliceRows 0 1 t == [0]"
        , m4545(n313)
        , m4550(n306)
        , m4712(n313, n311, n310, n309, n308, n307) )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4706):\n{e!s}")
    return(n391)

def m4700(n313, n311, n310, n309, n308, n307, n306):
    try:
        n318 = ["x"]
        n392 = default_table_test_table_test.testEqual( "sliceRows 0 0 t preserves schema"
        , m4531(n313)
        , n318
        , m4706(n313, n311, n310, n309, n308, n307, n306) )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4700):\n{e!s}")
    return(n392)

def m4323(n313, n311, n310, n309, n308, n307, n306):
    try:
        n393 = default_table_test_table_test.testEqual( "sliceRows 0 0 t == empty"
        , m4518(n313)
        , 0
        , m4700(n313, n311, n310, n309, n308, n307, n306) )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4323):\n{e!s}")
    return(n393)

def m4336():
    try:
        n306 = [0]
        n307 = [1]
        n308 = [2]
        n309 = [0, 1]
        n310 = [1, 2]
        n311 = [0, 1, 2]
        n312 = [0, 1, 2]
        n313 = default_table_py_table.morloc_asCol("x", n312)
        n394 = m4323(n313, n311, n310, n309, n308, n307, n306)
        n395 = default_table_test_table_test.printMsg("filterRows", n394)
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4336):\n{e!s}")
    return(n395)

def m4453(n285):
    try:
        n305 = ["x"]
        n396 = default_table_test_table_test.testEqual( "filterRows allFalse preserves schema"
        , m4401(n285)
        , n305
        , m4336() )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4453):\n{e!s}")
    return(n396)

def m4438(n287, n286, n285):
    try:
        n397 = default_table_test_table_test.testEqual( "filterRows mixed (mkX 3) keeps positions 0, 2"
        , m4388(n286)
        , m4394(n287)
        , m4453(n285) )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4438):\n{e!s}")
    return(n397)

def m4414(n287, n286, n285):
    try:
        n398 = default_table_test_table_test.testEqual( "filterRows allFalse (mkX 3) nrow = 0"
        , m4374(n285)
        , 0
        , m4438(n287, n286, n285) )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4414):\n{e!s}")
    return(n398)

def m4153(n287, n286, n285, n284):
    try:
        n399 = default_table_test_table_test.testEqual( "filterRows allTrue (mkX 3) == mkX 3"
        , m4360(n284)
        , m4366()
        , m4414(n287, n286, n285) )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4153):\n{e!s}")
    return(n399)

def m4166():
    try:
        n284 = [True, True, True]
        n285 = [False, False, False]
        n286 = [True, False, True]
        n287 = [0, 2]
        n400 = m4153(n287, n286, n285, n284)
        n401 = default_table_test_table_test.printMsg("pickRows", n400)
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4166):\n{e!s}")
    return(n401)

def m4299(n266, n263):
    try:
        n402 = default_table_test_table_test.testEqual( "pickRows [1] (mkX 3) picks the middle row"
        , m4247(n263)
        , m4253(n266)
        , m4166() )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4299):\n{e!s}")
    return(n402)

def m4284(n266, n265, n263, n262):
    try:
        n403 = default_table_test_table_test.testEqual( "pickRows duped (mkX 3) duplicates rows"
        , m4234(n262)
        , m4240(n265)
        , m4299(n266, n263) )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4284):\n{e!s}")
    return(n403)

def m4260(n266, n265, n264, n263, n262, n261):
    try:
        n404 = default_table_test_table_test.testEqual( "pickRows reversed (mkX 3) reverses"
        , m4219(n261)
        , m4225(n264)
        , m4284(n266, n265, n263, n262) )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4260):\n{e!s}")
    return(n404)

def m4043(n266, n265, n264, n263, n262, n261, n260):
    try:
        n405 = default_table_test_table_test.testEqual( "pickRows identity (mkX 3) == mkX 3"
        , m4205(n260)
        , m4211()
        , m4260(n266, n265, n264, n263, n262, n261) )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4043):\n{e!s}")
    return(n405)

def m4056():
    try:
        n260 = [0, 1, 2]
        n261 = [2, 1, 0]
        n262 = [0, 0, 1, 1, 2, 2]
        n263 = [1]
        n264 = [2, 1, 0]
        n265 = [0, 0, 1, 1, 2, 2]
        n266 = [1]
        n406 = m4043(n266, n265, n264, n263, n262, n261, n260)
        n407 = default_table_test_table_test.printMsg("distinctRows", n406)
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4056):\n{e!s}")
    return(n407)

def m4147(n250, n248):
    try:
        n408 = default_table_test_table_test.testEqual( "distinctRows of [1,2,1,3,2] == [1,2,3]"
        , m4116(n248)
        , n250
        , m4056() )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4147):\n{e!s}")
    return(n408)

def m4123(n251, n250, n248, n246):
    try:
        n409 = default_table_test_table_test.testEqual( "distinctRows of all-dup [7,7,7] == [7]"
        , m4106(n246)
        , m4109(n251)
        , m4147(n250, n248) )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4123):\n{e!s}")
    return(n409)

def m3882(n251, n250, n248, n246):
    try:
        n410 = default_table_test_table_test.testEqual( "distinctRows of all-distinct (mkX 3) == mkX 3"
        , m4093()
        , m4098()
        , m4123(n251, n250, n248, n246) )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3882):\n{e!s}")
    return(n410)

def m3895():
    try:
        n245 = [7, 7, 7]
        n246 = default_table_py_table.morloc_asCol("x", n245)
        n247 = [1, 2, 1, 3, 2]
        n248 = default_table_py_table.morloc_asCol("x", n247)
        n249 = [1, 2, 3]
        n250 = default_table_py_table.morloc_asCol("x", n249)
        n251 = [7]
        n411 = m3882(n251, n250, n248, n246)
        n412 = default_table_test_table_test.printMsg("sortRows", n411)
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3895):\n{e!s}")
    return(n412)

def m4010():
    try:
        n413 = default_table_test_table_test.testEqual( "sortRows [] (mkX 3) is a no-op"
        , m3959()
        , m3965()
        , m3895() )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m4010):\n{e!s}")
    return(n413)

def m3995(n219):
    try:
        n414 = default_table_test_table_test.testEqual( "sortRows [(\"x\", False)] (mkX 3) reverses"
        , m3943()
        , m3952(n219)
        , m4010() )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3995):\n{e!s}")
    return(n414)

def m3971(n219):
    try:
        n415 = default_table_test_table_test.testEqual( "sortRows [(\"x\", True)] of [2, 1, 0] == mkX 3"
        , m3925(n219)
        , m3935()
        , m3995(n219) )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3971):\n{e!s}")
    return(n415)

def m3752(n219):
    try:
        n416 = default_table_test_table_test.testEqual( "sortRows [(\"x\", True)] (mkX 3) == mkX 3 (already asc)"
        , m3908()
        , m3917()
        , m3971(n219) )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3752):\n{e!s}")
    return(n416)

def m3765():
    try:
        n219 = [2, 1, 0]
        n417 = m3752(n219)
        n418 = default_table_test_table_test.printMsg("getCol", n417)
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3765):\n{e!s}")
    return(n418)

def m3820():
    try:
        n218 = [0.0, 0.5, 1.0]
        n419 = default_table_test_table_test.testEqual( "getCol \"z\" mkXYZ3 == zs3"
        , m3793()
        , n218
        , m3765() )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3820):\n{e!s}")
    return(n419)

def m3801():
    try:
        n210 = ["0", "1", "2"]
        n420 = default_table_test_table_test.testEqual( "getCol \"y\" mkXYZ3 == ys3"
        , m3785()
        , n210
        , m3820() )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3801):\n{e!s}")
    return(n420)

def m3555():
    try:
        n202 = [0, 1, 2]
        n421 = default_table_test_table_test.testEqual( "getCol \"x\" (mkX 3) == xs3"
        , m3773()
        , n202
        , m3801() )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3555):\n{e!s}")
    return(n421)

def m3568():
    try:
        n422 = default_table_test_table_test.printMsg("setCol", m3555())
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3568):\n{e!s}")
    return(n422)

def m3701(n178, n173):
    try:
        n423 = default_table_test_table_test.testEqual( "setCol \"z\" newZs mkXYZ3 replaces the existing z column"
        , n173
        , n178
        , m3568() )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3701):\n{e!s}")
    return(n423)

def m3665(n178, n173):
    try:
        n424 = default_table_test_table_test.testEqual( "setCol \"z\" zs (mkXY 3) == mkXYZ3 (append new column)"
        , m3608()
        , m3615()
        , m3701(n178, n173) )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3665):\n{e!s}")
    return(n424)

def m3392(n178, n173):
    try:
        n425 = default_table_test_table_test.testEqual( "setCol \"y\" ys (mkX 3) == mkXY 3 (append new column)"
        , m3593()
        , m3600()
        , m3665(n178, n173) )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3392):\n{e!s}")
    return(n425)

def m3405():
    try:
        n166 = [9.0, 9.0, 9.0]
        n173 = default_table_py_table.morloc_setCol("z", n166, m3580())
        n178 = default_table_py_table.morloc_setCol("z", n166, m3585())
        n426 = m3392(n178, n173)
        n427 = default_table_test_table_test.printMsg("dropCols", n426)
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3405):\n{e!s}")
    return(n427)

def m3498():
    try:
        n428 = default_table_test_table_test.testEqual( "dropCols [\"absent\"] (mkX 3) == mkX 3 (no-op)"
        , m3438()
        , m3445()
        , m3405() )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3498):\n{e!s}")
    return(n428)

def m3451():
    try:
        n429 = default_table_test_table_test.testEqual( "dropCols [\"x\", \"z\"] mkXYZ3 == mkY 3"
        , m3426()
        , m3432()
        , m3498() )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3451):\n{e!s}")
    return(n429)

def m3226():
    try:
        n430 = default_table_test_table_test.testEqual( "dropCols [\"z\"] mkXYZ3 == mkXY 3"
        , m3413()
        , m3418()
        , m3451() )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3226):\n{e!s}")
    return(n430)

def m3239():
    try:
        n431 = default_table_test_table_test.printMsg("selectCols", m3226())
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3239):\n{e!s}")
    return(n431)

def m3319():
    try:
        n432 = default_table_test_table_test.testEqual( "selectCols [\"x\", \"y\"] mkXYZ3 == mkXY 3"
        , m3275()
        , m3281()
        , m3239() )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3319):\n{e!s}")
    return(n432)

def m3287():
    try:
        n433 = default_table_test_table_test.testEqual( "selectCols [\"y\"] (mkXY 3) == mkY 3"
        , m3262()
        , m3269()
        , m3319() )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3287):\n{e!s}")
    return(n433)

def m3119():
    try:
        n434 = default_table_test_table_test.testEqual( "selectCols [\"x\"] (mkXY 3) == mkX 3"
        , m3247()
        , m3254()
        , m3287() )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3119):\n{e!s}")
    return(n434)

def m3132():
    try:
        n435 = default_table_test_table_test.printMsg("selectColsDyn", m3119())
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3132):\n{e!s}")
    return(n435)

def m3163():
    try:
        n436 = default_table_test_table_test.testEqual( "selectColsDyn [\"y\", \"z\"] mkXYZ3 keeps two columns"
        , m3151()
        , 2
        , m3132() )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3163):\n{e!s}")
    return(n436)

def m3038():
    try:
        n437 = default_table_test_table_test.testEqual( "selectColsDyn [\"x\"] mkXYZ3 == mkX 3"
        , m3140()
        , m3145()
        , m3163() )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3038):\n{e!s}")
    return(n437)

def m3051():
    try:
        n438 = default_table_test_table_test.printMsg("renameCol", m3038())
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3051):\n{e!s}")
    return(n438)

def m3087():
    try:
        n90 = [0, 1, 2]
        n439 = default_table_test_table_test.testEqual( "rename then getCol round-trip"
        , m3073()
        , n90
        , m3051() )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m3087):\n{e!s}")
    return(n439)

def m2840():
    try:
        n440 = default_table_test_table_test.testEqual( "renameCol \"x\" \"a\" (mkX 3) renames"
        , m3059()
        , m3066()
        , m3087() )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m2840):\n{e!s}")
    return(n440)

def m2853():
    try:
        n441 = default_table_test_table_test.printMsg("rbind / cbind", m2840())
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m2853):\n{e!s}")
    return(n441)

def m2979():
    try:
        n442 = default_table_test_table_test.testEqual( "cbind (mkX 3) (mkY 3) == mkXY 3"
        , m2917()
        , m2925()
        , m2853() )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m2979):\n{e!s}")
    return(n442)

def m2955():
    try:
        n443 = default_table_test_table_test.testEqual( "cbind (mkX 3) (mkY 3) ncol = 2"
        , m2903()
        , 2
        , m2979() )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m2955):\n{e!s}")
    return(n443)

def m2931(n54):
    try:
        n444 = default_table_test_table_test.testEqual( "rbind (mkX 3) (mkX 3) == doubled column"
        , m2889()
        , n54
        , m2955() )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m2931):\n{e!s}")
    return(n444)

def m2730(n54):
    try:
        n445 = default_table_test_table_test.testEqual( "rbind (mkX 3) (mkX 3) nrow = 6"
        , m2873()
        , 6
        , m2931(n54) )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m2730):\n{e!s}")
    return(n445)

def m2743():
    try:
        n53 = [0, 1, 2, 0, 1, 2]
        n54 = default_table_py_table.morloc_asCol("x", n53)
        n446 = m2730(n54)
        n447 = default_table_test_table_test.printMsg( "Composition / derived"
        , n446 )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m2743):\n{e!s}")
    return(n447)

def m2834(n39, n37):
    try:
        n448 = default_table_test_table_test.testEqual( "reverseRows t == pickRows (reverse (range 0 (nrow t - 1))) t"
        , m2798(n39)
        , m2812(n37)
        , m2743() )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m2834):\n{e!s}")
    return(n448)

def m2828(n39, n37, n36):
    try:
        n449 = default_table_test_table_test.testEqual( "tail k t == sliceRows (nrow t - k) (nrow t) t"
        , m2779(n39)
        , m2791(n36)
        , m2834(n39, n37) )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m2828):\n{e!s}")
    return(n449)

def m2724(n39, n37, n36, n35):
    try:
        n450 = default_table_test_table_test.testEqual( "head k t == sliceRows 0 k t"
        , m2765(n39)
        , m2770(n35)
        , m2828(n39, n37, n36) )
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m2724):\n{e!s}")
    return(n450)

def m2711():
    try:
        n35 = [0, 1]
        n36 = [1, 2]
        n37 = [2, 1, 0]
        n38 = [0, 1, 2]
        n39 = default_table_py_table.morloc_asCol("x", n38)
        n451 = m2724(n39, n37, n36, n35)
        n452 = default_table_test_table_test.printResult(n451)
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m2711):\n{e!s}")
    return(n452)

def m2718():
    try:
        n453 = m2711()[0]
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m2718):\n{e!s}")
    return(n453)

def m1():
    try:
        n454 = default_root_py_core.morloc_eq(0, m2718())
    except Exception as e:
            raise RuntimeError(f"Error (pool daemon in m1):\n{e!s}")
    return(morloc.put_value(n454, mlc_schema_table[0]))
# AUTO include manifolds end


# AUTO include dispatch start
dispatch = {
    1: m1,
}
remote_dispatch = {
}
# AUTO include dispatch end

def run_job(client_fd: int) -> None:
    try:
        # Free SHM from previous dispatch result (consumed by caller)
        morloc.flush_shm_tracker()
        client_data = morloc.stream_from_client(client_fd)

        if(morloc.is_local_call(client_data)):
            (mid, args) = morloc.read_morloc_call_packet(client_data)

            try:
                result = dispatch[mid](*args)
            except Exception as e:
                result = morloc.make_fail_packet(str(e))

        elif(morloc.is_remote_call(client_data)):
            (mid, args) = morloc.read_morloc_call_packet(client_data)

            try:
                result = remote_dispatch[mid](*args)
            except Exception as e:
                result = morloc.make_fail_packet(str(e))

        elif(morloc.is_ping(client_data)):
            result = morloc.pong(client_data)

        else:
            raise ValueError("Expected a ping or call type packet")

        # Flush stdout BEFORE sending the result back. The nexus prints its
        # own output (the return value) right after receiving this response.
        # Both processes share the same stdout fd, so if we flush after sending,
        # the nexus can print first, causing out-of-order output.
        sys.stdout.flush()

        morloc.send_packet_to_foreign_server(client_fd, result)

    except Exception as e:
        # Try to send a fail packet back to the caller before giving up.
        # This may fail (e.g., broken pipe from a timed-out ping), which is OK.
        try:
            result = morloc.make_fail_packet(str(e))
            morloc.send_packet_to_foreign_server(client_fd, result)
        except Exception:
            pass
        print(f"job failed: {e!s}", file=sys.stderr)
    finally:
        # Safety-net flush for any output from error handling paths
        sys.stdout.flush()
        # close child copy
        morloc.close_socket(client_fd)


def _send_fd(sock, fd):
    """Send a file descriptor over a Unix domain socket."""
    sock.sendmsg([b'\x00'],
                 [(_socket.SOL_SOCKET, _socket.SCM_RIGHTS,
                   array.array('i', [fd]))])

def _recv_fd(sock):
    """Receive a file descriptor from a Unix domain socket."""
    msg, ancdata, flags, addr = sock.recvmsg(1, _socket.CMSG_SPACE(4))
    if not msg and not ancdata:
        raise EOFError("Connection closed")
    for cmsg_level, cmsg_type, cmsg_data in ancdata:
        if (cmsg_level == _socket.SOL_SOCKET and
                cmsg_type == _socket.SCM_RIGHTS):
            a = array.array('i')
            a.frombytes(cmsg_data[:4])
            return a[0]
    raise RuntimeError("No fd received in ancillary data")


WORKER_IDLE_TIMEOUT = 5.0  # seconds before an idle worker exits

def worker_process(job_fd, tmpdir, shm_basename, shutdown_flag, busy_count, total_workers, wakeup_w):
    # Reset signal handlers inherited from main. If user code inside run_job
    # calls multiprocessing.Pool (or anything else that forks and later
    # SIGTERMs its own children), those grandchildren would otherwise inherit
    # main's signal_handler and flip the shared shutdown_flag, causing main
    # to SIGKILL this worker mid-response. See the multiprocessing-py-1 bug.
    signal.signal(signal.SIGTERM, signal.SIG_DFL)
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    morloc.set_fallback_dir(tmpdir)
    morloc.shinit(shm_basename, 0, 0xffff)
    _init_worker_tracking(busy_count, total_workers, wakeup_w)
    sock = _socket.fromfd(job_fd, _socket.AF_UNIX, _socket.SOCK_STREAM)
    os.close(job_fd)  # sock owns a dup'd copy
    last_activity = time.monotonic()
    try:
        while not shutdown_flag.value:
            rlist, _, _ = select.select([sock.fileno()], [], [], 0.01)
            if shutdown_flag.value:
                break
            if rlist:
                try:
                    client_fd = _recv_fd(sock)
                    run_job(client_fd)
                    last_activity = time.monotonic()
                except (EOFError, OSError):
                    break
            elif total_workers.value > 1 and time.monotonic() - last_activity > WORKER_IDLE_TIMEOUT:
                break
    except BaseException as e:
        # Catch-all for errors that escape run_job's own exception handling:
        # MemoryError, KeyboardInterrupt, SystemExit, or bugs in the worker
        # loop itself. Without this, the worker dies silently and the nexus
        # only sees "failed to read response header" with no indication of
        # what went wrong in the pool.
        #
        # Race condition: the nexus detects the broken socket and may start
        # its clean_exit tear-down (SIGTERM -> SIGKILL) while this print is
        # still buffered. We flush immediately to maximize the chance the
        # message reaches the terminal before we are killed. stderr is
        # line-buffered (set in __main__), but the flush is a safety net for
        # edge cases (redirected stderr, forked-process buffer state).
        import traceback
        print(f"morloc pool worker fatal error: {e!s}", file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
        sys.stderr.flush()
    finally:
        sock.close()


def signal_handler(sig, frame):
    global daemon
    # Ignore further SIGTERM/SIGINT during cleanup. Python processes pending
    # signals between bytecodes, including while another signal handler is
    # running, so a second SIGTERM arriving mid-cleanup would otherwise
    # re-enter this handler and double-free the daemon pointer.
    try:
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        signal.signal(signal.SIGINT, signal.SIG_IGN)
    except Exception:
        pass
    shutdown_flag.value = True
    if _shutdown_wakeup_fd >= 0:
        try:
            os.write(_shutdown_wakeup_fd, b'!')
        except OSError:
            pass
    # Capture the daemon pointer into a local and clear the global BEFORE
    # invoking close_daemon. If a pending signal still slips through and
    # re-enters this handler, it will see daemon=None and skip the free.
    d = daemon
    daemon = None
    if d is not None:
        morloc.close_daemon(d)


def client_listener(job_fd, socket_path, tmpdir, shm_basename, shutdown_flag):
    global daemon
    daemon = morloc.start_daemon(socket_path, tmpdir, shm_basename, 0xffff)
    sock = _socket.fromfd(job_fd, _socket.AF_UNIX, _socket.SOCK_STREAM)
    os.close(job_fd)  # sock owns a dup'd copy

    while not shutdown_flag.value:
        try:
            client_fd = morloc.wait_for_client(daemon)
        except Exception as e:
            print(f"In python daemon, failed to connect to client: {e!s}", file=sys.stderr)
            continue

        if client_fd > 0:
            try:
                _send_fd(sock, client_fd)
            except Exception as e:
                print(f"In python daemon, failed to start worker: {e!s}", file=sys.stderr)
            finally:
                morloc.close_socket(client_fd)
    sock.close()



if __name__ == "__main__":
    # Line-buffer stderr so diagnostic output is not lost when pool is killed.
    # stdout is left fully buffered for performance (genome-scale piping) and
    # flushed explicitly after each job and during shutdown.
    sys.stderr.reconfigure(line_buffering=True)

    # Request SIGTERM when the parent process (nexus) dies.
    # Without this, SIGKILL on the nexus leaves pool processes orphaned
    # and their SHM segments leak in /dev/shm.
    try:
        import ctypes
        _PR_SET_PDEATHSIG = 1
        ctypes.CDLL("libc.so.6", use_errno=True).prctl(_PR_SET_PDEATHSIG, signal.SIGTERM)
    except Exception:
        pass  # non-Linux: skip (macOS uses kqueue for this)

    shutdown_flag = Value('b', False)  # Shared flag

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Health check: confirm imports loaded and print version
    if len(sys.argv) > 1 and sys.argv[1] == "--health":
        sys.stdout.write('{"status":"ok","version":"0.82.0"}\n')
        sys.exit(0)

    # Process arguments passed from the nexus
    try:
        socket_path = sys.argv[1]
        tmpdir = sys.argv[2]
        shm_basename = sys.argv[3]
    except IndexError:
        print("Usage: script.py <socket_path> <tmpdir> <shm_basename>")
        sys.exit(1)

    global_state["tmpdir"] = tmpdir

    # Shared job queue: listener writes fds to write_sock, workers read from read_sock.
    # Only idle workers (blocked in recvmsg) pick up jobs, preventing the round-robin
    # deadlock where a callback gets dispatched to a busy worker.
    read_sock, write_sock = _socket.socketpair(_socket.AF_UNIX, _socket.SOCK_STREAM)

    num_workers = 1
    workers = []

    # Shared counters for dynamic worker spawning.
    # Workers increment busy_count before foreign_call and decrement after.
    # When all workers are busy, main process spawns a new one.
    busy_count = RawValue(ctypes.c_int, 0)
    total_workers = RawValue(ctypes.c_int, num_workers)
    wakeup_r, wakeup_w = os.pipe()
    os.set_blocking(wakeup_r, False)
    _shutdown_wakeup_fd = wakeup_w

    # Keep a dup of the read end so we can spawn new workers later
    spare_read_fd = os.dup(read_sock.fileno())

    for i in range(num_workers):
        worker = Process(target=worker_process,
                         args=(read_sock.fileno(), tmpdir, shm_basename, shutdown_flag,
                               busy_count, total_workers, wakeup_w))
        worker.start()
        workers.append(worker)
    read_sock.close()  # main/listener don't need the read end (spare_read_fd kept)

    # Start client listener process
    listener_process = Process(
        target=client_listener,
        args=(write_sock.fileno(), socket_path, tmpdir, shm_basename, shutdown_flag)
    )
    listener_process.start()
    write_sock.close()  # main doesn't need the write end

    # Main loop: monitor wake-up pipe, spawn new workers when all are busy,
    # and reap idle workers that have exited.
    while not shutdown_flag.value:
        rlist, _, _ = select.select([wakeup_r], [], [], 0.01)
        if rlist:
            try:
                os.read(wakeup_r, 4096)  # drain pipe
            except OSError:
                pass

        # Reap dead workers (idle timeout or error exit)
        alive = []
        for w in workers:
            if w.is_alive():
                alive.append(w)
            else:
                w.join(timeout=0)
                w.close()
        workers = alive
        total_workers.value = max(1, len(workers))

        # Spawn a new worker if all are busy (or all have exited)
        if len(workers) == 0 or busy_count.value >= total_workers.value:
            w = Process(target=worker_process,
                        args=(spare_read_fd, tmpdir, shm_basename, shutdown_flag,
                              busy_count, total_workers, wakeup_w))
            w.start()
            workers.append(w)
            total_workers.value = len(workers)

    # Shutdown sequence
    os.close(wakeup_r)
    os.close(wakeup_w)
    os.close(spare_read_fd)

    # 1. Stop listener first
    listener_process.terminate()
    listener_process.join(timeout=0.001)
    listener_process.kill()
    listener_process.join()  # Final blocking reap
    listener_process.close()

    # 2. Terminate workers with escalating force
    for p in workers:
        if p.is_alive():
            p.kill()
        p.join()  # Final blocking reap
        p.close()

    sys.exit(0)
