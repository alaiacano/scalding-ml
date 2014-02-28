import numpy as np
from numpy import pi

t = np.linspace(0, 1.0, 2001)
xlow = np.sin(2 * np.pi * 5 * t)
xhigh = np.sin(2 * np.pi * 250 * t)
x = xlow + xhigh

# Now create a lowpass Butterworth filter with a cutoff of 0.125 times
# the Nyquist rate, or 125 Hz, and apply it to x with filtfilt.  The
# result should be approximately xlow, with no phase shift.

from scipy import signal
b, a = signal.butter(8, 0.125)

# b:
# 8.88199322e-07,   7.10559458e-06,   2.48695810e-05,
# 4.97391620e-05,   6.21739525e-05,   4.97391620e-05,
# 2.48695810e-05,   7.10559458e-06,   8.88199322e-07

# a:
#  1.0       ,  -5.98842478,  15.88837987, -24.35723742,
# 23.57037937, -14.72938334,   5.80019014,  -1.31502712,
#  0.13135067

y = signal.filtfilt(b, a, x)

with open("noisey.tsv", "wb") as f:
    for pt in x:
        f.write(str(pt) + "\n")

with open("filtered.tsv", "wb") as f:
    for pt in y:
        f.write(str(pt) + "\n")
