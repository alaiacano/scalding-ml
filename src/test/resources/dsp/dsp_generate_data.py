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
y = signal.filtfilt(b, a, x)

with open("noisey.tsv", "wb") as f:
    for pt in x:
        f.write(str(pt) + "\n")

with open("filtered.tsv", "wb") as f:
    for pt in y:
        f.write(str(pt) + "\n")
