# ncd-nn

## Description:
ncd-nn is a simple utility to calculate nearest-neighbor of files using [normalized compression distance](https://en.wikipedia.org/wiki/Normalized_Compression_Distance).  This was written to identify files that are near duplicates - cases where the file contents were essentially identical, but small differences caused checksum-based duplication checking to miss them.

The original objective was to use this on PDF files.  In my hands, it doesn't perform very well on them.  However, it works reasonably well on text files; including text extracted from PDF files.

## Installation:
### Requirements:
ncd-nn requires [python](http://www.python.org) with the multiprocessing module.  It was written to run under both python2 and python3 (and does on at least 2.6.2 and 3.1.2), but version testing has not been exhaustive.

### Install
Put ncd-nn.py somewhere in your path and make sure it's got execute permissions.

## Usage:
For input files read from data_directory, and output written to ncd_report.txt: "ncd-nn.py -d data_directory -o ncd_report.txt"
By default, all files with the extension ".txt" with the data directory are checked, and the number of processes used are equal to the number of logical processors.  For more details, see the help message ("ncd-nn.py -h" or "ncd-nn.py --help").

## Known Problems:
* This is uglier than I'd like, primarily due to the use of a global variable for multiprocessing
* Missing the original citation


