#!/usr/bin/env python

''' text file nearest neighbor using normalized compression distance '''

#modification of txtnn5.py for in-memory usage 
#modification of txtnn6.py to keep compressed copy of data in memory as well
# based on benchmarking vs txtnn3.py on abel (two processors), keeping a 
# compressed copy in memory and using multiple processes give cut execution 
# time to ~25% of original.  Keeping the uncompressed copy in memory alone
# does not seem to reduce runtime much, so disk i/o may not be a bottleneck.
# On a system with more processors, the run time improvement should increase
# further.

import operator
from multiprocessing import Pool, cpu_count

import bz2
import os
import os.path

#filesystem utilities
def recursive_ls(wdir):
    ''' list all files (recursively) in dir'''
    if not os.path.exists(wdir): #deal with ~/
        wdir = os.path.expanduser(wdir)
    dlst = [ wdir ]
    flst = []
    while len(dlst) > 0:
        d = dlst[0]
        dlst.remove(d)
        f = [ os.path.join(d, s) for s in os.listdir(d) ]
        for dn in [ s for s in f if os.path.isdir(s) ]:
            dlst.append( dn )
        #for fn in filter( lambda s : not os.path.isdir(s), f):
        for fn in [ s for s in f if not os.path.isdir(s) ]:
            flst.append( fn)
    return flst

def filter_by_extension(file_lst, ext_lst):
    ''' return only files with extensions present in ext_lst'''
    if None == ext_lst:
        return file_lst
    def f(fname):
        ''' worker function for true/false if file extension in ext_lst '''
        ( _ ,e) = os.path.splitext(fname.lower() )
        return (e in ext_lst)
    return [ fn for fn in file_lst if f(fn) ]

#interesting work

#global txtpaths (needed for shared state)
txtpaths = []

def compressed_size(z):
    ''' calculate compressed size of data '''
    return float( len( bz2.compress(z) ) )

def init(datadir, extlst ):
    ''' initialize global/shared data '''
    global txtpaths
    fnames = filter_by_extension( recursive_ls(datadir) , extlst )
    def loader(fname):
        ''' file loader '''
        inp = open(fname, 'r')
        dat = inp.read()
        inp.close()
        zdat = compressed_size( dat )
        return (fname, dat, zdat, )
    txtpaths = [ loader(f) for f in fnames ]

def run_nn_single(tpl_a ):
    ''' nearest neighbor for single path (uses global txtpaths) '''
    dist = 0.0
    (txtpath_a, dat_a, zdat_a, ) = tpl_a
    txtpath_nn = None
    for (txtpath_b, dat_b, zdat_b, ) in txtpaths:
        if txtpath_a < txtpath_b:
            xyz = compressed_size( dat_a + dat_b )
            d = (xyz - min(zdat_a, zdat_b)) / max( zdat_a, zdat_b )
            if ( d < dist) or ( None == txtpath_nn ):
                dist = d
                txtpath_nn = txtpath_b
    return (txtpath_a, txtpath_nn, dist, )

#support functions for option parsing
def usage():
    ''' show usage information '''
    print('ncd-nn')
    print('options: ')
    print('\t-v --verbose \t\t\tverbose output during processing')
    #TODO - actually do something with verbose flag
    print('\t-h --help\t\t\tshow this usage information')
    print('\t-n --nproc=NUMBER\t\tnumber of processes (default is number of logical CPUs)')
    print('\t-d --datadir=DIRECTORY\t\tdirectory containing files to process')
    print('\t-o --output=FILENAME\t\toutput file for nearest-neighbor report')
    print('\t-e --extensions=.EXT1,EXT2\tlist of file extensions to process')

def parse_extension_list(s):
    ''' extract valid file extensions from option string '''
    return [e.strip() for e in s.split(',') ]


if __name__ == '__main__':
    import getopt
    import sys

    #option parsing
    try:
        opts, args = getopt.getopt(sys.argv[1:], 'hvn:d:o:e:',['help','verbose','nproc=','datadir=','output=','extensions='])
    except getopt.GetoptError, err:
        print(err)
        usage()
        sys.exit(1)

    #default values
    verobse = False
    nproc = cpu_count()
    ddir = 'data/'
    reportfile = 'nn-report.txt'
    extlst = ['.txt']

    #process options
    if 0 != len(args):
        usage()
        sys.exit(1) #shouldn't be any arguments

    for o,a in opts:
        if o in ('-h','--help'):
            usage()
            sys.exit(0)
        elif o in ('-v','--verbose'):
            verbose = True
        elif o in ('-n','--nproc'):
            try:
                nproc = int(a)
            except ValueError:
                usage()
                sys.exit(1)
        elif o in ('-d','--datadir'):
            ddir = a
        elif o in ('-o','--output'):
            reportfile = a
        elif o in ('-e','--extensions'):
            extlst = parse_extension_list(a)
        else:
            usage()
            assert False, 'unrecognized program option'

    #input check
    if not os.path.exists( ddir ):
        usage()
        sys.exit(1)

    #main work
    init(ddir, extlst)
    pool = Pool( nproc )
    nnlst0 = pool.map( run_nn_single, txtpaths )
    nnlst = [ (a,b,d,) for (a,b,d,) in nnlst0 if None != b ]
    nnlst.sort( key = operator.itemgetter(2) )
    opf = open(reportfile,'w')
    for tpl in nnlst:
        opf.write( '%s , %s : %f\n'%tpl)
    opf.close()
