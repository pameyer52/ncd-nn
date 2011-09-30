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

#actual work

#global txtpaths (needed for shared state)
txtpaths = []

def compressed_size(z):
    ''' calculate compressed size of data '''
    return float( len( bz2.compress(z) ) )

def init(datadir):
    ''' initialize global/shared data '''
    global txtpaths
    fnames = filter_by_extension( recursive_ls(datadir) , ['.txt'] )
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

if __name__ == '__main__':
    ddir = 'data/'
    reportfile = 'nn-report.txt'
    init(ddir)
    pool = Pool( cpu_count() )
    nnlst0 = pool.map( run_nn_single, txtpaths )
    nnlst = [ (a,b,d,) for (a,b,d,) in nnlst0 if None != b ]
    nnlst.sort( key = operator.itemgetter(2) )
    opf = open(reportfile,'w')
    for tpl in nnlst:
        opf.write( '%s , %s : %f\n'%tpl)
    opf.close()
