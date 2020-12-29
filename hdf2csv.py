#!/home/data/local/bin/python3.7

############################################################################
#           read in CIWS Gridded VIL dataset and convert to csv            #
############################################################################

# TODO: REMOVE 'Netcdf4' from these filenames:
#  edu.mit.ll.wx.ciws.QuantizedVIL.Netcdf4.1km.20200828T231000Z.nc
#               files/QuantizedVIL.Netcdf4.1km.20200828T231000Z.gpkg

import re
import h5py  # Yipee!  (finally, this works on cygwin/windows)
import numpy as np

# -----------------------------------------------------------

# the 6km aggregated grid size is arbitrary!

agg_size = 6

# -----------------------------------------------------------
# data types:
#   _h5  hdf5
#   _np  numpy
#   _str  string
# -----------------------------------------------------------

vil_quantization_table = {
       -1:  '0',
        0:  '0',  # s.b. 0a
       13:  '0',  # s.b. 1a
       63:  '1',  # s.b. 1a
      113:  '1',  # s.b. 1b
      216:  '1',  # s.b. 1c
      317:  '2',
     1449:  '3',
     2902:  '4',
     4981:  '5',
    13240:  '6',
}
# Notes: Level xxx is depicted as Level 0 in Standard Mode and as Level 1a in Winter
# Mode. Levels 1a, 1b, and 1c are all depicted as Level 1 in Standard Mode

# ---- from page 12 of CIWS Product Description Revision 2.0:
# The information in this table should not be hard-coded into
# client software for decoding purposes. Instead, the threshold
# values and associated intensity levels may be extracted from the
# VIL variable attributes (see Appendix A for NetCDF listings in
# CDL).

echo_top_quantization_table = {
       -1:  '0',
        0:  '0',
        5:  'A',
       10:  'B',
       15:  'C',
       20:  'D',
       25:  'E',
       30:  'F',
       35:  'G',
       40:  'H',
       45:  'I',
       50:  'J',
       55:  'K',
}
# ----------------------------------------------------------

# ---- a function that returns the char vil value

def vil_func(x):
    return(vil_quantization_table[x])

# ---- create a numpy vectorization of that function

vil_vec_func = np.vectorize(vil_func)

# ----------------------------------------------------------

# ---- a function that returns the char vil value

def et_func(x):
    return(echo_top_quantization_table[x])

# ---- create a numpy vectorization of that function

et_vec_func = np.vectorize(et_func)

# ----------------------------------------------------------

# the main function to convert an HDF5 to a csv-like text file

def convert_numpy_to_csv(nv_np, fn_d):

    # ---- apply the vectorized function to the entire numpy array

    print("running vectorized function on numpy array")

    if fn_d['product'] == "QuantizedVIL":
        cv_np = vil_vec_func(nv_np)   # <<<<<<<<<<  the main event

    if fn_d['product'] == "QuantizedEchoTop":
        cv_np = et_vec_func(nv_np)   # <<<<<<<<<<  the main event

    # ----
    outfd = open(fn_d['csv'], 'w')

    # ---- now begin building the output .csv text

    row = 0       # counter of 1km rows in dataset
                  # when get to the end of a 6km group, write it out

    set6km = { }  # a dict (keys are 0,1,2,3,4,5) of the full lines (5120 chars)
                  # of each (1km) row

    col6km = int(5120/agg_size)

    print("converting numpy to text")

    # ---- iterate over each (1km) row
    for one_row in cv_np[0,0]:

        # ---- form one (large) string of this entire row
        this_row_st = "".join(one_row)

        # ---- and save it until we get to the end of the 6km set of rows
        set6km[row % agg_size]= this_row_st

        if (row % agg_size) == 5:   # if this is the end of a 6km grouping...

            # ---- output this 6km row
            # ---- in groups of 6km columns

            for col in range(col6km):

                vil_rowcol = []  # keep it as a list until needed as a complete str
                for sub_row in range(agg_size):
                    vil_rowcol.append( set6km[sub_row][col*agg_size : col*agg_size+agg_size])

                # ---- now put those together as the VIL csv string
                vil_str = "".join(vil_rowcol)

                # if line has something other than just 0's and 1's then print it
                # FIXME: not sure what pattern to eliminate here...
                # _     was for testing
                # 01    is for vil
                # ABCDE is for et

                if not re.match("^[_01ABCDE]+$", vil_str):

                    # and make it look like that goofy mitre format:
                    str2 = vil_str.lstrip('0')
                    zero_pad = len(vil_str) - len(str2)

                    outfd.write('"%s",%d,%d,%d,"%s"\n' % \
                          (fn_d['ymd'], int(row/agg_size), col, zero_pad, str2))

            # -------- end of: do this 6km row -----------

        row += 1

    outfd.close()

# ----------------------------------------------------------------------

# open & read the hdf5 file, select out the VIL, cvt to numpy, and call the above

def convert_hdf5_file_to_csv(subdir, fn):

    fn_d = parse_filename(fn)

    # ---- read in the entire hdf5 / netcdf dataset

    f_h5 = h5py.File(subdir + fn_d['fn'], 'r')

    # ---- extract/identify the data group that we want
    # ---- and btw convert to numpy

    if fn_d['product'] == "QuantizedVIL":
        nv_np = f_h5['VIL'][:]

    if fn_d['product'] == "QuantizedEchoTop":
        nv_np = f_h5['ECHO_TOP'][:]

    convert_numpy_to_csv(nv_np, fn_d)

    print("written:", fn_d['csv'])

    return(fn_d['csv'])

# ----------------------------------------------------------------------

# inputs:
#  edu.mit.ll.wx.ciws.QuantizedVIL.Netcdf4.1km.20200828T231000Z.nc
#  edu.mit.ll.wx.ciws.QuantizedEchoTop.Netcdf4.1km.20200829T193500Z.nc

subdir = "files/"  # FIXME

prod_prefix = "edu.mit.ll.wx.ciws"

def parse_filename(fn):

    fn_b = fn[len(prod_prefix)+1:]   # drop the "edu..ciws." part

    product = re.search("^[^.]*",fn_b).group(0)

    sdate = re.search("202[0-9]*",fn_b).group(0)
    y_m_d = sdate[:4] + '-' + sdate[4:6] + '-' + sdate[6:]
    timestamp = re.search("202[0-9TZ]*",fn_b).group(0)

    csv_filename = subdir + product + '.' + timestamp + ".csv"

    fn_dict = {
        "fn"        : fn,           # orig filename (w/o subdir)
        "csv"       : csv_filename,
        "product"   : product,      # QuantizedVIL, QuantizedEchoTop
        "timestamp" : timestamp,    # 20200829T193500Z
        "ymd"       : y_m_d,        # 2020-08-25
    }

    return(fn_dict)

