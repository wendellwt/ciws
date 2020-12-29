#!/usr/bin/python3.7

############################################################################
#                  fetch data from MIT LL Atom data feed                   #
############################################################################

# Consider: use argparse to set sleep parameter

# ---------------------------------------------------------------------

import json
import wget
import time
import datetime
import feedparser
import geopandas as gpd
from dateutil import parser

ciws_atom_feed = 'http://ngen-ciws.wx.ll.mit.edu/atomfeeds/ciwsDataFeed.atom'

persist_fn = 'atom_persist.json'

subdir = "files/"

polling_sleep = 15 * 60  # seconds between atom feed polling (15 mins)

# ---------------------------------------------------------------------

import hdf2csv
import xml2gpkg
import csv2gpkg
import gdf2postg

print("imports finished")

# ======================== read ciws grid (once) ============================

# read in this (large) file once and pass it to subs as needed

print("reading grid.")

entire_grid = gpd.read_file("static_data/ciws_grid.gpkg")

entire_grid.rename( columns={ "geometry" : "poly" }, inplace=True)

print("done grid.")

# ============================= xxxxxxxx =================================

# ---- convert datetimes to text times
#      and write in persistent data

def write_persistent(pd):

    for k in pd:
        pd[k]["last_updated"] = pd[k]["last_updated"].isoformat()

    with open(persist_fn, 'w') as fp:
        json.dump(pd, fp, sort_keys=True, indent=4)

# ------------------------------------------------------------------

# ---- read in persistent data
#      and convert text times to datetime

def read_persistent():

    with open(persist_fn, 'r') as fp:
        pd = json.load(fp)

    for k in pd:
        pd[k]["last_updated"] = parser.isoparse( pd[k]["last_updated"] )

    return(pd)

# ------------------------------------------------------------------

# ---- actually retrieve this dataset from MIT LL

def retrieve_this(ent):

    print("---- retrieving:")
    print(ent["title"])
    print("    ", ent["id"])
    #rint("    ", ent["links"])
    #rint("    ", ent["links"][0]["href"])
    print("    ", ent["updated"])

    url = ent["links"][0]["href"]
    print("url=", url)

    p = url.rfind('/')
    filename = subdir + url[p+1:]

    print("fn=", filename)

    wget.download(url, filename)

    print()

    return(url[p+1:])  # plain filename

# =========================== process new file ===============================

# process an new gridded product (which is in hdf5) by doing these conversions:
#   * hdf5/netcdf to .csv that is somewhat similar to that other
#          (i.e., interpret hdf5 and write out as text)
#   * .csv to geopackage
#          (i.e., make our own custom union of those text polygons and write
#           into a geopackage file)
#   * geopackage to postgis
#          (i.e., write the geopackage to the d.b.)

def process_new_file_grid(nc_file):

    print("processing request:", nc_file)

    csvfn = hdf2csv.convert_hdf5_file_to_csv(subdir, nc_file)

    print("converted to csv")

    csvfn = csvfn[len(subdir):]  # remove sudir (?)

    new_gf = csv2gpkg.convert_csv_to_gpkg(csvfn, entire_grid)

    print("gpkg written")

    # gdf needs a datetime column; use the timestamp from the filename

    gdf2postg.read_and_insert_current_vil(new_gf, nc_file)

    print("nc inserted into postg")


# ------------------------------------------------------------------

#  loop FOREVER, reading "Atom feed" (which turns out to be just a
#         directory of urls)

while True:

    persist_data = read_persistent()

    print("query at:", datetime.datetime.utcnow() )
    atom = feedparser.parse(ciws_atom_feed)

    for ent in atom["entries"]:

        p = ent["id"].rfind(':')
        short_id = ent["id"][p+1:]

        if short_id in persist_data:

            print()
            print(ent["title"])

            pe = persist_data[short_id]

            # see if their update time is > our last retrieve time
            their_updated = parser.isoparse( ent["updated"] )
            print("    last_updated :", pe['last_updated'])
            print("    their_updated:", their_updated)

            if their_updated > pe['last_updated']:
                print("time to retrieve")

                filename = retrieve_this(ent)

                # ---- if it is a grid product, send to common site
                if filename.endswith(".nc"):

                    process_new_file_grid(filename)

                # ---- if it is a contour product, cvt to geopandas and send

                if filename.endswith(".xml.gz"):

                    print("parse:", filename)
                    res_gf = xml2gpkg.do_parse_xml(subdir + filename)

                    res_fn = xml2gpkg.write_gf(subdir + filename, res_gf)

                    res_fn =  res_fn[len(subdir):]

                    gdf2postg.read_and_insert_forecast_vil(res_fn)

                    print("xml inserted into postg")

                # ----
                persist_data[short_id]['last_updated'] = \
                           datetime.datetime.now(datetime.timezone.utc)
            else:
                print("not yet")

    # after everything is finished, record what we did
    write_persistent(persist_data)

    print("sleeping...")
    time.sleep(polling_sleep)

