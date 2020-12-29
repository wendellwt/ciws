
#############################################################################
#                     insert geodata into postgis                           #
#############################################################################

# FIXME: how to tell current VIL from current ET ???

# --------------------------------------------------------------------------

import os
import re
import pytz
import datetime
import geopandas as gpd
import sqlalchemy
from geoalchemy2 import Geometry, WKTElement

dbloc = ""

# ======================= use this to use postgis on webfaction

# note: requires another window to do this:
#    $ ssh -L 55432:127.0.0.1:55432 wendell@web510.webfaction.com

# puser = "wendell"
# ppass = ""
# phost = "127.0.0.1"
# pport = "55432"
# ppgdb = "ciwsdb"
# dbloc = " INTO REMOTE"
# ======================= use this to store remotely to webfaction

# ======================= use this to use local postgis
pg_str = "postgresql://%s:%s@%s:%s/%s" % \
          ( os.environ['PGUSER'],
            os.environ['PGPASSWORD'],
            os.environ['PGHOST'],
            os.environ['PGPORT'],
            os.environ['PGDATABASE'] )

# ---- NOTE: sqlalchmey is used JUST because it can write a
#      GeoPandas dataframe

engine = sqlalchemy.create_engine(pg_str)  # Q: ok to do this here???

# q: commit & close db conn/engine?

subdir = "files/" # could be better

# ==================================== carrotseverywhere database

#    $ ssh wendell@opal4.opalstack.com -L 55432:127.0.0.1:5432 -N

cev_str = "postgresql://%s:%s@%s:%s/%s" % \
        ( os.environ["CEV_USER"],
          os.environ["CEV_PASSWORD"],
          os.environ["CEV_HOST"],
          os.environ["CEV_PORT"],   # note: non-standard for forwarding
          os.environ["CEV_DATABASE"]  )

print("connecting to cev")
try:
    cev_engine = sqlalchemy.create_engine(cev_str)  # Q: ok to do this here???
    print("conneced to cev")
except:
    print("BAD connect to cev; run this in other terminal:")
    print("   ssh wendell@opal4.opalstack.com -L 55432:127.0.0.1:5432 -N")
    cev_engine = None
# ==================================== carrotseverywhere database

# ------------------------------------------------------------------------

#  insert current VIL geo df into postgis
#  (doesn't actually do a read; filename is used just to get date and wtype)

def read_and_insert_current_vil(cur_vil_et_gf, nc_file):

    print("nc_file=", nc_file)

    # ---- get current timestamp from filename and insert it as a gf column

    sdate = re.search("202[0-9]*T[0-9]*Z",nc_file).group(0)
    #    20200825T212500Z
    d=datetime.datetime.strptime(sdate, "%Y%m%dT%H%M%SZ")
    utc = pytz.timezone('UTC')
    d_utc=utc.localize(d)

    cur_vil_et_gf['ctime'] = d_utc

    # ---- oops!  forgot to do this!!!!!!!!!!!!!!!!!
    wtype = "unk"
    if re.search("VIL",nc_file):
        wtype = "vil"
    if re.search("EchoTop",nc_file):
        wtype = "et"

    cur_vil_et_gf['wtype'] = wtype

    # ---- oops!

    # and turn GeoDataFrame into a pandas DataFrame with WKT in the geom column
    cur_vil_et_gf['wx_geog'] = cur_vil_et_gf['geom'].apply( \
                             lambda x: WKTElement(x.wkt, srid=4326))

    # ---- drop the geometry column as it is now duplicative
    cur_vil_et_gf.drop('geom', 1, inplace=True)

    # Use 'dtype' to specify column's type
    # For the geom column, we will use GeoAlchemy's type 'Geometry'

    cur_vil_et_gf.to_sql("ciws_obs", engine, if_exists='append', index=False,
                         dtype={'wx_geog': Geometry('Polygon', srid=4326)})

    # TODO: close d.b.

    print("ciws polys inserted", dbloc, ':', wtype, " ", str(len(cur_vil_et_gf)))

    # --------------------- carrots everywhere
    try:
        cur_vil_et_gf.to_sql("ciws_obs", cev_engine, if_exists='append', index=False,
                         dtype={'wx_geog': Geometry('Polygon', srid=4326)})
        print("ciws polys inserted TO CEV:", dbloc, ':', wtype, " ", str(len(cur_vil_et_gf)))
    except:
        print("BAD insert into cev")
    # --------------------- carrots everywhere

# ------------------------------------------------------------------------

# read geopackage of forecase vil polygons and insert into postgis

def read_and_insert_forecast_vil(a_file):

    print("read:", a_file)

    fcst_vil_gf = gpd.read_file(subdir + a_file)

    # ---- rename columns

    fcst_vil_gf.rename( columns={
        "fcst_analysis_id"     : "fcst_id",
        "fcst_analysis_time"   : "analysis_time",
        "fcst_sampling_time"   : "sampling_time"    }, inplace=True)

    # ---- oops!  forgot to do this!!!!!!!!!!!!!!!!!
    wtype = "unk"
    if re.search("Vil",a_file):
        wtype = "vil"
    if re.search("EchoTop",a_file):  # TODO: confirm spelling/case when retrieved
        wtype = "et"

    fcst_vil_gf['wtype'] = wtype

    # ---- see if good file:

    if len(fcst_vil_gf) == 0:
        print("fcst vil is EMPTY.")
        return(None)

    # ---- and turn GeoDataFrame into a pandas DataFrame with WKT in the geom column

    try:
        fcst_vil_gf['wx_geog'] = fcst_vil_gf['geometry']    \
                              .apply(lambda x: WKTElement(x.wkt, srid=4326))
    except:
        print("fcst vil problem with wkt.")
        return(None)

    # ---- drop the geometry column as it is now duplicative
    fcst_vil_gf.drop('geometry', 1, inplace=True)

    # Use 'dtype' to specify column's type
    # For the geom column, we will use GeoAlchemy's type 'Geometry'

    fcst_vil_gf.to_sql("ciws_fcst", engine, if_exists='append', index=False,
                         dtype={'wx_geog': Geometry('Polygon', srid=4326)})

    print("fcst vil inserted", dbloc, ':', str(len(fcst_vil_gf)))

    # --------------------- carrots everywhere
    try:
        fcst_vil_gf.to_sql("ciws_fcst", cev_engine, if_exists='append', index=False,
                         dtype={'wx_geog': Geometry('Polygon', srid=4326)})
        print("fcst vil inserted TO CEV:", dbloc, ':', str(len(fcst_vil_gf)))
    except:
        print("BAD insert into cev")
    # --------------------- carrots everywhere
