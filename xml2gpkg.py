#!/home/wendell/usr/local/bin/python3.7

############################################################################
#             parse Contour XML into GeoPandas df of polygons              #
############################################################################

import sys
import gzip
import datetime
import geopandas as gpd

from bs4 import BeautifulSoup
from shapely import wkt
from shapely.geometry import MultiPolygon

# Sean Giles said that he liked using "import xml.etree.ElementTree as ET";
# I tried it, but bs4 was easier

# ========================================================================

uninteresting = ( "procedure", "observedProperty",
                 "featureOfInterest", "parameter" )

#----  parse the xml of one of the (30, 60, and 120) forecasts

def do_forecast(fcst_elem):

    # ---- peek ahead and get one of the analysis times
    #      (we assume that they're all the same, so any one will suffice)

    fm = fcst_elem.find("wx:forecastAnalysisTime") # they're all the same, right?

    res_dct = {"fcst_analysis_id"   : fcst_elem["gml:id"],
               "fcst_analysis_time" : datetime.datetime.strptime(fm.text,
                                            "%Y-%m-%dT%H:%M:%SZ")
               }

    # hmm..., is this the 30, 60, 120 min for loop?

    for child in fcst_elem:
        #verb print("    ", child.name)

        if child.name in uninteresting:
            #verb print("        ignored")
            continue

        if child.name == "samplingTime":
            #verb print("         sampling:", child.find("timePosition").text)

            res_dct["fcst_sampling_time"] =      \
                     datetime.datetime.strptime(
                             child.find("timePosition").text,
                             "%Y-%m-%dT%H:%M:%SZ")

        if child.name == "forecastAnalysisTime":
            #verb print("         fcstAnaly:", child.find("timePosition").text)
            pass

        num_polys = 0
        list_of_polys = [ ]

        if child.name == "result":

            # wx:FeatureCollection
            #     wx:featureMember

            for fm in child.find_all("wx:featureMember"):
                num_polys += 1

                # not really needed if they're all put into one MultiPolygon:

                pshapely = decode_geom_into_shapely(fm.find("wx:geometry").text)

                list_of_polys.append(pshapely)

            res_dct["polys"] = MultiPolygon( list_of_polys )

    return(res_dct)

# ---------------------------------------------------------------

# begin really awkward way to put into shapely format...

def decode_geom_into_shapely(polly):

    without_comma = polly.split()

    # https://stackoverflow.com/questions/1621906/is-there-a-way-to-split-a-string-by-every-nth-separator-in-python
    # ISSUE: x,y, or lng,lat???

    bb = [" ".join(without_comma[i:i+2]) for i in range(0, len(without_comma), 2)]

    cc = [ bb[n].split()[1] + ' ' + bb[n].split()[0] for n in range(len(bb))]

    with_comma = ','.join(cc)

    pstr = "POLYGON((" + with_comma + "))"

    pshp = wkt.loads(pstr)

    return(pshp)

# =========================================================================

# the one child of that featureMember is one Forecast

def do_feat_memb(fm):

    for fcst in fm:
        res_dct = do_forecast(fcst)

    return(res_dct)

# =========================================================================

# the featureMembers of the top FeatureCollection are 3 featureMemebers
# which are the Forecasts

def do_top_level_fcoll(top_fc):

    fcst_list = [ ]

    for fc_fm in top_fc:
        fcst_item = do_feat_memb(fc_fm)
        fcst_list.append(fcst_item)

    return(fcst_list)

# #########################################################################

# read in the main xml file, parse the polygons, and return a geopandas df

def do_parse_xml(fn):

    # ---- 1) parse xml into list of dictionaries

    with gzip.open(fn) as fp:
        soup = BeautifulSoup(fp, "lxml-xml")

    for top_fc in soup:  # there had better only be one, and it had better
                         # be the top-level FeatureCollection

        res_lst = do_top_level_fcoll(top_fc)

    # ---- 2) convert dict to GeoPandas

    res_gf = gpd.GeoDataFrame(res_lst, geometry="polys", crs="EPSG:4326" )

    return(res_gf)

# ---------------------------------------------------------------------

# write out the geopandas df into a geopackage
#   (for potential testing in jupyter + ipyleaflet)

# note: fn is the _original_ filename, not this one

def write_gf(fn, res_gf):

    # ---- write (to same dir, w/o edu...ciws part, extension is '.gpkg')

    p = fn.find('/')
    dirn = fn[:p]       # dirname/
    ofn = fn[:-7]       # ".xml.gz"
    p = ofn.rfind('.')  # dirname/edu.mit.ll.wx.ciws."
    rsp_fn = dirn + '/' + ofn[p+1:] + ".gpkg"

    res_gf.to_file(rsp_fn, driver="GPKG")

    print("written to:", rsp_fn)
    return(rsp_fn)

# #########################################################################

if __name__ == "__main__":

    fn = "files/edu.mit.ll.wx.ciws.Standard_VilForecastContours_20200817T210000Z.xml.gz"
    fn = sys.argv[1]

    res_gf = do_parse_xml(fn)

    print(res_gf)

    write_gf(fn, res_gf)
