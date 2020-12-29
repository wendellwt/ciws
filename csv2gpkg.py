#!/home/data/local/bin/python3.7

#############################################################################
#           union of .csv data of hdf into multipolygons                    #
#############################################################################

import csv
import geopandas as gpd
from shapely.ops import cascaded_union

# only make polygons if they contain more than this many 6km cells:
smallest_size_of_poly = 4

subdir = "files/"

# ======================================================================

def read_csv_make_polys(fn):

    # key   == tuple of (row,col)
    # value == polyogn id
    grid_of_polys = { }

    # key   == polygon id
    # value == list of (row,col) tuples that make up this poly
    poly_made_from = { }

    poly_id = 1  # id of next poly to use

    with open(fn, newline='') as csvfile:
        spamreader = csv.reader(csvfile, delimiter=',', quotechar='"')
        for row in spamreader:

            xrow = int(row[1])
            xcol = int(row[2])

            #print("looking at: row=", xrow, " gcol=", xcol)

            gkey  = (xrow,   xcol  )
            below = (xrow-1, xcol  )
            left  = (xrow,   xcol-1)

            # -------------------------------------
            if below in grid_of_polys:
                if left in grid_of_polys:

                    # add myself and all polys to left to the one below
                    # print(">> has both below and left")

                    # first, add ourselves to below poly
                    below_pid = grid_of_polys[below]
                    grid_of_polys[gkey] = below_pid

                    poly_made_from[below_pid].add(gkey)

                    # next add polys of left's id to the one from below

                    left_pid  = grid_of_polys[left]

                    change_these = poly_made_from[left_pid]

                    poly_made_from[below_pid] = poly_made_from[below_pid].union(change_these)

                    # print("b.6, len=", len(change_these))

                    for chg_key in change_these:
                        grid_of_polys[chg_key] = below_pid

                    # and remove the old one
                    # but NOT if they're the same!
                    if left_pid != below_pid:
                        del poly_made_from[left_pid]

                else:
                    # add this to below poly
                    # print(">> has below only")

                    below_pid = grid_of_polys[below]
                    grid_of_polys[gkey] = below_pid
                    poly_made_from[below_pid].add(gkey)

            else:
                if left in grid_of_polys:

                    # add this to left poly
                    # print(">> has left only")

                    left_pid = grid_of_polys[left]
                    grid_of_polys[gkey] = left_pid
                    poly_made_from[left_pid].add(gkey)


                else:
                    # begin new polygon
                    # print(">> has neither left nor below")
                    grid_of_polys[gkey] = poly_id
                    poly_made_from[poly_id] = set( [gkey,] )
                    poly_id += 1

            # -------------------------------------

    #print("grid=", grid_of_polys)
    #print("polys=", poly_made_from)

    return(poly_made_from)

# =======================================================================

def convert_poly_list_to_gpd(poly_made_from, entire_grid):

    # start with an empty dataframe
    d = {'id': [], 'geom': []}
    gdf = gpd.GeoDataFrame(d, geometry='geom', crs="EPSG:4326")

    #gdf.set_geometry('geom', inplace=True)  # make sure it is a GeoPandas
    # however, the above may cause errors

    for k in poly_made_from:
        if len(poly_made_from[k]) > smallest_size_of_poly:

            as_strings = [str(t[0]) + '.' + str(t[1]) for t in poly_made_from[k]]

            a_poly = entire_grid.loc[ entire_grid['row_col'].isin(as_strings) ]

            boundary = gpd.GeoSeries(cascaded_union(a_poly['poly']))

            gdf = gdf.append({'id':k, 'geom':boundary[0]}, ignore_index=True)

    print("poly loop finished.")

    gdf.set_geometry('geom', inplace=True)  # make sure it is a GeoPandas

    return(gdf)

#####################################################################

def convert_csv_to_gpkg(fn, entire_grid):

    poly_made_from = read_csv_make_polys(subdir + fn)

    # ---- print out some stuff
    #for k in poly_made_from:
    #    if len(poly_made_from[k]) > smallest_size_of_poly:
    #        print(k, len(poly_made_from[k]), poly_made_from[k])

    gdf = convert_poly_list_to_gpd(poly_made_from, entire_grid)

    # change name from .csv as needed
    ofn  = subdir + fn[:-4] + ".gpkg"
    gjfn = subdir + fn[:-4] + ".gjson"

    if len(gdf) > 0:

        gdf.to_file(ofn, driver="GPKG")
        print("written: ", ofn)

        # and write the same out to geojson for testing in ipyleaflet
        gdf.to_file(gjfn, driver="GeoJSON")
        print("written: ", gjfn)

    else:
        print("not writing 0-length files")

    return(gdf)

