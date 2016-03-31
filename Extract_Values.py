import os
from osgeo import gdal,ogr

#src_rs = r'C:\Embalses\productos\chla_T5.img' raster a extraer valores
#src_shp = r'C:\Users\Diego\Desktop\delete\puntosEmb.shp' shape de puntos para extraer los valores


def get_point_values(rs):

	src_shp = r'C:\Users\Diego\Desktop\delete\puntosEmb.shp' #El shape deberia ser siempre el mismo. 
	src_ds=gdal.Open(os.path.join(r'C:\Embalses\productos', rs) 
	gt=src_ds.GetGeoTransform()
	rb=src_ds.GetRasterBand(1)

	ds=ogr.Open(src_shp)
	lyr=ds.GetLayer()
	for feat in lyr:
	    geom = feat.GetGeometryRef()
	    mx,my=geom.GetX(), geom.GetY()
	    print mx, my
	    px = int((mx - gt[0]) / gt[1]) 
	    py = int((my - gt[3]) / gt[5]) 

	    intval=rb.ReadAsArray(px,py,1,1)
	    print intval[0] 

