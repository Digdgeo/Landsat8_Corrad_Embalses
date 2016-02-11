######## PROTOCOLO AUTOMATICO PARA LA GENERACION DE INDICES APLICADOS #######
#######        A AGUAS CONTINENTALES CON LANDSAT 8 Y SENTINEL 2        ######
######                                                                  #####
####                        Autor: Diego Garcia Diaz                     ####
###                      email: digd.geografo@gmail.com                   ###
##            GitHub: https://github.com/Digdgeo/Landsat8_Corrad_Embalses  ##
#                        Sevilla 01/01/2016-28/02/2016                      #

# coding: utf-8

import os, shutil, re, time, subprocess, pandas, rasterio, sys, urllib
import numpy as np
import matplotlib.pyplot as plt
from osgeo import gdal, gdalconst

class Product(object):
    
    def __init__(self, ruta_rad):
        
        self.ruta_escena = ruta_rad
        self.rad = os.path.split(self.ruta_escena)[0]
        self.raiz = os.path.split(self.rad)[0]
        self.productos = os.path.join(self.raiz, 'productos')
        if 'l8oli' in self.ruta_escena:
            self.sat = 'L8'
        else:
            self.sat =  'S2A'
        if self.sat == 'L8':
            for i in os.listdir(self.ruta_escena):
                if re.search('img$', i):
                    
                    banda = i[-6:-4]
                    
                    if banda == 'b1':
                        self.b1 = os.path.join(self.ruta_escena, i)
                    elif banda == 'b2':
                        self.b2 = os.path.join(self.ruta_escena, i)
                    elif banda == 'b3':
                        self.b3 = os.path.join(self.ruta_escena, i)
                    elif banda == 'b4':
                        self.b4 = os.path.join(self.ruta_escena, i)
                    elif banda == 'b5':
                        self.b5 = os.path.join(self.ruta_escena, i)
                    elif banda == 'b6':
                        self.b6 = os.path.join(self.ruta_escena, i)
                    elif banda == 'b7':
                        self.b7 = os.path.join(self.ruta_escena, i)
                    elif banda == 'b9':
                        self.b9 = os.path.join(self.ruta_escena, i)
                    
    def ndvi(self):
        
        outfile = os.path.join(self.productos, 'ndvi.img')
        print outfile
        
        if self.sat == 'L8':
            
            with rasterio.open(self.b5) as nir:
                NIR = nir.read()
                print NIR, NIR.min(), NIR.max()
                
            with rasterio.open(self.b4) as red:
                RED = red.read()
                
            ndvi = NIR-RED / NIR+RED
            
            profile = nir.meta
            profile.update(dtype=rasterio.float32)

            with rasterio.open(outfile, 'w', **profile) as dst:
                dst.write(ndvi.astype(rasterio.float32)) 