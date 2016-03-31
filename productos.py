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
    
    def __init__(self, ruta_rad, rec = "NO"):
        
        self.ruta_escena = ruta_rad
        self.escena = os.path.split(self.ruta_escena)[1]
        self.rad = os.path.split(self.ruta_escena)[0]
        self.raiz = os.path.split(self.rad)[0]
        self.ori = os.path.join(self.raiz, os.path.join('ori', self.escena))
        self.data = os.path.join(self.raiz, 'data')
        self.temp = os.path.join(self.data, 'temp')
        self.productos = os.path.join(self.raiz, 'productos')
        self.rec = rec
        if 'l8oli' in self.ruta_escena:
            self.sat = 'L8'
        elif 'S2A' in i:
            self.sat =  'S2A'
        else:
            print 'No identifico el satelite'

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
        
        elif self.sat == 'S2A':

            for i in os.listdir(self.ruta_escena):
                if re.search('img$', i):
                    
                    banda = i[-6:-4]

                    if banda == 'B01':
                        self.b1 = os.path.join(self.ruta_escena, i)
                    elif banda == 'B02':
                        self.b2 = os.path.join(self.ruta_escena, i)
                    elif banda == 'B03':
                        self.b3 = os.path.join(self.ruta_escena, i)
                    elif banda == 'B04':
                        self.b4 = os.path.join(self.ruta_escena, i)
                    elif banda == 'B05':
                        self.b5 = os.path.join(self.ruta_escena, i)
                    elif banda == 'B06':
                        self.b6 = os.path.join(self.ruta_escena, i)
                    elif banda == 'B07':
                        self.b7 = os.path.join(self.ruta_escena, i)
                    elif banda == 'B08':
                        self.b8 = os.path.join(self.ruta_escena, i)
                    elif banda == 'B8A':
                        self.b8a = os.path.join(self.ruta_escena, i)
                    elif banda == 'B09':
                        self.b9 = os.path.join(self.ruta_escena, i)
                    elif banda == 'B10':
                        self.b10 = os.path.join(self.ruta_escena, i)
                    elif banda == 'B11':
                        self.b11 = os.path.join(self.ruta_escena, i)

        
    def ndvi(self):
        
        outfile = os.path.join(self.productos, self.escena + '_ndvi.img')
        print outfile
        
        if self.sat == 'L8':
            
            with rasterio.open(self.b5) as nir:
                NIR = nir.read()
                
            with rasterio.open(self.b4) as red:
                RED = red.read()
            
            num = NIR-RED
            den = NIR+RED
            ndvi = num/den
            
            profile = nir.meta
            profile.update(dtype=rasterio.float32)

            with rasterio.open(outfile, 'w', **profile) as dst:
                dst.write(ndvi.astype(rasterio.float32)) 

    def ndwi(self):

        outfile = os.path.join(self.productos, self.escena + '_ndwi.img')
        print outfile
        
        if self.sat == 'L8':
            
            with rasterio.open(self.b5) as nir:
                NIR = nir.read()
                
            with rasterio.open(self.b3) as green:
                GREEN = green.read()
            
            num = GREEN-NIR
            den = GREEN+NIR
            ndvi = num/den
            
            profile = nir.meta
            profile.update(dtype=rasterio.float32)

            with rasterio.open(outfile, 'w', **profile) as dst:
                dst.write(ndvi.astype(rasterio.float32))


    def mndwi(self):

        outfile = os.path.join(self.productos, self.escena + '_mndwi.img')
        print outfile
        
        if self.sat == 'L8':
            
            with rasterio.open(self.b6) as swir1:
                SWIR1 = swir1.read()
                
            with rasterio.open(self.b3) as green:
                GREEN = green.read()
            
            num = GREEN-SWIR1
            den = GREEN+SWIR1
            ndvi = num/den
            
            profile = swir1.meta
            profile.update(dtype=rasterio.float32)

            with rasterio.open(outfile, 'w', **profile) as dst:
                dst.write(ndvi.astype(rasterio.float32)) 


    

#TURBIDEZ
class Turbidez(Product):


    def ntu_bus2009(self):
            
            outfile = os.path.join(self.productos, 'ntu_bus2009.img')
            print outfile
            
            if self.sat == 'L8':
                    
                with rasterio.open(self.b4) as red:
                    RED = red.read()
                    
                ntu = 1.195 + 14.45*RED
                
                profile = red.meta
                profile.update(dtype=rasterio.float32)

                with rasterio.open(outfile, 'w', **profile) as dst:
                    dst.write(ntu.astype(rasterio.float32)) 
                    
    def ntu_chen(self):
        
        outfile = os.path.join(self.productos, 'ntu_chen.img')
        print outfile
        
        if self.sat == 'L8':
                
            with rasterio.open(self.b3) as green:
                GREEN = green.read()
                
            ntu = -439.52 * GREEN + 22.913
            
            profile = green.meta
            profile.update(dtype=rasterio.float32)

            with rasterio.open(outfile, 'w', **profile) as dst:
                dst.write(ntu.astype(rasterio.float32)) 
    
    def wti(self):
        
        outfile = os.path.join(self.productos, 'wti.img')
        print outfile
        
        if self.sat == 'L8':
                
            with rasterio.open(self.b4) as red:
                RED = red.read()
                
            with rasterio.open(self.b5) as nir:
                NIR = nir.read()
                
            wti = 0.91 * RED + 0.43 * NIR
            
            profile = red.meta
            profile.update(dtype=rasterio.float32)

            with rasterio.open(outfile, 'w', **profile) as dst:
                dst.write(wti.astype(rasterio.float32)) 

    
#CLOROFILA



class Clorofila(Product):

    def gNdvi(self):

        outfile = os.path.join(self.productos, self.escena + '_gNdvi.img')
        print outfile
        
        if self.sat == 'L8':
            
            with rasterio.open(self.b5) as nir:
                NIR = nir.read()
                
            with rasterio.open(self.b3) as green:
                GREEN = green.read()
            
            num = NIR-GREEN
            den = NIR+GREEN
            ndvi = num/den
            
            profile = nir.meta
            profile.update(dtype=rasterio.float32)

            with rasterio.open(outfile, 'w', **profile) as dst:
                dst.write(ndvi.astype(rasterio.float32)) 
    

    def chla_Theologu_1(self):


        outfile = os.path.join(self.productos, 'chla_T1.img')

        if self.sat == 'L8':

            with rasterio.open(self.b2) as blue:
                BLUE = blue.read()

            with rasterio.open(self.b3) as green:
                GREEN = green.read()
                
            with rasterio.open(self.b4) as red:
                RED = red.read()
                
                            
            chla = (BLUE-RED)/GREEN
            
            profile = red.meta
            profile.update(dtype=rasterio.float32)

            with rasterio.open(outfile, 'w', **profile) as dst:
                dst.write(chla.astype(rasterio.float32))

        if self.rec == "NO":

            shape = os.path.join(self.data, 'Embalses.shp')
            crop = "-crop_to_cutline"
            
            #usamos Gdalwarp para realizar las mascaras, llamandolo desde el modulo subprocess
            cmd = ["gdalwarp", "-dstnodata" , "0" , "-cutline", ]
            path_masks = os.path.join(self.temp, 'masks')
            if not os.path.exists(path_masks):
                os.makedirs(path_masks)

            salida = os.path.join(path_masks, 'Embalses_chla_1.TIF')
            cmd.insert(4, shape)
            cmd.insert(5, crop)
            cmd.insert(6, outfile)
            cmd.insert(7, salida)

            proc = subprocess.Popen(cmd,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
            stdout,stderr=proc.communicate()
            exit_code=proc.wait()

            if exit_code: 
                raise RuntimeError(stderr)

    def chla_Theologu_2(self):


        outfile = os.path.join(self.productos, 'chla_T2.img')

        if self.sat == 'L8':

            with rasterio.open(self.b2) as blue:
                BLUE = blue.read()

            with rasterio.open(self.b5) as nir:
                NIR = nir.read()
                           
                            
            chla = BLUE-NIR
            
            profile = blue.meta
            profile.update(dtype=rasterio.float32)

            with rasterio.open(outfile, 'w', **profile) as dst:
                dst.write(chla.astype(rasterio.float32))

        if self.rec == "NO":

            shape = os.path.join(self.data, 'Embalses.shp')
            crop = "-crop_to_cutline"
            
            #usamos Gdalwarp para realizar las mascaras, llamandolo desde el modulo subprocess
            cmd = ["gdalwarp", "-dstnodata" , "0" , "-cutline", ]
            path_masks = os.path.join(self.temp, 'masks')
            if not os.path.exists(path_masks):
                os.makedirs(path_masks)

            salida = os.path.join(path_masks, 'Embalses_chla_2.TIF')
            cmd.insert(4, shape)
            cmd.insert(5, crop)
            cmd.insert(6, outfile)
            cmd.insert(7, salida)

            proc = subprocess.Popen(cmd,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
            stdout,stderr=proc.communicate()
            exit_code=proc.wait()

            if exit_code: 
                raise RuntimeError(stderr)
        

    def chla_Theologu_3(self):


        outfile = os.path.join(self.productos, 'chla_T3.img')

        if self.sat == 'L8':

            with rasterio.open(self.b5) as nir:
                NIR = nir.read()
                
            with rasterio.open(self.b6) as swir1:
                SWIR1 = swir1.read()           
                            
            chla = np.exp(NIR/SWIR1)
            
            profile = nir.meta
            profile.update(dtype=rasterio.float32)

            with rasterio.open(outfile, 'w', **profile) as dst:
                dst.write(chla.astype(rasterio.float32))

        if self.rec == "NO":

            shape = os.path.join(self.data, 'Embalses.shp')
            crop = "-crop_to_cutline"
            
            #usamos Gdalwarp para realizar las mascaras, llamandolo desde el modulo subprocess
            cmd = ["gdalwarp", "-dstnodata" , "0" , "-cutline", ]
            path_masks = os.path.join(self.temp, 'masks')
            if not os.path.exists(path_masks):
                os.makedirs(path_masks)

            salida = os.path.join(path_masks, 'Embalses_chla_3.TIF')
            cmd.insert(4, shape)
            cmd.insert(5, crop)
            cmd.insert(6, outfile)
            cmd.insert(7, salida)

            proc = subprocess.Popen(cmd,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
            stdout,stderr=proc.communicate()
            exit_code=proc.wait()

            if exit_code: 
                raise RuntimeError(stderr)


    def chla_Theologu_4(self):


        outfile = os.path.join(self.productos, 'chla_T4.img')

        if self.sat == 'L8':

            with rasterio.open(self.b2) as blue:
                BLUE = blue.read()
                
            with rasterio.open(self.b3) as green:
                GREEN = green.read()           
                            
            chla = BLUE-GREEN
            
            profile = blue.meta
            profile.update(dtype=rasterio.float32)

            with rasterio.open(outfile, 'w', **profile) as dst:
                dst.write(chla.astype(rasterio.float32)) 

        if self.rec == "NO":

            shape = os.path.join(self.data, 'Embalses.shp')
            crop = "-crop_to_cutline"
            
            #usamos Gdalwarp para realizar las mascaras, llamandolo desde el modulo subprocess
            cmd = ["gdalwarp", "-dstnodata" , "0" , "-cutline", ]
            path_masks = os.path.join(self.temp, 'masks')
            if not os.path.exists(path_masks):
                os.makedirs(path_masks)

            salida = os.path.join(path_masks, 'Embalses_chla_4.TIF')
            cmd.insert(4, shape)
            cmd.insert(5, crop)
            cmd.insert(6, outfile)
            cmd.insert(7, salida)

            proc = subprocess.Popen(cmd,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
            stdout,stderr=proc.communicate()
            exit_code=proc.wait()

            if exit_code: 
                raise RuntimeError(stderr)

    def chla_Theologu_5(self):


        outfile = os.path.join(self.productos, 'chla_T5.img')

        for i in os.listdir(self.ori):
            if i.endswith('Fmask.img') | i.endswith('Fmask.TIF'):
                cloud = os.path.join(self.ori, i)

        with rasterio.open(cloud) as src:
                CLOUD = src.read()

        if self.sat == 'L8':

            with rasterio.open(self.b1) as ca:
                CA = ca.read()
                
            with rasterio.open(self.b3) as green:
                GREEN = green.read()  
                            
            chla = CA-GREEN
            chla = np.where(CLOUD == 1, chla, np.nan)
            
            profile = ca.meta
            profile.update(dtype=rasterio.float32)

            with rasterio.open(outfile, 'w', **profile) as dst:
                dst.write(chla.astype(rasterio.float32)) 

        if self.rec != "NO":

            print "comenzando el recorte con los embalses"

            shape = os.path.join(self.data, 'Embalses.shp')
            crop = "-crop_to_cutline"
            
            #usamos Gdalwarp para realizar las mascaras, llamandolo desde el modulo subprocess
            cmd = ["gdalwarp", "-dstnodata" , "0" , "-cutline", ]
            path_masks = os.path.join(self.temp, 'masks')
            if not os.path.exists(path_masks):
                os.makedirs(path_masks)

            salida = os.path.join(path_masks, 'Embalses_chla_5.TIF')
            cmd.insert(4, shape)
            cmd.insert(5, crop)
            cmd.insert(6, outfile)
            cmd.insert(7, salida)

            proc = subprocess.Popen(cmd,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
            stdout,stderr=proc.communicate()
            exit_code=proc.wait()

            if exit_code: 
                raise RuntimeError(stderr)