######## PROTOCOLO AUTOMATICO PARA LA GENERACION DE INDICES APLICADOS #######
#######        A AGUAS CONTINENTALES CON LANDSAT 8 Y SENTINEL 2        ######
######                                                                  #####
####                        Autor: Diego Garcia Diaz                     ####
###                      email: digd.geografo@gmail.com                   ###
##            GitHub: https://github.com/Digdgeo/Landsat8_Corrad_Embalses  ##
#                        Sevilla 01/01/2016-28/02/2016                      #

# coding: utf-8

import os, shutil, re, time, subprocess, pandas, rasterio, sys, urllib, fiona, sqlite3
import numpy as np
import matplotlib.pyplot as plt
from osgeo import gdal, gdalconst
from datetime import datetime, date

class Product(object):
    
    def __init__(self, shape, ruta_rad, rec = "NO"):
        
        self.shape = shape
        self.ruta_escena = ruta_rad
        self.escena = os.path.split(self.ruta_escena)[1]
        self.rad = os.path.split(self.ruta_escena)[0]
        self.raiz = os.path.split(self.rad)[0]
        self.ori = os.path.join(self.raiz, os.path.join('ori', self.escena))
        self.data = os.path.join(self.raiz, 'data')
        self.temp = os.path.join(self.data, 'temp')
        self.productos = os.path.join(self.raiz, 'productos')
        self.vals = {}
        self.rec = rec
        if 'l8oli' in self.ruta_escena:
            self.sat = 'L8'
        elif 'se2A' in self.ruta_escena:
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

            ######!!!!!!!!!!!HAY QUE INCLUIR LA ENTRADA EN LA TABLA ESCENAS CUANDO SEA S2A!!!!!!!!!!!!!!!!

            for i in os.listdir(self.ruta_escena):
                if re.search('tif$', i):
                    
                    banda = i[-7:-4]
                   
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

    
        #BASE DE DATOS SQLITE!
        #
        #
        #Abrimos la base de datos para rellenar los valores relativos a los puntos y a los indices
        #Lo primero vamos a rellenar la tabla con los puntos en el caso de que no estuvieran ya en la tabla
        #Si hubiera algun punto nuevo se anadiria, si no lo obviaria
        conn = sqlite3.connect(r'C:\Embalses\data\Embalses.db')
        cur = conn.cursor()
        print "Opened database successfully"       

        #Vamos a abrir el shape con fiona para obtener los datos
        vals = {}
        with fiona.open(self.shape, 'r') as shp:
            for i in shp.values():
                
                x = int(i['properties']['X'])
                y = int(i['properties']['Y'])
                id = i['properties']['id']#sacamos el id del punto
                emb = i['properties']['Nombre']

                self.vals[id] = [x, y, emb]

                cur.execute('''INSERT OR IGNORE INTO Puntos (id, Coordenada_X, Coordenada_Y, Nombre) 
                    VALUES ( ?, ?, ?, ?)''', (id, x, y, emb));
                #vals[id] = [x, y, emb]
                conn.commit()

        print 'puntos insertados en la Base de datos'

        for i in self.vals.items():
            print i        

        if self.sat == 'S2A':

            print self.sat

             #Creamos la base de datos y la primera tabla Escenas
        

            conn.execute('''CREATE TABLE IF NOT EXISTS 'Escenas' (
                            'Escena'    TEXT NOT NULL PRIMARY KEY UNIQUE,
                            'Sat' TEXT,
                            'Path'  INTEGER,
                            'Row'   INTEGER,
                            'Fecha_Escena'  DATE,
                            'Fecha_Procesado'   DATETIME
                            )''');

            try:

                cur.execute('''INSERT OR REPLACE INTO Escenas (Escena, Sat, Fecha_Escena, Fecha_Procesado) 
                    VALUES ( ?, ?, ?, ?)''', (self.escena, self.sat, date(int(self.escena[:4]), int(self.escena[4:6]), int(self.escena[6:8])), datetime.now()));
                conn.commit()
                print 'escena insertada en la base de datos'

            except Exception as e: 
                
                print e

        conn.close()

        

    def get_val_indice(self, raster, indice, desc):

        '''con este metodo cogeremos los valores de las formulas y los escribiremos en la base de datos'''

        #Abrimos la base de datos
        conn = sqlite3.connect(r'C:\Embalses\data\Embalses.db')
        cur = conn.cursor()
        print "Opened database successfully"

        cur.execute('''INSERT OR IGNORE INTO Indices (id, indice) 
                    VALUES (?, ?)''', (indice, desc));
        conn.commit()
        
        #abrimos el raster e insertamos los indices
        with rasterio.open(raster) as src:  
                
            for n, e in self.vals.items():
                for val in src.sample([(e[0], e[1])]):
                    if str(float(val)) != '-3.40282346639e+38' or str(float(val)) != '-999.0':
                        #insertamos los valores en la tabla Puntos-Indices. Hay que coger el id del indice y el id de Puntos-Escenas. Consulta!!?!?!?!?
                        cur.execute('''INSERT OR REPLACE INTO Puntos_Indices (id_indices, id_puntos, id_escenas, valor) 
                    VALUES ( ?, ?, ?, ?)''', (indice, n, self.escena, float(val)));
                
                conn.commit()
                
        conn.close()

    def recorte(self, escena, raster):

        outrec = raster[:-4] + '_rec.img'
        print outrec
        
        for i in os.listdir(escena):
            if i.endswith('Fmask.img') | i.endswith('Fmask.TIF'):
                cloud = os.path.join(escena, i)
                
        with rasterio.open(cloud) as src:
            CLOUD = src.read()
            
        with rasterio.open(raster) as ind:
            INDICE = ind.read()
           
        indicerec = np.where(CLOUD == 1, INDICE, np.nan)
        
        profile = ind.meta
        profile.update(dtype=rasterio.float32)

        with rasterio.open(outrec, 'w', **profile) as dst:
            dst.write(indicerec.astype(rasterio.float32)) 

    def ndvi(self):

        indice = 'ndvi'
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

        elif self.sat == 'S2A':

            with rasterio.open(self.b6) as nir:
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

    
############################CLOROFILA########################################

class Clorofila(Product):

    def OC4v4(self):

        indice = 'OC4v4'
        desc = 'Ocean Colour Scene NASA Algoritm v.4'
        enlace = ''
        outfile = os.path.join(self.productos, self.escena + '_OC4v4.img')
        #Fuente: SeaWiFS Postlaunch Calibration and Validation Analyses, Part 3
        print outfile

        #Son las mismas bandas para Sentinel 2 que para Landsat 8

        with rasterio.open(self.b1) as banda1:  
            B1 = banda1.read()
        
        with rasterio.open(self.b2) as banda2:
            B2 = banda2.read()

        with rasterio.open(self.b3) as banda3:
            B3 = banda3.read()
            
        maxi = np.maximum(B1, np.maximum(B2, B3))

        ######FORMULA OC4v4########

        R = np.log10(maxi)
        exp = 0.366 - (3.067 * R) + (1.930 *  np.power(R, 2)) + (0.649 * np.power(R, 3)) - (1.532 * np.power(R, 4))
        OC4 = np.true_divide(np.power(10,exp), 1000)

        profile = banda1.meta
        profile.update(dtype=rasterio.float32)

        with rasterio.open(outfile, 'w', **profile) as dst:
            dst.write(OC4.astype(rasterio.float32))

        self.get_val_indice(outfile, indice, desc)


    def OC4v6(self):

        indice = 'OC4v6'
        desc = 'Ocean Colour Scene NASA Algoritm v.6'
        enlace = 'An Introduction to Ocean Remote Sensing, pag 176' #http://oceancolor.gsfc.nasa.gov/cms/reprocessing/r2009/ocv6
        outfile = os.path.join(self.productos, self.escena + '_OC4v6.img')
        #Fuente: SeaWiFS Postlaunch Calibration and Validation Analyses, Part 3
        print outfile

        #Son las mismas bandas para Sentinel 2 que para Landsat 8

        with rasterio.open(self.b1) as banda1:  
            B1 = banda1.read()
        
        with rasterio.open(self.b2) as banda2:
            B2 = banda2.read()

        with rasterio.open(self.b3) as banda3:
            B3 = banda3.read()
            
        
        ######FORMULA OC4v6. OROGINALMENTE TAMBIEN CONTEMPLA PARA CALCULAR 'R' EL MAXIMO DEL RATIO 510/555 PERO NI EN L8 NI S2A HAY BANDA PARA 510########

        R = np.log10(np.maximum(np.true_divide(B1,B3), np.true_divide(B2,B3)))
        exp = 0.372 - (2.994 * R) + (2.722 *  np.power(R, 2)) - (1.226 * np.power(R, 3)) - (0.568 * np.power(R, 4))
        OC4v6 = np.power(10,exp)

        profile = banda1.meta
        profile.update(dtype=rasterio.float32)

        with rasterio.open(outfile, 'w', **profile) as dst:
            dst.write(OC4v6.astype(rasterio.float32))

        
        self.get_val_indice(outfile, indice, desc)
        self.recorte(self.ori, outfile)
        


    def OC3M_551(self):

        indice = 'OC3M_551'
        desc = 'Ocean Colour Scene NASA Algoritm v.4'
        enlace = 'http://oceancolor.gsfc.nasa.gov/cms/reprocessing/r2009/ocv6'
        outfile = os.path.join(self.productos, self.escena + '_OC3M_551.img')
        
        print outfile

        #Son las mismas bandas para Sentinel 2 que para Landsat 8

        with rasterio.open(self.b1) as banda1:  
            B1 = banda1.read()
        
        with rasterio.open(self.b2) as banda2:
            B2 = banda2.read()

        with rasterio.open(self.b3) as banda3:
            B3 = banda3.read()
            
        blue = np.maximum(B1,B2)

        ######FORMULA OC4########

        R = np.log10(np.true_divide(blue, B3))
        exp = 0.2424 - (2.5828 * R) + (1.7057 *  np.power(R, 2)) - (0.3415 * np.power(R, 3)) - (0.8818 * np.power(R, 4))
        OC3M = np.power(10,exp)

        profile = banda1.meta
        profile.update(dtype=rasterio.float32)

        with rasterio.open(outfile, 'w', **profile) as dst:
            dst.write(OC3M.astype(rasterio.float32))

        self.get_val_indice(outfile, indice, desc)


    def OC2(self):

        indice = 'OC2v4'
        desc = 'Ocean Colour Scene NASA Algoritm OC2v.4'
        outfile = os.path.join(self.productos, self.escena + '_OC2_3.img')
        #Fuente: SeaWiFS Postlaunch Calibration and Validation Analyses, Part 3
        print outfile

        #Son las mismas bandas para Sentinel 2 que para Landsat 8

        with rasterio.open(self.b3) as banda3:
            B3 = banda3.read()

        ######FORMULA OC2########

        R = np.log10(B3)
        exp = (0.319 - (2.336 * R) + (0.879 *  R**2) + (0.879 * R**2) - (0.135 * R**3) - 0.071)
        OC2 = 10**exp 

        profile = banda3.meta
        profile.update(dtype=rasterio.float32)

        with rasterio.open(outfile, 'w', **profile) as dst:
            dst.write(OC2.astype(rasterio.float32))

        self.get_val_indice(outfile, indice, desc)


    def OC3(self):

        indice = 'OC3'
        desc = 'Ocean Colour Scene NASA Algoritm v.3'
        outfile = os.path.join(self.productos, self.escena + '_OC3.img')
        print outfile
        
        #Son las mismas bandas para Sentinel 2 que para Landsat 8

        with rasterio.open(self.b1) as banda1:  
            B1 = banda1.read()
        
        with rasterio.open(self.b2) as banda2:
            B2 = banda2.read()

        with rasterio.open(self.b3) as banda3:
            B3 = banda3.read()
            
        maxi = np.maximum(B1, B2)

        ######FORMULA OC3########

        #COEF = np.true_divide(maxi, B3)

        R = np.log10(np.true_divide(maxi, B3))
        exp = (0.2830 * R) + (2.753 * R) + (1.457 *  np.power(R, 2)) + (0.659 * np.power(R, 3)) - (1.403 * np.power(R, 4))
        OC3 = np.power(10,exp) #cambiado exp por power aqui

        profile = banda1.meta
        profile.update(dtype=rasterio.float32)

        with rasterio.open(outfile, 'w', **profile) as dst:
            dst.write(OC3.astype(rasterio.float32))

        self.get_val_indice(outfile, indice, desc)



    def OC3M(self):

        indice = 'OC3Mv6'
        desc = 'Ocean Colour Scene NASA Algoritm OC3 for MODIS (Landsat 8) bands'
        Enlace = 'An Introduction to Ocean Remote Sensing, pag 179'
        outfile = os.path.join(self.productos, self.escena + '_OC3Mv6.img')
        print outfile
        
        #Son las mismas bandas para Sentinel 2 que para Landsat 8

        with rasterio.open(self.b1) as banda1:  
            B1 = banda1.read()
        
        with rasterio.open(self.b2) as banda2: 
            B2 = banda2.read()

        with rasterio.open(self.b3) as banda3:
            B3 = banda3.read()
        

        R = np.log10(np.maximum(np.true_divide(B1,B3), np.true_divide(B2,B3)))
        
        ######FORMULA OC3########

        #COEF = np.true_divide(maxi, B3)

        exp = 0.2424 - (2.742 * R) + (1.802 * np.power(R,2)) + (0.002 * np.power(R,3)) - (1.228 * np.power(R,4))
        OC3Mv6 = np.power(10,exp)

        profile = banda1.meta
        profile.update(dtype=rasterio.float32)

        with rasterio.open(outfile, 'w', **profile) as dst:
            dst.write(OC3Mv6.astype(rasterio.float32))

        self.get_val_indice(outfile, indice, desc)



    def P_Mayo(self):

        indice = 'P_Mayo'
        desc = 'Mayo et al 2015. ProtocoloF'
        outfile = os.path.join(self.productos, self.escena + '_PMayo.img')
        print outfile
        
        #Son las mismas bandas para Sentinel 2 que para Landsat 8

        with rasterio.open(self.b4) as banda4:  
            B4 = banda4.read()
        
        with rasterio.open(self.b2) as banda2:
            B2 = banda2.read()

        with rasterio.open(self.b3) as banda3:
            B3 = banda3.read()

        ######FORMULA Mayo et al 1995########

        div = np.true_divide((B2-B4), B3)
        exp = -0.98
        chl = 0.164*np.power(div, exp)
        #0.164*((B2-B4)/B3)**-0.98

        profile = banda2.meta
        profile.update(dtype=rasterio.float32)

        with rasterio.open(outfile, 'w', **profile) as dst:
            dst.write(chl.astype(rasterio.float32))

        self.get_val_indice(outfile, indice, desc)


    def P_Gilardino(self):

        indice = 'P_Gilardino'
        desc = 'Mayo et al 2001. ProtocoloF'
        outfile = os.path.join(self.productos, self.escena + '_PGilardino.img')
        print outfile
        
        #Son las mismas bandas para Sentinel 2 que para Landsat 8

        with rasterio.open(self.b1) as banda1:  
            B1 = banda1.read()
        
        with rasterio.open(self.b2) as banda2:
            B2 = banda2.read()

        ######FORMULA Gilardino et al 2001########

        chl = (11.28 * B1) - (8.96 * B2) - 3.28
        
        profile = banda2.meta
        profile.update(dtype=rasterio.float32)

        with rasterio.open(outfile, 'w', **profile) as dst:
            dst.write(chl.astype(rasterio.float32))

        self.get_val_indice(outfile, indice, desc)


    def fai(self):

        ''' Floating Algae Index Referencia XXX PONER UN NOMBRE-CODIGO PARA LA REFERENCIA'''

        outfile = os.path.join(self.productos, self.escena + '_fai.img')
        print outfile
        
        if self.sat == 'L8':

            with rasterio.open(self.b4) as red:
                RED = red.read()
                
            with rasterio.open(self.b5) as nir:
                NIR = nir.read()

            with rasterio.open(self.b6) as swir1:
                SWIR1 = swir1.read()
            
            num = NIR-RED
            den = SWIR1-RED
            res = num/den
            fai = RED + res * (SWIR1-RED)
            
            profile = nir.meta
            profile.update(dtype=rasterio.float32)

            with rasterio.open(outfile, 'w', **profile) as dst:
                dst.write(fai.astype(rasterio.float32)) 

        elif self.sat == 'S2A':

            with rasterio.open(self.b4) as red:
                RED = red.read()
                
            with rasterio.open(self.b8) as nir:
                NIR = nir.read()

            with rasterio.open(self.b11) as swir1:
                SWIR1 = swir1.read()
            
            num = NIR-RED
            den = SWIR1-RED
            res = num/den
            fai = RED + res * (SWIR1-RED)
            
            profile = nir.meta
            profile.update(dtype=rasterio.float32)

            with rasterio.open(outfile, 'w', **profile) as dst:
                dst.write(fai.astype(rasterio.float32))


    def evi(self):

        ''' Floating Algae Index Referencia XXX PONER UN NOMBRE-CODIGO PARA LA REFERENCIA'''

        outfile = os.path.join(self.productos, self.escena + '_evi.img')
        print outfile
        
        if self.sat == 'L8':

            with rasterio.open(self.b4) as red:
                RED = red.read()
                
            with rasterio.open(self.b5) as nir:
                NIR = nir.read()

            with rasterio.open(self.b2) as blue:
                BLUE = blue.read()
            
            num = NIR-RED
            den = NIR + (6 * RED - 7.5 * BLUE + 1)
            res = num/den
            evi = 2.5 * res
            
            profile = nir.meta
            profile.update(dtype=rasterio.float32)

            with rasterio.open(outfile, 'w', **profile) as dst:
                dst.write(evi.astype(rasterio.float32)) 

        elif self.sat == 'S2A':

            with rasterio.open(self.b4) as red:
                RED = red.read()
                
            with rasterio.open(self.b8) as nir:
                NIR = nir.read()

            with rasterio.open(self.b2) as blue:
                BLUE = blue.read()
            
            num = NIR-RED
            den = NIR + (6 * RED - 7.5 * BLUE + 1)
            res = num/den
            evi = 2.5 * res
            
            profile = nir.meta
            profile.update(dtype=rasterio.float32)

            with rasterio.open(outfile, 'w', **profile) as dst:
                dst.write(evi.astype(rasterio.float32)) 


    def ndci(self):

        ''' Normalized Difference Chlorophyll Index Referencia (es la misma formula que el Chlorophyll Spectral Index CSI) XXX PONER UN NOMBRE-CODIGO PARA LA REFERENCIA
        ESTE METODO SOLO SE PUEDE APLICAR A SENTINEL 2!!!'''

        outfile = os.path.join(self.productos, self.escena + '_ndci.img')
        print outfile
        
        if self.sat == 'S2A':

            with rasterio.open(self.b4) as red:
                RED = red.read()
                
            with rasterio.open(self.b5) as red_edge1:
                RE1 = red_edge1.read()
            
            num = RE1-RED
            den = RE1+RED
            ndci = np.true_divide(num,den)
            
            profile = red.meta
            profile.update(dtype=rasterio.float32)

            with rasterio.open(outfile, 'w', **profile) as dst:
                dst.write(ndci.astype(rasterio.float32)) 

        else:

            print 'Este metodo solo es aplicable a Sentinel 2'


    def ndci2(self):

        ''' Normalized Difference Chlorophyll Index Referencia XXX PONER UN NOMBRE-CODIGO PARA LA REFERENCIA
        Gitelson et al 2006. ESTE METODO SOLO SE PUEDE APLICAR A SENTINEL 2!!!'''

        outfile = os.path.join(self.productos, self.escena + '_ndci2.img')
        print outfile
        
        if self.sat == 'S2A':

            with rasterio.open(self.b4) as red:
                RED = red.read()
                
            with rasterio.open(self.b5) as red_edge1:
                RE1 = red_edge1.read()

            with rasterio.open(self.b6) as red_edge2:
                RE2 = red_edge2.read()
            
            num = RE2
            den = RE1-RED
            ndci2 = num/den
            
            profile = red.meta
            profile.update(dtype=rasterio.float32)

            with rasterio.open(outfile, 'w', **profile) as dst:
                dst.write(ndci2.astype(rasterio.float32)) 

        else:

            print 'Este metodo solo es aplicable a Sentinel 2'


    def ndbi(self):

        ''' Floating Algae Index Referencia XXX PONER UN NOMBRE-CODIGO PARA LA REFERENCIA'''

        outfile = os.path.join(self.productos, self.escena + '_ndbi.img')
        print outfile
        
        if self.sat == 'L8' or self.sat == 'S2A':

            with rasterio.open(self.b4) as red:
                RED = red.read()
                
            with rasterio.open(self.b3) as green:
                GREEN = green.read()
            
            num = GREEN-RED
            den = GREEN+RED
            ndbi = num/den
                       
            profile = red.meta
            profile.update(dtype=rasterio.float32)

            with rasterio.open(outfile, 'w', **profile) as dst:
                dst.write(ndbi.astype(rasterio.float32)) 



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


    def Gitelson_Blue(self):

        indice = 'GBlue'
        desc = 'Ocean Colour Scene NASA Algoritm OC3 for MODIS (Landsat 8) bands'
        Enlace = 'An Introduction to Ocean Remote Sensing, pag 179'
        outfile = os.path.join(self.productos, self.escena + '_GBlue.img')
        print outfile
        
        #Son las mismas bandas para Sentinel 2 que para Landsat 8

        with rasterio.open(self.b1) as banda1:  
            B1 = banda1.read()
                
        ######FORMULA########

        GBlue = 2.45 + 0.16 * B1

        profile = banda1.meta
        profile.update(dtype=rasterio.float32)

        with rasterio.open(outfile, 'w', **profile) as dst:
            dst.write(GBlue.astype(rasterio.float32))

        self.get_val_indice(outfile, indice, desc)


    def Gitelson_Red(self):

        indice = 'GRed'
        desc = 'Ocean Colour Scene NASA Algoritm OC3 for MODIS (Landsat 8) bands'
        Enlace = 'An Introduction to Ocean Remote Sensing, pag 179'
        outfile = os.path.join(self.productos, self.escena + '_GRed.img')
        print outfile
        
        #Son las mismas bandas para Sentinel 2 que para Landsat 8

        with rasterio.open(self.b4) as banda4:  
            B4 = banda4.read()
                
        ######FORMULA########

        GRed = -1.46 + 0.195 * B4

        profile = banda4.meta
        profile.update(dtype=rasterio.float32)

        with rasterio.open(outfile, 'w', **profile) as dst:
            dst.write(GRed.astype(rasterio.float32))

        self.get_val_indice(outfile, indice, desc)
                            

class ficocianina(Product):

    def Gitelson(self):

        indice = 'Gitelson'
        desc = 'Ocean Colour Scene NASA Algoritm OC3 for MODIS (Landsat 8) bands'
        Enlace = 'An Introduction to Ocean Remote Sensing, pag 179'
        outfile = os.path.join(self.productos, self.escena + '_Gitelson.img')
        print outfile
        
        #Son las mismas bandas para Sentinel 2 que para Landsat 8

        with rasterio.open(self.b4) as banda4:  
            B4 = banda4.read()
                
        ######FORMULA########

        GRed = -2.85 + 0.145 * B4

        profile = banda4.meta
        profile.update(dtype=rasterio.float32)

        with rasterio.open(outfile, 'w', **profile) as dst:
            dst.write(GRed.astype(rasterio.float32))

        self.get_val_indice(outfile, indice, desc)


    def Bennett_Borogard(self):

        indice = 'BB'
        desc = 'Remote detection and seasonal patterns of phycocyanin, carotenoid and chlorophyll pigments in eutrophic waters'
        #Enlace = 'https://www.researchgate.net/publication/285636096_Remote_detection_and_seasonal_patterns_of_phycocyanin_carotenoid_and_chlorophyll_pigments_in_eutrophic_waters'
        outfile = os.path.join(self.productos, self.escena + '_BB.img')
        print outfile
        
        #Son las mismas bandas para Sentinel 2 que para Landsat 8

        with rasterio.open(self.b4) as banda4:  
            B4 = banda4.read()
                
        ######FORMULA########

        GRed = -2.85 + 0.145 * B4

        profile = banda4.meta
        profile.update(dtype=rasterio.float32)

        with rasterio.open(outfile, 'w', **profile) as dst:
            dst.write(GRed.astype(rasterio.float32))

        self.get_val_indice(outfile, indice, desc)


    def CdomAbs(self):


        indice = 'CDomAbs420'
        desc = 'Using_Satellite_Remote_Sensing_to_Estimate_the_Colored_Dissolved_Organic_Matter_Absorption_Coefficient_in_Lakes'
        #Enlace = 'https://www.researchgate.net/publication/225543129_Using_Satellite_Remote_Sensing_to_Estimate_the_Colored_Dissolved_Organic_Matter_Absorption_Coefficient_in_Lakes'
        outfile = os.path.join(self.productos, self.escena + '_CDom420.img')
        print outfile
        
        #Son las mismas bandas para Sentinel 2 que para Landsat 8

        with rasterio.open(self.b2) as banda2:  
            B2 = banda2.read()

        with rasterio.open(self.b3) as banda3:  
            B3 = banda3.read()
                
        ######FORMULA########

        CDom420 = 5.20 * np.power(np.true_divide(B2,B3), -2.76)

        profile = banda3.meta
        profile.update(dtype=rasterio.float32)

        with rasterio.open(outfile, 'w', **profile) as dst:
            dst.write(CDom420.astype(rasterio.float32))

        self.get_val_indice(outfile, indice, desc)



    def Dsung(self):


        indice = 'Dsung'
        desc = 'Estimating phycocyanin pigment concentration in productive inland waters using Landsat measurements: A case study in Lake Dianchi'
        #Enlace = 'https://www.osapublishing.org/view_article.cfm?gotourl=https%3A%2F%2Fwww.osapublishing.org%2FDirectPDFAccess%2F77639BCA-9CAF-CD02-8727ADD9C68E55B9_311043%2Foe-23-3-3055.pdf%3Fda%3D1%26id%3D311043%26seq%3D0%26mobile%3Dno&org='
        outfile = os.path.join(self.productos, self.escena + '_Dsung.img')
        print outfile
        
        #Son las mismas bandas para Sentinel 2 que para Landsat 8

        with rasterio.open(self.b2) as banda2:  
            B2 = banda2.read()

        with rasterio.open(self.b3) as banda3:  
            B3 = banda3.read()

        with rasterio.open(self.b4) as banda4:  
            B4 = banda4.read()

        if self.sat == 'L8':

            with rasterio.open(self.b5) as banda5:  
                B5 = banda5.read()

        else:

            with rasterio.open(self.b8) as banda8:  
                B5 = banda8.read()
                
        ######FORMULA########

        exp = 23.639 + (134.985 * B2) - (245.109 * B3) + (305.103 * B4) - (40.472 * B5) - (7.864 * np.true_divide(B5,B4)) + (6.471 * np.true_divide(B5,B3)) + (4.732 * np.true_divide(B5,B2)) - (35.839 * np.true_divide(B4,B3)) + (7.915 * np.true_divide(B4,B2)) - (4.867 * np.true_divide(B3,B2))

        PC = np.power(10,exp)

        profile = banda3.meta
        profile.update(dtype=rasterio.float32)

        with rasterio.open(outfile, 'w', **profile) as dst:
            dst.write(PC.astype(rasterio.float32))

        self.get_val_indice(outfile, indice, desc)