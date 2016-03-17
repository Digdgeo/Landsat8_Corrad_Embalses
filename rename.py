######## PROTOCOLO AUTOMATICO PARA LA CORRECCION RADIOMETRICA DE ESCENAS LANDSAT 8 #######
######                                                                              ######
####                        Autor: Diego Garcia Diaz                                  ####
###                      email: digd.geografo@gmail.com                                ###
##            GitHub: https://github.com/Digdgeo/Landsat8_Corrad_Embalses               ##
#                        Sevilla 01/01/2016-31/03/2016                                   #

import os, time, re

def rename(ruta):

    '''Esta funcion hace el rename de todas las escenas en una carpeta (por defecto 'C:\Embalses\ori'), desde su nomenclatura en formato USGS 
    al formato YearMonthDaySatPath_Row. Funciona para Landsat5-8. Si hubiera algun problema como posibles escenas duplicadas, 
    imprime la escena que da error y pasa a la siguiente. Las escenas que va renombrando correctamente son impresas en tambien en pantalla

                    LC82020342014224LGN00 --->   20140812l8oli202_34

    '''

    sats = {'LC8': 'l8oli', 'LE7': 'l7etm', 'LT5': 'l5tm'}
    fecha=time.strftime("%d-%m-%Y")
    
    
    for i in os.listdir(ruta):
    
        if re.search("^L\S[0-9]", i) and os.path.isdir(os.path.join(ruta, i)):
            
            escena = os.path.join(ruta, i)
            sat = i[:3]
            path =  i[3:6]
            row = i[7:9]
            fecha = time.strptime(i[9:13] + " " + i[13:16], '%Y %j')
            year = str(fecha.tm_year)
            month = str(fecha.tm_mon)
            if len(month) == 1:
                month = '0' + month
            day = str(fecha.tm_mday)
            if len(day) == 1:
                day = '0' + day

            outname = os.path.join(ruta, year +  month  + day + sats[sat] + path + "_" + row)

            try:
                os.rename(escena, outname)
                print 'Escena', escena, 'renombrada a', outname
                
            except Exception as e:
                print e, escena
                continue
            
            
if __name__ == "__main__":
    rename(r'C:\Embalses\ori')