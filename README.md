# Landsat8_Corrad_Embalses

Este repositorio guardará el código necesario para corregir radiométricamente, mediante el método de sustracción del objeto oscuro (DOS) escenas landsat 8 y aplicar en ellas distintos índices para optimizar la interpolación de distintos parámetros biofísicos medidos en embalses. La corrección de las escenas Landsat 8 se realiza con Python (GDAL y rasterio como librerías principales), para las escenas Sentinel 2A se usarán las imágenes corregidas vía Semi Automatic Classification Plugin de QGIS. 

El tratamiento posterior una vez obtenidas las reflectividades en superficie se lleva a cabo nuevamente en Python.

Fecha 2016/03/03
