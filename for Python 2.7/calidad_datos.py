# -*- coding: utf-8 -*-

''' RIASC Data Quality Evaluation (RDQE) RDQE evaluates the quality of a data sample,
    considering the origin of the data (data source) and its typology.

    Copyright (C) 2019  by RIASC Universidad de Leon (Enrique Pinto González, Noemí De Castro García y Miguel Carriegos Vieira)
    This file is part of Data Quality Evaluation (RDQE)
    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.
    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.
    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <https://www.gnu.org/licenses/>.

	You can find more information about the project at https://github.com/amunc/DataQuality'''


# import itertools
# import multiprocessing as mp
# from joblib import Parallel, delayed
# import functools
from time import time
import lib_calidad_datos as cd



def main():
    """
    """

###############################################################################
#                                                                             #
# Lectura de las características de la muestra de datos. Carga de ficheros de #
# configuracion y carga del listado de ficheros de input                      #
#                                                                             #
###############################################################################

    # Lectura del separador de datos y periodo de la muestra de datos
    separador, data_period = cd.leer_caracteristicas_muestra()

    inicio = time()
    print "Calculando..."

    # Carga de los ficheros de parametrizacion de tipologias de eventos y de
    #   fuentes de datos (data_source.ini y event_typology.ini)
    data_source_parser = cd.cargar_configuracion_fuentes()
    event_typology_parser = cd.cargar_configuracion_tipologias()

    # Carga del listado de ficheros de input
    lista_ficheros_input = cd.cargar_ficheros_input()


###############################################################################
#                                                                             #
# Calculo de las dimensiones de calidad                                       #
#                                                                             #
###############################################################################

    # Cálculo de las dimensiones de cantidad, completitud, fiabilidad y severidad.
    #   Se realizará fichero a fichero (por chunks) después será neceario agrupar los resultados.
    valoracion = cd.valorar_dimensiones(lista_ficheros_input, separador, data_source_parser, event_typology_parser)

    # Agrupación de todos las valoraciones de los diferentes chunks de datos
    valoracion = cd.compute_valoracion(valoracion)

    # Cálculo del nivel de información (en este punto solo tenemos el número total de campos)
    valoracion = cd.valorar_nivel_informacion(valoracion)

    # Cálculo del precio por dato
    valoracion = cd.valorar_precio_por_dato(valoracion, data_period)


###############################################################################
#                                                                             #
# Calculo de las dimensiones de calidad normalizadas.                         #
#                                                                             #
###############################################################################

    # Cáclulo de cantidad normalizada
    valoracion = cd.calcular_cantidad_normalizada(valoracion)

    # Cálculo de dimensiones de calidad normalizadas
    valoracion = cd.calcular_valores_normalizados(valoracion)

    # Cálculo del precio por dato normalizado
    valoracion = cd.calcular_precio_normalizado(valoracion, event_typology_parser)


###############################################################################
#                                                                             #
# Calculo de los niveles de calidad para el informe y valoración de calidad   #
# de cada fuente por tipología.                                               #
#                                                                             #
###############################################################################

    # Cálculo de niveles
    valoracion = cd.calcular_niveles(valoracion, event_typology_parser)

    # Valoracion de calidad las fuentes, por tipologia:
    valoracion = cd.valorar_calidad_tipologia(valoracion)

    # Valoración de la exclusibidad de las fuentes, por tipologías:
    valoracion = cd.valorar_exclusividad(valoracion)


###############################################################################
#                                                                             #
# Valoracion total de la calidad las fuentes                                  #
#                                                                             #
###############################################################################

    valoracion_fuentes = cd.valorar_calidad_global(valoracion)

###############################################################################
#                                                                             #
# Generacion de informes                                                      #
#                                                                             #
###############################################################################

    cd.generar_informe_fuentes(valoracion, valoracion_fuentes)
    cd.generar_informe_tipologias(valoracion)
    cd.generar_informe_ranking(valoracion_fuentes)
    cd.generar_plots(valoracion, 'Tipologia')
    cd.generar_plots(valoracion, 'Data source')


    fin = time()
    tiempo = fin - inicio
    print 'Tiempo de ejecución: ', tiempo



###############################################################################
if __name__ == "__main__":
    main()
