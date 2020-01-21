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


from __future__ import division
import sys
import base64
# import numpy as np
import os
#import shutil
import gc
import codecs
import configparser as conp
import glob
import itertools
import unicodedata
import copy
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import xhtml2pdf.pisa as pisa
from jinja2 import Environment, FileSystemLoader


# import logging
# logging.basicConfig(filename='log.txt')
# log = logging.getLogger()
# log.setLevel(logging.ERROR)


# Definición ruta y ficheros de trabajo:
BASE_PATH = os.path.dirname(os.path.abspath(__file__))
CONFIG_DIR = 'config\\'
INPUT_DIR = 'input\\'
OUTPUT_DIR = 'output\\'
TEMP_DIR = os.path.join(BASE_PATH, 'temp')
DATA_SOURCE_CONFIG_FILE = 'data_source.ini'
EVENT_TYPOLOGY_CONFIG_FILE = 'event_typology.ini'
ENCODING = 'utf-8'


# Número máximo de líneas a tratar en cada iteración:
CHUNKSIZE = 800000


# Campos base de datos:
FIELD_TYPOLOGY = 'name'
FIELD_DATA_SOURCE = 'devicevendor'
FIELD_FIABILITY = 'flexnumber1'
FIELD_SEVERITY = 'deviceseverity'


# Campos para el procesado de la muestra de datos
SORT_FIELDS = ['Tipologia', 'Data source']
ADDITION_FIELDS = ['Cantidad',
                   'Completitud',
                   'Nivel de informacion',
                   'Veracidad',
                   'Veracidad desconocida',
                   'Relevancia alta',
                   'Relevancia media',
                   'Relevancia baja',
                   'Relevancia desconocida']
CONCATENATION_FIELDS = SORT_FIELDS + ADDITION_FIELDS


# Definición numérica de las valoraciones de las dimiensiones de calidad:
GOOD_LEVEL = 2
ACCEPTABLE_LEVEL = 1
BAD_LEVEL = 0


# Definición numérica de las valoraciones de consistencia:
EQUIVALENCIA_CONSISTENCIA_NUMERICA = {'Baja': 0,
                                      'Media': 1,
                                      'Alta': 2,
                                      'Muy Alta': 3}


# Definición de los pesos de los distintos niveles de valoración:
W1 = 1
W2 = 0.5
W3 = -1


# Definición del cojunto de dimensiones que puntuan para la calidad:
DIMENSIONES = ['Cantidad nivel',
               'Completitud nivel',
               'Nivel de informacion nivel',
               'Veracidad nivel',
               'Veracidad desconocida nivel',
               'Frecuencia nivel',
               'Consistencia nivel',
               'Precio por dato nivel']


# Definición de constantes para los reports:
EVENT_TYPOLOGY = 'Tipologia'
TITULO_FUENTES = 'Informe de calidad de la fuente'
TITULO_TIPOLOGIAS = 'Informe de calidad de la tipologia'
TITULO_RANKING = 'Ranking de las fuentes de datos'
DATA_SOURCE = 'Data source'
FUENTE = 'Fuente de datos'


# Definición del conjunto de dimensiones que se mostrarán gráficamente:
COMPARISON_PLOTS_DIMENSIONS = ['Cantidad',
                               'Completitud',
                               'Nivel de informacion',
                               'Veracidad',
                               'Veracidad desconocida',
                               'Consistencia',
                               'Precio por dato']


# Mensajes de pantalla:
INPUT_MSG_001 = 'Indicate the value separator character, in csv file: '
INPUT_MSG_002 = 'Indicate the period (in days) to which the sample refers: '


# Mensajes de aviso:
WARNING_MSG_101 = 'WARNING: Data source %s configuration could not be loaded. Please, check file %s'


# Mensajes de error:
ERROR_MSG_201 = 'ERROR: Data source configuration file can not be opened'
ERROR_MSG_202 = 'ERROR: Event typology configuration file can not be opened'
ERROR_MSG_203 = 'ERROR: No se han encotrado ficheros de entrada'
ERROR_MSG_204 = 'ERROR: Data sample file can not be opened'
ERROR_MSG_205 = 'ERROR: Configuration file error: settings are not valid for data sample'
ERROR_MSG_206 = 'ERROR: Data sample file can not be opened'
ERROR_MSG_207 = 'ERROR: Configuration file error: attribute campos_obligatorios does not exist'
ERROR_MSG_208 = 'ERROR: Configuration file error: atribute %s does not exist'





###############################################################################
#                                                                             #
# DEFINICIÓN DE FUNCIONES                                                     #
#                                                                             #
###############################################################################

def leer_caracteristicas_muestra():
    """
    Reads the character to separate data sample files values and the period of time to which the data refer (in days).

    Parameters
    ----------
    None

    Input
    -----
    sep: input from keyboard
         Character to separate values in the .csv data file.
    per: input from keyboard
         Number of days to which the data refer.

    Returns
    -------
    sep: char
         Character to separate data saple files values.
    per: float
         Period of time to which the data refer (in days)

    Example
    -------
    >>> leer_caracteristicas_muestra()
    [Returns a character to separate csv values, and an integer that indicates the time period which the sample refers.]
    """

    sep = input(INPUT_MSG_001)
    per = float(input(INPUT_MSG_002))


    return sep, per



###############################################################################

def cargar_configuracion_fuentes():
    """
    Loads the .ini datasources configuration file into a configparser.

    Parameters
    ----------
    None

    Returns
    -------
    d_s_p: ConfigParser
           Datasources configuration structure

    Example
    -------
    >>> cargar_configuracion_fuentes()
    [It returns a configparser with datasources configuration.]
    """

    path_to_configuration_data_source_file = os.path.join(BASE_PATH, CONFIG_DIR, DATA_SOURCE_CONFIG_FILE)
    d_s_p = conp.ConfigParser()
    try:
        d_s_p.read(path_to_configuration_data_source_file, encoding=ENCODING)
    except Exception:
        print ERROR_MSG_201
        sys.exit()


    return d_s_p



###############################################################################
def cargar_configuracion_tipologias():
    """
    Loads the .ini event tipologies configuration file into a configparser.

    Parameters
    ----------
    None

    Returns
    -------
    e_t_p: configparser
           Event typologies configuration structure

    Example
    -------
    >>> cargar_configuracion_tipologias()
    [It returns a configparser with evetn tipologies configuration.]
    """

    path_to_configuration_event_typology_file = os.path.join(BASE_PATH, CONFIG_DIR, EVENT_TYPOLOGY_CONFIG_FILE)
    e_t_p = conp.ConfigParser()
    try:
        e_t_p.read(path_to_configuration_event_typology_file, encoding=ENCODING)
    except Exception:
        print ERROR_MSG_202
        sys.exit()


    return e_t_p



###############################################################################
def cargar_ficheros_input():
    """
    Obtains the list of data sample files.

    Parameters
    ----------
    None

    Returns
    -------
    lista_fic_input: list
                     List of data sample files .csv, contained in the input directory.

    >>> cargar_ficheros_input()
    [It returns a list with .csv data sample files.]
    """

    path_to_input_files = os.path.join(BASE_PATH, INPUT_DIR)
    #fic_input = listdir(path_to_input_files)
    lista_fic_input = glob.glob(path_to_input_files + '*.csv')
#    if len(lista_fic_input) == 0:
    if  not lista_fic_input:
        print ERROR_MSG_203
        sys.exit()


    return lista_fic_input



###############################################################################
def cargar_fichero_muestra_by_chunks(fic, separ):
    """
    Loads a chunk of the .csv data file into a dataframe.

    Parameters
    ----------
    fic: string
         File name.
    separ: char
           Character to separate values in the .csv data file.
    chunksize: int
               Size of the read chunks (number of rows).

    Returns
    -------
    dat: pandas dataframe
         Data sample.
    """

    dat = pd.DataFrame()
    path_to_sample_file = os.path.join(BASE_PATH, INPUT_DIR, fic)
    try:
        dat = pd.read_csv(path_to_sample_file, sep=separ, chunksize=CHUNKSIZE)
    except Exception:
        print ERROR_MSG_206
        sys.exit()


    return dat



###############################################################################
def inicializar_estructura_valoracion(t_f, d_s_p):
    """
    Initializes the evaluation structure with all "datasource-event typology" combina   tions.
    In each item, sets datasource properties using .ini configuration file.
    If datasource is not defined in the .ini configuration file, it is not included in the return dataframe.
    If return dataframe is empty (there is no any datatasource defined in the .ini configuration file), the program returns an error and ends.

    Parameters
    ----------
    t_f: pandas dataframe
         Set of Event typology - Data source.
    d_s_p: ConfigParser
           Datasource configuration structure

    Returns
    -------
    val: pandas dataframe
         Evaluation structure for each Data source - Event typology.

    Example
    -------
    >>> inicializar_estructura_valoracion(tipologia_fuente, data_source_parser)
    [It generates a dataframe with combinations of datasources and event typologies.
    In each of them, it initializes the values obtained from .ini configuration files.
    If there is no configuration in the .ini file, then the item is not included in the return dataframe.]
    """

    val = pd.DataFrame(columns=('Tipologia',
                                'Data source',
                                'Data source type',
                                'Valoracion datos obsoletos',
                                'Tasa falsos positivos',
                                'Tasa datos duplicados',
                                'Cantidad',
                                'Cantidad normalizada',
                                'Cantidad nivel',
                                'Completitud',
                                'Numero campos obligatorios',
                                'Completitud normalizada',
                                'Completitud nivel',
                                'Nivel de informacion',
                                'Nivel de informacion normalizada',
                                'Nivel de informacion nivel',
                                'Veracidad',
                                'Veracidad normalizada',
                                'Veracidad nivel',
                                'Veracidad desconocida',
                                'Veracidad desconocida normalizada',
                                'Veracidad desconocida nivel',
                                'Frecuencia',
                                'Frecuencia normalizada',
                                'Frecuencia nivel',
                                'Consistencia',
                                'Consistencia normalizada',
                                'Consistencia nivel',
                                'Relevancia alta',
                                'Relevancia alta normalizada',
                                'Relevancia media',
                                'Relevancia media normalizada',
                                'Relevancia baja',
                                'Relevancia baja normalizada',
                                'Relevancia desconocida',
                                'Relevancia desconocida normalizada',
                                'Precio',
                                'Precio por dato',
                                'Precio por dato normalizada',
                                'Precio por dato nivel',
                                'Valoracion manual',
                                'Calidad',
                                'Exclusividad'))

    for i in range(len(t_f)):
        tipologia = t_f.iloc[i][FIELD_TYPOLOGY]
        fuente = t_f.iloc[i][FIELD_DATA_SOURCE]

        try:
            val = val.append({'Tipologia': tipologia,
                              'Data source': fuente,
                              'Data source type': d_s_p.get(fuente, 'tipo',),
                              'Valoracion datos obsoletos': d_s_p.get(fuente, 'valoracion_datos_obsoletos',),
                              'Tasa falsos positivos': d_s_p.get(fuente, 'tasa_falsos_positivos',),
                              'Tasa datos duplicados': d_s_p.get(fuente, 'tasa_datos_duplicados',),
                              'Cantidad': 0,
                              'Completitud': 0,
                              'Nivel de informacion': 0,
                              'Veracidad': 0,
                              'Veracidad desconocida': 0,
                              'Frecuencia': d_s_p.get(fuente, 'frecuencia'),
                              'Frecuencia normalizada': d_s_p.get(fuente, 'frecuencia'),
                              'Consistencia': d_s_p.get(fuente, 'consistencia'),
                              'Consistencia normalizada': d_s_p.get(fuente, 'consistencia'),
                              'Relevancia alta': 0,
                              'Relevancia media': 0,
                              'Relevancia baja': 0,
                              'Relevancia desconocida': 0,
                              'Precio': float(d_s_p.get(fuente, 'precio')),
                              'Valoracion manual': d_s_p.get(fuente, 'valoracion_manual')
                             }, ignore_index=True)
        except Exception:
            print WARNING_MSG_101 % (fuente, DATA_SOURCE_CONFIG_FILE)

    if val.empty:
        print ERROR_MSG_205
        sys.exit()


    return val



###############################################################################
def eliminar_columnas_innecesarias(dat, e_t_p, l_t):
    """
    Starting from the list of typologies defined in the configuration file and included in the data file, this function deletes from the data sample all those features that are not necessary for the evaluation of the quality of the data.

    Parameters
    ----------
    dat: pandas dataframe
         Data sample
    e_t_p: ConfigParser
           Event typology configuration structure.
    l_t: list
         List of event tipologies.

    Returns
    -------
    data_reducido: pandas dataframe
                   Reduced data sample (without unnecessary features)

    Example
    -------
    >>> eliminar_columnas_innecesarias(data, event_typology_parser, lista_tipologias)
    [It returns dataframe data without unnecessary features]
    """

    campos_necesarios = [FIELD_TYPOLOGY, FIELD_DATA_SOURCE, FIELD_FIABILITY, FIELD_SEVERITY]

    tmp = e_t_p.get('Default Section', 'campos_obligatorios')
    tmp = tmp.replace('\n', '')
    campos_config = tmp.split(",")

    campos_necesarios += campos_config

    for tip in l_t:
        try:
            tmp = e_t_p.get(tip, 'campos_obligatorios')
            tmp = tmp.replace('\n', '')
            campos_config = tmp.split(",")

            campos_necesarios += campos_config
        except Exception:
            pass

    campos_necesarios = set(campos_necesarios)

    data_reducido = dat.loc[:, campos_necesarios]


    return data_reducido



###############################################################################
def replace_by_threshold(df, column, threshold, recodified):
    '''
    Replace the column by the values in recodified as specified by threshold

    Parameters
    ----------
    df: pandas.DataFrame
        The data
    column: object
            The column whose values will be replaced
    threshold: list
               The threshold values. Its lenght must be equal to 2
    recodified: XXX
                The values to be substituted into the data column. Its lenght must be equal to 3.
    '''

    col_data = pd.to_numeric(df[column], errors='coerce')
    nulled = pd.isnull(col_data)
    lower = (col_data <= threshold[0]) & (col_data > 1)
    middle = (col_data > threshold[0]) & (col_data < threshold[1])
    higher = col_data >= threshold[1]
    col_data[higher] = recodified[-1]
    col_data[middle] = recodified[-2]
    col_data[lower] = recodified[-3]
    col_data[nulled] = 1
    df[column] = col_data



###############################################################################
def redefinir_datos_fiabilidad_severidad(dat):
    """
    Modifies the data sample to standardize severity and fiability values.

    Parameters
    ----------
    dat: pandas dataframe
         Data sample.

    Returns
    -------
    dat: pandas dataframe
         Data sample with standardized reliability and severity values.

    Example
    -------
    >>> redefinir_datos_fiabilidad_severidad(data)
    Returns data sample with standardized severity and fiability values.
    """

    threshold_split = [4, 8]
    valores_recodificados = [3, 6, 9]

    #target = FIELD_SEVERITY
    #dat[target] = dat[target].apply(lambda x: pretratar_valor_target(x, threshold_split, valores_recodificados))

    #target = FIELD_FIABILITY
    #dat[target] = dat[target].apply(lambda x: pretratar_valor_target(x, threshold_split, valores_recodificados))

    replace_by_threshold(dat, FIELD_SEVERITY, threshold_split, valores_recodificados)
    replace_by_threshold(dat, FIELD_FIABILITY, threshold_split, valores_recodificados)


    return dat



###############################################################################
def obtener_campos_obligatorios(tip, e_t_p):
    """
    Return the mandatory fields for the selected typology. This fields are collected from the configuration file.
    First, the field names will be tried to recover from the typology section. If they are not defined, they are retrieved from default section.
    This list will be used as a reference to consider whether a data is complete or not.

    Parameters
    ----------
    tip: string
         Event typology.
    e_t_p: ConfigParser
           Event typologies configuration structureqqqqq

    Returns
    -------
    cam_obl: string list
             Mandatory fields for tip typology, sepparated by commas.

    Example
    -------
    >>> obtener_campos_obligatorios('IP Bot', event_typology_parser)
    [Aggregated Event Count, Correlated Event Count, End Time, Event ID, External ID, Locality, Destination Address, Destination Geo Country Code, Target Address, Agent Asset ID]
    """

    cam_obl = []
    try:
        tmp = e_t_p.get(tip, 'campos_obligatorios')
        tmp = tmp.replace('\n', '')
        cam_obl = tmp.split(",")
    except Exception:
        try:
            tmp = e_t_p.get('Default Section', 'campos_obligatorios')
            tmp = tmp.replace('\n', '')
            cam_obl = tmp.split(",")
        except Exception:
            print ERROR_MSG_207
            sys.exit()


    return cam_obl



###############################################################################
def valorar_completitud(val, dat, i, tip, e_t_p):
    """
    Evaluates data completeness. This function checks how many data contain all mandatory fields, for a specific event typology.

    Parameters
    ----------
    val: pandas dataframe
         Evaluation structure for each Data source - Event typology.
    dat: pandas dataframe
         Data sample for the current Data source and Event typology.
    i: integer
       Current index in dataframe val.
    tip: string
         Event typology.
    e_t_p: ConfigParser
           Event typology configuration structure.

    Returns
    -------
    val: pandas dataframe
         Evaluation structure for each Data source - Event typology.

    Example
    -------
    >>> valorar_completitud(valoracion, data_aux, i,  tipologia, event_typology_parser, cantidad)
    Returns evaluation dataframe updated (completeness dimension).
    """

#    # Cálculo de la completitud para la tipologia-fuente actual
#    campos_obligatorios = obtener_campos_obligatorios(tip, e_t_p)
#
#    data_compl = dat.dropna(subset=campos_obligatorios)
#
#    completitud = len(data_compl)
#    val.loc[i, 'Completitud'] += completitud
#
#    # Almacenamos el número de campos obligatorios para después normalizar
#    val.loc[i, 'Numero campos obligatorios'] = len(campos_obligatorios)
#
#    # Cálculo del nivel de información la tipologia-fuente actual
#    campos_totales = 0
#
#    for col in campos_obligatorios:
#        data_nivelinf = dat[pd.notnull(dat[col])]
#        campos_totales = campos_totales + len(data_nivelinf)
#
#        # De momento, no calculamos el nivel de información. Solo añadimos los campos totales
#        # nivel_de_informacion = campos_totales / cantidad
#    val.loc[i, 'Nivel de informacion'] += campos_totales
#
#
#    return val


    # Cálculo de la completitud para la tipologia-fuente actual
    campos_obligatorios = obtener_campos_obligatorios(tip, e_t_p)

    #data_compl = dat.dropna(subset=campos_obligatorios)

    #completitud = len(data_compl)
    mask = pd.notnull(dat[campos_obligatorios]).values
    completitud = np.sum(np.all(mask, axis=1))
    #val.loc[i, 'Completitud'] += completitud

    # Almacenamos el número de campos obligatorios para después normalizar
    val.loc[i, 'Numero campos obligatorios'] = len(campos_obligatorios)

    # Cálculo del nivel de información la tipologia-fuente actual
    #campos_totales = 0

    #for col in campos_obligatorios:
    #    data_nivelinf = dat[pd.notnull(dat[col])]
    #    campos_totales = campos_totales + len(data_nivelinf)

        # De momento, no calculamos el nivel de información. Solo añadimos los campos totales
        # nivel_de_informacion = campos_totales / cantidad
    campos_totales = np.sum(mask)
    modified = ['Completitud', 'Nivel de informacion']
    #val.loc[i, 'Nivel de informacion'] += campos_totales
    val.loc[i, modified] += [completitud, campos_totales]


    return val



###############################################################################
def obtener_parametro(parametro, tip, c_p):
    """
    Get an item value from comfiguration file. First, it is looked for in the typology section. If it does not exist, it is looked for in the defalut section.

    Parameters
    ----------
    parametro: string
               .ini file key which we want to obtain the value.
    tip: string
         Event typology.
    c_p : ConfigParser
          Configuration file values.

    Returns
    -------
    valor_parametro: string
                     Value of the configuration file element with key = parameter.

    Example
    -------
    >>> obtener_parametro('veracidad_deseado', tip, e_t_p)
    0.5
    """

    try:
        valor_parametro = c_p.get(tip, parametro)
    except Exception:
        try:
            valor_parametro = c_p.get('Default Section', parametro)
        except Exception:
            print ERROR_MSG_208 % parametro
            sys.exit()


    return valor_parametro



###############################################################################
def valorar_veracidad(val, dat, i, tip, e_t_p):
    """
    Evaluates data accuracy/credibility (reliability).
    This function checks:
        · Reliability: How many data reach the reference reliability level, for a specific event typology.
        . Unknow reliability level: What is the unknow reliability level in received data, for a specific event typology.

    Parameters
    ----------
    val: pandas dataframe
         Evaluation structure for each Data source - Event typology.
    dat: pandas dataframe
         Data sample for the current Data source and Event typology.
    i: integer
       Current index in dataframe val.
    tip: string
         Event typology.
    e_t_p: ConfigParser
           Event tipology configuration structure.

    Returns
    -------
    val: pandas dataframe
         Evaluation structure for each Data source - Event typology.

    Example
    -------
    >>> valorar_veracidad(valoracion, data_aux, i, tipologia, event_tipology_parser)
    Returns evaluation dataframe updated (reliability dimensions).
    """

    # Cálculo de la veracidad para la tipologia-fuente actual
    veracidad_referencia = int(obtener_parametro('veracidad_referencia', tip, e_t_p))
    #veracidad = len(dat[dat[FIELD_FIABILITY] >= veracidad_referencia])
    veracidad = np.sum(dat[FIELD_FIABILITY] >= veracidad_referencia)
    #val.loc[i, 'Veracidad'] += veracidad

    # Cálculo de la veracidad desconocida para la tipologia-fuente actual
    #veracidad_desconocida = len(dat[dat[FIELD_FIABILITY] <= 1])
    veracidad_desconocida = np.sum(dat[FIELD_FIABILITY] <= 1)
    #val.loc[i, 'Veracidad desconocida'] += veracidad_desconocida

    val.loc[i, ['Veracidad', 'Veracidad desconocida']] += [veracidad, veracidad_desconocida]


    return val



###############################################################################
def valorar_relevancia(val, dat, i):
    """
    Evaluates the data distribution into the different levels of severity.

    Parameters
    ----------
    val: pandas dataframe
         Evaluation structure for each Data source - Event typology.
    dat: pandas dataframe
         Data sample for the current Data source and Event typology.
    i: integer
       Current index in dataframe val.

    Returns
    -------
    val: pandas dataframe
         Evaluation structure for each Data source - Event typology.

    Example
    -------
    >>> valorar_relevancia(valoracion, i,  tipologia, event_tipology_parser)
    Returns evaluation dataframe updated (severity dimensions).
    """

    # Cálculo de la relevancia alta para la tipologia-fuente actual
    #relevancia_alta = len(dat[dat[FIELD_SEVERITY] >= 8])
    relevancia_alta = np.sum(dat[FIELD_SEVERITY] >= 8)
    #val.loc[i, 'Relevancia alta'] += relevancia_alta

    # Cálculo de la relevancia media para la tipologia-fuente actual
    #relevancia_media = len(dat[(dat[FIELD_SEVERITY] >= 5) & (dat[FIELD_SEVERITY] < 8)])
    relevancia_media = np.sum((dat[FIELD_SEVERITY] >= 5) & (dat[FIELD_SEVERITY] < 8))
    #val.loc[i, 'Relevancia media'] += relevancia_media

    # Cálculo de la relevancia baja para la tipologia-fuente actual
    #relevancia_baja = len(dat[(dat[FIELD_SEVERITY] >= 2) & (dat[FIELD_SEVERITY] < 5)])
    relevancia_baja = np.sum((dat[FIELD_SEVERITY] >= 2) & (dat[FIELD_SEVERITY] < 5))
    #val.loc[i, 'Relevancia baja'] += relevancia_baja

    # Cálculo de la relevancia desconocida para la tipologia-fuente actual
    #relevancia_desconocida = len(dat[(dat[FIELD_SEVERITY] < 2)])
    relevancia_desconocida = np.sum(dat[FIELD_SEVERITY] < 2)
    #val.loc[i, 'Relevancia desconocida'] += relevancia_desconocida

    relevancias = ['Relevancia alta', 'Relevancia media', 'Relevancia baja', 'Relevancia desconocida']
    val.loc[i, relevancias] += [relevancia_alta, relevancia_media, relevancia_baja, relevancia_desconocida]


    return val



###############################################################################
def eliminar_tildes(val):
    """
    Remove tildes from evaluation dataframe's typology variable.

    Parameters
    ----------
    val: pandas dataframe
         Evaluation structure for each Data source - Event typology.

    Returns
    -------
    val: pandas dataframe
         Evaluation structure for each Data source - Event typology.

    Example
    -------
    >>> eliminar_tildes(valoracion)
    Returns evaluation dataframe updated (without tildes in typology variable).
    """

    val['Tipologia'] = val['Tipologia'].apply(lambda x: unicodedata.normalize('NFKD', x.decode('utf-8')).encode('utf-8').decode('ascii', 'ignore'))


    return val



###############################################################################
def process_chunk(data, d_s_p, e_t_p):
    '''
    Process a data chunk from a data file and creates the evaluation structure for the data contained within.

    Parameters
    ----------
    data: pandas.Dataframe
          A chunk from a data file
    d_s_p: ConfigParser
           Datasources configuration structure
    e_t_p: ConfigParser
           Event typologies configuration structure

    Returns
    -------
    valoracion: pandas.DataFrame
                Evaluation structure for each Data source - Event typology in the chunk

    Example
    -------
    >>> process_chunk(chunk, data_source_parser, event_typology_parser)
    Returns evaluation dataframe updated (quantity, completeness, reliability and severity dimensions).
    '''

    #try:
    # data.to_csv('chunks/%f.csv' % np.random.random(), header=True, index=False)
    pairs = list(set(zip(data[FIELD_TYPOLOGY], data[FIELD_DATA_SOURCE])))
    tip_fue = pd.DataFrame(data=pairs, columns=[FIELD_TYPOLOGY, FIELD_DATA_SOURCE])
    valoracion = inicializar_estructura_valoracion(tip_fue, d_s_p)
    lista_tipologias = list(set(valoracion['Tipologia']))
    data = eliminar_columnas_innecesarias(data, e_t_p, lista_tipologias)
    data = redefinir_datos_fiabilidad_severidad(data)

    # itera solo sobre los pares tipologia - fuente que sabemos que están presentes
    for tipologia, fuente in itertools.izip(valoracion['Tipologia'], valoracion['Data source']):
        data_aux = data[(data[FIELD_TYPOLOGY] == tipologia) & (data[FIELD_DATA_SOURCE] == fuente)]

        # Calculo de medidas relacionadas con la dimension de CANTIDAD (I):
        #       La cantidad normalizada y el nivel de calidad se calcularan al
        #       final del proceso, ya que necesitan utilizar los datos de todas
        #       las fuentes
        cantidad = int(len(data_aux))
            # Índice del par tipologia-fuente en la estructura de valoracion
        i = np.nonzero(((valoracion['Tipologia'] == tipologia) & (valoracion['Data source'] == fuente)))[0][0]
        valoracion.loc[i, 'Cantidad'] += cantidad

        # Calculo de medidas relacionadas con la dimension de COMPLETITUD:
        valoracion = valorar_completitud(valoracion, data_aux, i, tipologia, e_t_p)

        # Calculo de medidas relacionadas con la dimension de VERACIDAD:
        valoracion = valorar_veracidad(valoracion, data_aux, i, tipologia, e_t_p)

        # Calculo de medidas relacionadas con la dimension de RELEVANCIA:
        valoracion = valorar_relevancia(valoracion, data_aux, i)

    #except Exception as e:
    #    log.error(str(e))

    valoracion = eliminar_tildes(valoracion)


    return valoracion

# Posible problema: tener una lista de 10 ** 6 o más estructuras de valoración
# parciales si el número de chunks totales es muy elevado. En este caso
# sería mejor que process_chunk no devolviese la estrucutra de valoración
# sino que la guardase con to_csv(mode='a'). Luego se podría leer el archivo
# resultante por chunks y ejecutar compute_valoracion progresivamente.



###############################################################################
def valorar_dimensiones(lis_fic, separ, d_s_p, e_t_p):
    """
    Obtiene estructuras de evalucación para todos los ficheros de entrada.

    Parameters
    ----------
    lis_fic: list.
             List of data sample files .csv, contained in the input directory.
    separ: char
           Character to separate values in the .csv data file.
    d_s_p: ConfigParser
           Datasource configuration structure
    e_t_p: ConfigParser
           Event typologies configuration structure

    Returns
    -------
    val: pandas dataframe
         Evaluation structure for each Data source - Event typology.

    Example
    -------
    >>> valorar_dimensiones(lista_ficheros_input, separador, data_source_parser, event_typology_parser)
    Returns evaluation dataframe updated (quantity, completeness, reliability and severity dimensions).
    Results are separated by chunks.
    """

    val = pd.DataFrame()
    i = 0
    for path in lis_fic:
        reader = cargar_fichero_muestra_by_chunks(path, separ)
        for chunk in reader:
            val_aux = pd.DataFrame()
            val_aux = process_chunk(chunk, d_s_p, e_t_p)
            val.append(val_aux)
            del chunk
            del val_aux
            i += 1
#            print('Chunk número', i)
            gc.collect()
        reader.close()
        del reader


    return val



###############################################################################
def compute_valoracion(valoracion_chunks):
    '''xxxREV
    Combine all of the partial evaluation strucutres computed per chunk.

    Parameters
    ----------
    valoracion_chunks: list<pandas.DataFrame>
        A list of all the evaluation structures computed per chunk

    Returns
    -------
    valoracion: pandas.DataFrame
        Evaluation structure for each Data source - Event typology.
    '''

    res = pd.concat(valoracion_chunks)
    del valoracion_chunks

    # Selecciona únicamente los valores para los cuales tiene sentido sumar
    grouped = res[CONCATENATION_FIELDS]

    # Agrupa por tipologia y fuente y suma
    grouped = grouped.groupby(by=SORT_FIELDS, as_index=False).sum(axis=0)

    # Genera una estructura de valoracion con una sola fila para cada par
    # tipologia-fuente y ordena las filas por tipologia y fuente
    # Este ordenamiento se hace para despues poder insertar las columnas
    # con las dimensiones sumadas anteriores
    valoracion = res.drop_duplicates(subset=SORT_FIELDS).sort_values(by=SORT_FIELDS)

    # Ordena la suma por tipologia-fuente. Así las filas de valoracion y
    # grouped estarán alineadas
    grouped = grouped.sort_values(by=SORT_FIELDS)

    # Se reinicia el índice para que consista en enteros ascendentes y
    # contiguos. Si no se hace esto, la siguiente sentencia fracasará
    # horriblemente
    valoracion.reset_index(inplace=True, drop=True)

    # Sustituye los valores sumados en la estrucutra de valoración
    valoracion[ADDITION_FIELDS] = grouped[ADDITION_FIELDS]


    return valoracion



###############################################################################
def valorar_nivel_informacion(val):
    """
    Evaluates data information level. This function checks what is the mandatory fields average in received data, for a specific event typology.

    Parameters
    ----------
    val: pandas dataframe
         Evaluation structure for each Data source - Event typology.

    Returns
    -------
    val: pandas dataframe
         Evaluation structure for each Data source - Event typology.

    Example
    -------
    >>> valorar_nivel_informacion(valoracion)
    Returns evaluation dataframe updated (information level dimension).
    """

    #for i in range(len(val)):
        # Cuando se evaluo la completitud se computaron los campos obligatorios
        #   recibidos en el campo 'Nivel de informacion' del dataframe de
        #   valoracion. Ahora solo hace falta dividir el total de campos
        #   obligatorios recibidos entre la cantidad de registros recibidos,
        #   para cada una de las fuentes y tipolologías.

    #    campos = val.loc[i, 'Nivel de informacion']
    #    cant = val.loc[i, 'Cantidad']
    #    campos, cant = val.loc[i, ['Nivel de informacion', 'Cantidad']]
    #    nivel_informacion = float(campos) / float(cant)
    #    val.loc[i, 'Nivel de informacion'] = round(nivel_informacion, 3)

    val['Nivel de informacion'] = (val['Nivel de informacion'] / val['Cantidad']).round(3)


    return val



###############################################################################
def valorar_precio_por_dato(val, d_p):
    """
    Evaluates economic value of the data.

    Parameters
    ----------
    val: pandas dataframe
         Evaluation structure for each Data source - Event typology.
    d_p: int
         Period of time to which the data refer (in days)

    Returns
    -------
    val: pandas dataframe
         Evaluation structure for each Data source - Event typology.

    Example
    -------
    >>> valorar_precio_por_dato(valoracion, data_period)
    Returns evaluation dataframe updated (price per data dimension).
    """

    # Diccionario para almacenar las cantidades de datos totales aportadas por cada tipología
    dict_cantidad_por_fuente = {}
    for fuente in set(val['Data source']):
        df_aux = val[val['Data source'] == fuente]
        total = sum(df_aux['Cantidad'])
        dict_aux = {fuente: total}
        dict_cantidad_por_fuente.update(dict_aux)

    del df_aux

    # Se recorre el datasource de valoración y se va completando el dato de precio por dato
    for i in range(len(val)):
        fue = val.loc[i, 'Data source']

        # Cálculo del precio por dato para la tipologia-fuente actual
        precio = val.loc[i, 'Precio']
        cantidad_fuente = dict_cantidad_por_fuente.get(fue)
        precio_por_dato = (float(precio) * float(d_p)) / (float(cantidad_fuente) * 365)
        precio_por_dato = round(precio_por_dato, 6)

        val.loc[i, 'Precio por dato'] = precio_por_dato


    return val



###############################################################################
def calcular_cantidad_normalizada(val):
    """
    It calculates the normalized value of Quantity dimension (Quantity divided by largest Quantity for that typology provided by any data source).

    Parameters
    ----------
    val: pandas dataframe
         Evaluation structure for each Data source - Event typology.

    Returns
    -------
    val: pandas dataframe
         Evaluation structure for each Data source - Event typology.

    Example
    -------
    >>> calcular_cantidad_normalizada(valoracion)
    [Returns evaluation dataframe updated (normalized quantity dimension).]
    """

    # Diccionario para almacenar los datos máximos de una tipología, aportados por una sola fuente
    dict_tipologias_maximo = {}
    for tipologia in set(val['Tipologia']):
        df_aux = val[val['Tipologia'] == tipologia]
        maximo = max(df_aux['Cantidad'])
        dict_aux = {tipologia: maximo}
        dict_tipologias_maximo.update(dict_aux)

    del df_aux

    # Se recorre el datasource de valoración y se va completando el campo de cantidad normalizada
    for i in range(len(val)):
        tip = val.loc[i, 'Tipologia']

        # Cálculo de la cantidad normalizada para la tipologia-fuente actual
        cantidad = val.loc[i, 'Cantidad']
        maximo_tipologia = dict_tipologias_maximo.get(tip)
        cantidad_normalizada = float(cantidad) / float(maximo_tipologia)
        cantidad_normalizada = round(cantidad_normalizada, 3)

        val.loc[i, 'Cantidad normalizada'] = cantidad_normalizada


    return val



###############################################################################
def calcular_valores_normalizados(val):
    """
    It calculates the normalized values of completeness, information level, accuracy (reliability and unknow reliability) and relevance dimensions (Quality dimension divided by Quantity).

    Parameters
    ----------
    val: pandas dataframe
         Evaluation structure for each Data source - Event typology.

    Returns
    -------
    val: pandas dataframe
         Evaluation structure for each Data source - Event typology.

    Example
    -------
    >>> calcular_cantidad_normalizada(valoracion)
    [Returns evaluation dataframe updated (normalized values of completeness, information level, accuracy and relevance dimensions).]
    """

    for i in range(len(val)):
        cantidad = val.loc[i, 'Cantidad']

        # Completitud normalizada
        completitud_normalizada = float(val.loc[i, 'Completitud']) / float(cantidad)
        completitud_normalizada = round(completitud_normalizada, 3)
        val.loc[i, 'Completitud normalizada'] = completitud_normalizada

        # Nivel de informacion normalizada
        numero_campos_obligatorios = val.loc[i, 'Numero campos obligatorios']
        nivel_informacion_normalizada = float(val.loc[i, 'Nivel de informacion']) / float(numero_campos_obligatorios)
        nivel_informacion_normalizada = round(nivel_informacion_normalizada, 3)
        val.loc[i, 'Nivel de informacion normalizada'] = nivel_informacion_normalizada

        # Veracidad normalizada
        veracidad_normalizada = float(val.loc[i, 'Veracidad']) / float(cantidad)
        veracidad_normalizada = round(veracidad_normalizada, 3)
        val.loc[i, 'Veracidad normalizada'] = veracidad_normalizada

        # Veracidad desconocida normalizada
        veracidad_desconocida_normalizada = float(val.loc[i, 'Veracidad desconocida']) / float(cantidad)
        veracidad_desconocida_normalizada = round(veracidad_desconocida_normalizada, 3)
        val.loc[i, 'Veracidad desconocida normalizada'] = veracidad_desconocida_normalizada

        # Relevancia alta normalizada
        relevancia_alta_normalizada = float(val.loc[i, 'Relevancia alta']) / float(cantidad)
        relevancia_alta_normalizada = round(relevancia_alta_normalizada, 3)
        val.loc[i, 'Relevancia alta normalizada'] = relevancia_alta_normalizada

        # Relevancia media normalizada
        relevancia_media_normalizada = float(val.loc[i, 'Relevancia media']) / float(cantidad)
        relevancia_media_normalizada = round(relevancia_media_normalizada, 3)
        val.loc[i, 'Relevancia media normalizada'] = relevancia_media_normalizada

        # Relevancia baja normalizada
        relevancia_baja_normalizada = float(val.loc[i, 'Relevancia baja']) / float(cantidad)
        relevancia_baja_normalizada = round(relevancia_baja_normalizada, 3)
        val.loc[i, 'Relevancia baja normalizada'] = relevancia_baja_normalizada

        # Relevancia desconocida normalizada
        relevancia_desconocida_normalizada = float(val.loc[i, 'Relevancia desconocida']) / float(cantidad)
        relevancia_desconocida_normalizada = round(relevancia_desconocida_normalizada, 3)
        val.loc[i, 'Relevancia desconocida normalizada'] = relevancia_desconocida_normalizada

    # Una vez hemos calculado el nivel de información normalizado, borramos la columna 'Numero
    #   campos obligatorios' del dataframe, ya que no se va a utilizar más.
    val.drop(['Numero campos obligatorios'], axis='columns', inplace=True)


    return val



###############################################################################
def calcular_precio_normalizado(val, e_t_p):
    """
    Evaluates economic value of the data.
    It calculates the normalized economic value of the data (Price per data Reference price for this typology).

    Parameters
    ----------
    val: pandas dataframe
         Evaluation structure for each Data source - Event typology.
    e_t_p: ConfigParser
           Event typologies configuration structure

    Returns
    -------
    val: pandas dataframe
         Evaluation structure for each Data source - Event typology.

    Example
    -------
    >>> calcular_precio_normalizado(valoracion, event_typology_parser)
    [Returns evaluation dataframe updated (normalized price per data dimension).]
    """

    for i in range(len(val)):
        tipologia = val.loc[i, 'Tipologia']
        precio_por_dato = float(val.loc[i, 'Precio por dato'])
        precio_referencia = obtener_parametro('precio_por_dato_referencia', tipologia, e_t_p)
        precio_por_dato_normalizada = float(precio_por_dato) / float(precio_referencia)
        precio_por_dato_normalizada = round(precio_por_dato_normalizada, 6)

        val.loc[i, 'Precio por d-ato normalizada'] = precio_por_dato_normalizada


    return val



###############################################################################
def obtener_segundos(cadena):
    """
    Get the value in seconds from a string with format hh:mm:ss.

    Parameters
    ----------
    cadena: string
            Time value with hh:mm:ss format.

    Returns
    -------
    segs: int
          Time value in seconds.

    Example
    -------
    >>> obtener_segundos('01:30:45')
    5445
    """

    tupla = cadena.split(":")
    segs = int(tupla[0]) * 3600 + int(tupla[1]) * 60 + int(tupla[0])


    return segs



###############################################################################
def calcular_niveles(val, e_t_p):
    """
    It sets quality level (good, acceptable or bad) in each quality dimension.

    Parameters
    ----------
    val: pandas dataframe
         Evaluation structure for each Data source - Event typology.
    e_t_p: ConfigParser
           Event typologies configuration structure

    Returns
    -------
    val: pandas dataframe
         Evaluation structure for each Data source - Event typology.

    Example
    -------
    calcular_niveles(valoracion, event_typology_parser)
    [Returns evaluation dataframe updated (levels).]
    """

    for i in range(len(val)):
        tip = val.loc[i, 'Tipologia']

        # Asignación del nivel de cantidad
        cantidad_normalizada = val.loc[i, 'Cantidad normalizada']
        umbral_deseado = float(obtener_parametro('cantidad_deseado', tip, e_t_p))
        umbral_minimo = float(obtener_parametro('cantidad_minimo', tip, e_t_p))

        if cantidad_normalizada >= umbral_deseado:
            nivel = GOOD_LEVEL
        elif (cantidad_normalizada >= umbral_minimo) & (cantidad_normalizada < umbral_deseado):
            nivel = ACCEPTABLE_LEVEL
        else:
            nivel = BAD_LEVEL
        val.loc[i, 'Cantidad nivel'] = nivel

        del umbral_deseado, umbral_minimo, nivel

        # Asignación del nivel de completitud
        completitud_normalizada = val.loc[i, 'Completitud normalizada']
        umbral_deseado = float(obtener_parametro('completitud_deseado', tip, e_t_p))
        umbral_minimo = float(obtener_parametro('completitud_minimo', tip, e_t_p))

        if completitud_normalizada >= umbral_deseado:
            nivel = GOOD_LEVEL
        elif (completitud_normalizada >= umbral_minimo) & (completitud_normalizada < umbral_deseado):
            nivel = ACCEPTABLE_LEVEL
        else:
            nivel = BAD_LEVEL

        val.loc[i, 'Completitud nivel'] = nivel

        del umbral_deseado, umbral_minimo, nivel

        # Asignación del nivel de nivel de información
        nivel_de_informacion_normalizada = val.loc[i, 'Nivel de informacion normalizada']
        umbral_deseado = float(obtener_parametro('nivel_de_informacion_deseado', tip, e_t_p))
        umbral_minimo = float(obtener_parametro('nivel_de_informacion_minimo', tip, e_t_p))

        if nivel_de_informacion_normalizada >= umbral_deseado:
            nivel = GOOD_LEVEL
        elif (nivel_de_informacion_normalizada >= umbral_minimo) & (nivel_de_informacion_normalizada < umbral_deseado):
            nivel = ACCEPTABLE_LEVEL
        else:
            nivel = BAD_LEVEL

        val.loc[i, 'Nivel de informacion nivel'] = nivel

        del umbral_deseado, umbral_minimo, nivel

        # Asignación del nivel de veracidad
        veracidad_normalizada = val.loc[i, 'Veracidad normalizada']
        umbral_deseado = float(obtener_parametro('veracidad_deseado', tip, e_t_p))
        umbral_minimo = float(obtener_parametro('veracidad_minimo', tip, e_t_p))

        if veracidad_normalizada >= umbral_deseado:
            nivel = GOOD_LEVEL
        elif (veracidad_normalizada >= umbral_minimo) & (veracidad_normalizada < umbral_deseado):
            nivel = ACCEPTABLE_LEVEL
        else:
            nivel = BAD_LEVEL

        val.loc[i, 'Veracidad nivel'] = nivel

        del umbral_deseado, umbral_minimo, nivel

        # Asignación del nivel de veracidad desonocida
        veracidad_desconocida_normalizada = val.loc[i, 'Veracidad desconocida normalizada']
        umbral_deseado = float(obtener_parametro('veracidad_desconocida_deseado', tip, e_t_p))
        umbral_minimo = float(obtener_parametro('veracidad_desconocida_minimo', tip, e_t_p))

        if veracidad_desconocida_normalizada <= umbral_deseado:
            nivel = GOOD_LEVEL
        elif (veracidad_desconocida_normalizada <= umbral_minimo) & (veracidad_desconocida_normalizada > umbral_deseado):
            nivel = ACCEPTABLE_LEVEL
        else:
            nivel = BAD_LEVEL

        val.loc[i, 'Veracidad desconocida nivel'] = nivel

        del umbral_deseado, umbral_minimo, nivel

        # Asignación del nivel de frecuencia
        frecuencia_normalizada = val.loc[i, 'Frecuencia normalizada']
        frecuencia_umbral_deseado = obtener_parametro('frecuencia_deseado', tip, e_t_p)
        frecuencia_umbral_minimo = obtener_parametro('frecuencia_minimo', tip, e_t_p)

            # Conversión de los datos a segundos, para poder compararlos
        frecuencia_normalizada_seg = obtener_segundos(frecuencia_normalizada)
        frecuencia_umbral_deseado_seg = obtener_segundos(frecuencia_umbral_deseado)
        frecuencia_umbral_minimo_seg = obtener_segundos(frecuencia_umbral_minimo)

        if frecuencia_normalizada_seg <= frecuencia_umbral_deseado_seg:
            nivel = GOOD_LEVEL
        elif (frecuencia_normalizada_seg <= frecuencia_umbral_minimo_seg) & (frecuencia_normalizada_seg > frecuencia_umbral_deseado_seg):
            nivel = ACCEPTABLE_LEVEL
        else:
            nivel = BAD_LEVEL

        val.loc[i, 'Frecuencia nivel'] = nivel

        del nivel

        #Asignación del nivel de consistencia
        consistencia_normalizada = val.loc[i, 'Consistencia normalizada']
        consistencia_umbral_deseado = obtener_parametro('consistencia_deseado', tip, e_t_p)
        consistencia_umbral_minimo = obtener_parametro('consistencia_minimo', tip, e_t_p)

            # Conversión de los datos a numéricos, para poder compararlos
        consistencia_normalizada_numerica = EQUIVALENCIA_CONSISTENCIA_NUMERICA.get(consistencia_normalizada)
        consistencia_umbral_deseado_numerico = EQUIVALENCIA_CONSISTENCIA_NUMERICA.get(consistencia_umbral_deseado)
        consistencia_umbral_minimo_numerico = EQUIVALENCIA_CONSISTENCIA_NUMERICA.get(consistencia_umbral_minimo)

        if consistencia_normalizada_numerica >= consistencia_umbral_deseado_numerico:
            nivel = GOOD_LEVEL
        elif (consistencia_normalizada_numerica >= consistencia_umbral_minimo_numerico) & (consistencia_normalizada_numerica < consistencia_umbral_deseado_numerico):
            nivel = ACCEPTABLE_LEVEL
        else:
            nivel = BAD_LEVEL

        val.loc[i, 'Consistencia nivel'] = nivel

        del nivel

        #Asignación del nivel de precio por dato
        precio_por_dato_normalizada = val.loc[i, 'Precio por dato normalizada']
        umbral_deseado = float(obtener_parametro('precio_por_dato_deseado', tip, e_t_p))
        umbral_minimo = float(obtener_parametro('precio_por_dato_minimo', tip, e_t_p))

        if precio_por_dato_normalizada <= umbral_deseado:
            nivel = GOOD_LEVEL
        elif (precio_por_dato_normalizada <= umbral_minimo) & (precio_por_dato_normalizada > umbral_deseado):
            nivel = ACCEPTABLE_LEVEL
        else:
            nivel = BAD_LEVEL

        val.loc[i, 'Precio por dato nivel'] = nivel

        del umbral_deseado, umbral_minimo, nivel


    return val



###############################################################################
def valorar_calidad_tipologia(val):
    """
    Evaluates datasource quality level in each tipology.
    The quality is calculated using the levels in each data quality dimension, and applying the evaluation weights.

    Parameters
    ----------
    val: pandas dataframe
         Evaluation structure for each Data source - Event typology.

    Returns
    -------
    val: pandas dataframe
         Evaluation structure for each Data source - Event typology.

    Example
    -------
    >>> valorar_calidad_tipologia(valoracion)
    Returns evaluation dataframe updated (quality for each typology).
    """

    for i in range(len(val)):
        calidad = 0.0
        for dim in DIMENSIONES:
            puntuacion = val.loc[i, dim]
            if puntuacion == GOOD_LEVEL:
                calidad = calidad + W1
            elif puntuacion == ACCEPTABLE_LEVEL:
                calidad = calidad + W2
            elif puntuacion == BAD_LEVEL:
                calidad = calidad + W3
            else:
                pass

        calidad = calidad / len(DIMENSIONES)
        calidad = round(calidad, 3)

        val.loc[i, 'Calidad'] = calidad


    return val



###############################################################################
def valorar_exclusividad(val):
    """
    Evaluates datasource quality level in each tipology.
    The quality is calculated using the levels in each data quality dimension, and applying the evaluation weights.

    Parameters
    ----------
    val: pandas dataframe
         Evaluation structure for each Data source - Event typology.

    Returns
    -------
    val: pandas dataframe
         Evaluation structure for each Data source - Event typology.

    Example
    -------
    >>> valorar_calidad_tipologia(valoracion)
    Returns evaluation dataframe updated (quality for each typology).
    """

    for i in range(len(val)):
        tip = val.loc[i, 'Tipologia']
        val_aux = val[val['Tipologia'] == tip]
        val_aux = val_aux.reset_index()
        lista_fuentes = []
        for j in range(len(val_aux)):
            if (val_aux.loc[j, 'Data source']) != (val.loc[i, 'Data source']):
                lista_fuentes.append(val_aux.loc[j, 'Data source'])

        lista_fuentes_cadena = ', '.join(lista_fuentes)
        val.loc[i, 'Exclusividad'] = lista_fuentes_cadena


    return val



###############################################################################
def valorar_calidad_global(val):
    """
    Evaluates datasource global quality level.

    Parameters
    ----------
    val: pandas dataframe
         Evaluation structure for each Data source - Event typology.

    Returns
    -------
    val_fuentes: pandas dataframe
                 Data source evaluation structure.

    Example
    -------
    >>> valorar_calidad_global(valoracion)
    Returns datasource quality evaluation dataframe.
    """

    val_fuentes = pd.DataFrame(columns=(FIELD_DATA_SOURCE,
                                        'Tipo',
                                        'Tipologias',
                                        'Valoracion datos obsoletos',
                                        'Tasa falsos positivos',
                                        'Tasa datos duplicados',
                                        'Precio',
                                        'Valoracion manual',
                                        'Calidad',
                                        'Diversidad',
                                        'Total'))

    total_tip = len(set(val['Tipologia']))

    for fuente in set(val['Data source']):
        df_aux = val[val['Data source'] == fuente]
        t_f = df_aux.loc[df_aux.index[0], 'Data source type']
        tip = len(df_aux)
        vdo = df_aux.loc[df_aux.index[0], 'Valoracion datos obsoletos']
        tfp = df_aux.loc[df_aux.index[0], 'Tasa falsos positivos']
        tdd = df_aux.loc[df_aux.index[0], 'Tasa datos duplicados']
        pre = df_aux.loc[df_aux.index[0], 'Precio']
        pre = round(pre, 2)
        v_m = df_aux.loc[df_aux.index[0], 'Valoracion manual']
        cal = df_aux['Calidad'].mean()
        cal = round(cal, 3)
        div = float(tip) / float(total_tip)
        div = round(div, 3)
        tot = cal + div
        tot = round(tot, 3)

        val_fuentes = val_fuentes.append({FIELD_DATA_SOURCE: fuente,
                                          'Tipo': t_f,
                                          'Tipologias': tip,
                                          'Precio': pre,
                                          'Valoracion datos obsoletos': vdo,
                                          'Tasa falsos positivos': tfp,
                                          'Tasa datos duplicados': tdd,
                                          'Valoracion manual': v_m,
                                          'Calidad': cal,
                                          'Diversidad': div,
                                          'Total': tot
                                         }, ignore_index=True)

    del df_aux


    return val_fuentes



###############################################################################
#   GENERACIÓN DE INFORMES                                                    #
###############################################################################
def encode_image(path_to_image):
    """
    XXX
    """

    with open(path_to_image, "rb") as image_file:
        encoded_string = base64.b64encode(image_file.read())
    print(type("data:image/png;base64,"), type(encoded_string))
    print b'data:image/png;base64,' + encoded_string


    return b'data:image/png;base64,' + encoded_string



###############################################################################
def crear_report_fuentes(path, tit, fue_dat, lista_cabecera, df_fuente_obs, df_raw_data, df_quality_data, df_normalized_data, lista_valoracion):
    """
    XXX
    """

    env = Environment(loader=FileSystemLoader('.'))
    ruta_plantilla_temporal = 'temp_html.html'
    template = env.get_template('general_execution_template.html')

    template_vars = {"title": tit,
                     "sufijo_title": fue_dat,
                     "general_information_execution": '',
                     #"logo": encode_image(os.path.join(BASE_PATH,"logo.jpg").replace('\'',''))
                     "logo": os.path.join(BASE_PATH, "logo.jpg").replace('\'', '')
                    }

    # Datos globales de la fuente
    tabla_formateada = "<h3>Informacion global de la fuente de datos:</h3>"
    tabla_formateada += "<table width='100%' border='1' cellspacing='0' cellpadding='2'>"
    tabla_formateada += "<tr>"
    for atributo in lista_cabecera:
        tabla_formateada += "<td align='center' class='black'><strong>" + str(atributo) + "</strong></td>"
    tabla_formateada += "</tr><tr>"
    for atributo in lista_cabecera:
        tabla_formateada += "<td align='center'>" + (str(df_fuente_obs[atributo].values[0])).replace('.', ',') + "</td>"
    tabla_formateada += "</tr></table>"

#    template_vars["general_information_execution"] = tabla_formateada

    # Datos en crudo
    tabla_formateada += "<br/><h3>Datos en bruto por tipologia:</h3>"
    tabla_formateada += "<table width='100%' border='1' cellspacing='0' cellpadding='2'><tr>"
    for nombre_atributo in df_raw_data.columns:
        if nombre_atributo == EVENT_TYPOLOGY:
            tabla_formateada += "<td align='center' class='black letra ancho' colspan='2'>" + nombre_atributo + "</td>"
        else:
            tabla_formateada += "<td align='center' class='black letra ancho' >" + nombre_atributo + "</td>"
    tabla_formateada += "</tr>"
    for ind in range(len(df_raw_data)):
        obs = df_raw_data.iloc[ind:ind+1]
        tabla_formateada += "<tr>"
        for atributo in obs.columns:
            if atributo == EVENT_TYPOLOGY:
                tabla_formateada += "<td align='center' colspan='2'>" + (str(obs[atributo].values[0])).replace('.', ',') + "</td>"
            else:
                tabla_formateada += "<td align='center'>" + (str(obs[atributo].values[0])).replace('.', ',') + "</td>"
        tabla_formateada += "</tr>"
    tabla_formateada += "</table>"

    # Datos normalizados
    tabla_formateada += "<br/><h3>Datos normalizados por tipologia:</h3>"
    tabla_formateada += "<table width='100%' border='1' cellspacing='0' cellpadding='2'>"
    nombres_columnas_datos_normalizados = df_normalized_data.columns
    lista_atributos_normalizados = []
    lista_nombres_niveles = []

    for elemento in nombres_columnas_datos_normalizados:
        if 'nivel' not in elemento:
            lista_atributos_normalizados.append(elemento)
        else:
            lista_nombres_niveles.append(elemento)

    tabla_formateada += "<tr>"

    # Imprimimos cabecera
    for elemento in lista_atributos_normalizados:
        if elemento == EVENT_TYPOLOGY:
            tabla_formateada += "<td align='center' class='black letra ancho' colspan='2'>" + elemento + "</td>"
        else:
            elemento = elemento.replace(' normalizada', '')
            tabla_formateada += "<td align='center' class='black letra ancho'>" + elemento + "</td>"
    tabla_formateada += "</tr>"

    # Imprimimos datos normalizados
    for ind in range(len(df_normalized_data)):
        tabla_formateada += "<tr>"
        obs = df_normalized_data.iloc[ind:ind+1]
#        print(obs)
        for nombre_atributo in lista_atributos_normalizados:
            valor = obs[nombre_atributo].values[0]
#            print(valor)
            nombre_atributo_nivel = nombre_atributo.replace('normalizada', 'nivel')
#            print(nombre_atributo_nivel)
            atributo_nivel = ''
            if nombre_atributo_nivel in lista_nombres_niveles:
                atributo_nivel = obs[nombre_atributo_nivel].values[0]
                if atributo_nivel == 0:
                    tabla_formateada += "<td class='bad' align='center'>" + (str(valor)).replace('.', ',') + "</td>"
                elif atributo_nivel == 1:
                    tabla_formateada += "<td class='acceptable' align='center'>" + (str(valor)).replace('.', ',') + "</td>"
                elif atributo_nivel == 2:
                    tabla_formateada += "<td class='good' align='center'>" + (str(valor)).replace('.', ',') + "</td>"
            else:
                if nombre_atributo_nivel == EVENT_TYPOLOGY:
                    tabla_formateada += "<td align='center' colspan='2'>" + (str(valor)).replace('.', ',') + "</td>"
                else:
                    tabla_formateada += "<td align='center'>" + str(valor) + "</td>"
        tabla_formateada += "</tr>"
    tabla_formateada += "</table>"


    # Valoración por tipología
    tabla_formateada += "<br/><h3>Evaluacion por tipologia:</h3>"
    tabla_formateada += "<table align='center' width='60%' border='1' cellspacing='0' cellpadding='2'><tr>"
    for nombre_atributo in df_quality_data.columns:
        tabla_formateada += "<td align='center' class='black letra ancho' >" + nombre_atributo + "</td>"
    tabla_formateada += "</tr>"
    for ind in range(len(df_quality_data)):
        obs = df_quality_data.iloc[ind:ind+1]
        tabla_formateada += "<tr>"
        for atributo in obs.columns:
            tabla_formateada += "<td align='center'>" + (str(obs[atributo].values[0])).replace('.', ',') + "</td>"
        tabla_formateada += "</tr>"
    tabla_formateada += "</table>"


    # Valoracion final
    tabla_formateada += "<br/><h3>Evaluacion final:</h3>"
    tabla_formateada += "<table align='center' width='60%' border='1' cellspacing='0' cellpadding='2'>"
    tabla_formateada += "<tr>"
    for atributo in lista_valoracion:
        tabla_formateada += "<td align='center' class='black'><strong>" + str(atributo) + "</strong></td>"
    tabla_formateada += "</tr><tr>"
    for atributo in lista_valoracion:
        tabla_formateada += "<td align='center'>" + (str(round(df_fuente_obs[atributo].values[0], 3))).replace('.', ',') + "</td>"
    tabla_formateada += "</tr></table>"

    template_vars["general_information_execution"] = tabla_formateada

    # Generamos el html
    with codecs.open(ruta_plantilla_temporal, 'w', encoding='utf-8') as output_file:
        renderizado = template.render(template_vars)
        # renderizado = renderizado.encode('utf8', 'replace')
        # print renderizado
        output_file.write(renderizado)

#        output_file = codecs.open(ruta_plantilla_temporal, mode="w", encoding="utf8")
#        renderizado = template.render(template_vars)
#        output_file.write(renderizado.decode("utf8", "replace"))

    # Generamos el pdf
    with codecs.open(ruta_plantilla_temporal, 'r') as html_leido:
#        pdf_resultante = os.path.join('.', "Informe_"+fuente_datos+".pdf")
        pdf_resultante = os.path.join(path, "Informe_fuente_"+fue_dat+".pdf")
        with open(pdf_resultante, "wb") as fichero_intermedio:
            pisa.CreatePDF(html_leido.read(), fichero_intermedio)  # ,encoding='cp1252')
    if os.path.exists(ruta_plantilla_temporal):
        os.remove(ruta_plantilla_temporal)



###############################################################################
def generar_informe_fuentes(val, val_fuentes):
    """
    XXX
    """

    path_to_output = os.path.join(BASE_PATH, OUTPUT_DIR)
    for i in range(len(val_fuentes)):
        obs = val_fuentes.iloc[i:i+1]
        vendor = obs[FIELD_DATA_SOURCE].values[0]
        df_tipologia_vendor = val[val['Data source'] == vendor]
        df_raw_typology_data = df_tipologia_vendor[['Tipologia', 'Cantidad', 'Completitud', 'Nivel de informacion', 'Veracidad', 'Veracidad desconocida', 'Frecuencia', 'Consistencia', 'Relevancia alta', 'Relevancia media', 'Relevancia baja', 'Relevancia desconocida', 'Precio por dato']]
        df_normalized_typology = df_tipologia_vendor[['Tipologia', 'Cantidad normalizada', 'Completitud normalizada', 'Nivel de informacion normalizada', 'Veracidad normalizada', 'Veracidad desconocida normalizada', 'Frecuencia normalizada', 'Consistencia normalizada', 'Relevancia alta normalizada', 'Relevancia media normalizada', 'Relevancia baja normalizada', 'Relevancia desconocida normalizada', 'Precio por dato normalizada', 'Cantidad nivel', 'Completitud nivel', 'Nivel de informacion nivel', 'Veracidad nivel', 'Veracidad desconocida nivel', 'Frecuencia nivel', 'Consistencia nivel', 'Precio por dato nivel']]
        df_quality_tiplogy = df_tipologia_vendor[['Tipologia', 'Calidad', 'Exclusividad']]
        crear_report_fuentes(path_to_output, TITULO_FUENTES, vendor, ['Tipo', 'Tipologias', 'Valoracion datos obsoletos', 'Tasa falsos positivos', 'Tasa datos duplicados', 'Precio', 'Valoracion manual'], obs, df_raw_typology_data, df_quality_tiplogy, df_normalized_typology, ['Calidad', 'Diversidad', 'Total'])



###############################################################################
def crear_report_tipologias(path, tit, tip, df_raw_data, df_normalized_data, df_quality_data):
    """
    XXX
    """

    env = Environment(loader=FileSystemLoader('.'))
    ruta_plantilla_temporal = 'temp_html.html'
    template = env.get_template('general_execution_template.html')

    template_vars = {"title": tit,
                     "sufijo_title": tip,
                     "general_information_execution": '',
                     #"logo": encode_image(os.path.join(BASE_PATH,"logo.jpg").replace('\'',''))
                     "logo": os.path.join(BASE_PATH, "logo.jpg").replace('\'', '')
                    }

    # Datos en crudo
    tabla_formateada = "<br/><h3>Datos en bruto por fuente de datos:</h3>"
    tabla_formateada += "<table width='100%' border='1' cellspacing='0' cellpadding='2'><tr>"
    for nombre_atributo in df_raw_data.columns:
        if nombre_atributo == DATA_SOURCE:
            nombre_atributo = FUENTE
            tabla_formateada += "<td align='center' class='black letra ancho' colspan='2'>" + nombre_atributo + "</td>"
        else:
            tabla_formateada += "<td align='center' class='black letra ancho' >" + nombre_atributo + "</td>"
    tabla_formateada += "</tr>"
    for ind in range(len(df_raw_data)):
        obs = df_raw_data.iloc[ind:ind+1]
        tabla_formateada += "<tr>"
        for atributo in obs.columns:
            if atributo == DATA_SOURCE:
                tabla_formateada += "<td align='center' colspan='2'>" + (str(obs[atributo].values[0])).replace('.', ',') + "</td>"
            else:
                tabla_formateada += "<td align='center'>" + (str(obs[atributo].values[0])).replace('.', ',') + "</td>"
        tabla_formateada += "</tr>"
    tabla_formateada += "</table>"

    # Datos normalizados
    tabla_formateada += "<br/><h3>Datos normalizados por fuente de datos:</h3>"
    tabla_formateada += "<table width='100%' border='1' cellspacing='0' cellpadding='2'>"
    nombres_columnas_datos_normalizados = df_normalized_data.columns
    lista_atributos_normalizados = []
    lista_nombres_niveles = []

    for elemento in nombres_columnas_datos_normalizados:
        if 'nivel' not in elemento:
            lista_atributos_normalizados.append(elemento)
        else:
            lista_nombres_niveles.append(elemento)

    tabla_formateada += "<tr>"

    # Imprimimos cabecera
    for elemento in lista_atributos_normalizados:
        if elemento == DATA_SOURCE:
            elemento = FUENTE
            tabla_formateada += "<td align='center' class='black letra ancho' colspan='2'>" + elemento + "</td>"
        else:
            elemento = elemento.replace(' normalizada', '')
            tabla_formateada += "<td align='center' class='black letra ancho'>" + elemento + "</td>"
    tabla_formateada += "</tr>"

    # Imprimimos datos normalizados
    for ind in range(len(df_normalized_data)):
        tabla_formateada += "<tr>"
        obs = df_normalized_data.iloc[ind:ind+1]
#        print(obs)
        for nombre_atributo in lista_atributos_normalizados:
            valor = obs[nombre_atributo].values[0]
#            print(valor)
            nombre_atributo_nivel = nombre_atributo.replace('normalizada', 'nivel')
#            print(nombre_atributo_nivel)
            atributo_nivel = ''
            if nombre_atributo_nivel in lista_nombres_niveles:
                atributo_nivel = obs[nombre_atributo_nivel].values[0]
                if atributo_nivel == 0:
                    tabla_formateada += "<td class='bad' align='center'>" + (str(valor)).replace('.', ',') + "</td>"
                elif atributo_nivel == 1:
                    tabla_formateada += "<td class='acceptable' align='center'>" + (str(valor)).replace('.', ',') + "</td>"
                elif atributo_nivel == 2:
                    tabla_formateada += "<td class='good' align='center'>" + (str(valor)).replace('.', ',') + "</td>"
            else:
                if nombre_atributo_nivel == DATA_SOURCE:
                    tabla_formateada += "<td align='center' colspan='2'>" + (str(valor)).replace('.', ',') + "</td>"
                else:
                    tabla_formateada += "<td align='center'>" + str(valor) + "</td>"
        tabla_formateada += "</tr>"
    tabla_formateada += "</table>"


    # Clasificación por calidad
    tabla_formateada += "<br/><h3>Clasificacion fuentes de datos:</h3>"
    tabla_formateada += "<table align='center' width='30%' border='1' cellspacing='0' cellpadding='2'><tr>"
    for nombre_atributo in df_quality_data.columns:
        if nombre_atributo == DATA_SOURCE:
            nombre_atributo = FUENTE
            tabla_formateada += "<td align='center' class='black letra ancho' colspan='2'>" + nombre_atributo + "</td>"
        else:
            tabla_formateada += "<td align='center' class='black letra ancho' >" + nombre_atributo + "</td>"
    tabla_formateada += "</tr>"
    for ind in range(len(df_quality_data)):
        obs = df_quality_data.iloc[ind:ind+1]
        tabla_formateada += "<tr>"
        for atributo in obs.columns:
            if atributo == DATA_SOURCE:
                tabla_formateada += "<td align='center' colspan='2'>" + (str(obs[atributo].values[0])).replace('.', ',') + "</td>"
            else:
                tabla_formateada += "<td align='center'>" + (str(obs[atributo].values[0])).replace('.', ',') + "</td>"
        tabla_formateada += "</tr>"
    tabla_formateada += "</table>"


    template_vars["general_information_execution"] = tabla_formateada

    # Generamos el html
    with codecs.open(ruta_plantilla_temporal, 'w', encoding='utf-8') as output_file:
        renderizado = template.render(template_vars)
        # renderizado = renderizado.encode('utf8', 'replace')
        # print renderizado
        output_file.write(renderizado)

#        output_file = codecs.open(ruta_plantilla_temporal, mode="w", encoding="utf8")
#        renderizado = template.render(template_vars)
#        output_file.write(renderizado.decode("utf8", "replace"))

    # Generamos el pdf
    with codecs.open(ruta_plantilla_temporal, 'r') as html_leido:
#        pdf_resultante = os.path.join('.', "Informe_"+fuente_datos+".pdf")
        pdf_resultante = os.path.join(path, "Informe_tipologia_"+tip+".pdf")
        with open(pdf_resultante, "wb") as fichero_intermedio:
            pisa.CreatePDF(html_leido.read(), fichero_intermedio)  # ,encoding='cp1252')
    if os.path.exists(ruta_plantilla_temporal):
        os.remove(ruta_plantilla_temporal)



###############################################################################
def generar_informe_tipologias(val):
    """
    XXX
    """

    path_to_output = os.path.join(BASE_PATH, OUTPUT_DIR)
    for tipologia in set(val['Tipologia']):
        df_tipologia = val[val['Tipologia'] == tipologia]
        df_tipologia.sort_values(['Tipologia', 'Calidad', 'Data source'], ascending=[True, False, True], inplace=True)
        df_raw_typology_data = df_tipologia[['Data source', 'Cantidad', 'Completitud', 'Nivel de informacion', 'Veracidad', 'Veracidad desconocida', 'Frecuencia', 'Consistencia', 'Relevancia alta', 'Relevancia media', 'Relevancia baja', 'Relevancia desconocida', 'Precio por dato']]
        df_normalized_typology = df_tipologia[['Data source', 'Cantidad normalizada', 'Completitud normalizada', 'Nivel de informacion normalizada', 'Veracidad normalizada', 'Veracidad desconocida normalizada', 'Frecuencia normalizada', 'Consistencia normalizada', 'Relevancia alta normalizada', 'Relevancia media normalizada', 'Relevancia baja normalizada', 'Relevancia desconocida normalizada', 'Precio por dato normalizada', 'Cantidad nivel', 'Completitud nivel', 'Nivel de informacion nivel', 'Veracidad nivel', 'Veracidad desconocida nivel', 'Frecuencia nivel', 'Consistencia nivel', 'Precio por dato nivel']]

        df_quality_tiplogy = df_tipologia[['Data source', 'Calidad']]

        crear_report_tipologias(path_to_output, TITULO_TIPOLOGIAS, tipologia, df_raw_typology_data, df_normalized_typology, df_quality_tiplogy)



###############################################################################
def crear_report_ranking(path, titulo, df_val_fuentes):
    """
    XXX
    """

    env = Environment(loader=FileSystemLoader('.'))
    ruta_plantilla_temporal = 'temp_html.html'
    template = env.get_template('general_execution_template.html')

    template_vars = {"title": titulo,
                     "sufijo_title": '',
                     "general_information_execution": '',
                     #"logo": encode_image(os.path.join(BASE_PATH,"logo.jpg").replace('\'',''))
                     "logo": os.path.join(BASE_PATH, "logo.jpg").replace('\'', '')
                    }

    # Clasificación por calidad
    tabla_formateada = "<br/><h3>Clasificacion de fuentes de datos:</h3>"
    tabla_formateada += "<table align='center' width='60%' border='1' cellspacing='0' cellpadding='2'><tr>"
    for nombre_atributo in df_val_fuentes.columns:
        if nombre_atributo == FIELD_DATA_SOURCE:
            nombre_atributo = FUENTE
            tabla_formateada += "<td align='center' class='black letra ancho' colspan='2' >" + nombre_atributo + "</td>"
        else:
            tabla_formateada += "<td align='center' class='black letra ancho' >" + nombre_atributo + "</td>"
    tabla_formateada += "</tr>"
    for ind in range(len(df_val_fuentes)):
        obs = df_val_fuentes.iloc[ind:ind+1]
        tabla_formateada += "<tr>"
        for atributo in obs.columns:
            if atributo == FIELD_DATA_SOURCE:
                tabla_formateada += "<td align='center' colspan='2'>" + (str(obs[atributo].values[0])).replace('.', ',') + "</td>"
            else:
                tabla_formateada += "<td align='center'>" + (str(obs[atributo].values[0])).replace('.', ',') + "</td>"
        tabla_formateada += "</tr>"
    tabla_formateada += "</table>"


    template_vars["general_information_execution"] = tabla_formateada

    # Generamos el html
    with codecs.open(ruta_plantilla_temporal, 'w', encoding='utf-8') as output_file:
        renderizado = template.render(template_vars)
        # renderizado = renderizado.encode('utf8', 'replace')
        # print renderizado
        output_file.write(renderizado)

#        output_file = codecs.open(ruta_plantilla_temporal, mode="w", encoding="utf8")
#        renderizado = template.render(template_vars)
#        output_file.write(renderizado.decode("utf8", "replace"))

    # Generamos el pdf
    with codecs.open(ruta_plantilla_temporal, 'r') as html_leido:
#        pdf_resultante = os.path.join('.', "Informe_"+fuente_datos+".pdf")
        pdf_resultante = os.path.join(path, "Ranking fuentes"+".pdf")
        with open(pdf_resultante, "wb") as fichero_intermedio:
            pisa.CreatePDF(html_leido.read(), fichero_intermedio)  # ,encoding='cp1252')
    if os.path.exists(ruta_plantilla_temporal):
        os.remove(ruta_plantilla_temporal)



###############################################################################
def generar_informe_ranking(val_fuentes):
    """
    XXX
    """

    path_to_output = os.path.join(BASE_PATH, OUTPUT_DIR)

    df_valoracion_fuentes = val_fuentes[[FIELD_DATA_SOURCE, 'Tipo', 'Precio', 'Calidad', 'Diversidad', 'Total']]
    df_valoracion_fuentes.sort_values(['Total', 'Calidad', 'Diversidad', FIELD_DATA_SOURCE], ascending=[False, False, False, True], inplace=True)

    crear_report_ranking(path_to_output, TITULO_RANKING, df_valoracion_fuentes)



###############################################################################
# Functions for plotting                                                      #
###############################################################################

def makedir(directory_path):
    '''
    Creates a new directory, if it exists, it does nothing.

    Parameters
    ----------
    directory_path: str
        The path of the directory to be created
    '''

    try:
        os.mkdir(directory_path)
    except WindowsError:
        pass



###############################################################################
def process_valoracion_tipologia(valoracion_tipologia, plot_col):
    '''
    Extracts an argument tuple for matplotlib's bar chart from the evaluation structure.
    Sorts the raw dimension values from highest to lowest, so the first bar will have the highest value and so on.
    Also paints the bars with the color corresponding to the level of the dimension.

    Parameters
    ----------
    valoracion_tipologia: pandas.DataFrame
                          Evaluation structure for a single event typology.

    '''

    # Se ordenan los valores de mayor a menor, excepto precio por dato, en el que menor es mejor
    ascending = valoracion_tipologia.columns[1] in ['Precio por dato', 'Veracidad desconocida']
    val = copy.deepcopy(valoracion_tipologia)
    val.sort_values(by=valoracion_tipologia.columns[1], ascending=ascending, inplace=True)

    # Mapeo de los niveles a colores
    dict_map = {GOOD_LEVEL: 'g', ACCEPTABLE_LEVEL: 'y', BAD_LEVEL: 'r'}


    # Los argumentos
    # x: las etiquetas del eje X: las fuentes básicamente
    # height: los valores del eje Y: el valor en bruto para la dimensión
    # color: el color de las barras: correspondientes al nivel
    return {'x': list(val[plot_col].values.ravel()),
            'height': list(val[val.columns[1]].values.ravel()),
            'color': list(map(lambda x: dict_map[x], val[val.columns[2]]))
           }



###############################################################################
def plot_comparison_sources(valoracion_tipologia, tipologia, dimension, save_path, plot_col):
    '''
    Saves a plot of the comparison of a certain data quality dimension
    between all data sources that offer it.

    Parameters
    ----------
    valoracion_tipologia: pandas.DataFrame
        Evaluation structure for a single event typology.
    tipologia: str
        The selected typology
    dimension: str
        The selected data quality dimension
    save_path: str
        Directory in which to save the generated plot
    '''

    plot_kwargs = process_valoracion_tipologia(valoracion_tipologia, plot_col)
    plt.figure()
    plt.xticks(rotation=-45)
    plt.title(u'Valoración de la dimensión %s en % s' % (dimension, tipologia))
    # Otherwise the scale of the Y axis may be such that the bar will be not visible
    plt.ylim(bottom=0)
    index = -(dimension in ['Precio por dato', 'Veracidad desconocida'])
    top = plot_kwargs['height'][index] + plot_kwargs['height'][index] * 0.05
    if not top:
        top = 1
    plt.ylim(top=top)
    plt.xlabel(u"Fuentes")
    plt.ylabel(dimension)
    plt.bar(**plot_kwargs)
    plt.savefig(os.path.join(save_path, dimension), bbox_inches='tight')
    plt.close()



###############################################################################
def generar_plots(valoracion, tipo_plot):
    '''
    Generates comparison plots between data sources for each of the the data
    quality dimensions.

    Plots are saved in temp folder by default, with one subfolder for
    each typology and within them, a png plot for each quality dimension.

    Parameters
    ----------
    valoracion: pandas.DataFrame
                Evaluation structure for each Data source - Event typology.
    tipo_plot: string
               It indicates the plot type: 'Tipología' o 'Data source'

    Returns
    -------
    None
    '''

    valoracion = valoracion.copy(deep=True)
    # Sustituye la consistencia por un valor numérico
    valoracion['Consistencia'] = map(lambda x: EQUIVALENCIA_CONSISTENCIA_NUMERICA[x], valoracion['Consistencia'])

    # Elimina plots creados en anteriores ejecuciones
    # delete_directory_contents(TEMP_DIR)

    # Tipologias evaluadas
    if tipo_plot == 'Data source':
        subdirectorio = 'Fuentes'
    elif tipo_plot == 'Tipologia':
        subdirectorio = 'Tipologias'
    path = os.path.join(TEMP_DIR, subdirectorio)
    makedir(path)

    tipologias = set(valoracion[tipo_plot])
    for tip in tipologias:
        tipologia_path = os.path.join(path, tip)
        # Crea el directorio para plots de esta tipologia
        makedir(tipologia_path)
        for dim in COMPARISON_PLOTS_DIMENSIONS:
            # Extrae fuente, dimension en bruto y su nivel.
            valoracion_tipologia = valoracion.loc[valoracion[tipo_plot] == tip, [tipo_plot, dim, dim + ' nivel']]
            # Crea y guarda el plot
            plot_comparison_sources(valoracion_tipologia, tip, dim, tipologia_path, tipo_plot)
