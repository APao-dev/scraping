#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script para hacer scraping sobre la página de los
distribuidores de Kommerling en España:
https://www.kommerling.es/red-oficial/

Descargando los datos de todos los distribuidores,
y generando un CSV.

Ejecución ~$ python3 baja_kommerling_ok.py


"""

from bs4 import BeautifulSoup
import requests
import pickle
import pprint
import datetime


def get_urls_provincias(url_red_oficial):
    """
    Partiendo de la url "red-oficial", descarga y devuelve las urls
    de las distintas provincias, para procesarlas posteriormente
    y obtener las urls de cada distribuidor individual.

    :param url_red_oficial:
    :return: l_urls
    """
    l_urls = []
    soup = BeautifulSoup(requests.get(url=url_red_oficial).content)
    for tag in soup.find_all('a', {'class': ["province-container"]}):
        l_urls.append('https://www.kommerling.es' + tag['href'])

    return sorted(set(l_urls))



def get_urls_distribuidores(url_provincia):
    """
    Utilizando la url_provincia descargada en get_urls_provincias(),
    descarga y devuelve las urls de los distribuidores de cada provincia,
    para procesarlos posteriormente y obtener los datos de cada distribuidor individual.

    :param url_provincia:
    :return: l_datos
    """

    l_urls = []
    soup = BeautifulSoup(requests.get(url=url_provincia).content)
    for tag in soup.find_all('a', {'class': ["RedResults__mapItems"]}):
        l_urls.append('https://www.kommerling.es' + tag['href'])

    return sorted(set(l_urls))


def get_datos_distribuidor(url_distribuidor):
    """

    :param url_distribuidor:
    :return: d_datos
    """

    d_datos = {
        'url_kommerling' : url_distribuidor
        }
    soup = BeautifulSoup(requests.get(url=url_distribuidor).content)
    d_datos['nombre'] = soup.find('div', {'class': ["RedContact__data-title"]}).text.strip()
    try:
        d_datos['telf'] = soup.find('div', {'class': ["RedContact__data-phone"]}).text.split('Teléfono:')
        d_datos['telf'] = d_datos['telf'][-1].strip()
    except:
        d_datos['telf'] = ''

    try:
        d_datos['movil'] = soup.find('div', {'class': ["RedContact__data-mobile"]}).text.split('Teléfono Móvil:')
        d_datos['movil'] = d_datos['movil'][-1].strip()
    except:
        d_datos['movil'] = ''

    try:
        d_datos['web'] = soup.find('div', {'class': ["RedContact__data-web"]}).text.strip()
    except:
        d_datos['web'] = ''

    dir_completa = soup.find_all('div', {'class': ["RedContact__data-address"]})
    d_datos['direccion'] = dir_completa[0].text.strip()
    try:
        d_datos['cp_poblacion'] = dir_completa[1].text.strip()
    except:
        d_datos['cp_poblacion'] = ''
    try:
        d_datos['provincia'] = dir_completa[2].text.strip()
    except:
        d_datos['provincia'] = ''

    try:
        lat_lon = soup.find_all('div', {'class': ["RedContact__data-location"]})[0]\
            .find('div', {'id': 'mrmilu-map'}).find('div', {'ng-controller': 'MapController'})
        d_datos['gps_lat'] = lat_lon['latitude']
        d_datos['gps_lon'] = lat_lon['longitude']
    except:
        d_datos['gps_lon'] = ''
        d_datos['gps_lat'] = ''

    return d_datos



def baja_todos_distribuidores(debug=False, graba_parciales=True):
    
    

    l_urls_distribuidor = []
    l_datos = []
    print('Obteniendo URLs de provincias ...')
    for url_provincia in get_urls_provincias('https://www.kommerling.es/red-oficial/'):
        print(f'Obteniendo distribuidores de {url_provincia}')
        l_urls_distribuidor += get_urls_distribuidores(url_provincia)

    tot_distribs = len(l_urls_distribuidor)
    for i, url_distribuidor in enumerate(l_urls_distribuidor) :
        print(f'Bajando {i+1} de {tot_distribs}')
        l_datos.append(get_datos_distribuidor(url_distribuidor))

        if debug:
            pprint.pprint(l_datos[-1])

        # Va grabando el resultado de los ya descargados en disco
        if graba_parciales:
            filename = 'kommerling_distribs.pkl'
            print(f'Grabando resultado en "{filename}"')
            with open(filename, 'wb') as f:
                pickle.dump(l_datos, f)

    return l_datos


def graba_csv(l_datos, filename):
    """
    Recibe una lista de dicts, que procesa y convierte en CSV.

    :param l_datos:
    :param filename:
    :return:
    """
    # {'cp_poblacion': '50005 Zaragoza',
    #  'direccion': 'C/ Toledo 8',
    #  'gps_lat': '41.647782',
    #  'gps_lon': '-0.895786',
    #  'movil': '645 91 98 46',
    #  'nombre': 'VENTANAS DIKTER S.L.',
    #  'provincia': 'Zaragoza',
    #  'telf': '976 56 14 28',
    #  'url_kommerling': 'https://www.kommerling.es/red-oficial/zaragoza/zaragoza/ventanas-dikter-sl',
    #  'web': 'https://www.dikterzaragoza.com'}

    l_keys = ['nombre', 'direccion', 'cp_poblacion', 'provincia',
              'telf', 'movil', 'web', 'gps_lat', 'gps_lon', 'url_kommerling']

    f = open(filename, 'w')
    f.write( ' ; '.join(l_keys) + ' ; \n' )
    for d_orig in l_datos:
        # Creamos todos las keys del diccionario, para evitar errores al volcarlas
        # (puede ser que esa key no haya venido del scrapping)
        d = {k: d_orig.get(k, '') for k in l_keys}
        txt_linea = ''
        txt_linea += f"{d['nombre']} || {d['direccion']} || {d['cp_poblacion']} || {d['provincia']} || "
        txt_linea += f"{d['telf']} || {d['movil']} || {d['web']} || {d['gps_lat']} || {d['gps_lon']} || "
        txt_linea += f"{d['url_kommerling']} || \n"
        # Eliminamos ; , " y '
        txt_linea = txt_linea.replace(';', '').replace('"', '').replace("'", '')
        # Cambiamos el separador de campos por ;
        txt_linea = txt_linea.replace(' || ', ' ; ')
        f.write(txt_linea)
    f.close()

    print(f"\n\n\t ---> Grabados {len(l_datos)} en fichero: '{filename}'\n")



if __name__ == '__main__':
    print(f"\n\t Iniciando descarga de resultados de Kommerling España ...\n")
    # utilizamos los datos grabados en disco (si existen)
    try:
        l_datos = pickle.load(open('kommerling_distribs.pkl', 'rb'))
        print(f'Utilizamos los {len(l_datos)} ya descargados')
    except:
        print(f'No hay datos descargados. Iniciamos descarga. ', datetime.datetime.now())
        l_datos = baja_todos_distribuidores(debug=True, graba_parciales=False)

    ahora = datetime.datetime.now().strftime("%Y%m%d_")
    graba_csv(l_datos=l_datos, filename=ahora+'kommerling.csv')
    print("Proceso finalizado. ", datetime.datetime.now())

