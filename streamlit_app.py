'''
Codigo para la creacion de una aplicacion para la realizacion de estudios
operacionales en streamlit
'''

from urllib.request import urlopen
from time import sleep
from boto3 import Session
from zipfile import ZipFile
import io
import datetime as dt
import locale
import numpy as np
import pandas as pd
import streamlit as st

# Definition of functions and constants
SERVICE = "http://mesonet.agron.iastate.edu/cgi-bin/request/asos.py?"
MAX_ATTEMPTS = 6
letters = ['A', 'B', 'C', 'D']
text_act = ''
text_prop = ''
download_status = False


def get_METAR(set_station, dates):
    '''
    timestamps in UTC to request data for
    Needed METAR data is downloaded and transformed
    '''

    service = (
        f'{SERVICE}data=all&tz=Etc/UTC&format=comma&latlon=no'
        f'&missing=null&trace=null&report_type=1&report_type=2&'
        f'{dates[0].strftime("year1=%Y&month1=%m&day1=%d&")}'
        f'{dates[1].strftime("year2=%Y&month2=%m&day2=%d&")}'
        )

    data = download_data(f'{service}&station={set_station}').split("\n", 5)[-1]
    if data == '':
        return ''
    data_df = pd.read_csv(io.StringIO(data), sep=',')
    data_df = (
        data_df[['valid', 'vsby', 'skyc1', 'skyc2', 'skyc3', 'skyc4',
                 'skyl1', 'skyl2', 'skyl3', 'skyl4', 'drct', 'sknt', 'metar']]
        .assign(
            vsby=round(data_df['vsby'] * 1609.34),
            Ceil=data_df.apply(lambda row: get_ceil({
                row.skyc1: row.skyl1, row.skyc2: row.skyl2,
                row.skyc3: row.skyl3, row.skyc4: row.skyl4
                }),
                axis=1),
            RVR=data_df.apply(lambda row: get_RVR(row.metar), axis=1),
            MetarTime=pd.to_datetime(data_df['valid']),
            Direccion=data_df['drct'].interpolate(),
            Vis=lambda df: df['vsby'].interpolate(),
            Viento=data_df['sknt'].interpolate(),
            NoseWind=lambda df:
                df['Viento']*np.cos((rwy_or - df['Direccion']) * np.pi/180),
            CrossWind=lambda df:
                df['Viento']*np.sin((rwy_or - df['Direccion']) * np.pi/180),
            CAVOK=0,
            PistaViento=lambda df: df['NoseWind'].map(
                lambda x: rwy if x >= 10 else rwy_opp
                )
            )
        .dropna(subset=['MetarTime'])
        .sort_values('MetarTime')
        )
    data_df['Clouds'] = data_df['Ceil'].str[0]
    data_df['Ceil'] = data_df['Ceil'].str[1]

    data_df.loc[data_df['RVR'] == 2000, 'RVR'] = data_df['Vis']
    data_df.loc[
        data_df['metar'].str.contains('CAVOK', case=False), 'CAVOK'
        ] = 1
    data_df = data_df[[
            'Ceil', 'Clouds', 'RVR', 'MetarTime', 'Direccion', 'Vis',
            'Viento', 'NoseWind', 'CrossWind', 'CAVOK', 'PistaViento'
            ]]

    return data_df


def download_data(uri):
    '''
    Fetch the data from the IEM
    The IEM download service has some protections in place to keep the number
    of inbound requests in check.  This function implements an exponential
    backoff to keep individual downloads from erroring.
    Args:
      uri (string): URL to fetch
    Returns:
      string data
    '''
    attempt = 0
    while attempt < MAX_ATTEMPTS:
        try:
            data = urlopen(uri, timeout=300).read().decode("utf-8")
            if data is not None and not data.startswith("ERROR"):
                return data
        except Exception as exp:
            print('Download_data(%s) failed with %s' % (uri, exp))
            sleep(5)
        attempt += 1

    print('Exhausted attempts to download, returning empty data')
    return ''


def get_ceil(clouds):
    for c in clouds:
        if c in ['BKN', 'OVC', 'VV ']:
            return c, clouds[c]
    return '', 10000


def get_RVR(metar):
    metar_list = metar.split()
    for block in metar_list:
        slash = block.find('/')
        if (
                block[1:slash] == rwy and block.count('/') == 1
                and len(block) == len(rwy) + 7
                ):
            if block[slash+1].isalpha():
                return int(block[slash+2:])
            else:
                return int(block[slash+1:-1])
    return 2000


@st.cache
def convert_df(df):
    '''
    Important: Cache the conversion to prevent computation on every rerun
    '''
    return df.to_csv(index=False).encode('utf-8')


# Change to Spanish
locale.setlocale(locale.LC_ALL, '')

# Read secrets
AWS_ACCESS_KEY_ID = st.secrets['AWS_ACCESS_KEY_ID']
AWS_SECRET_ACCESS_KEY = st.secrets['AWS_SECRET_ACCESS_KEY']

# Read luces.csv
if 'lig' not in st.session_state:
    st.session_state.lig = (
        pd.read_csv(
            's3://proyectos-internos/Estudios_Operacionales/Luces.csv',
            sep=';',
            storage_options={
                'key': AWS_ACCESS_KEY_ID,
                'secret': AWS_SECRET_ACCESS_KEY
            },
            usecols=[0, 1, 3]
            )
        .rename(columns={'CLASIFICACIÓN AIR-OPS': 'Air_Ops'})
        )
    st.session_state.lig.columns = (
        st.session_state.lig.columns.str.title().to_list()
        )

# Read AIR_OPS.csv
if 'air_ops' not in st.session_state:
    st.session_state.air_ops = pd.read_csv(
        's3://proyectos-internos/Estudios_Operacionales/AIR_OPS.csv',
        sep=',',
        storage_options={
            'key': AWS_ACCESS_KEY_ID,
            'secret': AWS_SECRET_ACCESS_KEY
        }
    )

# Read CAT_APC.csv
if 'apc' not in st.session_state:
    st.session_state.apc = (
        pd.read_csv(
            's3://proyectos-internos/Estudios_Operacionales/CAT_APC.csv',
            sep=',',
            storage_options={
                'key': AWS_ACCESS_KEY_ID,
                'secret': AWS_SECRET_ACCESS_KEY
                }
            )
        .rename(columns={'codigo OACI': 'ACFT'})
        )

# Read ACFT_equivalencias_V2.csv
if 'asi_df' not in st.session_state:
    st.session_state.asi_df = (
        pd.read_csv(
            's3://proyectos-internos/Estudios_Operacionales/'
            'ACFT_equivalencias_V2.csv',
            sep=',',
            storage_options={
                'key': AWS_ACCESS_KEY_ID,
                'secret': AWS_SECRET_ACCESS_KEY
                }
            )
        .rename(columns={
            'Tipo\xa0de\xa0aeronave': 'ACFT',
            'Tipo\xa0de\xa0aeronave SUSTITUTO': 'ACFT_sus'
            })
        .merge(
            st.session_state.apc, left_on='ACFT_sus', right_on='ACFT',
            how='left', suffixes=('', '_')
            )
        .drop(['ACFT_sus', 'ACFT_'], axis=1)
        .rename(columns={'TipoAeronave': 'ACFT'})
        )
    st.session_state.apc = pd.concat([
        st.session_state.apc, st.session_state.asi_df
        ])

st.header('Herramienta para el desarrollo de estudios operacionales.')

# Read Palestra.csv
if 'palestra' not in st.session_state:
    with st.spinner(
            'Cargando datos para el funcionamiento de la aplicación. '
            'Esta carga sólo se realizará una vez durante la sesión'
            ):
        session = Session(
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
        bucket = session.resource('s3').Bucket('proyectos-internos')
        prefix_objs = bucket.objects.filter(
            Prefix='Estudios_Operacionales/palestra'
            )
        palestra_list = []
        for obj in prefix_objs:
            palestra_list.append(
                pd.read_csv(
                    f's3://proyectos-internos/{obj.key}',
                    storage_options={
                        'key': AWS_ACCESS_KEY_ID,
                        'secret': AWS_SECRET_ACCESS_KEY
                        },
                    usecols=[
                        'HoraArribada', 'TipoAeronave',
                        'ReglasVuelo', 'PistaArr'
                        ]
                    )
                .dropna(subset=['HoraArribada', 'PistaArr'])
                .loc[lambda df: df['ReglasVuelo'] != 'V']
                .drop('ReglasVuelo', axis=1)
                .assign(PistaArr=lambda df: df['PistaArr'].str[:8])
                )
        st.session_state.palestra = pd.concat(palestra_list)

if 'down_st' not in st.session_state:
    st.session_state.down_st = False

# An airport is selected
rwy = False
send_sd = False

airps = (
    [''] +
    st.session_state.lig['Aeropuerto'].drop_duplicates().sort_values()
    .tolist()
    )
airp = st.selectbox('Seleccione un aeropuerto', airps)

# A runway is selected
if airp != '':
    rwys = (
        [''] +
        st.session_state.lig.loc[
            st.session_state.lig['Aeropuerto'] == airp
            ]['Pista'].str[3:].tolist()
        )
    rwy = st.selectbox('Seleccione una pista', rwys)
    if rwy != '':
        rwy_int = int(rwy[:2])
        rwy_or = rwy_int * 10
        if rwy_int > 18:
            rwy_opp = rwy_int - 18
        else:
            rwy_opp = rwy_int + 18
        rwy_opp = str(rwy_opp).zfill(2)
        if rwy[-1] == 'R':
            rwy_opp = f'{rwy_opp}L'
        elif rwy[-1] == 'L':
            rwy_opp = f'{rwy_opp}R'
        elif rwy[-1] == 'C':
            rwy_opp = f'{rwy_opp}C'

# Needed data for the study is inserted
if rwy:
    with st.form(key='study_data'):
        st_title = st.text_input("Nombre del estudio:", f'{airp}-{rwy}')
        st_date = st.text_input(
            "Fecha de entrada en vigor:",
            dt.date.today().strftime('%B_%Y').title()
            )
        dates = st.date_input(
            'Rango de fechas a estudiar:',
            value=(
                dt.date(year=2010, month=1, day=1),
                dt.date.today() - dt.timedelta(days=1)
                ),
            min_value=dt.date(year=2010, month=1, day=1),
            max_value=dt.date.today() - dt.timedelta(days=1)
            )

        st.subheader('Minimos actuales')
        min_act = []
        for let in 'ABCD':
            min_act.append(
                st.number_input(
                    f'CAT {let}', 100, 9999, step=10, key=f'act_{let}'
                    )
                )

        st.subheader('Minimos a implantar')
        min_prop = []
        for let in 'ABCD':
            min_prop.append(
                st.number_input(
                    f'CAT {let}', 100, 9999, step=10, key=f'new_{let}'
                    )
                )

        send_sd = st.form_submit_button(
            label='Enviar datos del estudio'
            )

# METARs are obtained
if send_sd:
    st.session_state.down_st = False
    if len(dates) < 2:
        st.error(
            'El rango de fechas introducido no es correcto, pruebe a '
            'introducir una fecha de inicio y otra de final del estudio'
            )
    else:
        with st.spinner('Obteniendo METAR'):
            metar = get_METAR(airp, dates)
        st.subheader('Datos METAR obtenidos')
        with st.spinner('Obtenemos datos de luces y RVR'):
            luz = st.session_state.lig.loc[
                (st.session_state.lig['Aeropuerto'] == airp) &
                (st.session_state.lig['Pista'] == f'RWY{rwy}')
                ].iloc[0, 2]

            rvr_act = []
            rvr_prop = []

            for level_act, level_new in zip(min_act, min_prop):
                rvr_act.append(st.session_state.air_ops.loc[
                    (st.session_state.air_ops['DH_Min'] <= level_act) &
                    (st.session_state.air_ops['DH_Max'] >= level_act),
                    luz
                    ].iloc[0])

                rvr_prop.append(st.session_state.air_ops.loc[
                    (st.session_state.air_ops['DH_Min'] <= level_new) &
                    (st.session_state.air_ops['DH_Max'] >= level_new),
                    luz
                    ].iloc[0])

            for pos, let in enumerate(letters):
                text_act += f'CAT {let}: {rvr_act[pos]}  \n'
                text_prop += f'CAT {let}: {rvr_prop[pos]}  \n'

        # st.markdown(f'Luces: {luz}')
        # st.markdown(f'RVR mínimo actual:  \n {text_act}')
        # st.markdown(f'RVR mínimo propuesto:  \n {text_prop}')

        with st.spinner('Se crea la configuracion'):
            config_data = {
                'Aeropuerto': airp,
                'Pista': rwy,
                'Luces': luz,
                'CAT_App': letters,
                'Min_Act': min_act,
                'Min_Prop': min_prop,
                'RVR_Min_Act': rvr_act,
                'RVR_Min_Prop': rvr_prop,
                'Nombre_Estudio': st_title,
                'Fecha_Vigor': st_date
                }
            st.session_state.csv_config = pd.DataFrame(config_data)

            rvr_lis = [rvr_act, rvr_prop]
            min_lis = [min_act, min_prop]
            for pos, val in enumerate(letters):
                for col, vec in zip(
                        ['Vis', 'RVR', 'Ceil'],
                        [rvr_lis, rvr_lis, min_lis]
                        ):
                    for tim, elem in enumerate(['Act_', '']):
                        metar[f'OK_{col}_{elem}CAT_{val}'] = 0
                        metar.loc[
                            metar[col] > vec[tim][pos],
                            f'OK_{col}_{elem}CAT_{val}'
                            ] = 1
        st.subheader('Datos de configuracion obtenidos')
        with st.spinner('Se leen datos de Palestra'):
            palestra = (
                st.session_state.palestra
                .loc[st.session_state.palestra['PistaArr'].str[:4] == airp]
                .merge(
                    st.session_state.apc, left_on='TipoAeronave',
                    right_on='ACFT', how='left'
                    )
                .assign(
                    HoraArribada=lambda df: pd.to_datetime(df['HoraArribada']),
                    MetarTime=lambda df: df['HoraArribada'].dt.floor('30 min'),
                    APC=lambda df: df['APC'].fillna('Ops_Sin_CAT')
                    )
                .loc[lambda df:
                     (df['HoraArribada'] >=
                      dt.datetime.combine(dates[0], dt.datetime.min.time())) &
                     (df['HoraArribada'] <=
                      dt.datetime.combine(dates[1], dt.datetime.max.time()))
                     ]
                .drop('ACFT', axis=1)
                .sort_values('HoraArribada')
                )
            st.session_state.palestra_df = palestra.copy()

            count_pal = (
                palestra
                .groupby(['MetarTime', 'APC'], as_index=False).size()
                .rename(columns={'size': 'Ops'})
                .pivot(index='MetarTime', values='Ops', columns='APC')
                .reset_index()
                .fillna(0)
                )
        st.subheader('Datos de Palestra obtenidos')

        with st.spinner('Se cruza Palestra con los METAR'):
            merged_metar = (
                pd.merge_asof(
                    metar, palestra[['HoraArribada', 'PistaArr']],
                    left_on='MetarTime', right_on='HoraArribada',
                    direction='nearest', tolerance=pd.Timedelta(seconds=3600)
                    )
                .drop('HoraArribada', axis=1)
                .merge(count_pal, how='left', on='MetarTime')
                .rename(columns={
                    'A': 'Ops_CAT_A', 'B': 'Ops_CAT_B', 'C': 'Ops_CAT_C',
                    'D': 'Ops_CAT_D'
                    })
                .assign(
                    PistaArr=lambda df: (
                        df['PistaArr'].fillna(df['PistaViento'])
                        .fillna(method='ffill', limit=3)
                        )
                    )
                .fillna(0)
                )
            metar_rwy = merged_metar.loc[
                merged_metar['PistaArr'] == f'{airp}-rwy'
                ]
            st.session_state.merged_metar = merged_metar.copy()
            st.session_state.metar_rwy = metar_rwy.copy()
        st.subheader('Cruce de Palestra y METAR realizado')

        with st.spinner('Se obtienen los datos de frustradas'):
            try:
                frus = (
                    pd.read_excel(
                        's3://proyectos-internos/Estudios_Operacionales/'
                        'frustradas diario de novedades.xlsx',
                        sheet_name=f'MotorAire_{airp}',
                        converters={'Pista': str.upper},
                        usecols=[
                            'Indicativo', 'Fecha/Hora UTC', 'Pista', 'Causa'
                            ],
                        storage_options={
                            'key': AWS_ACCESS_KEY_ID,
                            'secret': AWS_SECRET_ACCESS_KEY
                            }
                        )
                    .rename(columns={
                        'Fecha/Hora UTC': 'HoraFrustrada',
                        'Causa': 'Causa1'})
                    .loc[lambda df: df['Pista'] == rwy]
                    .drop('Pista', axis=1)
                    )
                frus['Causa1'] = frus['Causa1'].str.split(':').str[0]
                frus[['Causa1', 'Causa2']] = (
                    frus['Causa1'].str.split('_', expand=True)
                    )
                st.session_state.frus = frus.copy()
            except ValueError:
                frus = pd.DataFrame(
                    columns=['Indicativo, HoraFrustrada', 'Causa1', 'Causa2']
                    )
                st.warning('Enaire no cubre este aeropuerto')
        st.subheader('Datos de frustradas obtenidos')
        st.session_state.down_st = True
        st.balloons()

if st.session_state.down_st:
    with open(f'{st_title}.zip', 'rb') as file:
        with ZipFile(f'{st_title}.zip', 'w') as zipObj:
            zipObj.writestr(
                'configuracion.csv', convert_df(st.session_state.csv_config)
                )
            zipObj.writestr(
                'metar_filtrado.csv', convert_df(st.session_state.merged_metar)
                )
            zipObj.writestr(
                'metar_pista.csv', convert_df(st.session_state.metar_rwy)
                )
            zipObj.writestr(
                'palestra.csv', convert_df(st.session_state.palestra_df)
                )
            zipObj.writestr(
                'frustradas.csv', convert_df(st.session_state.frus)
                )
            st.session_state.zip = f'{st_title}.zip'
        st.download_button(
            'Descargar ficheros', file, file_name=f'{st_title}.zip'
            )
