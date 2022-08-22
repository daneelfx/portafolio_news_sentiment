from Impala_Helper import Helper
from logger import logger

import math
import pandas as pd
import os
import shutil
import unidecode
from deep_translator import GoogleTranslator

def is_match(search_term, text_content):
  
  MAX_DISTANCE = 2
  
  search_term_parts = search_term.split()
  num_search_terms = len(search_term_parts)

  if num_search_terms <= 1 and search_term in text_content:
    return True
  
  text_content_parts = text_content.split()

  idx = 0
  idx_found = math.inf

  for part_idx, part_str in enumerate(text_content_parts):
        
    if part_str.startswith(search_term_parts[idx]):
      idx += 1
      idx_found = part_idx

      if idx == num_search_terms:
        return True
    
    if idx_found + MAX_DISTANCE < part_idx:
      idx = 0
      idx_found = math.inf

  return False

def clean_news(input_dataframe):

  input_dataframe = input_dataframe.astype(str)

  input_dataframe['search_term_lowercase'] = input_dataframe['search_term'].apply(lambda search_term: unidecode.unidecode(search_term).lower())
  input_dataframe['news_text_content_lowercase'] = input_dataframe['news_text_content'].apply(lambda content: unidecode.unidecode(content).lower())

  input_dataframe = input_dataframe[input_dataframe.apply(lambda row: is_match(row['search_term_lowercase'], row['news_text_content_lowercase']), axis = 1)]

  del input_dataframe['search_term_lowercase']
  del input_dataframe['news_text_content_lowercase']

  return input_dataframe.drop_duplicates(subset = ['news_title', 'news_subtitle', 'news_text_content'])

def get_date_inputs():
  months = {'enero': '01', 'febrero': '02', 'marzo': '03', 'abril': '04', 'mayo': '05', 'junio': '06', 
          'julio': '07', 'agosto': '08', 'septiembre': '09', 'octubre': '10', 'noviembre': '11', 'diciembre': '12'}

  print('*' * 40, ' FECHA INICIAL', '*' * 40, sep = '\n')

  while True:
    start_month = input('Ingrese el mes (el nombre):\n').strip().lower()
    if start_month in months:
      break
    print(f'Ingresó un valor inválido: {start_month}')

  while True:
    start_year = input('Ingrese el año (el número):\n').strip()
    if len(start_year) == 4:
      break
    print(f'Ingresó un valor inválido: {start_year}')

  print('*' * 40, 'FECHA FINAL', '*' * 40, sep = '\n')

  while True:
    end_month = input('Ingrese el mes (el nombre):\n').strip().lower()
    if end_month in months:
      break
    print(f'Ingresó un valor inválido: {end_month}')

  while True:
    end_year = input('Ingrese el año (el número):\n').strip()
    if len(end_year) == 4:
      break
    print(f'Ingresó un valor inválido: {end_year}')

  print('*' * 40)
  print('\nFecha inicial:', f'{start_month.capitalize()} de {start_year}')
  print('Fecha final:', f'{end_month.capitalize()} de {end_year}')

  return months[start_month], start_year, months[end_month], end_year

def save_local(dir_name, result_filename):

  file_to_delete = f'{result_filename}.csv'

  if os.path.exists(file_to_delete):
    os.remove(file_to_delete)

  try:
    all_news = pd.DataFrame()
    for current_file in os.listdir(dir_name):
      current_news = pd.read_csv(f'{dir_name}/{current_file}')
      current_news = current_news.astype(str)
      all_news = pd.concat([all_news, current_news])
      
    all_news.to_csv(f'{result_filename}.csv', index = False)
    print(f'LAS NOTICIAS FUERON GUARDADAS EXITOSAMENTE COMO {result_filename}.csv')
  except:
    print('¡ERROR!: INTENTE DESCARGAR Y LIMPIAR LAS NOTICIAS NUEVAMENTE')

Translator = GoogleTranslator(source='es', target='en')

def get_translation(content):
  parts = content.split('.')
  translated = ''
  for part in parts:
    try:
      translated_part = Translator.translate(part)
    except:
      translated_part = part
    finally:
      if translated_part:  
        translated += translated_part + '.'
        
  return translated[:-1]

def translate_news(news_dataframe, files_location, batch_size = 10):

  news_dataframe = news_dataframe.astype(str)
  translation = pd.DataFrame()
  num_steps = math.ceil(news_dataframe.shape[0] / batch_size)

  if os.path.exists(files_location):
    if os.listdir(files_location):
      shutil.rmtree(files_location)
      os.mkdir(files_location)
  else:
    os.mkdir(files_location)

  for step in range(num_steps):

    english_news_part = news_dataframe.copy().iloc[step * batch_size: (step + 1) * batch_size, :]

    english_news_part['news_title_english'] = english_news_part['news_title'].apply(lambda title: get_translation(title))
    english_news_part['news_subtitle_english'] = english_news_part['news_subtitle'].apply(lambda subtitle: get_translation(subtitle))
    english_news_part['news_text_content_english'] = english_news_part['news_text_content'].apply(lambda content: get_translation(content))

    del english_news_part['news_title']
    del english_news_part['news_subtitle']
    del english_news_part['news_text_content']

    english_news_part.to_csv(f'{files_location}/part-{step}.csv', index = False)

    print(f'{files_location}-{step}')

  for step in range(num_steps):
    to_append = pd.read_csv(f'{files_location}/part-{step}.csv')
    to_append = to_append.astype(str)
    translation = pd.concat([translation, to_append])

  return translation

def upload_to_lz(dataframe, database, table_name):
  dataframe = dataframe.astype(str)
  cache = {'connStr' : 'DSN=impala-prod', 'db' : database, 'verbose' : True}
  log = logger(pathlog = 'logs' , logName = 'nombre_log.log')
  hp = Helper(cache , logger = log)

  full_path = f'{database}.{table_name}'
  credentials_filename = 'credentials.json'
  hp.fromPandasDF(dataframe, full_path, credentials_filename)


  
