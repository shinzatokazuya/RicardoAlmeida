import pandas as pd
from cs50 import SQL
import os

# caminho para o arquivo xlsx (Excel)
file_path = 'servicos/banco_de_dados/planilhas/Serviços.xlsx'

db = SQL('sqlite:///servicos/banco_de_dados)


