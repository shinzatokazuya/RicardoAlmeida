# app.py
import sqlite3
from flask import Flask, request, render_template
from cs50 import SQL

app = Flask(__name__)

# Variavel do Banco de dados, importando!
db = SQL("sqlite:///Servicos.db")

@app.route('/')
def index():
    """ Mostrar na página os inputs para colocar os dados """
    

@app.route('/salvar_servico', methods=['POST'])
def salvar_servico():


if __name__ == '__main__':
    app.run(debug=True)

