from flask import Flask, render_template, url_for, request, redirect, url_for, session,flash
#from forms import FormLogin, FormCriarConta
from flask import Blueprint
import psycopg2.extras
import psycopg2 #pip install psycopg2 
from funcoes import login_required
import pandas as pd

app = Flask(__name__, static_url_path='/static/css')

lista_usuarios = ['saul1','saul2','saul3','saul4']

login_bp = Blueprint('login', __name__)

app.config['SECRET_KEY'] = '7e7fca40778a914195d999409f9b6f14'

DB_HOST = "database-1.cdcogkfzajf0.us-east-1.rds.amazonaws.com"
DB_NAME = "postgres"
DB_USER = "postgres"
DB_PASS = "15512332"

@login_bp.route('/home')
@login_required
def home():  # Página de login
    
    return render_template('login/home.html')

@login_bp.route('/login', methods=['POST','GET'])
def login(): # Lógica de login
    
    if 'loggedin' in session:
        print("If 1")   
    # Usuário já está logado, redirecione para a página inicial
        return redirect(url_for('routes.Index'))
    
    else:
        print("Else 1")   
        msg=''
        
        if request.method == 'POST' and 'username' in request.form and 'password' in request.form:
            print("If 2")   
            email = request.form['username']
            pw = request.form['password']

            email = "'" + email + "'"
            pw = "'" + pw + "'"

            conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST)
            cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            cur.execute("""SELECT * FROM tb_contas WHERE email = {} AND pw = {}""".format(email, pw))
            account = cur.fetchone()

            if account:
                session['loggedin'] = True
                session['id'] = account['id']
                session['username'] = account['email']
                session['setor'] = account['setor']
                session['identificador'] = account['identificador']

                print(session['setor'],session['identificador'])

                return redirect(url_for("routes.Index",email=email))
            else:   
                print("Else 2")        
                flash('Usuário ou Senha inválida')

        return render_template('login/login.html', msg = msg)

@login_bp.route('/logout')
def logout(): # Botão de logout
	session.pop('loggedin', None)
	session.pop('id', None)
	session.pop('username', None)
	return redirect(url_for('login.login'))

@login_bp.route('/contato')
@login_required
def contato(): # Página de contatos (Excluir)
    return render_template('login/contato.html')

@login_bp.route('/usuarios')
@login_required
def usuarios(): # Página de usuários (Excluir)
    return render_template('login/usuarios.html', lista_usuarios = lista_usuarios)

if __name__ == '__main__':
    app.run(debug=True)