from flask import Flask, render_template, url_for
#from forms import FormLogin, FormCriarConta
from flask import Blueprint

app = Flask(__name__, static_url_path='/static/css')

lista_usuarios = ['saul1','saul2','saul3','saul4']

login_bp = Blueprint('login', __name__)

app.config['SECRET_KEY'] = '7e7fca40778a914195d999409f9b6f14'

@login_bp.route('/home')
def home():
    return render_template('login/home.html')

@login_bp.route('/login', methods=['GET','POST'])
def login():
    return render_template('login/login.html')

# @app.route('/signup')
# def signup():
#     form_criarconta = FormCriarConta()
#     return render_template('signup.html', form_criarconta = form_criarconta)

@login_bp.route('/contato')
def contato():
    return render_template('login/contato.html')

@login_bp.route('/usuarios')
def usuarios():
    return render_template('login/usuarios.html', lista_usuarios = lista_usuarios)

if __name__ == '__main__':
    app.run(debug=True)