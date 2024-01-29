function exibirMensagem(tipo, texto) {
    var mensagemElement = document.getElementById('mensagem');

    // Define o texto da mensagem
    mensagemElement.innerText = texto;

    // Define a classe de estilo com base no tipo
    mensagemElement.className = 'sucesso';
    if (tipo === 'aviso') {
      mensagemElement.className = 'aviso';
    }

    // Exibe a mensagem
    mensagemElement.style.top = '20px'; // Define a posição para exibir a mensagem

    // Aguarda 3 segundos e esconde a mensagem
    setTimeout(function () {
      mensagemElement.style.top = '-204px'; // Define a posição para esconder a mensagem
    }, 3000); // 3000 milissegundos = 3 segundos
}