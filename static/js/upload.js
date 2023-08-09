const pdfInput = document.getElementById('pdfInput'); // Obtém o elemento de entrada de arquivo
const pdfTableBody = document.getElementById('pdfTableBody'); // Obtém o corpo da tabela onde os PDFs serão listados
const atualizarPdfButton = document.getElementById('atualizar_pdf'); // Obtém o botão de atualização

pdfInput.addEventListener('change', async () => { // Adiciona um ouvinte de evento para o input de arquivo quando o valor muda
    const files = pdfInput.files; // Obtém os arquivos selecionados

    for (const file of files) { // Loop através dos arquivos selecionados
        const reader = new FileReader(); // Cria um leitor de arquivo

        reader.onload = function (e) { // Define a função para executar quando o arquivo é lido
            const pdfUrl = e.target.result; // Obtém a URL do arquivo lido
            const pdfName = file.name; // Obtém o nome do arquivo
            addPdfToTable(pdfName, pdfUrl); // Chama a função para adicionar o PDF à tabela
        };

        reader.readAsDataURL(file); // Lê o arquivo como uma URL de dados
    }
});

pdfTableBody.addEventListener('click', (event) => { // Adiciona um ouvinte de evento para o corpo da tabela quando ocorre um clique
    if (event.target.classList.contains('remove-link')) { // Verifica se o elemento clicado tem a classe 'remove-link'
        const row = event.target.closest('tr'); // Obtém a linha (tr) mais próxima que envolve o elemento clicado
        pdfTableBody.removeChild(row); // Remove a linha da tabela
    }
});

function addPdfToTable(pdfName, pdfUrl) { // Função para adicionar um PDF à tabela
    const row = document.createElement('tr'); // Cria um elemento de linha (tr)
    
    const pdfLinkCell = document.createElement('td'); // Cria uma célula (td) para o link do PDF
    const pdfLink = document.createElement('div'); // Cria um elemento div para o link
    pdfLink.href = pdfUrl; // Define a URL do link
    pdfLink.textContent = pdfName; // Define o texto do link como o nome do PDF
    pdfLink.download = pdfName; // Define o atributo de download do link
    pdfLinkCell.appendChild(pdfLink); // Adiciona o link à célula
    row.appendChild(pdfLinkCell); // Adiciona a célula à linha

    const downloadCell = document.createElement('td'); // Cria uma célula (td) para o link de download
    const downloadLink = document.createElement('a'); // Cria um elemento de link para download
    downloadCell.style.textAlign = 'left'; // Define o alinhamento à esquerda para a célula de download
    downloadLink.className = 'link-download'; // Define a classe do link de download
    downloadLink.innerHTML = '<i class="fas fa-download"></i>'; // Define o ícone de download
    downloadLink.href = pdfUrl; // Define a URL do link de download
    downloadLink.download = pdfName; // Define o atributo de download do link de download
    downloadCell.appendChild(downloadLink); // Adiciona o link de download à célula
    row.appendChild(downloadCell); // Adiciona a célula à linha

    const removeCell = document.createElement('td'); // Cria uma célula (td) para o link de remoção
    const removeLink = document.createElement('a'); // Cria um elemento de link para remoção
    removeLink.classList.add('remove-link'); // Adiciona a classe 'remove-link' ao link de remoção
    removeCell.style.textAlign = 'left'; // Define o alinhamento à esquerda para a célula de remoção
    removeLink.textContent = '❌'; // Define o ícone de remoção
    removeLink.style.cursor = 'pointer'; // Define o cursor para parecer um ponteiro quando o mouse passa por cima
    removeCell.appendChild(removeLink); // Adiciona o link de remoção à célula
    row.appendChild(removeCell); // Adiciona a célula à linha

    pdfTableBody.appendChild(row); // Adiciona a linha ao corpo da tabela
}
