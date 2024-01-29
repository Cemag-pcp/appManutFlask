function formatarDataBr(dataString) {
    // Cria um objeto Date a partir da string
    var data = new Date(dataString);
    
    // Obtém os componentes da data
    var dia = data.getDate();
    var mes = data.getMonth() + 1; // Adiciona 1 porque os meses são indexados a partir de 0
    var ano = data.getFullYear(); // Obtém os últimos dois dígitos do ano
    var hora = data.getHours();
    var minuto = data.getMinutes();
    var segundo = data.getSeconds();

    // Formata a string com zero à esquerda para garantir dois dígitos para o dia, mês, hora, minuto e segundo
    var formatoDesejado = `${dia.toString().padStart(2, '0')}/${mes.toString().padStart(2, '0')}/${ano.toString()} ${hora.toString().padStart(2, '0')}:${minuto.toString().padStart(2, '0')}:${segundo.toString().padStart(2, '0')}`;

    console.log(formatoDesejado);

    return formatoDesejado;
}


function formatarDataBrSemHora(dataString) {
    // Cria um objeto Date a partir da string
    var data = new Date(dataString);
    
    // Obtém os componentes da data
    var dia = data.getDate();
    var mes = data.getMonth() + 1; // Adiciona 1 porque os meses são indexados a partir de 0
    var ano = data.getFullYear(); // Obtém os últimos dois dígitos do ano
    

    // Formata a string com zero à esquerda para garantir dois dígitos para o dia, mês, hora, minuto e segundo
    var formatoDesejado = `${dia.toString().padStart(2, '0')}/${mes.toString().padStart(2, '0')}/${ano.toString()}`;

    console.log(formatoDesejado);

    return formatoDesejado;
}
