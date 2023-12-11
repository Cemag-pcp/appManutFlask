
var isEditRoute = window.location.pathname.includes("/edit/");

if(isEditRoute){
    $("#ok_edicao").click(function() {
        // Obtenha os valores dos campos
        var numero_execucao = $("#n_ordem").val();
        var numeroOrdemValue = $("#numero_ordem").val();

        console.log(numero_execucao)
        console.log(numeroOrdemValue)

        // Construa os dados a serem enviados
        var data = {
            numero_execucao: numero_execucao,
            numeroOrdemValue: numeroOrdemValue
        };

        // Use Ajax para enviar a requisição para o backend do Flask
        $.ajax({
            type: 'POST',
            url: '/envio_ok',
            contentType: 'application/json;charset=UTF-8',
            data: JSON.stringify(data),
            success: function(response) {
                showLoading();
                if(response === "Número da Ordem já enviado"){
                    alert(response)
                    hideLoading();
                    return
                } else {
                    alert("Enviado com sucesso")
                }
                location.reload();
            },
            error: function(error) {
                console.error('Erro na requisição:', error);
            }
        });
    });
}
