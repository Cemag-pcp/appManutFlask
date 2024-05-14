
var maquinaParadaSim = document.getElementById('maq-real-parada-1')
var maquinaParadaNao = document.getElementById('maq-real-parada-2')
var execucaoMaquinaSim  = document.getElementById('exec-maq-parada-1')
var execucaoMaquinaNao  = document.getElementById('exec-maq-parada-2')

maquinaParadaSim.addEventListener('change', function(){

    execucaoMaquinaSim.checked = true;
    execucaoMaquinaNao.disabled = true;

});

maquinaParadaNao.addEventListener('change', function(){

    execucaoMaquinaSim.checked = false;
    execucaoMaquinaNao.disabled = false;

});


function verificarSelecao() {
    // Verificar o grupo de botões de rádio para a primeira pergunta
    var radios1 = document.getElementsByName('name-maq-real-parada');
    var selectedValue1 = false;
    for (var i = 0; i < radios1.length; i++) {
        if (radios1[i].checked) {
            selectedValue1 = true;
            break;
        }
    }

    // Verificar o grupo de botões de rádio para a segunda pergunta
    var radios2 = document.getElementsByName('name-exec-maq-parada');
    var selectedValue2 = false;
    for (var j = 0; j < radios2.length; j++) {
        if (radios2[j].checked) {
            selectedValue2 = true;
            break;
        }
    }

    // Verificar o grupo de botões de rádio para a terceira pergunta
    var radios3 = document.getElementsByName('name-apos-exec-maq-parada');
    var selectedValue3 = false;
    for (var k = 0; k < radios3.length; k++) {
        if (radios3[k].checked) {
            selectedValue3 = true;
            break;
        }
    }

    if (!selectedValue1 || !selectedValue2 || !selectedValue3) {
        return false;
    } else {
        return true;
    }
};

$('#salvar_edicao').on('click', function () {

    if (!verificarSelecao()) {
        exibirMensagem('aviso', 'Preencha todos os campos obrigatórios');
        return;
    }

    $('#salvar_edicao').prop('disabled', false);

    var tipoManutencao = document.getElementById('selectTipoManutencao').value;
    var areaManutencao = document.getElementById('areaManutencao').value;
    var observacao = document.getElementById('descmanutencao').value;
    var operadorSelecionado = $('#operador option:selected').length;

    if (tipoManutencao === '' | areaManutencao === '' | observacao === '' | operadorSelecionado === 0) {
        exibirMensagem('aviso', 'Preencha todos os campos obrigatórios.');
        hideLoading();
        return;
    }

    var pvlye = document.getElementById('pvlye').value;
    var pa_plus = document.getElementById('pa-plus').value;
    var tratamento = document.getElementById('tratamento').value;
    var ph_agua = document.getElementById('ph-agua').value;
    var maquina = document.getElementById('maquina').value;
    var inputSolicitante = document.getElementById('inputSolicitante').value;

    if (maquina === 'ETE' && inputSolicitante === 'Automático') {
        if (pvlye === '' || pa_plus === '' || tratamento === '' || ph_agua === '') {
            exibirMensagem('aviso', 'Preencha todos os campos obrigatórios.');
            hideLoading();
            return;
        }
    }

    showLoading();

    // Objeto para armazenar os dados
    var dados = {};

    // Obtém todos os elementos de input do tipo radio com o mesmo name
    var radios_1 = document.getElementsByName('name-maq-real-parada');

    // Itera sobre os elementos e verifica qual está marcado
    for (var i = 0; i < radios_1.length; i++) {
        if (radios_1[i].checked) {
            // Aqui você pode usar radios_1[i].value para obter o valor da opção marcada
            dados["maq-real-parada"] = radios_1[i].value;
            // Interrompe o loop, pois já encontrou a opção marcada
            break;
        }
    }
    
    var radios_2 = document.getElementsByName('name-exec-maq-parada');

    // Itera sobre os elementos e verifica qual está marcado
    for (var i = 0; i < radios_2.length; i++) {
        if (radios_2[i].checked) {
            // Aqui você pode usar radios_2[i].value para obter o valor da opção marcada
            dados["exec-maq-parada"] = radios_2[i].value;
            // Interrompe o loop, pois já encontrou a opção marcada
            break;
        }
    }

    var radios_3 = document.getElementsByName('name-apos-exec-maq-parada');

    // Itera sobre os elementos e verifica qual está marcado
    for (var i = 0; i < radios_3.length; i++) {
        if (radios_3[i].checked) {
            // Aqui você pode usar radios_3[i].value para obter o valor da opção marcada
            dados["apos-exec-maq-parada"] = radios_3[i].value;
            // Interrompe o loop, pois já encontrou a opção marcada
            break;
        }
    }

    // Lista de IDs dos elementos que não são checkboxes
    var idsNaoCheckboxes = [
        'setor',
        'inputSolicitante',
        'dataAbertura',
        'n_ordem',
        'inputRisco',
        'maquina',
        'inputEquipamentoEmFalha',
        'codigoEquipamento',
        'setorMaqSolda',
        'qual_ferramenta',
        'problema',
        'pvlye',
        'pa-plus',
        'tratamento',
        'ph-agua',
        'selectTipoManutencao',
        'areaManutencao',
        'data_edit',
        'operador',
        'descmanutencao',
        'statusLista'
    ];

    // Loop através dos IDs dos elementos que não são checkboxes
    idsNaoCheckboxes.forEach(function (id) {
        // Atribui ao objeto dados
        dados[id] = $('#' + id).val();
    });

    var numeroOs = $('#numeroOrdem').text();

    dados.numeroOs = numeroOs;

    // Envia os dados para o backend usando AJAX
    $.ajax({
        type: 'POST',
        url: 'executar-ordem',
        data: JSON.stringify(dados),
        contentType: 'application/json',
        success: function(response) {
            // Trata a resposta do servidor, se necessário
            console.log(response);
            exibirMensagem('sucesso','Salvo.')

            $('#descmanutencao').val(''); // Define o valor do select para vazio

            $('#modalExecucao').modal('hide');

            hideLoading();
        },
        error: function(error) {
            // Trata erros aqui
            hideLoading();
            console.error('Erro na requisição AJAX:', error);
            exibirMensagem('aviso','Preencha todos os campos obrigatórios.');

        }
    });

});