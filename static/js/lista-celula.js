$(document).ready(function () {

    var table = $('#dataTable').DataTable();

    // Função para obter valores únicos de uma coluna
    function getUniqueColumnValues(columnIndex) {
        return table.column(columnIndex, { search: 'applied' }).data().unique().sort().toArray();
    }

    // Função para popular as opções de um datalist
    function populateDatalistOptions(inputId, datalistId, columnIndex) {
        var ulList = $('#' + datalistId);
        ulList.empty(); // Limpa as opções existentes

        var uniqueValues = getUniqueColumnValues(columnIndex);

        // Adiciona as opções à lista
        uniqueValues.forEach(function (value) {
            ulList.append('<li value="' + value + '">' + value + '</li>');
        });

        // Adiciona manipulador de eventos para os itens da lista
        ulList.on('click', 'li', function () {
            var selectedValue = $(this).attr('value');
            $('#' + inputId).val(selectedValue); // Preenche o input com o valor selecionado
            ulList.hide(); // Oculta a lista após a seleção
            console.log(selectedValue);
            applyCelulaFilter(selectedValue);
        });
        console.log(uniqueValues);
    }

    // Função para exibir a lista ao clicar no input
    function showListOnInputClick(inputId, datalistId, columnIndex) {
        $('#' + inputId).on('click', function () {
            $('#' + datalistId).show(); // Exibe a lista ao receber clicar
            populateDatalistOptions(inputId, datalistId, columnIndex);
        });

        $('#' + inputId).on('blur', function () {
            // Adicione alguma lógica de ocultar a lista se necessário
            setTimeout(function() {
                $('#' + datalistId).hide(); // Oculta a lista após um pequeno atraso
            }, 100);
        });
    }

    // Função para aplicar o filtro com base na data
    function applyDateFilter() {
        var columnIndex = $('#dataCarga').data('column');
        var filterValue = $('#dataCarga').val();

        console.log('Column Index:', columnIndex);
        console.log('Filter Value:', filterValue);

        // Formata a data usando Moment.js
        if (filterValue === '') {
            filterValue = moment().format('DD/MM/YYYY');
        } else {
            filterValue = moment(filterValue, 'YYYY-MM-DD').format('DD/MM/YYYY');
        }

        // Define o filtro na coluna específica
        table.column(columnIndex).search(filterValue).draw();

        populateDatalistOptions('celula', 'listaCelulas', 1);
    }

    function applyColorFilter() {
        var columnIndex = $('#celula').data('column');
        var filterValue = $('#celula').val();

        console.log('Column Index:', columnIndex);
        console.log('Filter Value:', filterValue);

        // Define o filtro na coluna específica
        table.column(columnIndex).search(filterValue).draw();
    }

    function applyCelulaFilter(selectedValue) { 
        var columnIndex = $('#celula').data('column');

        // Define o filtro na coluna específica
        table.column(columnIndex).search(selectedValue).draw();
    }

    // Carrega as opções do datalist quando a página é carregada
    populateDatalistOptions('celula', 'listaCelulas', 1);
    showListOnInputClick('celula', 'listaCelulas', 1);

    // Aplica o filtro ao carregar a página
    applyDateFilter();

    // Adiciona campos de entrada para filtrar por coluna
    $('#dataCarga').on('input', function () {
        applyDateFilter();
    });

    $('#celula').on('input', function () {
        applyColorFilter();
    });

    function limparCampoCelula() {
        $('#celula').val('');
        $('#listaCelulas').hide();
        populateDatalistOptions('celula', 'listaCelulas', 1);
    }

    // Adiciona o evento de clique ao ícone "x" para limpar o campo 'celula'
    $('.clear-icon').on('click', function () {
        limparCampoCelula();
    });

});