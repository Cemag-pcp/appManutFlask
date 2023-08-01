$(document).ready(function() {

    $('#tabela_graficos1').DataTable({
        "aLengthMenu": [[3, 5, 10, 25, -1], [3, 5, 10, 25, "All"]],
        "iDisplayLength": 10
    });

    $('#tabela_graficos2').DataTable({
        "aLengthMenu": [[3, 5, 10, 25, -1], [3, 5, 10, 25, "All"]],
        "iDisplayLength": 6
    });

    $('#tabela_graficos3').DataTable({
        "aLengthMenu": [[3, 5, 10, 25, -1], [3, 5, 10, 25, "All"]],
        "iDisplayLength": 6
    });

    $('#tabela_graficos4').DataTable({
        "aLengthMenu": [[3, 5, 10, 25, -1], [3, 5, 10, 25, "All"]],
        "iDisplayLength": 6
    });

    $('#tabela_graficos5').DataTable({
        "aLengthMenu": [[3, 5, 10, 25, -1], [3, 5, 10, 25, "All"]],
        "iDisplayLength": 6
    });

    $('#tabela_graficos6').DataTable({
        "aLengthMenu": [[3, 5, 10, 25, -1], [3, 5, 10, 25, "All"]],
        "iDisplayLength": 6
    });
    // Oculta o campo "Show entries"
    $('div.dataTables_length').hide();
});