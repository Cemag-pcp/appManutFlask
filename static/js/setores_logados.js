document.addEventListener("DOMContentLoaded", function () {
    var identificadorLogado = document.getElementById("identificador_logado").value;
    var setorLogado = document.getElementById("setor_logado").value;
    var setorFilter = document.getElementById("setor-filter");
    var dropdowns = document.querySelectorAll(".dropdownsss");
    var menu = document.getElementById("menu-button");
    var voltar = document.getElementById("voltar");

    if (identificadorLogado === '2') {
        for (var i = 0; i < setorFilter.options.length; i++) {
            if (setorFilter.options[i].value === setorLogado) {
                setorFilter.options[i].setAttribute("selected", "selected");
                break;
            }
        }
        setorFilter.disabled = true;
        menu.style.display = "none";
        voltar.style.display = "none";

        // Oculta os elementos com a classe "dropdownsss"
        dropdowns.forEach(function (dropdown) {
            dropdown.style.display = "none";
        });
    }

    applyFilters();
});
