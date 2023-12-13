document.addEventListener("DOMContentLoaded", function () {
    var setorLogado = document.querySelector(".setor_logado").value;
    var identificadorLogado = document.querySelector(".identificador_logado").value;
    var setorFilter = document.getElementById("setor-filter");
    var dropdowns = document.querySelectorAll(".dropdownsss");
    var botaoVoltar = document.querySelectorAll(".botao-voltar");
    var menuButton = document.querySelectorAll(".menu-button");
    var elementos  = document.querySelectorAll(".form-control1");
    var operadores  = document.querySelector(".operadores");
    var toggles_campos = document.querySelector(".toggles_campos");
    var salvar_edicao = document.getElementById("salvar_edicao");
    var ok_edicao = document.getElementById("ok_edicao");
    var maquinas_preventivas = document.getElementById("maquinas_preventivas");

    // Verifica se a rota atual contém "/edit/"
    var isEditRoute = window.location.pathname.includes("/edit/");
    var semanas_52 = window.location.pathname.includes("/52semanas");

    if (identificadorLogado === '2') {
        if (!isEditRoute) {
            for (var i = 0; i < setorFilter.options.length; i++) {
                if (setorFilter.options[i].value === setorLogado) {
                    setorFilter.options[i].setAttribute("selected", "selected");
                    break;
                }
            }
            setorFilter.disabled = true;
    
            // Oculta os elementos com a classe "dropdownsss"
            dropdowns.forEach(function (dropdown) {
                dropdown.style.display = "none";
            });

            // Oculta os elementos com a classe "botao-voltar"
            botaoVoltar.forEach(function (botao) {
                botao.style.display = "none";
            });
            
        } else{
            operadores.style.display= "none";

            toggles_campos.style.display="none";

            salvar_edicao.style.display = "none";

            if(maquinas_preventivas.value === "True"){
                ok_edicao.style.display = "block";
            } else {
                ok_edicao.style.display = "none";
            }
        }

        elementos.forEach(function(elemento) {
            elemento.disabled= true;
        });
    
        // Oculta os elementos com a classe "menu-button"
        
        menuButton.forEach(function (menu) {
            menu.style.display = "none";
        });
        
    }

    // Chama a função applyFilters() apenas se não estiver na rota "/edit/"
    if (!isEditRoute || !semanas_52) {
        applyFilters();
    }
});