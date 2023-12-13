$(document).ready(function() {
  $(".expandable-row").hide();

  $(".clickable-row").click(function() {
    $(this).toggleClass("expanded").next(".expandable-row").slideToggle(0);

    // Encontrar o ícone dentro do primeiro <td> da linha clicada
    var icon = $(this).find("td:first-child i");

    // Alternar entre os ícones
    if ($(this).hasClass("expanded")) {
      // Se a classe "expanded" estiver presente, use o ícone "fa-caret-down"
      icon.removeClass("fa-caret-right").addClass("fa-caret-down");
    } else {
      // Se a classe "expanded" não estiver presente, use o ícone "fa-caret-right"
      icon.removeClass("fa-caret-down").addClass("fa-caret-right");
    }
  });
});
