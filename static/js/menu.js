// Obtém o elemento do botão "Abrir Menu"
var menuButton = document.querySelector(".menu-button");

// Obtém o elemento do menu
var menu = document.getElementById("menu");

// Adiciona um ouvinte de evento de clique ao botão "Abrir Menu"
menuButton.addEventListener("click", function() {
  // Verifica se o menu está ativo
  if (menu.classList.contains("active")) {
    // Remove a classe 'active' do menu e do botão para fechar o menu
    menu.classList.remove("active");
    menuButton.classList.remove("active");
  } else {
    // Adiciona a classe 'active' ao menu e ao botão para abrir o menu
    menu.classList.add("active");
    menuButton.classList.add("active");
  }
});
