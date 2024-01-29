// function showLoading() {
//     const div = document.createElement('div');
//     div.classList.add('loading','centralize');

//     const label = document.createElement('label');
//     label.innerText = 'Carregando...';

//     div.appendChild(label);

//     document.body.appendChild(div);

//     setTimeout(() => hideLoading(),2000);
// }
// function hideLoading() {
//     const loadings = document.getElementsByClassName('loading');
//     if(loadings.length){
//         loadings[0].remove();
//     }
// }


function showLoading() {
  $('#overlay').show();
  $('#spinner').show();
  // Adicione aqui sua lógica ou chamadas de AJAX, por exemplo
  setTimeout(function() {
    hideLoading();
  }, 3000); // Tempo em milissegundos (3 segundos no exemplo)
}

function hideLoading() {
  $('#overlay').hide();
  $('#spinner').hide();
}
