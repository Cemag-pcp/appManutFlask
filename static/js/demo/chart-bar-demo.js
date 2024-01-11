// Set new default font family and font color to mimic Bootstrap's default styling
Chart.defaults.global.defaultFontFamily = 'Nunito', '-apple-system,system-ui,BlinkMacSystemFont,"Segoe UI",Roboto,"Helvetica Neue",Arial,sans-serif';
Chart.defaults.global.defaultFontColor = '#858796';

function number_format(number, decimals, dec_point, thousands_sep) {
  // *     example: number_format(1234.56, 2, ',', ' ');
  // *     return: '1 234,56'
  number = (number + '').replace(',', '').replace(' ', '');
  var n = !isFinite(+number) ? 0 : +number,
    prec = !isFinite(+decimals) ? 0 : Math.abs(decimals),
    sep = (typeof thousands_sep === 'undefined') ? ',' : thousands_sep,
    dec = (typeof dec_point === 'undefined') ? '.' : dec_point,
    s = '',
    toFixedFix = function(n, prec) {
      var k = Math.pow(10, prec);
      return '' + Math.round(n * k) / k;
    };
  // Fix for IE parseFloat(0.55).toFixed(0) = 0;
  s = (prec ? toFixedFix(n, prec) : '' + Math.round(n)).split('.');
  if (s[0].length > 3) {
    s[0] = s[0].replace(/\B(?=(?:\d{3})+(?!\d))/g, sep);
  }
  if ((s[1] || '').length < prec) {
    s[1] = s[1] || '';
    s[1] += new Array(prec - s[1].length + 1).join('0');
  }
  return s.join(dec);
}

// var eixo_y = {{ dados_mtbf_maquina | safe }};
//     var eixo_x = {{ labels_mtbf_maquina | safe }};
//     var ctx = document.getElementById('grafico1').getContext('2d');

//     var grafico = new Chart(ctx, {
//         type: 'bar',
//         data: {
//             labels: eixo_x,
//             datasets: [{
//                 color: 'black', 
//                 font: {
//                     size: 14 
//                 },
//                 label: 'MTBF',
//                 data: eixo_y,
//                 fill: false,
//                 tension: 0.1,
//                 backgroundColor: [
//                     'rgba(255, 99, 132, 1)',
//                     'rgba(255, 159, 64, 1)',
//                     'rgba(255, 205, 86, 1)',
//                     'rgba(75, 192, 192, 1)',
//                     'rgba(54, 162, 235, 1)',
//                     'rgba(153, 102, 255, 1)',
//                     'rgba(201, 203, 207, 1)'
//                 ],
//                 borderColor: [
//                     'rgb(255, 99, 132)',
//                     'rgb(255, 159, 64)',
//                     'rgb(255, 205, 86)',
//                     'rgb(75, 192, 192)',
//                     'rgb(54, 162, 235)',
//                     'rgb(153, 102, 255)',
//                     'rgb(201, 203, 207)'
//                 ],
//                 borderWidth: 1
//             }]
//         },
//         options: {
//             responsive: true,
//             scales: {
//                 x: {
//                     display: true, 
//                 },
//                 y: {
//                     display: true, 
//                 }
//             },
//             plugins: {
//                 title: {
//                     display: true,
//                     text: 'MTBF por máquina',
//                     font: {
//                         size: 18,
//                         weight: 'bold' 
//                     },
//                     padding: {
//                         top: 10,
//                         bottom: 30
//                     }
//                 }
//             }
//         }
//     });