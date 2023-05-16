var ctx = document.getElementById('grafico1').getContext('2d');
var grafico1 = new Chart(ctx, {
  type: 'doughnut',
  data: {
    labels: ['Janeiro', 'Fevereiro', 'Março', 'Abril', 'Maio', 'Junho','Julho','Agosto','Setembro','Outubro','Novembro','Dezembro'],
    datasets: [{
      label: '# of Votes',
      data: [12, 19, 3, 5, 2, 3],
      backgroundColor: ["rgb(255, 99, 132,0.6)",
      "rgb(54, 162, 235,0.6)",
      "rgb(245, 126, 86,0.6)",
      "rgb(75, 192, 192,0.6)",
      "rgb(13, 52, 200,0.6)",
      "rgb(255, 159, 64,0.6)",
      "rgb(153, 102, 253,0.6)",
      "rgb(255, 109, 64,0.6)",
      "rgb(83, 162, 250,0.6)",
      "rgb(205, 159, 64,0.6)",
      "rgb(113, 182, 215,0.6)",
      "rgb(215, 159, 64,0.6)"],
      borderColor: ["rgb(255, 99, 132,0.6)",
      "rgb(54, 162, 235,0.6)",
      "rgb(245, 126, 86,0.6)",
      "rgb(75, 192, 192,0.6)",
      "rgb(13, 52, 200,0.6)",
      "rgb(255, 159, 64,0.6)",
      "rgb(153, 102, 253,0.6)",
      "rgb(255, 109, 64,0.6)",
      "rgb(83, 162, 250,0.6)",
      "rgb(205, 159, 64,0.6)",
      "rgb(113, 182, 215,0.6)",
      "rgb(215, 159, 64,0.6)"],
      borderWidth: 1
    }]
  },
  options: {
    responsive: false, // Permite que o gráfico se ajuste ao tamanho do contêiner
    maintainAspectRatio: false, // Desabilita a manutenção da proporção
    plugins: {
      title: {
          display: true,
          text: 'Custom Chart Title',
          padding: {
              top: 10,
              bottom: 30
          }
      }
    }
  }
});

var ctx = document.getElementById('grafico2').getContext('2d');
var grafico2 = new Chart(ctx, {
  type: 'bar',
  data: {
    labels: ['Janeiro', 'Fevereiro', 'Março', 'Abril', 'Maio', 'Junho','Julho','Agosto','Setembro','Outubro','Novembro','Dezembro'],
    datasets: [{
      label: '# of Votes',
      data: [3, 2, 8, 12, 10, 6, 9, 6, 8, 2, 14, 10],
      backgroundColor: ["rgb(255, 99, 132,0.6)",
      "rgb(54, 162, 235,0.6)",
      "rgb(245, 126, 86,0.6)",
      "rgb(75, 192, 192,0.6)",
      "rgb(13, 52, 200,0.6)",
      "rgb(255, 159, 64,0.6)",
      "rgb(153, 102, 253,0.6)",
      "rgb(255, 109, 64,0.6)",
      "rgb(83, 162, 250,0.6)",
      "rgb(205, 159, 64,0.6)",
      "rgb(113, 182, 215,0.6)",
      "rgb(215, 159, 64,0.6)"],
      borderColor: ["rgb(255, 99, 132,0.6)",
      "rgb(54, 162, 235,0.6)",
      "rgb(245, 126, 86,0.6)",
      "rgb(75, 192, 192,0.6)",
      "rgb(13, 52, 200,0.6)",
      "rgb(255, 159, 64,0.6)",
      "rgb(153, 102, 253,0.6)",
      "rgb(255, 109, 64,0.6)",
      "rgb(83, 162, 250,0.6)",
      "rgb(205, 159, 64,0.6)",
      "rgb(113, 182, 215,0.6)",
      "rgb(215, 159, 64,0.6)"],
      borderWidth: 1
    }]
  },
  options: {
    responsive: false, // Permite que o gráfico se ajuste ao tamanho do contêiner
    maintainAspectRatio: false, // Desabilita a manutenção da proporção
    scales: {
      y: {
        beginAtZero: true
      }
    }
  }
});

var ctx = document.getElementById('grafico3').getContext('2d');
var grafico3 = new Chart(ctx, {
  type: 'line',
  data: {
    labels: ['Janeiro', 'Fevereiro', 'Março', 'Abril', 'Maio', 'Junho','Julho','Agosto','Setembro','Outubro','Novembro','Dezembro'],
    datasets: [{
      label: '# of Votes',
      data: [3, 2, 8, 12, 10, 6, 9, 6, 8, 2, 14, 10],
      backgroundColor: ["rgb(255, 99, 132,0.6)",
      "rgb(54, 162, 235,0.6)",
      "rgb(245, 126, 86,0.6)",
      "rgb(75, 192, 192,0.6)",
      "rgb(13, 52, 200,0.6)",
      "rgb(255, 159, 64,0.6)",
      "rgb(153, 102, 253,0.6)",
      "rgb(255, 109, 64,0.6)",
      "rgb(83, 162, 250,0.6)",
      "rgb(205, 159, 64,0.6)",
      "rgb(113, 182, 215,0.6)",
      "rgb(215, 159, 64,0.6)"],
      borderColor: ["rgb(255, 99, 132,0.6)",
      "rgb(54, 162, 235,0.6)",
      "rgb(245, 126, 86,0.6)",
      "rgb(75, 192, 192,0.6)",
      "rgb(13, 52, 200,0.6)",
      "rgb(255, 159, 64,0.6)",
      "rgb(153, 102, 253,0.6)",
      "rgb(255, 109, 64,0.6)",
      "rgb(83, 162, 250,0.6)",
      "rgb(205, 159, 64,0.6)",
      "rgb(113, 182, 215,0.6)",
      "rgb(215, 159, 64,0.6)"],
      borderWidth: 1
    }]
  },
  options: {
    responsive: false, // Permite que o gráfico se ajuste ao tamanho do contêiner
    maintainAspectRatio: false, // Desabilita a manutenção da proporção
    scales: {
      y: {
        beginAtZero: true
      }
    }
  }
});

var ctx = document.getElementById('grafico4').getContext('2d');
var grafico4 = new Chart(ctx, {
  type: 'pie',
  data: {
    labels: ['Janeiro', 'Fevereiro', 'Março', 'Abril', 'Maio', 'Junho','Julho','Agosto','Setembro','Outubro','Novembro','Dezembro'],
    datasets: [{
      label: '# of Votes',
      data: [3, 2, 8, 12, 10, 6, 9, 6, 8, 2, 14, 10],
      backgroundColor: ["rgb(255, 99, 132,0.6)",
      "rgb(54, 162, 235,0.6)",
      "rgb(245, 126, 86,0.6)",
      "rgb(75, 192, 192,0.6)",
      "rgb(13, 52, 200,0.6)",
      "rgb(255, 159, 64,0.6)",
      "rgb(153, 102, 253,0.6)",
      "rgb(255, 109, 64,0.6)",
      "rgb(83, 162, 250,0.6)",
      "rgb(205, 159, 64,0.6)",
      "rgb(113, 182, 215,0.6)",
      "rgb(215, 159, 64,0.6)"],
      borderColor: ["rgb(255, 99, 132,0.6)",
      "rgb(54, 162, 235,0.6)",
      "rgb(245, 126, 86,0.6)",
      "rgb(75, 192, 192,0.6)",
      "rgb(13, 52, 200,0.6)",
      "rgb(255, 159, 64,0.6)",
      "rgb(153, 102, 253,0.6)",
      "rgb(255, 109, 64,0.6)",
      "rgb(83, 162, 250,0.6)",
      "rgb(205, 159, 64,0.6)",
      "rgb(113, 182, 215,0.6)",
      "rgb(215, 159, 64,0.6)"],
      borderWidth: 1
    }]
  },
  options: {
    responsive: false, // Permite que o gráfico se ajuste ao tamanho do contêiner
    maintainAspectRatio: false, // Desabilita a manutenção da proporção
    scales: {
      y: {
        beginAtZero: true
      }
    }
  }
});