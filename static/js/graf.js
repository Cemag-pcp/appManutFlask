var ctx = document.getElementById('grafico1').getContext('2d');
var grafico1 = new Chart(ctx, {
  type: 'doughnut',
  data: {
    labels: ['Red', 'Blue', 'Yellow', 'Green', 'Purple', 'Orange'],
    datasets: [{
      label: '# of Votes',
      data: [12, 19, 3, 5, 2, 3],
      backgroundColor: ["rgb(255, 99, 132,0.6)",
      "rgb(54, 162, 235,0.6)",
      "rgb(255, 206, 86,0.6)",
      "rgb(75, 192, 192,0.6)",
      "rgb(153, 102, 255,0.6)",
      "rgb(255, 159, 64,0.6)"],
      borderColor: ["rgb(255, 99, 132,0.6)",
      "rgb(54, 162, 235,0.6)",
      "rgb(255, 206, 86,0.6)",
      "rgb(75, 192, 192,0.6)",
      "rgb(153, 102, 255,0.6)",
      "rgb(255, 159, 64,0.6)"],
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

var ctx = document.getElementById('grafico2').getContext('2d');
var grafico2 = new Chart(ctx, {
  type: 'bar',
  data: {
    labels: ['Red', 'Blue', 'Yellow', 'Green', 'Purple', 'Orange'],
    datasets: [{
      label: '# of Votes',
      data: [3, 2, 8, 12, 10, 6],
      backgroundColor: ["rgb(255, 99, 132,0.6)",
      "rgb(54, 162, 235,0.6)",
      "rgb(255, 206, 86,0.6)",
      "rgb(75, 192, 192,0.6)",
      "rgb(153, 102, 255,0.6)",
      "rgb(255, 159, 64,0.6)"],
      borderColor: ["rgb(255, 99, 132,0.6)",
      "rgb(54, 162, 235,0.6)",
      "rgb(255, 206, 86,0.6)",
      "rgb(75, 192, 192,0.6)",
      "rgb(153, 102, 255,0.6)",
      "rgb(255, 159, 64,0.6)"],
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
    labels: ['Red', 'Blue', 'Yellow', 'Green', 'Purple', 'Orange'],
    datasets: [{
      label: '# of Votes',
      data: [3, 2, 8, 12, 10, 6],
      backgroundColor: ["rgb(255, 99, 132,0.6)",
      "rgb(54, 162, 235,0.6)",
      "rgb(255, 206, 86,0.6)",
      "rgb(75, 192, 192,0.6)",
      "rgb(153, 102, 255,0.6)",
      "rgb(255, 159, 64,0.6)"],
      borderColor: ["rgb(255, 99, 132,0.6)",
      "rgb(54, 162, 235,0.6)",
      "rgb(255, 206, 86,0.6)",
      "rgb(75, 192, 192,0.6)",
      "rgb(153, 102, 255,0.6)",
      "rgb(255, 159, 64,0.6)"],
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
