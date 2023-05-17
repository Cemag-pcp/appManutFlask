const dataFromDatabase2 = [3, 2];
const dataFromDatabase3 = [3, 2];
const dataFromDatabase4 = [6, 5];


var dataFromDatabase1 = {{ grafico1.to_dict(orient='records') | tojson }};

var ctx = document.getElementById('grafico1').getContext('2d');
var grafico1 = new Chart(ctx, {
    type: 'doughnut',
    data: {
        labels: dataFromDatabase1.map(item => item.dataabertura),
        datasets: [{
            label: '# of Votes',
            data: dataFromDatabase1.map(item => item.qt_os_abertas),
            backgroundColor: ["rgb(255, 99, 132,0.6)", "rgb(54, 162, 235,0.6)"],
            borderColor: ["rgb(255, 99, 132,0.6)", "rgb(54, 162, 235,0.6)"],
            borderWidth: 1
        }]
    },
    options: {
        responsive: false,
        maintainAspectRatio: false,
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
    labels: ['Janeiro', 'Fevereiro'],
    datasets: [{
      label: '# of Votes',
      data: dataFromDatabase2,
      backgroundColor: ["rgb(255, 99, 132,0.6)",
      "rgb(54, 162, 235,0.6)"],
      borderColor: ["rgb(255, 99, 132,0.6)",
      "rgb(54, 162, 235,0.6)"],
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
    labels: ['Janeiro', 'Fevereiro'],
    datasets: [{
      label: '# of Votes',
      data: dataFromDatabase3,
      backgroundColor: ["rgb(255, 99, 132,0.6)",
      "rgb(54, 162, 235,0.6)"],
      borderColor: ["rgb(255, 99, 132,0.6)",
      "rgb(54, 162, 235,0.6)"],
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
    labels: ['Janeiro', 'Fevereiro'],
    datasets: [{
      label: '# of Votes',
      data: dataFromDatabase4,
      backgroundColor: ["rgb(255, 99, 132,0.6)",
      "rgb(54, 162, 235,0.6)"],
      borderColor: ["rgb(255, 99, 132,0.6)",
      "rgb(54, 162, 235,0.6)"],
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