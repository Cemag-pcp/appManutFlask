function mostrarAlertaSenha() {
    const senhaInserida = prompt("Digite a senha:");
    const senhaCorreta = "Cemagtian";
  
    if (senhaInserida === senhaCorreta) {
      window.location.href = "/funcionarios";
    } else {
      alert("Senha incorreta. Tente novamente.");
    }
  }