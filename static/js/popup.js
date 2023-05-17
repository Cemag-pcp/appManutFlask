// JavaScript to handle the popup
  function showPopup(imageSrc) {
    var popup = document.getElementById('popup');
    var popupImg = document.getElementById('popup-img');
    popup.style.display = 'flex';
    popupImg.src = imageSrc;
  }
  
  function closePopup() {
    var popup = document.getElementById('popup');
    popup.style.display = 'none';
  }