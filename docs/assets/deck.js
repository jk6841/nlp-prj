// Deck viewer: fit-to-window scaling, keyboard/click navigation, #n in the URL.
(function () {
  const deck = document.querySelector(".deck");
  const slides = Array.from(deck.querySelectorAll(":scope > .jc-slide"));
  const total = slides.length;
  let current = 0;

  function fit() {
    const scale = Math.min(window.innerWidth / 1920, window.innerHeight / 1080);
    deck.style.setProperty("--deck-scale", scale);
  }

  function show(index) {
    current = Math.max(0, Math.min(total - 1, index));
    slides.forEach((s, i) => s.classList.toggle("is-active", i === current));
    slides.forEach((s, i) => {
      const page = s.querySelector(".jc-slide__page");
      if (page) page.textContent = String(i + 1).padStart(2, "0") + " / " + String(total).padStart(2, "0");
    });
    history.replaceState(null, "", "#" + (current + 1));
  }

  document.addEventListener("keydown", (e) => {
    if (["ArrowRight", "ArrowDown", "PageDown", " "].includes(e.key)) { e.preventDefault(); show(current + 1); }
    if (["ArrowLeft", "ArrowUp", "PageUp"].includes(e.key)) { e.preventDefault(); show(current - 1); }
    if (e.key === "Home") show(0);
    if (e.key === "End") show(total - 1);
  });

  deck.addEventListener("click", (e) => {
    show(e.clientX > window.innerWidth / 2 ? current + 1 : current - 1);
  });

  window.addEventListener("resize", fit);
  fit();
  show((parseInt(location.hash.slice(1), 10) || 1) - 1);
})();
