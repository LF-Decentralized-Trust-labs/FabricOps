(function () {
  const header = document.querySelector("[data-elevate]");
  const navToggle = document.querySelector(".nav-toggle");
  const navLinks = document.querySelectorAll(".nav-links a[href^='#']");
  const copyButtons = document.querySelectorAll("[data-copy-target]");
  const tabButtons = document.querySelectorAll("[data-tab]");
  const drawerPanels = document.querySelectorAll("[data-panel]");

  function setHeaderState() {
    if (!header) return;
    header.classList.toggle("is-elevated", window.scrollY > 12);
  }

  setHeaderState();
  window.addEventListener("scroll", setHeaderState, { passive: true });

  if (navToggle) {
    navToggle.addEventListener("click", () => {
      const isOpen = document.body.classList.toggle("nav-open");
      navToggle.setAttribute("aria-expanded", String(isOpen));
    });
  }

  navLinks.forEach((link) => {
    link.addEventListener("click", () => {
      document.body.classList.remove("nav-open");
      if (navToggle) {
        navToggle.setAttribute("aria-expanded", "false");
      }
    });
  });

  tabButtons.forEach((button) => {
    button.addEventListener("click", () => {
      const target = button.dataset.tab;
      tabButtons.forEach((tab) => {
        const active = tab === button;
        tab.classList.toggle("is-active", active);
        tab.setAttribute("aria-selected", String(active));
      });
      drawerPanels.forEach((panel) => {
        const active = panel.dataset.panel === target;
        panel.classList.toggle("is-active", active);
        panel.hidden = !active;
      });
    });
  });

  copyButtons.forEach((button) => {
    button.addEventListener("click", async () => {
      const target = document.getElementById(button.dataset.copyTarget || "");
      if (!target) return;
      const text = target.textContent || "";
      try {
        await navigator.clipboard.writeText(text.trim());
        button.classList.add("is-copied");
        button.textContent = "Copied";
        window.setTimeout(() => {
          button.classList.remove("is-copied");
          button.textContent = "Copy";
        }, 1800);
      } catch (_error) {
        button.textContent = "Select";
      }
    });
  });

  const sections = [...document.querySelectorAll("main section[id]")];
  const observer = new IntersectionObserver(
    (entries) => {
      entries.forEach((entry) => {
        if (!entry.isIntersecting) return;
        navLinks.forEach((link) => {
          link.classList.toggle("is-active", link.getAttribute("href") === `#${entry.target.id}`);
        });
      });
    },
    { rootMargin: "-38% 0px -48% 0px" },
  );

  sections.forEach((section) => observer.observe(section));
})();
