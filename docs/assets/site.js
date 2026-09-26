(function () {
  const header = document.querySelector("[data-elevate]");
  const navToggle = document.querySelector("[data-nav-toggle]");
  const navLinks = document.querySelectorAll(".nav-links a[href^='#']");
  const copyButtons = document.querySelectorAll("[data-copy-target]");
  const tabButtons = document.querySelectorAll("[data-tab]");
  const panels = document.querySelectorAll("[data-panel]");

  function setHeaderState() {
    if (header) header.classList.toggle("is-elevated", window.scrollY > 10);
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
      navToggle?.setAttribute("aria-expanded", "false");
    });
  });

  tabButtons.forEach((button) => {
    button.addEventListener("click", () => {
      const target = button.dataset.tab;
      tabButtons.forEach((tab) => {
        const isActive = tab === button;
        tab.classList.toggle("is-active", isActive);
        tab.setAttribute("aria-selected", String(isActive));
      });
      panels.forEach((panel) => {
        const isActive = panel.dataset.panel === target;
        panel.classList.toggle("is-active", isActive);
        panel.hidden = !isActive;
      });
    });
  });

  copyButtons.forEach((button) => {
    button.addEventListener("click", async () => {
      const target = document.getElementById(button.dataset.copyTarget || "");
      if (!target) return;
      const text = (target.textContent || "").replace(/^\$ /gm, "").trim();
      try {
        await navigator.clipboard.writeText(text);
        button.classList.add("is-copied");
        button.textContent = "Copied";
        window.setTimeout(() => {
          button.classList.remove("is-copied");
          button.textContent = "Copy";
        }, 1600);
      } catch (_error) {
        button.textContent = "Select";
      }
    });
  });

  const sections = [...document.querySelectorAll("main section[id]")];
  const sectionObserver = new IntersectionObserver(
    (entries) => {
      entries.forEach((entry) => {
        if (!entry.isIntersecting) return;
        navLinks.forEach((link) => {
          link.classList.toggle("is-active", link.getAttribute("href") === `#${entry.target.id}`);
        });
      });
    },
    { rootMargin: "-36% 0px -52% 0px" },
  );
  sections.forEach((section) => sectionObserver.observe(section));

  const reveals = document.querySelectorAll(".reveal");
  const revealObserver = new IntersectionObserver(
    (entries, observer) => {
      entries.forEach((entry) => {
        if (!entry.isIntersecting) return;
        entry.target.classList.add("is-visible");
        observer.unobserve(entry.target);
      });
    },
    { threshold: 0.12 },
  );
  reveals.forEach((element) => revealObserver.observe(element));
})();
