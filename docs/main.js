/* hev search docs - hevsearch.com */

document.addEventListener("DOMContentLoaded", function () {
  // ── Mobile sidebar toggle ──
  var toggle = document.querySelector(".menu-toggle");
  var sidebar = document.querySelector(".sidebar");
  if (toggle && sidebar) {
    toggle.addEventListener("click", function () {
      sidebar.classList.toggle("open");
    });
    // Close sidebar when clicking outside on mobile
    document.addEventListener("click", function (e) {
      if (
        sidebar.classList.contains("open") &&
        !sidebar.contains(e.target) &&
        !toggle.contains(e.target)
      ) {
        sidebar.classList.remove("open");
      }
    });
  }

  // ── Copy buttons on code blocks ──
  document.querySelectorAll("pre").forEach(function (block) {
    var wrapper = document.createElement("div");
    wrapper.className = "code-wrapper";
    block.parentNode.insertBefore(wrapper, block);
    wrapper.appendChild(block);

    var btn = document.createElement("button");
    btn.className = "copy-btn";
    btn.textContent = "Copy";
    wrapper.appendChild(btn);

    btn.addEventListener("click", function () {
      var code = block.querySelector("code")
        ? block.querySelector("code").textContent
        : block.textContent;
      navigator.clipboard.writeText(code).then(function () {
        btn.textContent = "Copied";
        setTimeout(function () {
          btn.textContent = "Copy";
        }, 1500);
      });
    });
  });

  // ── Collapsible endpoint blocks ──
  document.querySelectorAll(".endpoint-header").forEach(function (header) {
    header.addEventListener("click", function () {
      var body = header.nextElementSibling;
      if (body && body.classList.contains("endpoint-body")) {
        var isHidden = body.style.display === "none";
        body.style.display = isHidden ? "block" : "none";
        header.setAttribute("aria-expanded", isHidden ? "true" : "false");
      }
    });
  });

  // ── Highlight active sidebar link ──
  var currentPage = window.location.pathname.split("/").pop() || "index.html";
  document.querySelectorAll(".sidebar a").forEach(function (link) {
    var href = link.getAttribute("href");
    if (href === currentPage) {
      link.classList.add("active");
    }
  });
});
